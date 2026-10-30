package messaging;

import database.TransactionStatusQueries;
import events.DecodedEvent;
import events.EnvelopeCodec;
import events.RecordStream;
import events.TopicNames;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.CatalogItemPublished;
import selfcheckout.events.v1.CatalogSnapshotPublished;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.ServiceProcesses;
import support.TestSchema;
import support.TransactionTestSchema;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The catalog event contract over the live broker (FR-014, FR-018; replaces rpc/InventoryCatalogContractTest):
 * a real Inventory process publishes its whole catalog through its outbox at startup onto the compacted,
 * single-partition catalog topic, and a real Transaction process replays it from offset 0 into its replica,
 * under a per-run topic prefix.
 */
class CatalogEventContractTest {
    private static final int PRODUCTS = 2_000;

    @Test
    void anInventoryStartupPublishesTheWholeCatalogAndTransactionReplaysItWithinFiveSeconds() throws Exception {
        KafkaTestSupport.requireBroker();
        TopicNames names = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(names);
        try (InventoryTestSchema stock = InventoryTestSchema.create(TestSchema.Options.defaults().withOwnUser());
             TransactionTestSchema transactions = TransactionTestSchema.create(TestSchema.Options.defaults().withOwnUser())) {
            String version = stock.seed(PRODUCTS, 100);
            try (ServiceProcesses.ServiceProcess inventory = startInventory(stock, names)) {
                awaitCatalogPublished(stock, inventory);
                assertEquals(PRODUCTS, stock.queryLong("SELECT COUNT(*) FROM outbox_event WHERE event_type = 'CatalogItemPublished'"));
                assertEquals(1, stock.queryLong("SELECT COUNT(*) FROM outbox_event WHERE event_type = 'CatalogSnapshotPublished'"));
                assertEquals(1, stock.queryLong("SELECT COUNT(DISTINCT created_at) FROM outbox_event"),
                        "the 2,001 rows were recorded by one publication");
                assertEquals("__snapshot__", stock.queryString(
                        "SELECT routing_key FROM outbox_event ORDER BY outbox_seq DESC LIMIT 1"), "the marker is recorded last");
                assertEquals(0, stock.queryLong("SELECT COUNT(*) FROM outbox_event WHERE aggregate_key IS NOT NULL"));
                assertNotNull(stock.queryString("SELECT published_at FROM catalog_meta WHERE meta_key = 1"));

                List<DecodedEvent> records = readCatalog(names, PRODUCTS + 1);
                for (int i = 0; i < PRODUCTS; i++) {
                    DecodedEvent item = records.get(i);
                    assertEquals("CatalogItemPublished", item.type().typeName());
                    assertEquals("inventory", item.producer());
                    assertEquals(1, item.schemaVersion());
                    assertEquals(version, item.correlationId(), "the correlation ID is the catalog version");
                    CatalogItemPublished payload = item.payload(CatalogItemPublished.class);
                    assertEquals(InventoryTestSchema.sku(i + 1), payload.getSku(), "items in SKU order");
                    assertEquals(payload.getSku(), item.routingKey(), "the record key is the SKU");
                    assertEquals("Item " + (i + 1), payload.getName());
                    assertEquals(InventoryTestSchema.price(i + 1).toPlainString(), payload.getPrice(),
                            "the price is a decimal string with two fraction digits");
                    assertEquals(version, payload.getCatalogVersion());
                }
                DecodedEvent marker = records.get(PRODUCTS);
                assertEquals("CatalogSnapshotPublished", marker.type().typeName());
                assertEquals("__snapshot__", marker.routingKey());
                assertEquals(PRODUCTS, marker.offset(), "the marker follows every item on the single partition");
                CatalogSnapshotPublished snapshot = marker.payload(CatalogSnapshotPublished.class);
                assertEquals(version, snapshot.getCatalogVersion());
                assertEquals(PRODUCTS, snapshot.getProductCount());

                try (ServiceProcesses.ServiceProcess transaction = startTransaction(transactions, names)) {
                    long ready = System.nanoTime();
                    TransactionStatusQueries status = new TransactionStatusQueries(transactions.pool());
                    while (!version.equals(status.read().replicaVersion())) {
                        if (System.nanoTime() - ready > TimeUnit.SECONDS.toNanos(5)) {
                            fail("replica_version did not reach " + version + " within 5 s" + System.lineSeparator()
                                    + transaction.output());
                        }
                        Thread.sleep(50);
                    }
                    assertEquals(PRODUCTS, status.read().replicaProducts());
                    assertEquals(404, ServiceProcesses.httpStatus(transaction.httpPort(), "/transactions/missing"),
                            "Transaction kept serving while it replayed");
                }
            }
        }
    }

    @Test
    void aBrokerRestartWhileTransactionReplaysStillEndsAtTheCatalogVersionWithNoPartialSwap() throws Exception {
        KafkaTestSupport.requireBrokerControl();
        TopicNames names = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(names);
        try (InventoryTestSchema stock = InventoryTestSchema.create(TestSchema.Options.defaults().withOwnUser());
             TransactionTestSchema transactions = TransactionTestSchema.create(TestSchema.Options.defaults().withOwnUser())) {
            String version = stock.seed(PRODUCTS, 100);
            try (ServiceProcesses.ServiceProcess inventory = startInventory(stock, names)) {
                awaitCatalogPublished(stock, inventory);
            }
            KafkaTestSupport.stopBroker();
            try (ServiceProcesses.ServiceProcess transaction = startTransaction(transactions, names)) {
                TransactionStatusQueries status = new TransactionStatusQueries(transactions.pool());
                assertNull(status.read().replicaVersion(), "nothing can be read while the broker is down");
                KafkaTestSupport.startBroker();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
                while (true) {
                    TransactionStatusQueries.Status now = status.read();
                    if (now.replicaVersion() != null) {
                        assertEquals(version, now.replicaVersion());
                        assertEquals(PRODUCTS, transactions.queryLong("SELECT COUNT(*) FROM product_replica"),
                                "a version is installed only with all of its products");
                        break;
                    }
                    if (System.nanoTime() > deadline) {
                        fail("The replica never converged after the broker restart" + System.lineSeparator()
                                + transaction.output());
                    }
                    Thread.sleep(100);
                }
            } finally {
                KafkaTestSupport.requireBroker();
            }
        }
    }

    private static ServiceProcesses.ServiceProcess startInventory(InventoryTestSchema schema, TopicNames names)
            throws Exception {
        ServiceProcesses.ServiceProcess inventory = ServiceProcesses.service("inventory").database(schema)
                .httpPort(ServiceProcesses.freePort()).kafka(names).start();
        inventory.awaitHttp("/items", 200, Duration.ofSeconds(60));
        return inventory;
    }

    private static ServiceProcesses.ServiceProcess startTransaction(TransactionTestSchema schema, TopicNames names)
            throws Exception {
        ServiceProcesses.ServiceProcess transaction = ServiceProcesses.service("transaction").database(schema)
                .httpPort(ServiceProcesses.freePort()).kafka(names).start();
        transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(60));
        return transaction;
    }

    /** Waits until Inventory's publication committed and the broker confirmed every catalog row. */
    private static void awaitCatalogPublished(InventoryTestSchema stock, ServiceProcesses.ServiceProcess inventory)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (stock.queryLong("SELECT COUNT(*) FROM outbox_event WHERE published_at IS NOT NULL") < PRODUCTS + 1) {
            if (System.nanoTime() > deadline) fail("The catalog was not published" + System.lineSeparator() + inventory.output());
            Thread.sleep(50);
        }
    }

    /** Reads the catalog topic's single partition from offset 0 outside any group. */
    private static List<DecodedEvent> readCatalog(TopicNames names, int count) {
        List<DecodedEvent> events = new ArrayList<>();
        try (RecordStream stream = KafkaTestSupport.streams("test-catalog-reader", null).get()) {
            RecordStream.Partition partition = new RecordStream.Partition(names.catalog(), 0);
            stream.assign(List.of(partition));
            stream.seek(partition, 0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (events.size() < count && System.nanoTime() < deadline) {
                for (RecordStream.Record record : stream.poll(Duration.ofMillis(200))) {
                    events.add(EnvelopeCodec.decode(record.value(), record.key())
                            .at(record.topic(), record.partition(), record.offset()));
                }
            }
        }
        assertEquals(count, events.size(), "records on " + names.catalog());
        return events;
    }
}
