package integration;

import database.ConnectionPool;
import database.RelayStore;
import events.TopicNames;
import messaging.KafkaClients;
import messaging.KafkaRecordSender;
import messaging.KafkaScanEventSink;
import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.InProcessCheckout;
import support.InProcessScanDelivery;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.ServiceHttp;
import support.ServiceProcesses;
import support.SwitchableScanEventSink;
import support.TestSchema;
import support.TransactionTestSchema;
import transaction.ScanEventSink;
import transaction.TransactionOperations;

import java.math.BigDecimal;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * User Story 2 scenarios 1-3 and SC-004: Start, Scan, and Lookup never wait for the broker (FR-015). With the
 * broker unreachable they succeed with the captured names and prices, including after a Transaction restart,
 * and none of them takes as long as the producers' {@code max.block.ms}; accepted scans wait unpublished in
 * {@code accepted_scan}, and once the broker is back every one is published and counted exactly once, with
 * stock unchanged.
 *
 * <ul>
 *   <li>In this JVM the checkout bus is paused and the scan relay publishes through a real Kafka producer whose
 *       bootstrap address refuses connections, so each send blocks for metadata as with a stopped broker. A
 *       failed round stays near one {@code max.block.ms} however many rows it carries, and the relay holds no
 *       pool connection while it sends (T138).</li>
 *   <li>A Transaction process starts and serves with a broker address that does not even resolve (T137).</li>
 *   <li>With {@code KAFKA_TEST_BROKER_CONTROL=1}, the live broker is stopped under the three service processes,
 *       Transaction is restarted while it is down, and the broker is started again
 *       ({@code deploy/kafka/stop-broker.ps1}, {@code start-broker.ps1 -NoTopics}).</li>
 * </ul>
 */
class ScanWithoutBrokerTest {
    /** The producers' {@code max.block.ms} ({@code KafkaClients}): a request that sent a record would wait this long. */
    private static final Duration MAX_BLOCK = Duration.ofMillis(1_000);

    @Test
    void withTheBrokerUnreachableStartScanAndLookupNeverWaitAndEveryScanIsCountedOnceAfterItReturns() throws Exception {
        Map<String, String> unreachable = Map.of(KafkaClients.BOOTSTRAP_VARIABLE, "127.0.0.1:" + ServiceProcesses.freePort());
        try (InProcessCheckout saga = InProcessCheckout.start(5, 10);
             InProcessScanDelivery delivery = InProcessScanDelivery.over(saga.bus, saga.names);
             KafkaRecordSender stopped = KafkaClients.sender(unreachable, "test-stopped-scan",
                     KafkaClients.DEFAULT_BATCH_LINGER_MS)) {
            long units = saga.inventorySchema.totalUnits();
            saga.bus.pauseBroker(true);
            SwitchableScanEventSink broker = new SwitchableScanEventSink();
            broker.up(new KafkaScanEventSink(stopped, saga.names));
            // The relay's own one-connection pool: it is busy only while the relay holds its connection.
            ConnectionPool relayPool = saga.transactionSchema.newPool("test.relay", 1, 1, null);
            RecordingSink rounds = new RecordingSink(broker, relayPool);
            delivery.startRelay(relayPool, rounds);

            TransactionOperations service = saga.checkout();
            String id = timed("Start", () -> service.start(new TransactionOperations.StartCommand("no-broker-1")))
                    .transactionId();
            for (int n = 1; n <= 5; n++) {
                int product = n;
                var scan = timed("Scan", () -> service.scan(new TransactionOperations.ScanCommand(id,
                        InventoryTestSchema.sku(product))));
                assertEquals("Item " + n, scan.name());
                assertEquals(InventoryTestSchema.price(n), scan.unitPrice());
                assertEquals(n, scan.itemCount());
            }
            assertEquals(6, timed("Scan", () -> service.scan(new TransactionOperations.ScanCommand(id,
                    InventoryTestSchema.sku(1)))).itemCount());
            BigDecimal total = InventoryTestSchema.price(1).add(InventoryTestSchema.price(1)).add(InventoryTestSchema.price(2))
                    .add(InventoryTestSchema.price(3)).add(InventoryTestSchema.price(4)).add(InventoryTestSchema.price(5));
            var lookup = timed("Lookup", () -> service.get(id));
            assertEquals(TransactionOperations.TransactionStatus.OPEN, lookup.status());
            assertEquals(6, lookup.itemCount());
            assertEquals(total, lookup.runningTotal());

            // A restarted Transaction: a new service instance over the same schema, and a new relay.
            saga.transaction.close();
            try (InProcessTransaction restarted = InProcessTransaction.over(saga.transactionSchema).pool(2, 8)
                    .bus(saga.bus, saga.names).build()) {
                delivery.startRelay(relayPool, rounds);
                TransactionOperations again = restarted.service;
                var survived = timed("Lookup", () -> again.get(id));
                assertEquals(lookup, survived, "the basket survived the restart");
                assertEquals(7, timed("Scan", () -> again.scan(new TransactionOperations.ScanCommand(id,
                        InventoryTestSchema.sku(2)))).itemCount());
                String after = timed("Start", () -> again.start(new TransactionOperations.StartCommand("no-broker-2")))
                        .transactionId();
                assertEquals(1, timed("Scan", () -> again.scan(new TransactionOperations.ScanCommand(after,
                        InventoryTestSchema.sku(5)))).itemCount());
                assertEquals(8, saga.transactionSchema.scanCount());

                List<Round> outage = rounds.awaitFailedRounds(8, 2, Duration.ofSeconds(60));
                for (Round round : outage) {
                    assertTrue(round.elapsed().compareTo(MAX_BLOCK.multipliedBy(3)) < 0, "a failed round of "
                            + round.rows() + " rows stops at its first failed send: " + round.elapsed().toMillis() + " ms");
                    assertEquals(0, round.activeWhileSending(), "the relay holds no pool connection while it sends");
                }
                assertEquals(8, saga.transactionSchema.undeliveredCount(), "accepted scans wait unpublished");
                assertEquals(0, delivery.analyticsSchema.storedEvents());

                broker.up(delivery.analytics.sink());
                saga.bus.pauseBroker(false);
                delivery.awaitEveryScanCountedOnce(saga.transactionSchema, Duration.ofSeconds(60));
                assertEquals(units, saga.inventorySchema.totalUnits(), "no Scan changed stock");
                delivery.stopRelay();
            }
        }
    }

    @Test
    void aTransactionProcessStartsAndServesWhileItsBrokerAddressDoesNotResolve() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create(TestSchema.Options.defaults().withOwnUser())) {
            schema.seedReplica(5, UUID.randomUUID().toString());
            int port = ServiceProcesses.freePort();
            ServiceHttp http = new ServiceHttp(port);
            ServiceProcesses.ServiceProcess transaction = startUnresolvable(schema, port);
            try {
                transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(30));
                String id = timed("Start", () -> http.start("unresolvable-1"));
                for (int n = 1; n <= 3; n++) {
                    int product = n;
                    HttpResponse<String> scan = timed("Scan", () -> http.scan(id, InventoryTestSchema.sku(product)));
                    assertEquals(200, scan.statusCode(), scan.body());
                    assertEquals("Item " + n, ServiceHttp.json(scan).get("name"));
                }
                Map<String, Object> lookup = ServiceHttp.json(timed("Lookup", () -> http.lookup(id)));
                assertEquals("OPEN", lookup.get("status"));
                assertEquals(3, ((Number) lookup.get("itemCount")).intValue());

                transaction.kill();
                transaction = startUnresolvable(schema, port);
                transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(30));
                assertEquals(lookup, ServiceHttp.json(timed("Lookup", () -> http.lookup(id))), "the basket survived");
                HttpResponse<String> next = timed("Scan", () -> http.scan(id, InventoryTestSchema.sku(4)));
                assertEquals(200, next.statusCode(), next.body());
                assertEquals(4, ((Number) ServiceHttp.json(next).get("itemCount")).intValue());
                assertEquals(4, schema.undeliveredCount(), "nothing can be published, and nothing is lost");
                assertTrue(transaction.isAlive(), transaction.output());
            } finally {
                transaction.close();
            }
        }
    }

    @Test
    void withTheLiveBrokerStoppedTheServicesKeepServingAndEveryScanIsCountedOnceAfterItReturns() throws Exception {
        KafkaTestSupport.requireBrokerControl();
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(5, 10, null)) {
            deployment.startAnalytics();
            ServiceHttp http = deployment.transactionHttp;
            long units = deployment.inventorySchema.totalUnits();
            String before = http.start("broker-up");
            assertEquals(200, http.scan(before, InventoryTestSchema.sku(1)).statusCode());
            deployment.awaitScansPublished(Duration.ofSeconds(30));

            KafkaTestSupport.stopBroker();
            Map<String, Object> lookup;
            String id;
            try {
                id = timed("Start", () -> http.start("broker-down"));
                for (int n = 1; n <= 5; n++) {
                    int product = n;
                    HttpResponse<String> scan = timed("Scan", () -> http.scan(id, InventoryTestSchema.sku(product)));
                    assertEquals(200, scan.statusCode(), scan.body());
                    Map<String, Object> body = ServiceHttp.json(scan);
                    assertEquals("Item " + n, body.get("name"));
                    assertEquals(InventoryTestSchema.price(n).doubleValue(), ((Number) body.get("unitPrice")).doubleValue(), 0.001);
                }
                lookup = ServiceHttp.json(timed("Lookup", () -> http.lookup(id)));
                assertEquals("OPEN", lookup.get("status"));
                assertEquals(5, ((Number) lookup.get("itemCount")).intValue());
                assertEquals(200, deployment.inventoryHttp.get("/items").statusCode(), "the catalog reads the database");
                assertEquals(200, deployment.analyticsHttp.get("/analytics/popular-items").statusCode());

                deployment.restartTransactionWithoutBroker();
                assertEquals(lookup, ServiceHttp.json(timed("Lookup", () -> http.lookup(id))), "the basket survived");
                HttpResponse<String> next = timed("Scan", () -> http.scan(id, InventoryTestSchema.sku(2)));
                assertEquals(200, next.statusCode(), next.body());
                assertEquals(6, ((Number) ServiceHttp.json(next).get("itemCount")).intValue());
                assertEquals(6, deployment.transactionSchema.undeliveredCount(), "the outage's scans wait unpublished");
            } finally {
                KafkaTestSupport.startBroker();
            }
            deployment.awaitAnalyticsHoldsEveryScan(Duration.ofSeconds(180));
            assertEquals(7, deployment.analyticsSchema.storedEvents());
            assertEquals(units, deployment.inventorySchema.totalUnits(), "no Scan changed stock");
        }
    }

    private static ServiceProcesses.ServiceProcess startUnresolvable(TransactionTestSchema schema, int port)
            throws Exception {
        return ServiceProcesses.service("transaction").database(schema).httpPort(port)
                .env(KafkaClients.BOOTSTRAP_VARIABLE, "kafka-broker.invalid:19092")
                .env(TopicNames.PREFIX_VARIABLE, KafkaTestSupport.runTopics().prefix()).start();
    }

    /** Runs {@code call} and asserts that it finished well within the producers' {@code max.block.ms}. */
    private static <T> T timed(String what, Callable<T> call) throws Exception {
        long started = System.nanoTime();
        T result = call.call();
        long elapsed = System.nanoTime() - started;
        assertTrue(elapsed < MAX_BLOCK.toNanos(), what + " took " + elapsed / 1_000_000
                + " ms, as long as a send blocked on broker metadata");
        return result;
    }

    /** One relay round as the sink saw it. */
    private record Round(int rows, Duration elapsed, int activeWhileSending, boolean delivered) { }

    /** Records every round's size, duration, and the relay pool's busy connections while it sends. */
    private static final class RecordingSink implements ScanEventSink {
        private final ScanEventSink delegate;
        private final ConnectionPool relayPool;
        private final List<Round> rounds = new CopyOnWriteArrayList<>();

        RecordingSink(ScanEventSink delegate, ConnectionPool relayPool) {
            this.delegate = delegate;
            this.relayPool = relayPool;
        }

        @Override
        public DeliveryResult deliver(List<RelayStore.UndeliveredScan> events) {
            int active = relayPool.occupancy().active();
            long started = System.nanoTime();
            DeliveryResult result = delegate.deliver(events);
            rounds.add(new Round(events.size(), Duration.ofNanos(System.nanoTime() - started),
                    Math.max(active, relayPool.occupancy().active()), result instanceof Delivered));
            return result;
        }

        /** Waits for {@code count} failed rounds of at least {@code rows} rows, recorded from now on. */
        List<Round> awaitFailedRounds(int rows, int count, Duration timeout) throws InterruptedException {
            int from = rounds.size();
            long deadline = System.nanoTime() + timeout.toNanos();
            while (true) {
                List<Round> failed = rounds.subList(from, rounds.size()).stream()
                        .filter(round -> !round.delivered() && round.rows() >= rows).toList();
                if (failed.size() >= count) return failed;
                if (System.nanoTime() > deadline) fail("Only " + failed.size() + " failed rounds of " + rows + " rows: " + rounds);
                Thread.sleep(20);
            }
        }
    }
}
