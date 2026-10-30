package integration;

import events.TopicNames;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.ServiceHttp;
import support.ServiceProcesses;
import support.TestSchema;
import support.TransactionTestSchema;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * User Story 2 scenarios 1, 2, 5, and 6 with real service processes: once the replica has loaded, Scan
 * needs neither Inventory nor Analytics, including across a Transaction restart (FR-006, FR-012, FR-015,
 * FR-019, FR-021, FR-022; SC-006, SC-007). The replica arrives through Inventory's catalog events on the live
 * broker (006 D9) and survives in Transaction's database. Inventory is stopped first and Transaction restarted
 * while it is down; once Inventory returns, every accepted scan has reached Analytics once. Then Analytics is
 * stopped: scanning goes on, the relay keeps publishing to the scan topic, and once Analytics returns it counts
 * every scan exactly once.
 */
class ScanWithoutPeersTest {

    @Test
    void startScanAndLookupWorkWithoutPeersAndAcrossATransactionRestart() throws Exception {
        try (InventoryTestSchema inventorySchema = InventoryTestSchema.create(TestSchema.Options.defaults().withOwnUser());
             TransactionTestSchema transactionSchema = TransactionTestSchema.create(TestSchema.Options.defaults().withOwnUser());
             AnalyticsTestSchema analyticsSchema = AnalyticsTestSchema.create(TestSchema.Options.defaults().withOwnUser())) {
            inventorySchema.seed(30, 10_000);
            long units = inventorySchema.totalUnits();
            KafkaTestSupport.requireBroker();
            TopicNames names = KafkaTestSupport.runTopics();
            KafkaTestSupport.createCatalogTopics(names);
            int httpPort = ServiceProcesses.freePort();
            int inventoryPort = ServiceProcesses.freePort();
            int analyticsPort = ServiceProcesses.freePort();
            ServiceHttp http = new ServiceHttp(httpPort);

            ServiceProcesses.ServiceProcess inventory = startInventory(inventorySchema, inventoryPort, names);
            ServiceProcesses.ServiceProcess analytics = null;
            ServiceProcesses.ServiceProcess transaction = null;
            try {
                transaction = startTransaction(transactionSchema, httpPort, names);
                awaitReplica(transactionSchema, transaction);
                analytics = startAnalytics(analyticsSchema, analyticsPort, names);
                inventory.close();
                assertFalse(inventory.isAlive(), "Inventory is down from here on");

                String id = http.start("no-peers-1");
                for (int n = 1; n <= 5; n++) assertOk(http.scan(id, InventoryTestSchema.sku(n)), n);
                assertOk(http.scan(id, InventoryTestSchema.sku(1)), 6);
                Map<String, Object> lookup = ServiceHttp.json(http.get("/transactions/" + id));
                assertEquals("OPEN", lookup.get("status"));
                assertEquals(6, ((Number) lookup.get("itemCount")).intValue());

                transaction.kill();
                transaction = startTransaction(transactionSchema, httpPort, names);
                transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(30));
                assertEquals(lookup, ServiceHttp.json(http.get("/transactions/" + id)), "the basket survived the restart");
                assertOk(http.scan(id, InventoryTestSchema.sku(2)), 7);
                String after = http.start("no-peers-2");
                assertOk(http.scan(after, InventoryTestSchema.sku(30)), 1);

                assertEquals(units, inventorySchema.totalUnits(), "scanning leaves stock unchanged (FR-021)");
                assertEquals(10_000, inventorySchema.stock(InventoryTestSchema.sku(1)));

                long rows = transactionSchema.scanCount();
                HttpResponse<String> unknown = http.scan(id, "NO-SUCH-SKU");
                assertEquals(404, unknown.statusCode(), unknown.body());
                assertEquals("SKU_NOT_FOUND", ServiceHttp.json(unknown).get("error"));
                transactionSchema.update("UPDATE `transaction` SET status = 'COMPLETED', completed_at = NOW(3) "
                        + "WHERE transaction_id = ?", after);
                HttpResponse<String> closed = http.scan(after, InventoryTestSchema.sku(1));
                assertEquals(409, closed.statusCode(), closed.body());
                assertEquals("TRANSACTION_NOT_OPEN", ServiceHttp.json(closed).get("error"));
                assertEquals(rows, transactionSchema.scanCount(), "rejected scans write no accepted_scan row");
                assertEquals(8, transactionSchema.scanCount());

                inventory = startInventory(inventorySchema, inventoryPort, names);
                awaitEveryScanCountedOnce(transactionSchema, analyticsSchema, transaction, analytics);
                assertEquals(units, inventorySchema.totalUnits(), "Inventory's return changes no stock");

                analytics.stop();
                assertFalse(analytics.isAlive(), "Analytics is down from here on");
                String third = http.start("no-peers-3");
                for (int n = 1; n <= 4; n++) assertOk(http.scan(third, InventoryTestSchema.sku(n + 10)), n);
                Map<String, Object> thirdLookup = ServiceHttp.json(http.get("/transactions/" + third));
                assertEquals("OPEN", thirdLookup.get("status"));
                assertEquals(4, ((Number) thirdLookup.get("itemCount")).intValue());
                assertEquals(12, transactionSchema.scanCount());
                assertEquals(8, analyticsSchema.storedEvents(), "nothing reaches Analytics while it is down");

                analytics = startAnalytics(analyticsSchema, analyticsPort, names);
                awaitEveryScanCountedOnce(transactionSchema, analyticsSchema, transaction, analytics);
                assertEquals(units, inventorySchema.totalUnits());
            } finally {
                if (transaction != null) transaction.close();
                if (analytics != null) analytics.close();
                inventory.close();
            }
        }
    }

    private static ServiceProcesses.ServiceProcess startTransaction(TransactionTestSchema schema, int httpPort,
                                                                    TopicNames names) throws Exception {
        return ServiceProcesses.service("transaction").database(schema).httpPort(httpPort).kafka(names).start();
    }

    private static ServiceProcesses.ServiceProcess startInventory(InventoryTestSchema schema, int httpPort,
                                                                  TopicNames names) throws Exception {
        ServiceProcesses.ServiceProcess inventory = ServiceProcesses.service("inventory").database(schema)
                .httpPort(httpPort).kafka(names).start();
        inventory.awaitHttp("/items", 200, Duration.ofSeconds(60));
        return inventory;
    }

    /** Starts (or restarts) Analytics and waits until its scan consumer holds the scan topic's partition. */
    private static ServiceProcesses.ServiceProcess startAnalytics(AnalyticsTestSchema schema, int httpPort,
                                                                  TopicNames names) throws Exception {
        String group = names.group(TopicNames.ANALYTICS_SCAN_INGEST_GROUP);
        Set<String> previous = KafkaTestSupport.memberIds(group);
        ServiceProcesses.ServiceProcess analytics = ServiceProcesses.service("analytics").database(schema)
                .httpPort(httpPort).kafka(names).start();
        analytics.awaitHttp("/analytics/popular-items", 200, Duration.ofSeconds(60));
        KafkaTestSupport.awaitGroupAssigned(group, names.scanAccepted(), TopicNames.Topic.SCAN_ACCEPTED.partitions(),
                1, previous, Duration.ofSeconds(60));
        return analytics;
    }

    private static void awaitReplica(TransactionTestSchema schema, ServiceProcesses.ServiceProcess transaction)
            throws Exception {
        transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(30));
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (schema.queryString("SELECT catalog_version FROM product_replica_meta WHERE meta_key = 1") == null) {
            if (System.nanoTime() > deadline) throw new AssertionError("The replica never loaded" + System.lineSeparator()
                    + transaction.output());
            Thread.sleep(100);
        }
        Thread.sleep(200);
    }

    /** Waits until every accepted scan is published and stored by Analytics, then asserts each is counted once. */
    private static void awaitEveryScanCountedOnce(TransactionTestSchema transactions, AnalyticsTestSchema analytics,
                                                  ServiceProcesses.ServiceProcess transaction,
                                                  ServiceProcesses.ServiceProcess analyticsProcess) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (transactions.undeliveredCount() > 0 || analytics.storedEvents() < transactions.scanCount()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Analytics did not catch up: unpublished=" + transactions.undeliveredCount()
                        + " accepted=" + transactions.scanCount() + " stored=" + analytics.storedEvents()
                        + System.lineSeparator() + transaction.output() + System.lineSeparator() + analyticsProcess.output());
            }
            Thread.sleep(100);
        }
        assertEquals(transactions.identities(), analytics.identities(), "every accepted scan is counted exactly once");
        analytics.assertPositionsContiguous();
    }

    private static void assertOk(HttpResponse<String> response, int itemCount) {
        assertEquals(200, response.statusCode(), response.body());
        assertEquals(itemCount, ((Number) ServiceHttp.json(response).get("itemCount")).intValue());
    }
}
