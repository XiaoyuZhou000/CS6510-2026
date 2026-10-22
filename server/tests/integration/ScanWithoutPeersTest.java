package integration;

import org.junit.jupiter.api.Test;
import support.GrpcTestSupport;
import support.InventoryTestSchema;
import support.ServiceHttp;
import support.ServiceProcesses;
import support.TestSchema;
import support.TransactionTestSchema;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * User Story 2 scenarios 1, 2, 5, and 6 with real service processes: once the replica has loaded, Scan
 * needs neither Inventory nor Analytics, including across a Transaction restart (FR-006, FR-012,
 * FR-019, FR-021, FR-022). Analytics is never started: Transaction does not call it until the relay
 * exists (US5), and nothing here waits for delivery.
 */
class ScanWithoutPeersTest {

    @Test
    void startScanAndLookupWorkWithoutPeersAndAcrossATransactionRestart() throws Exception {
        try (InventoryTestSchema inventorySchema = InventoryTestSchema.create(TestSchema.Options.defaults().withOwnUser());
             TransactionTestSchema transactionSchema = TransactionTestSchema.create(TestSchema.Options.defaults().withOwnUser())) {
            inventorySchema.seed(30, 10_000);
            long units = inventorySchema.totalUnits();
            int grpcPort = GrpcTestSupport.freePort();
            int httpPort = GrpcTestSupport.freePort();
            ServiceHttp http = new ServiceHttp(httpPort);

            ServiceProcesses.ServiceProcess inventory = ServiceProcesses.service("inventory").database(inventorySchema)
                    .httpPort(GrpcTestSupport.freePort()).grpcPort(grpcPort).start();
            ServiceProcesses.ServiceProcess transaction;
            try {
                inventory.awaitPort(grpcPort, Duration.ofSeconds(30));
                transaction = startTransaction(transactionSchema, httpPort, grpcPort);
                awaitReplica(transactionSchema, transaction);
            } finally {
                inventory.close();
            }
            assertFalse(inventory.isAlive(), "Inventory is down from here on");

            try {
                String id = http.start("no-peers-1");
                for (int n = 1; n <= 5; n++) assertOk(http.scan(id, InventoryTestSchema.sku(n)), n);
                assertOk(http.scan(id, InventoryTestSchema.sku(1)), 6);
                Map<String, Object> lookup = ServiceHttp.json(http.get("/transactions/" + id));
                assertEquals("OPEN", lookup.get("status"));
                assertEquals(6, ((Number) lookup.get("itemCount")).intValue());

                transaction.kill();
                transaction = startTransaction(transactionSchema, httpPort, grpcPort);
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
                assertEquals(8, transactionSchema.undeliveredCount(), "accepted rows stay undelivered until US5's relay");
            } finally {
                transaction.close();
            }
        }
    }

    private static ServiceProcesses.ServiceProcess startTransaction(TransactionTestSchema schema, int httpPort,
                                                                    int inventoryGrpcPort) throws Exception {
        return ServiceProcesses.service("transaction").database(schema).httpPort(httpPort)
                .grpcTarget("inventory", inventoryGrpcPort).grpcTarget("analytics", GrpcTestSupport.freePort()).start();
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

    private static void assertOk(HttpResponse<String> response, int itemCount) {
        assertEquals(200, response.statusCode(), response.body());
        assertEquals(itemCount, ((Number) ServiceHttp.json(response).get("itemCount")).intValue());
    }
}
