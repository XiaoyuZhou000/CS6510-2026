package performance;

import org.junit.jupiter.api.Test;
import support.GrpcTestSupport;
import support.InventoryTestSchema;
import support.ServiceHttp;
import support.ServiceProcesses;
import support.TestSchema;
import support.TransactionTestSchema;

import java.math.BigDecimal;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Every Transaction cache rebuilds after a restart with Inventory down: the product snapshot from
 * product_replica, and the transaction state lazily from transaction plus accepted_scan (R4, R19, R22).
 */
class CacheRebuildAfterRestartTest {

    @Test
    void theProductSnapshotAndTheTransactionStateRebuildFromTheDatabase() throws Exception {
        try (InventoryTestSchema inventorySchema = InventoryTestSchema.create(TestSchema.Options.defaults().withOwnUser());
             TransactionTestSchema transactionSchema = TransactionTestSchema.create(TestSchema.Options.defaults().withOwnUser())) {
            inventorySchema.seed(20, 10_000);
            int grpcPort = GrpcTestSupport.freePort();
            int httpPort = GrpcTestSupport.freePort();
            ServiceHttp http = new ServiceHttp(httpPort);

            String id;
            Map<String, Object> before;
            try (ServiceProcesses.ServiceProcess inventory = ServiceProcesses.service("inventory").database(inventorySchema)
                    .httpPort(GrpcTestSupport.freePort()).grpcPort(grpcPort).start()) {
                inventory.awaitPort(grpcPort, Duration.ofSeconds(30));
                ServiceProcesses.ServiceProcess transaction = startTransaction(transactionSchema, httpPort, grpcPort);
                try {
                    awaitReplica(transactionSchema, transaction);
                    id = http.start("rebuild-station");
                    assertScan(http.scan(id, "SKU-000001"), 1, "0.85", "Item 1");
                    assertScan(http.scan(id, "SKU-000001"), 2, "0.85", "Item 1");
                    assertScan(http.scan(id, "SKU-000002"), 3, "1.20", "Item 2");
                    before = ServiceHttp.json(http.get("/transactions/" + id));
                } finally {
                    transaction.kill();
                }
            }

            // Inventory is now down. A later refresh had changed two prices in the durable replica.
            transactionSchema.update("UPDATE product_replica SET price = 9.99 WHERE sku IN ('SKU-000001', 'SKU-000004')");
            transactionSchema.update("UPDATE product_replica SET catalog_version = ?", UUID.randomUUID().toString());
            transactionSchema.update("UPDATE product_replica_meta SET catalog_version = "
                    + "(SELECT catalog_version FROM product_replica LIMIT 1) WHERE meta_key = 1");

            try (ServiceProcesses.ServiceProcess restarted = startTransaction(transactionSchema, httpPort, grpcPort)) {
                restarted.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(30));
                Map<String, Object> after = ServiceHttp.json(http.get("/transactions/" + id));
                assertEquals(before, after, "Lookup sees the same durable basket");
                assertEquals(3, ((Number) after.get("itemCount")).intValue());
                assertEquals(2.90, ((Number) after.get("runningTotal")).doubleValue(), 0.001);

                assertScan(http.scan(id, "SKU-000001"), 4, "0.85", "Item 1");
                assertScan(http.scan(id, "SKU-000004"), 5, "9.99", "Item 4");
                assertScan(http.scan(id, "SKU-000003"), 6, "1.55", "Item 3");
                assertEquals(0.85 + 1.20 + 0.85 + 0.85 + 9.99 + 1.55,
                        ((Number) ServiceHttp.json(http.get("/transactions/" + id)).get("runningTotal")).doubleValue(), 0.001,
                        "the rebuilt lines kept the captured price; new SKUs used the durable replica");
                assertEquals(6, transactionSchema.scans(id).size());
                assertEquals(10_000, inventorySchema.stock("SKU-000001"), "scanning never touched stock");
            }
        }
    }

    static ServiceProcesses.ServiceProcess startTransaction(TransactionTestSchema schema, int httpPort, int inventoryGrpcPort)
            throws Exception {
        return ServiceProcesses.service("transaction").database(schema).httpPort(httpPort)
                .grpcTarget("inventory", inventoryGrpcPort).start();
    }

    /** Waits until the Transaction process has made Inventory's catalog durable and can price a Scan. */
    static void awaitReplica(TransactionTestSchema schema, ServiceProcesses.ServiceProcess transaction) throws Exception {
        transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(30));
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (schema.queryString("SELECT catalog_version FROM product_replica_meta WHERE meta_key = 1") == null) {
            if (System.nanoTime() > deadline) throw new AssertionError("The replica never loaded" + System.lineSeparator()
                    + transaction.output());
            Thread.sleep(100);
        }
        Thread.sleep(200);
    }

    static void assertScan(HttpResponse<String> response, int itemCount, String price, String name) {
        assertEquals(200, response.statusCode(), response.body());
        Map<String, Object> body = ServiceHttp.json(response);
        assertEquals(itemCount, ((Number) body.get("itemCount")).intValue());
        assertEquals(new BigDecimal(price).doubleValue(), ((Number) body.get("unitPrice")).doubleValue(), 0.0001);
        assertEquals(name, body.get("name"));
    }
}
