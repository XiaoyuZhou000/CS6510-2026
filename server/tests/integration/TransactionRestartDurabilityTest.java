package integration;

import api.json.Json;
import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.ServiceHttp;
import support.TransactionTestSchema;

import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Transaction durability across failures in the per-schema topology: each service runs against its own
 * disposable schema and user, and stock lives in Inventory's schema. Every method runs Inventory, Transaction,
 * and Analytics processes over the live broker, so every Complete goes through the Saga (T142) and every
 * acknowledged scan, and only those, must reach Analytics exactly once (US2 scenario 4, FR-029).
 */
class TransactionRestartDurabilityTest {
    private static final Duration DELIVERY = Duration.ofSeconds(60);

    @Test
    void failedDatabaseWriteReturnsInternalErrorWithoutAcknowledgingScan() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(2, 10, null)) {
            deployment.startAnalytics();
            TransactionTestSchema db = deployment.transactionSchema;
            ServiceHttp http = deployment.transactionHttp;
            String id = http.start("failure-station");

            db.setTableUnavailable("accepted_scan", true);
            assertError(http.scan(id, InventoryTestSchema.sku(1)), 500, "INTERNAL_ERROR");
            var open = Json.parseObject(http.get("/transactions/" + id).body());
            assertEquals("OPEN", open.get("status"));
            assertEquals(0, ((Number) open.get("itemCount")).intValue());
            assertEquals(0, ((Number) open.get("runningTotal")).doubleValue(), 0.001);
            assertEquals(10, deployment.inventorySchema.stock(InventoryTestSchema.sku(1)));
            assertEquals(0, db.transaction(id).orElseThrow().itemCount(), "the guarded UPDATE rolled back too");

            db.setTableUnavailable("accepted_scan", false);
            HttpResponse<String> recovered = http.scan(id, InventoryTestSchema.sku(1));
            assertEquals(200, recovered.statusCode(), recovered.body());
            assertEquals(1, ((Number) Json.parseObject(recovered.body()).get("itemCount")).intValue(),
                    "the unacknowledged Scan left nothing behind");
            assertEquals(1, db.scans(id).size());
            deployment.awaitAnalyticsHoldsEveryScan(DELIVERY);
            assertEquals(1, deployment.analyticsSchema.storedEvents(), "only the acknowledged scan is delivered");
        }
    }

    @Test
    void acknowledgedOpenBasketSurvivesProcessRestartAndCompletesExactlyOnce() throws Exception {
        String sku = InventoryTestSchema.sku(1);
        String other = InventoryTestSchema.sku(2);
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(5, 10, null)) {
            deployment.startAnalytics();
            ServiceHttp http = deployment.transactionHttp;
            HttpResponse<String> started = http.post("/transactions", "{\"stationId\":\"restart-station\"}");
            assertEquals(201, started.statusCode(), started.body());
            var start = Json.parseObject(started.body());
            String id = start.get("transactionId").toString();
            String startedAt = start.get("startedAt").toString();
            assertEquals(0, ((Number) start.get("itemCount")).intValue());
            assertEquals(0, ((Number) start.get("runningTotal")).doubleValue(), 0.001);
            assertEquals(200, http.scan(id, sku).statusCode());
            assertEquals(200, http.scan(id, sku).statusCode());
            assertEquals(200, http.scan(id, other).statusCode());
            assertEquals(10, deployment.inventorySchema.stock(sku));
            assertEquals(10, deployment.inventorySchema.stock(other));
            assertEquals(3, deployment.transactionSchema.scanCount());

            deployment.killTransaction();
            deployment.startTransaction();
            HttpResponse<String> lookup = http.lookup(id);
            assertEquals(200, lookup.statusCode(), lookup.body());
            var open = Json.parseObject(lookup.body());
            double total = InventoryTestSchema.price(1).doubleValue() * 2 + InventoryTestSchema.price(2).doubleValue();
            assertEquals("OPEN", open.get("status"));
            assertEquals(startedAt, open.get("startedAt"));
            assertEquals(3, ((Number) open.get("itemCount")).intValue());
            assertEquals(total, ((Number) open.get("runningTotal")).doubleValue(), 0.001);

            HttpResponse<String> completed = http.complete(id);
            assertEquals(200, completed.statusCode(), completed.body() + deployment.logs());
            var receipt = Json.parseObject(completed.body());
            assertEquals(3, ((Number) receipt.get("itemCount")).intValue());
            assertEquals(total, ((Number) receipt.get("totalAmount")).doubleValue(), 0.001);
            assertEquals(startedAt, receipt.get("startedAt"));
            assertNotNull(receipt.get("completedAt"));
            assertEquals(2, ((java.util.List<?>) receipt.get("lines")).size());
            assertEquals(8, deployment.inventorySchema.stock(sku));
            assertEquals(9, deployment.inventorySchema.stock(other));

            HttpResponse<String> duplicate = http.complete(id);
            assertEquals(409, duplicate.statusCode(), duplicate.body());
            assertEquals("TRANSACTION_NOT_OPEN", Json.parseObject(duplicate.body()).get("error"));
            assertEquals(8, deployment.inventorySchema.stock(sku));
            assertEquals(9, deployment.inventorySchema.stock(other));
            deployment.awaitAnalyticsHoldsEveryScan(DELIVERY);
            assertEquals(3, deployment.analyticsSchema.storedEvents(), "every acknowledged scan survived the crash");
        }
    }

    @Test
    void publicTransactionErrorsKeepTheirStatusesAndShapes() throws Exception {
        String sku = InventoryTestSchema.sku(1);
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(5, 10, null)) {
            deployment.startAnalytics();
            ServiceHttp http = deployment.transactionHttp;
            assertError(http.post("/transactions", "{}"), 400, "MISSING_STATION_ID");
            assertError(http.post("/transactions", "{\"stationId\":\"\"}"), 400, "MISSING_STATION_ID");
            String id = http.start("error-station");
            assertError(http.scan("missing", sku), 404, "TRANSACTION_NOT_FOUND");
            assertError(http.lookup("missing"), 404, "TRANSACTION_NOT_FOUND");
            assertError(http.post("/transactions/" + id + "/items", "{}"), 400, "MISSING_SKU");
            assertError(http.scan(id, "absent"), 404, "SKU_NOT_FOUND");
            assertError(http.complete(id), 409, "EMPTY_BASKET");
            assertEquals(200, http.scan(id, sku).statusCode());
            assertEquals(200, http.complete(id).statusCode());
            assertError(http.scan(id, sku), 409, "TRANSACTION_NOT_OPEN");
            assertError(http.complete(id), 409, "TRANSACTION_NOT_OPEN");
            deployment.awaitAnalyticsHoldsEveryScan(DELIVERY);
            assertEquals(1, deployment.analyticsSchema.storedEvents(), "no rejected scan is delivered");
        }
    }

    private static void assertError(HttpResponse<String> response, int status, String code) {
        assertEquals(status, response.statusCode(), response.body());
        var body = Json.parseObject(response.body());
        assertEquals(java.util.Set.of("error", "message"), body.keySet());
        assertEquals(code, body.get("error"));
        assertNotNull(body.get("message"));
    }
}
