package integration;

import api.json.Json;
import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.ServiceHttp;
import support.TransactionTestSchema;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An Inventory outage and restart in the per-schema topology: Transaction keeps running, a checkout during
 * the outage answers 500 and settles after Inventory returns, and the restarted Inventory reads the
 * committed sale instead of any old stock.
 *
 * <p>Migrates {@code inventoryRestartReadsCommittedSaleWhileTransactionRunsDuringOutage}. Its assertion
 * that Complete returns 200 while Inventory is down is SUPERSEDED by FR-030, FR-037, and the spec's
 * Availability and Failure Behavior table: Complete returns 500 INTERNAL_ERROR within the 8 s budget, the
 * attempt stays pending, and it settles as COMPLETED after Inventory restarts, with the stock value the
 * legacy test expected. Every other assertion is kept with its values. The legacy fixture's
 * {@code other-sku} and {@code race-sku}, two units each with threshold 1, are product 1 and product 2
 * here, so the catalog keeps the same SKU order.</p>
 */
class InventoryServiceIsolationTest {
    private static final String OTHER_SKU = InventoryTestSchema.sku(1);
    private static final String SKU = InventoryTestSchema.sku(2);

    @Test
    void inventoryRestartReadsCommittedSaleWhileTransactionRunsDuringOutage() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(2, 2)) {
            deployment.inventorySchema.update("UPDATE inventory SET low_stock_threshold = 1");
            ServiceHttp transactions = deployment.transactionHttp;
            ServiceHttp queries = deployment.inventoryHttp;
            assertEquals(List.of(OTHER_SKU, SKU), Json.getList(ServiceHttp.json(queries.get("/items")), "items")
                    .stream().map(row -> Json.asObject(row).get("sku")).toList());
            assertTrue(Json.getList(ServiceHttp.json(queries.get("/inventory/low-stock")), "alerts").isEmpty());

            deployment.killInventory();
            String id = transactions.start("inventory-outage");
            assertEquals(200, transactions.scan(id, SKU).statusCode(), "Scan needs no Inventory call");
            long started = System.nanoTime();
            HttpResponse<String> during = transactions.complete(id);
            long elapsed = System.nanoTime() - started;
            // SUPERSEDED: the legacy test expected 200 here (FR-030, FR-037).
            assertEquals(500, during.statusCode(), during.body());
            assertEquals("INTERNAL_ERROR", ServiceHttp.json(during).get("error"));
            assertTrue(elapsed < TimeUnit.SECONDS.toNanos(10), "within the 8 s budget, inside the client's 10 s timeout");
            assertEquals("OPEN", ServiceHttp.json(transactions.lookup(id)).get("status"));
            assertEquals(2, deployment.inventorySchema.stock(SKU), "stock is unchanged while the checkout is pending");
            assertTrue(deployment.transactionProcess().isAlive());

            deployment.startInventory();
            awaitCompleted(deployment, id);
            assertEquals("COMPLETED", ServiceHttp.json(transactions.lookup(id)).get("status"),
                    "settled without manual steps");
            assertEquals(1, deployment.inventorySchema.stock(SKU), "the value the legacy test expected");

            Map<String, Object> low = ServiceHttp.json(queries.get("/inventory/low-stock"));
            assertEquals(50, ((Number) low.get("threshold")).intValue());
            List<Object> alerts = Json.getList(low, "alerts");
            assertEquals(1, alerts.size());
            Map<String, Object> alert = Json.asObject(alerts.getFirst());
            assertEquals(SKU, alert.get("sku"));
            assertEquals(1, ((Number) alert.get("currentStock")).intValue());
            assertEquals(1, ((Number) alert.get("threshold")).intValue());
            assertTrue(Json.getList(ServiceHttp.json(queries.get("/inventory/low-stock?threshold=0")), "alerts").isEmpty());
            HttpResponse<String> invalid = queries.get("/inventory/low-stock?threshold=-1");
            assertEquals(400, invalid.statusCode());
            assertEquals("INVALID_THRESHOLD", ServiceHttp.json(invalid).get("error"));
            assertEquals(405, queries.post("/inventory/low-stock", "").statusCode());
            assertEquals(404, queries.get("/inventory/low-stock/extra").statusCode());
        }
    }

    private static void awaitCompleted(CheckoutDeployment deployment, String id) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            List<TransactionTestSchema.AttemptRow> attempts = deployment.transactionSchema.attempts(id);
            if (!attempts.isEmpty() && attempts.get(0).state().equals("COMPLETED")) return;
            if (System.nanoTime() > deadline) fail("the pending checkout never settled " + attempts + deployment.logs());
            Thread.sleep(100);
        }
    }
}
