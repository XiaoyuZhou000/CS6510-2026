package integration;

import api.json.Json;
import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.ServiceHttp;

import java.net.http.HttpResponse;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A reachable owner must report database failures without acknowledging unfinished work. Transaction's
 * own {@code transaction} table goes away and comes back, renamed through TransactionTestSchema, while
 * Inventory keeps serving from its own schema.
 */
class DatabaseOutageContractTest {
    private static final String SKU = InventoryTestSchema.sku(1);

    @Test
    void failedScanAndCompletionReturnInternalErrorThenCommittedStateRecovers() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(5, 2)) {
            ServiceHttp http = deployment.transactionHttp;
            String id = http.start("db-outage");
            long beforePosition = deployment.transactionSchema.scanCount();

            deployment.transactionSchema.setTableUnavailable("transaction", true);
            try {
                assertInternal(http.scan(id, SKU));
                assertInternal(http.post("/transactions/" + id + "/complete", "{}"));
                assertEquals(200, deployment.inventoryHttp.get("/items").statusCode(),
                        "Inventory's routes are unaffected by Transaction's outage");
            } finally {
                deployment.transactionSchema.setTableUnavailable("transaction", false);
            }
            assertEquals(beforePosition, deployment.transactionSchema.scanCount(), "failed scan must not commit an event");
            assertEquals(2, deployment.inventorySchema.stock(SKU), "failed completion must not deduct stock");
            Map<String, Object> afterOutage = Json.parseObject(http.lookup(id).body());
            assertEquals("OPEN", afterOutage.get("status"));
            assertEquals(0, ((Number) afterOutage.get("itemCount")).intValue());

            assertEquals(200, http.scan(id, SKU).statusCode());
            assertEquals(beforePosition + 1, deployment.transactionSchema.scanCount());
            deployment.transactionSchema.setTableUnavailable("transaction", true);
            try {
                assertInternal(http.post("/transactions/" + id + "/complete", "{}"));
                assertEquals(200, deployment.inventoryHttp.get("/inventory/low-stock").statusCode());
            } finally {
                deployment.transactionSchema.setTableUnavailable("transaction", false);
            }
            assertEquals(2, deployment.inventorySchema.stock(SKU));
            assertEquals("OPEN", Json.parseObject(http.lookup(id).body()).get("status"));
            HttpResponse<String> completed = http.post("/transactions/" + id + "/complete", "{}");
            assertEquals(200, completed.statusCode(), completed.body() + deployment.logs());
            assertEquals(1, deployment.inventorySchema.stock(SKU));
            assertEquals("COMPLETED", Json.parseObject(http.lookup(id).body()).get("status"));
        }
    }

    private static void assertInternal(HttpResponse<String> response) {
        assertEquals(500, response.statusCode(), response.body());
        assertEquals("INTERNAL_ERROR", Json.parseObject(response.body()).get("error"));
    }
}
