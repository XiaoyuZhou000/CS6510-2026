package integration;

import api.json.Json;
import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.LiveSchemas;
import support.ServiceHttp;

import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test: completing an already-completed transaction a second time must:
 * - return 409 TRANSACTION_NOT_OPEN on the duplicate request,
 * - leave stock decremented exactly once through the deployed services (FR-014, SC-004; US1).
 *
 * The first test requires a running deployment; stock is read in Inventory's schema with Inventory's own
 * credentials. The second starts its own Transaction and Inventory processes over the live broker (006) and
 * repeats the Complete after restarting each service and, with {@code KAFKA_TEST_BROKER_CONTROL=1}, the broker.
 */
public class DuplicateCompletionTest {

    private static final String BASE_URL = System.getProperty("SERVER_BASE_URL", "http://localhost:8080");

    private static final String TEST_SKU = "SKU-000010";

    static void requireServer() {
        try {
            HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(BASE_URL + "/items")).GET().build(),
                HttpResponse.BodyHandlers.discarding());
        } catch (ConnectException e) {
            assumeTrue(false, "Server not reachable at " + BASE_URL);
        } catch (Exception ignored) {}
    }

    @Test
    void duplicateCompletion_stockDecrementedExactlyOnce() throws Exception {
        requireServer();
        // Record initial stock
        int initialStock = LiveSchemas.stock(TEST_SKU);

        // Start + scan + complete (first time)
        String transactionId = startAndGetId("station-dup-test");
        scan(transactionId, TEST_SKU);
        HttpResponse<String> first = complete(transactionId);
        assertEquals(200, first.statusCode(),
            "First completion must succeed; got: " + first.body());

        // Stock should have dropped by 1
        int stockAfterFirst = LiveSchemas.stock(TEST_SKU);
        assertEquals(initialStock - 1, stockAfterFirst,
            "Stock must be decremented by exactly 1 after first completion");

        // Duplicate completion on the same transactionId
        HttpResponse<String> second = complete(transactionId);
        assertEquals(409, second.statusCode(),
            "Second completion must be rejected with 409");
        Map<String, Object> err = Json.parseObject(second.body());
        assertEquals("TRANSACTION_NOT_OPEN", err.get("error"),
            "Error code must be TRANSACTION_NOT_OPEN");

        // Stock must be unchanged after the duplicate
        int stockAfterDup = LiveSchemas.stock(TEST_SKU);
        assertEquals(stockAfterFirst, stockAfterDup,
            "Stock must not change on a duplicate completion attempt");
    }

    @Test
    void aRepeatedCompleteAfterRestartingEachServiceAndTheBrokerHasNoFurtherInventoryEffect() throws Exception {
        String sku = InventoryTestSchema.sku(1);
        try (CheckoutDeployment deployment = CheckoutDeployment.start(10, 50)) {
            ServiceHttp http = deployment.transactionHttp;
            String id = http.start("station-dup-restart");
            assertEquals(200, http.scan(id, sku).statusCode());
            HttpResponse<String> first = http.complete(id);
            assertEquals(200, first.statusCode(), first.body() + deployment.logs());
            assertEquals(49, deployment.inventorySchema.stock(sku));

            deployment.startInventory();
            assertNotOpenWithNoEffect(deployment, id, sku, "after restarting Inventory");
            deployment.startTransaction();
            assertNotOpenWithNoEffect(deployment, id, sku, "after restarting Transaction");
            if (KafkaTestSupport.brokerControlEnabled()) {
                KafkaTestSupport.restartBroker();
                assertNotOpenWithNoEffect(deployment, id, sku, "after restarting the broker");
            }
            assertEquals(1, deployment.inventorySchema.operationCount("APPLIED"), "Inventory applied the attempt once");
        }
    }

    private static void assertNotOpenWithNoEffect(CheckoutDeployment deployment, String id, String sku, String when)
            throws Exception {
        HttpResponse<String> repeated = deployment.transactionHttp.complete(id);
        assertEquals(409, repeated.statusCode(), when + ": " + repeated.body());
        assertEquals("TRANSACTION_NOT_OPEN", Json.parseObject(repeated.body()).get("error"), when);
        assertEquals(49, deployment.inventorySchema.stock(sku), when + ": no further deduction");
        assertEquals(1, deployment.transactionSchema.attempts(id).size(), when + ": no further attempt");
    }

    // ------------------------------------------------------------------ helpers

    private String startAndGetId(String stationId) throws Exception {
        HttpResponse<String> resp = post("/transactions",
            "{\"stationId\":\"" + stationId + "\"}");
        assertEquals(201, resp.statusCode());
        return Json.parseObject(resp.body()).get("transactionId").toString();
    }

    private void scan(String txId, String sku) throws Exception {
        HttpResponse<String> resp = post("/transactions/" + txId + "/items",
            "{\"sku\":\"" + sku + "\"}");
        assertEquals(200, resp.statusCode(),
            "Scan must succeed; got: " + resp.body());
    }

    private HttpResponse<String> complete(String txId) throws Exception {
        return post("/transactions/" + txId + "/complete", "");
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return HttpClient.newHttpClient().send(
            HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    }
}
