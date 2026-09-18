package contract;

import json.Json;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Contract tests for the transaction lifecycle:
 * POST /transactions, POST /transactions/{id}/items, POST /transactions/{id}/complete.
 *
 * Validates response shapes and status codes against the OpenAPI contract and error-codes.md
 * (FR-003, FR-004, FR-006, FR-007; US2 acceptance scenarios 1–2).
 *
 * Requires the server to be running. Override the base URL via the
 * SERVER_BASE_URL system property (default: http://localhost:8080).
 */
public class TransactionLifecycleTest {

    private static final String BASE_URL =
        System.getProperty("SERVER_BASE_URL", "http://localhost:8080");

    private static final String STATION_ID = "station-test-01";
    private static final String KNOWN_SKU   = "SKU-000001";

    @BeforeAll
    static void requireServerRunning() {
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest probe = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/items"))
                .GET()
                .build();
            client.send(probe, HttpResponse.BodyHandlers.discarding());
        } catch (ConnectException e) {
            assumeTrue(false,
                "Server not reachable at " + BASE_URL + " — start the server before running contract tests");
        } catch (Exception e) {
            // Any non-connect response means the server is up
        }
    }

    // ------------------------------------------------------------------ start (POST /transactions)

    @Test
    void startTransaction_returns201WithTransactionShape() throws Exception {
        HttpResponse<String> response = startTransaction(STATION_ID);

        assertEquals(201, response.statusCode(),
            "POST /transactions should return 201 Created");

        Map<String, Object> body = Json.parseObject(response.body());
        assertNotNull(body.get("transactionId"), "transactionId must be present");
        assertEquals(STATION_ID, body.get("stationId"), "stationId must match request");
        assertEquals("OPEN", body.get("status"), "Initial status must be OPEN");
        assertEquals(0, ((Number) body.get("itemCount")).intValue(), "Initial itemCount must be 0");
        assertEquals(0.0, ((Number) body.get("runningTotal")).doubleValue(), 0.001,
            "Initial runningTotal must be 0");
        assertNotNull(body.get("startedAt"), "startedAt must be present");
    }

    @Test
    void startTransaction_missingStationId_returns400() throws Exception {
        HttpResponse<String> response = postJson("/transactions", "{}");

        assertEquals(400, response.statusCode(),
            "POST /transactions with missing stationId should return 400");

        assertApiError(response.body(), "MISSING_STATION_ID");
    }

    @Test
    void startTransaction_blankStationId_returns400() throws Exception {
        HttpResponse<String> response = postJson("/transactions", "{\"stationId\":\"\"}");

        assertEquals(400, response.statusCode());
        assertApiError(response.body(), "MISSING_STATION_ID");
    }

    // ------------------------------------------------------------------ scan (POST /transactions/{id}/items)

    @Test
    void scanItem_returns200WithScanResultShape() throws Exception {
        String transactionId = startAndGetId(STATION_ID);

        HttpResponse<String> response = scanItem(transactionId, KNOWN_SKU);

        assertEquals(200, response.statusCode(),
            "POST /transactions/{id}/items should return 200 OK");

        Map<String, Object> body = Json.parseObject(response.body());
        assertEquals(transactionId, body.get("transactionId"), "transactionId must match");
        assertEquals(KNOWN_SKU, body.get("sku"), "sku must match scanned SKU");
        assertNotNull(body.get("name"),         "name must be present");
        assertNotNull(body.get("unitPrice"),     "unitPrice must be present");
        assertEquals(1, ((Number) body.get("itemCount")).intValue(), "itemCount must be 1 after first scan");
        assertTrue(((Number) body.get("runningTotal")).doubleValue() > 0,
            "runningTotal must be > 0 after a scan");
    }

    @Test
    void scanItem_unknownTransaction_returns404() throws Exception {
        HttpResponse<String> response = scanItem("nonexistent-tx-id", KNOWN_SKU);

        assertEquals(404, response.statusCode());
        assertApiError(response.body(), "TRANSACTION_NOT_FOUND");
    }

    @Test
    void scanItem_unknownSku_returns404() throws Exception {
        String transactionId = startAndGetId(STATION_ID);

        HttpResponse<String> response = scanItem(transactionId, "SKU-DOES-NOT-EXIST");

        assertEquals(404, response.statusCode());
        assertApiError(response.body(), "SKU_NOT_FOUND");
    }

    @Test
    void scanItem_completedTransaction_returns409() throws Exception {
        String transactionId = startAndGetId(STATION_ID);
        scanItem(transactionId, KNOWN_SKU);
        completeTransaction(transactionId);

        HttpResponse<String> response = scanItem(transactionId, KNOWN_SKU);

        assertEquals(409, response.statusCode());
        assertApiError(response.body(), "TRANSACTION_NOT_OPEN");
    }

    // ------------------------------------------------------------------ complete (POST /transactions/{id}/complete)

    @Test
    void completeTransaction_returns200WithReceiptShape() throws Exception {
        String transactionId = startAndGetId(STATION_ID);
        scanItem(transactionId, KNOWN_SKU);

        HttpResponse<String> response = completeTransaction(transactionId);

        assertEquals(200, response.statusCode(),
            "POST /transactions/{id}/complete should return 200 OK");

        Map<String, Object> body = Json.parseObject(response.body());
        assertEquals(transactionId, body.get("transactionId"), "transactionId must match");
        assertEquals(STATION_ID, body.get("stationId"), "stationId must match");
        assertNotNull(body.get("itemCount"),   "itemCount must be present");
        assertNotNull(body.get("totalAmount"), "totalAmount must be present");
        assertNotNull(body.get("startedAt"),   "startedAt must be present");
        assertNotNull(body.get("completedAt"), "completedAt must be present");
        assertNotNull(body.get("lines"),       "lines must be present");

        List<Object> lines = Json.getList(body, "lines");
        assertFalse(lines.isEmpty(), "Receipt must have at least one line");

        Map<String, Object> line = Json.asObject(lines.get(0));
        assertNotNull(line.get("sku"),       "line.sku must be present");
        assertNotNull(line.get("name"),      "line.name must be present");
        assertNotNull(line.get("unitPrice"), "line.unitPrice must be present");
        assertNotNull(line.get("quantity"),  "line.quantity must be present");
    }

    @Test
    void completeTransaction_unknownTransaction_returns404() throws Exception {
        HttpResponse<String> response = completeTransaction("nonexistent-tx-id");

        assertEquals(404, response.statusCode());
        assertApiError(response.body(), "TRANSACTION_NOT_FOUND");
    }

    @Test
    void completeTransaction_emptyBasket_returns409() throws Exception {
        String transactionId = startAndGetId(STATION_ID);

        HttpResponse<String> response = completeTransaction(transactionId);

        assertEquals(409, response.statusCode());
        assertApiError(response.body(), "EMPTY_BASKET");
    }

    @Test
    void completeTransaction_alreadyCompleted_returns409() throws Exception {
        String transactionId = startAndGetId(STATION_ID);
        scanItem(transactionId, KNOWN_SKU);
        completeTransaction(transactionId); // first completion

        HttpResponse<String> response = completeTransaction(transactionId); // duplicate

        assertEquals(409, response.statusCode());
        assertApiError(response.body(), "TRANSACTION_NOT_OPEN");
    }

    @Test
    void scanItem_accumulatesQuantity() throws Exception {
        String transactionId = startAndGetId(STATION_ID);
        scanItem(transactionId, KNOWN_SKU);
        HttpResponse<String> response = scanItem(transactionId, KNOWN_SKU);

        Map<String, Object> body = Json.parseObject(response.body());
        assertEquals(2, ((Number) body.get("itemCount")).intValue(),
            "Scanning the same SKU twice should yield itemCount=2");
    }

    // ------------------------------------------------------------------ helpers

    private String startAndGetId(String stationId) throws Exception {
        HttpResponse<String> resp = startTransaction(stationId);
        assertEquals(201, resp.statusCode(), "Expected 201 from POST /transactions");
        Map<String, Object> body = Json.parseObject(resp.body());
        return body.get("transactionId").toString();
    }

    private HttpResponse<String> startTransaction(String stationId) throws Exception {
        String payload = "{\"stationId\":\"" + stationId + "\"}";
        return postJson("/transactions", payload);
    }

    private HttpResponse<String> scanItem(String transactionId, String sku) throws Exception {
        String payload = "{\"sku\":\"" + sku + "\"}";
        return postJson("/transactions/" + transactionId + "/items", payload);
    }

    private HttpResponse<String> completeTransaction(String transactionId) throws Exception {
        return postJson("/transactions/" + transactionId + "/complete", "");
    }

    private HttpResponse<String> postJson(String path, String body) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + path))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void assertApiError(String body, String expectedCode) {
        Map<String, Object> err = Json.parseObject(body);
        assertEquals(expectedCode, err.get("error"),
            "ApiError.error must be '" + expectedCode + "', got body: " + body);
        assertNotNull(err.get("message"), "ApiError.message must be present");
    }
}
