package contract;

import json.Json;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Contract tests for GET /transactions/{id} (FR-012; US5 acceptance scenarios 1-2).
 *
 * Requires the server to be running. Override the base URL via the
 * SERVER_BASE_URL system property (default: http://localhost:8080).
 */
public class TransactionLookupTest {

    private static final String BASE_URL =
        System.getProperty("SERVER_BASE_URL", "http://localhost:8080");
    private static final String STATION_ID = "station-lookup-test";
    private static final String KNOWN_SKU = "SKU-000001";

    @BeforeAll
    static void requireServerRunning() {
        try {
            HttpRequest probe = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/items"))
                .GET()
                .build();
            HttpClient.newHttpClient().send(probe, HttpResponse.BodyHandlers.discarding());
        } catch (ConnectException e) {
            assumeTrue(false,
                "Server not reachable at " + BASE_URL + " — start the server before running contract tests");
        } catch (Exception e) {
            // Any non-connect response means the server is up.
        }
    }

    @Test
    void getOpenTransaction_returnsCurrentBasketTotals() throws Exception {
        String transactionId = startAndGetId();
        Map<String, Object> scan = parse(scanItem(transactionId));

        HttpResponse<String> response = getTransaction(transactionId);

        assertEquals(200, response.statusCode());
        Map<String, Object> body = parse(response);
        assertTransactionShape(body, transactionId, "OPEN");
        assertEquals(1, ((Number) body.get("itemCount")).intValue());
        assertEquals(((Number) scan.get("runningTotal")).doubleValue(),
            ((Number) body.get("runningTotal")).doubleValue(), 0.001);
    }

    @Test
    void getCompletedTransaction_fallsBackToPersistedTotals() throws Exception {
        String transactionId = startAndGetId();
        scanItem(transactionId);
        HttpResponse<String> receiptResponse = postJson(
            "/transactions/" + transactionId + "/complete", "");
        assertEquals(200, receiptResponse.statusCode());
        Map<String, Object> receipt = parse(receiptResponse);

        HttpResponse<String> response = getTransaction(transactionId);

        assertEquals(200, response.statusCode());
        Map<String, Object> body = parse(response);
        assertTransactionShape(body, transactionId, "COMPLETED");
        assertEquals(((Number) receipt.get("itemCount")).intValue(),
            ((Number) body.get("itemCount")).intValue());
        assertEquals(((Number) receipt.get("totalAmount")).doubleValue(),
            ((Number) body.get("runningTotal")).doubleValue(), 0.001);
    }

    @Test
    void getUnknownTransaction_returns404ApiError() throws Exception {
        HttpResponse<String> response = getTransaction("nonexistent-lookup-transaction");

        assertEquals(404, response.statusCode());
        Map<String, Object> body = parse(response);
        assertEquals("TRANSACTION_NOT_FOUND", body.get("error"));
        assertNotNull(body.get("message"));
    }

    private static void assertTransactionShape(Map<String, Object> body,
                                               String transactionId,
                                               String expectedStatus) {
        assertEquals(transactionId, body.get("transactionId"));
        assertEquals(STATION_ID, body.get("stationId"));
        assertEquals(expectedStatus, body.get("status"));
        assertInstanceOf(Number.class, body.get("itemCount"));
        assertInstanceOf(Number.class, body.get("runningTotal"));
        assertNotNull(body.get("startedAt"));
    }

    private String startAndGetId() throws Exception {
        HttpResponse<String> response = postJson(
            "/transactions", "{\"stationId\":\"" + STATION_ID + "\"}");
        assertEquals(201, response.statusCode());
        return parse(response).get("transactionId").toString();
    }

    private HttpResponse<String> scanItem(String transactionId) throws Exception {
        return postJson("/transactions/" + transactionId + "/items",
            "{\"sku\":\"" + KNOWN_SKU + "\"}");
    }

    private HttpResponse<String> getTransaction(String transactionId) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/transactions/" + transactionId))
            .GET()
            .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postJson(String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + path))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static Map<String, Object> parse(HttpResponse<String> response) {
        return Json.parseObject(response.body());
    }
}
