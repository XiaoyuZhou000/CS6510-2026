package integration;

import json.Json;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test: completing an already-completed transaction a second time must:
 * - return 409 TRANSACTION_NOT_OPEN on the duplicate request,
 * - leave stock decremented exactly once (idempotency, FR-009, SC-002; US2 acceptance scenario 5).
 *
 * Requires a running server AND a reachable MySQL instance.
 */
public class DuplicateCompletionTest {

    private static final String BASE_URL = System.getProperty("SERVER_BASE_URL", "http://localhost:8080");
    private static final String DB_URL   = System.getProperty("DB_URL",
        "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
    private static final String DB_USER  = System.getProperty("DB_USER", "root");
    private static final String DB_PASS  = System.getProperty("DB_PASS", "");

    private static final String TEST_SKU = "SKU-000010";

    @BeforeAll
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
        // Record initial stock
        int initialStock = queryStock(TEST_SKU);

        // Start + scan + complete (first time)
        String transactionId = startAndGetId("station-dup-test");
        scan(transactionId, TEST_SKU);
        HttpResponse<String> first = complete(transactionId);
        assertEquals(200, first.statusCode(),
            "First completion must succeed; got: " + first.body());

        // Stock should have dropped by 1
        int stockAfterFirst = queryStock(TEST_SKU);
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
        int stockAfterDup = queryStock(TEST_SKU);
        assertEquals(stockAfterFirst, stockAfterDup,
            "Stock must not change on a duplicate completion attempt");
    }

    // ------------------------------------------------------------------ helpers

    private int queryStock(String sku) throws Exception {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(
                "SELECT stock_quantity FROM inventory WHERE sku = ?")) {
            ps.setString(1, sku);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "SKU must exist in inventory: " + sku);
                return rs.getInt(1);
            }
        }
    }

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
