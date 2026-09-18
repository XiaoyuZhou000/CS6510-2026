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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test: after a successful completion response, the transaction and inventory
 * rows must already be committed and queryable immediately over a fresh, direct JDBC connection
 * (FR-010, Constitution II; US2 acceptance scenario 6).
 *
 * Requires a running server AND a reachable MySQL instance.
 */
public class CompletionDurabilityTest {

    private static final String BASE_URL = System.getProperty("SERVER_BASE_URL", "http://localhost:8080");
    private static final String DB_URL   = System.getProperty("DB_URL",
        "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
    private static final String DB_USER  = System.getProperty("DB_USER", "root");
    private static final String DB_PASS  = System.getProperty("DB_PASS", "");

    private static final String STATION = "station-durability-test";
    private static final String SKU_A   = "SKU-000100";
    private static final String SKU_B   = "SKU-000200";

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
    void completedTransaction_committedBeforeResponseReturned() throws Exception {
        int initialStockA = queryStock(SKU_A);
        int initialStockB = queryStock(SKU_B);

        // Start, scan two distinct SKUs, complete
        String txId = startAndGetId(STATION);
        scan(txId, SKU_A);
        scan(txId, SKU_A); // scan A twice
        scan(txId, SKU_B); // scan B once

        HttpResponse<String> completionResp = complete(txId);
        assertEquals(200, completionResp.statusCode(),
            "Completion must succeed; got: " + completionResp.body());

        Map<String, Object> receipt = Json.parseObject(completionResp.body());
        assertEquals(txId, receipt.get("transactionId"), "transactionId must match");

        // Verify durability via a FRESH DB connection (no caching)
        try (Connection fresh = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

            // 1. Transaction row must be COMPLETED with completed_at set
            try (PreparedStatement ps = fresh.prepareStatement(
                    "SELECT status, completed_at FROM `transaction` WHERE transaction_id = ?")) {
                ps.setString(1, txId);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "transaction row must exist immediately after success response");
                    assertEquals("COMPLETED", rs.getString("status"),
                        "Transaction status must be COMPLETED");
                    assertNotNull(rs.getTimestamp("completed_at"),
                        "completed_at must be non-null");
                }
            }

            // 2. transaction_line rows must exist for both SKUs
            try (PreparedStatement ps = fresh.prepareStatement(
                    "SELECT sku, quantity FROM transaction_line WHERE transaction_id = ? ORDER BY sku")) {
                ps.setString(1, txId);
                try (ResultSet rs = ps.executeQuery()) {
                    int rowCount = 0;
                    while (rs.next()) {
                        rowCount++;
                        String sku = rs.getString("sku");
                        int qty = rs.getInt("quantity");
                        if (SKU_A.equals(sku)) assertEquals(2, qty, "SKU_A quantity must be 2");
                        if (SKU_B.equals(sku)) assertEquals(1, qty, "SKU_B quantity must be 1");
                    }
                    assertEquals(2, rowCount, "Must have exactly 2 distinct SKU lines");
                }
            }

            // 3. Inventory must be decremented by the scanned quantities
            assertEquals(initialStockA - 2, queryStock(SKU_A),
                "Stock for SKU_A must be decremented by 2");
            assertEquals(initialStockB - 1, queryStock(SKU_B),
                "Stock for SKU_B must be decremented by 1");
        }
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
        assertEquals(200, resp.statusCode());
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
