package integration;

import api.json.Json;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test: two stations each scan the last remaining unit of the same SKU,
 * then complete concurrently — exactly one must succeed, the other must be rejected,
 * and the SKU's stock must never go negative through the layered server (FR-016; US1).
 *
 * Requires a running server AND a reachable MySQL instance.
 * Override via SERVER_BASE_URL and DB_* system properties.
 */
public class ConcurrentCompletionTest {

    private static final String BASE_URL = System.getProperty("SERVER_BASE_URL", "http://localhost:8080");
    private static final String DB_URL   = System.getProperty("DB_URL",
        "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
    private static final String DB_USER  = System.getProperty("DB_USER", "root");
    private static final String DB_PASS  = System.getProperty("DB_PASS", "");

    private static final String TEST_SKU = "SKU-000042";

    @BeforeAll
    static void requireServerAndDb() {
        try {
            HttpClient client = HttpClient.newHttpClient();
            client.send(HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/items")).GET().build(),
                HttpResponse.BodyHandlers.discarding());
        } catch (ConnectException e) {
            assumeTrue(false, "Server not reachable at " + BASE_URL);
        } catch (Exception ignored) {}
    }

    @Test
    void concurrentCompletion_exactlyOneSucceeds_stockNeverNegative() throws Exception {
        int originalStock;
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT stock_quantity FROM inventory WHERE sku = ?")) {
            ps.setString(1, TEST_SKU);
            try (ResultSet rows = ps.executeQuery()) {
                assertTrue(rows.next());
                originalStock = rows.getInt(1);
            }
        }
        AtomicInteger successes = new AtomicInteger(0);
        // Set SKU stock to exactly 1 unit via direct DB access
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(
                "UPDATE inventory SET stock_quantity = 1 WHERE sku = ?")) {
            ps.setString(1, TEST_SKU);
            assertEquals(1, ps.executeUpdate(), "SKU must exist in inventory");
        }

        try {
        // Start two transactions, each scanning the last unit
        String tx1 = startAndGetId("station-concurrent-A");
        String tx2 = startAndGetId("station-concurrent-B");
        scanItem(tx1, TEST_SKU);
        scanItem(tx2, TEST_SKU);

        // Complete both concurrently via real threads
        ExecutorService exec = Executors.newFixedThreadPool(2);

        AtomicInteger rejections = new AtomicInteger(0);

        Callable<Integer> completer1 = () -> {
            HttpResponse<String> r = completeTransaction(tx1);
            return r.statusCode();
        };
        Callable<Integer> completer2 = () -> {
            HttpResponse<String> r = completeTransaction(tx2);
            return r.statusCode();
        };

        List<Future<Integer>> futures = exec.invokeAll(List.of(completer1, completer2));
        exec.shutdown();

        for (Future<Integer> f : futures) {
            int code = f.get(10, TimeUnit.SECONDS);
            if (code == 200) successes.incrementAndGet();
            else if (code == 409) rejections.incrementAndGet();
            else fail("Unexpected status code: " + code);
        }

        assertEquals(1, successes.get(),
            "Exactly one completion should succeed");
        assertEquals(1, rejections.get(),
            "Exactly one completion should be rejected");

        // The losing checkout remains OPEN with its committed basket for a later retry.
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT status FROM `transaction` WHERE transaction_id IN (?,?)")) {
            ps.setString(1, tx1);
            ps.setString(2, tx2);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String firstStatus = rs.getString(1);
                assertTrue(rs.next());
                String secondStatus = rs.getString(1);
                assertEquals(java.util.Set.of("COMPLETED", "OPEN"),
                    java.util.Set.of(firstStatus, secondStatus));
                assertFalse(rs.next());
            }
        }

        // Verify final stock is exactly 0, never negative
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(
                "SELECT stock_quantity FROM inventory WHERE sku = ?")) {
            ps.setString(1, TEST_SKU);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Inventory row must exist");
                int finalStock = rs.getInt(1);
                assertEquals(0, finalStock,
                    "Final stock must be exactly 0 (one unit sold, none negative)");
            }
        }
        } finally {
            restoreStock(originalStock - successes.get());
        }
    }

    private static void restoreStock(int quantity) throws Exception {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(
                 "UPDATE inventory SET stock_quantity=? WHERE sku=?")) {
            ps.setInt(1, quantity);
            ps.setString(2, TEST_SKU);
            assertEquals(1, ps.executeUpdate());
        }
    }

    // ------------------------------------------------------------------ helpers

    private String startAndGetId(String stationId) throws Exception {
        HttpResponse<String> resp = postJson("/transactions",
            "{\"stationId\":\"" + stationId + "\"}");
        assertEquals(201, resp.statusCode());
        return Json.parseObject(resp.body()).get("transactionId").toString();
    }

    private void scanItem(String transactionId, String sku) throws Exception {
        HttpResponse<String> resp = postJson(
            "/transactions/" + transactionId + "/items",
            "{\"sku\":\"" + sku + "\"}");
        assertEquals(200, resp.statusCode(),
            "Scan must succeed; got " + resp.statusCode() + ": " + resp.body());
    }

    private HttpResponse<String> completeTransaction(String transactionId) throws Exception {
        return postJson("/transactions/" + transactionId + "/complete", "");
    }

    private HttpResponse<String> postJson(String path, String body) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        return client.send(
            HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    }
}
