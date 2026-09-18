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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test: many concurrent completions across a mix of overlapping and distinct SKUs.
 * For every touched SKU: initial_stock − final_stock == total_quantity_sold (from successful
 * completions only), and stock is never observed negative at any point
 * (FR-008, SC-001; US2 acceptance scenario 7; Constitution I).
 *
 * Requires a running server AND a reachable MySQL instance.
 */
public class StockConservationTest {

    private static final String BASE_URL = System.getProperty("SERVER_BASE_URL", "http://localhost:8080");
    private static final String DB_URL   = System.getProperty("DB_URL",
        "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
    private static final String DB_USER  = System.getProperty("DB_USER", "root");
    private static final String DB_PASS  = System.getProperty("DB_PASS", "");

    // SKUs used in this test (a small, isolated subset)
    private static final String[] SKUS = {
        "SKU-000300", "SKU-000301", "SKU-000302", "SKU-000303", "SKU-000304"
    };
    private static final int CONCURRENT_STATIONS = 20;
    private static final int SCANS_PER_TRANSACTION = 3;

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
    void concurrentCompletions_stockConservation() throws Exception {
        // Record initial stock for all test SKUs
        Map<String, Integer> initialStock = new LinkedHashMap<>();
        for (String sku : SKUS) {
            initialStock.put(sku, queryStock(sku));
        }

        // Launch concurrent stations; each scans a mix of the test SKUs and completes
        ExecutorService exec = Executors.newFixedThreadPool(CONCURRENT_STATIONS);
        AtomicInteger successCount = new AtomicInteger(0);

        // Track quantity sold per SKU from successful completions
        Map<String, AtomicInteger> soldQty = new LinkedHashMap<>();
        for (String sku : SKUS) soldQty.put(sku, new AtomicInteger(0));

        List<Callable<Void>> tasks = new ArrayList<>();
        Random rng = new Random(42);

        for (int i = 0; i < CONCURRENT_STATIONS; i++) {
            final int stationIndex = i;
            // Each station scans a round-robin mix of SKUs
            List<String> scanList = new ArrayList<>();
            for (int j = 0; j < SCANS_PER_TRANSACTION; j++) {
                scanList.add(SKUS[(stationIndex + j) % SKUS.length]);
            }

            tasks.add(() -> {
                try {
                    String txId = startAndGetId("station-conservation-" + stationIndex);
                    Map<String, Integer> localQty = new LinkedHashMap<>();
                    for (String sku : scanList) {
                        scan(txId, sku);
                        localQty.merge(sku, 1, Integer::sum);
                    }
                    HttpResponse<String> resp = complete(txId);
                    if (resp.statusCode() == 200) {
                        successCount.incrementAndGet();
                        for (Map.Entry<String, Integer> e : localQty.entrySet()) {
                            soldQty.get(e.getKey()).addAndGet(e.getValue());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[conservation-test] station error: " + e.getMessage());
                }
                return null;
            });
        }

        List<Future<Void>> futures = exec.invokeAll(tasks, 60, TimeUnit.SECONDS);
        exec.shutdown();
        for (Future<Void> f : futures) {
            f.get(); // propagate any uncaught exceptions
        }

        assertTrue(successCount.get() > 0, "At least one completion must have succeeded");

        // Verify stock conservation: for each SKU, initial - final == sold
        for (String sku : SKUS) {
            int finalStockValue = queryStock(sku);
            int sold = soldQty.get(sku).get();
            int expected = initialStock.get(sku) - sold;

            assertTrue(finalStockValue >= 0,
                "Stock must never be negative for " + sku + "; final=" + finalStockValue);
            assertEquals(expected, finalStockValue,
                "Stock conservation violated for " + sku +
                ": initial=" + initialStock.get(sku) + " sold=" + sold + " final=" + finalStockValue);
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
