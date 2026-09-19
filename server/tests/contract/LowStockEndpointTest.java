package contract;

import json.Json;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.*;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/** Requires a running server and its MySQL database; restores all fixture inventory. */
public class LowStockEndpointTest {
    private static final String BASE = System.getProperty("SERVER_BASE_URL", "http://localhost:8080");
    private static final String DB_URL = System.getProperty("DB_URL",
        "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
    private static final String SKU = "SKU-001998";
    private final HttpClient client = HttpClient.newHttpClient();

    @Test
    void thresholdsAreInclusivePerSkuAndOverridesAreRequestLocal() throws Exception {
        try (Connection conn = connect()) {
            int stock;
            int threshold;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT stock_quantity, low_stock_threshold FROM inventory WHERE sku=?")) {
                ps.setString(1, SKU);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    stock = rs.getInt(1);
                    threshold = rs.getInt(2);
                }
            }
            try {
                setInventory(conn, 3, 3);
                Instant before = Instant.now();
                Map<String, Object> defaults = get("");
                assertEquals(Set.of("threshold", "generatedAt", "alerts"), defaults.keySet());
                assertEquals(50, number(defaults, "threshold"));
                Instant generated = Instant.parse((String) defaults.get("generatedAt"));
                assertFalse(generated.isBefore(before));
                assertFalse(generated.isAfter(Instant.now()));
                Map<String, Object> alert = find(defaults);
                assertNotNull(alert, "Stock equal to the per-SKU default must be included");
                assertEquals(Set.of("sku", "name", "currentStock", "threshold", "triggeredAt"), alert.keySet());
                assertInstanceOf(String.class, alert.get("name"));
                assertEquals(3, number(alert, "currentStock"));
                assertEquals(3, number(alert, "threshold"));
                assertEquals(generated, Instant.parse((String) alert.get("triggeredAt")));

                assertNull(find(get("?threshold=2")));
                Map<String, Object> overridden = get("?threshold=3");
                assertEquals(3, number(overridden, "threshold"));
                assertNotNull(find(overridden));
                for (Object entry : Json.getList(overridden, "alerts")) {
                    Map<String, Object> row = Json.asObject(entry);
                    assertEquals(3, number(row, "threshold"));
                    assertTrue(number(row, "currentStock") <= 3);
                }
                assertEquals(Json.getList(overridden, "alerts").size(),
                    Json.getList(overridden, "alerts").stream()
                        .map(entry -> Json.asObject(entry).get("sku")).collect(Collectors.toSet()).size());
                assertEquals(3, number(find(get("?threshold=9")), "currentStock"));
                assertEquals(3, number(find(get("")), "threshold"));

                setInventory(conn, 4, 3);
                assertNull(find(get("")), "Stock above the stored threshold must be excluded");
                String tx = (String) post("/transactions", "{\"stationId\":\"low-stock-test\"}", 201).get("transactionId");
                post("/transactions/" + tx + "/items", "{\"sku\":\"" + SKU + "\"}", 200);
                assertNull(find(get("")), "Scanning must not change stock");
                post("/transactions/" + tx + "/complete", "", 200);
                assertEquals(3, number(find(get("")), "currentStock"));

                setInventory(conn, 0, 3);
                assertEquals(0, number(find(get("?threshold=0")), "threshold"));
                assertTrue(Json.getList(get("?threshold=-1"), "alerts").isEmpty());
            } finally {
                setInventory(conn, stock, threshold);
            }
        }
    }

    @Test
    void rejectsInvalidThresholdsAndUnmatchedRoutes() throws Exception {
        for (String value : new String[]{"abc", "1.5", "2147483648", ""}) {
            HttpResponse<String> response = request("GET", "/inventory/low-stock?threshold=" + value, "");
            assertEquals(400, response.statusCode());
            Map<String, Object> error = Json.parseObject(response.body());
            assertInstanceOf(String.class, error.get("error"));
            assertInstanceOf(String.class, error.get("message"));
        }
        assertEquals(404, request("GET", "/inventory/low-stock/extra", "").statusCode());
        assertEquals(405, request("POST", "/inventory/low-stock", "").statusCode());
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(DB_URL,
            System.getProperty("DB_USER", "root"), System.getProperty("DB_PASS", ""));
    }

    private void setInventory(Connection conn, int stock, int threshold) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE inventory SET stock_quantity=?, low_stock_threshold=? WHERE sku=?")) {
            ps.setInt(1, stock);
            ps.setInt(2, threshold);
            ps.setString(3, SKU);
            assertEquals(1, ps.executeUpdate());
        }
    }

    private Map<String, Object> get(String query) throws Exception {
        HttpResponse<String> response = request("GET", "/inventory/low-stock" + query, "");
        assertEquals(200, response.statusCode(), response.body());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
        return Json.parseObject(response.body());
    }

    private Map<String, Object> post(String path, String body, int status) throws Exception {
        HttpResponse<String> response = request("POST", path, body);
        assertEquals(status, response.statusCode(), response.body());
        return Json.parseObject(response.body());
    }

    private HttpResponse<String> request(String method, String path, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(BASE + path))
            .timeout(java.time.Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .method(method, HttpRequest.BodyPublishers.ofString(body)).build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private Map<String, Object> find(Map<String, Object> response) {
        return Json.getList(response, "alerts").stream().map(Json::asObject)
            .filter(row -> SKU.equals(row.get("sku"))).findFirst().orElse(null);
    }

    private int number(Map<String, Object> row, String key) {
        return ((Number) row.get(key)).intValue();
    }
}
