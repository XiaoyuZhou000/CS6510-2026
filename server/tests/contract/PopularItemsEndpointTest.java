package contract;

import analytics.AnalyticsHandlers;
import analytics.AnalyticsRecorder;
import com.sun.net.httpserver.HttpServer;
import json.Json;
import org.junit.jupiter.api.Test;
import support.AnalyticsDatabase;

import java.net.*;
import java.net.http.*;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class PopularItemsEndpointTest {
    @Test
    void latestMeansHighestWindowIdAndNamesComeFromCatalog() throws Exception {
        try (AnalyticsDatabase db = new AnalyticsDatabase()) {
            assertNull(db.windows.readLatestWindow(10));
            db.windows.writeWindow(501, 1500, List.of(
                new persistence.PopularWindowDao.PopularEntry("sku1", "ignored name", 900)));
            db.windows.writeWindow(1, 1000, List.of(
                new persistence.PopularWindowDao.PopularEntry("sku2", "ignored name", 700)));
            java.sql.Connection conn = db.pool.borrow();
            try (java.sql.Statement sql = conn.createStatement()) {
                sql.executeUpdate("UPDATE popular_window SET computed_at='2000-01-01 00:00:00' WHERE window_end=1000");
            } finally {
                db.pool.release(conn);
            }
            var latest = db.windows.readLatestWindow(10);
            assertEquals(1000, latest.windowEnd(), "Select by ID, not boundary or timestamp");
            assertEquals("Item 2", latest.items().getFirst().name());
            assertEquals(700, latest.items().getFirst().scanCount());
            assertTrue(db.windows.readLatestWindow(0).items().isEmpty());
            assertThrows(IllegalArgumentException.class, () -> db.windows.readLatestWindow(-1));
        }
    }

    @Test
    void reportsPersistedWindowWithRankedTopTenAndRequestLocalLimit() throws Exception {
        try (AnalyticsDatabase db = new AnalyticsDatabase(); HttpClient client = HttpClient.newHttpClient()) {
            AnalyticsRecorder recorder = new AnalyticsRecorder(db.catalog, db.windows);
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/analytics/popular-items", new AnalyticsHandlers(db.windows));
            server.start();
            String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/analytics/popular-items";
            try {
                Map<String, Object> empty = get(client, base);
                assertEquals(0, number(empty, "windowEnd"));
                assertEquals(0, number(empty, "windowStart"));
                assertTrue(Json.getList(empty, "items").isEmpty());
                Instant.parse((String) empty.get("computedAt"));

                for (int i = 1; i <= 12; i++) {
                    for (int n = 0; n < i * 10; n++) recorder.recordScan("sku" + i);
                }
                for (int n = 0; n < 220; n++) recorder.recordScan("sku12");
                db.awaitWindow(1000);
                for (int n = 0; n < 123; n++) recorder.recordScan("sku1");

                Map<String, Object> report = get(client, base);
                assertEquals(Set.of("windowSize", "slideInterval", "windowStart", "windowEnd", "computedAt", "items"), report.keySet());
                assertEquals(1000, number(report, "windowSize"));
                assertEquals(500, number(report, "slideInterval"));
                assertEquals(1, number(report, "windowStart"));
                assertEquals(1000, number(report, "windowEnd"));
                assertEquals(db.windows.readLatestWindow(10).computedAt(), Instant.parse((String) report.get("computedAt")));
                List<Object> items = Json.getList(report, "items");
                assertEquals(10, items.size());
                for (int rank = 1; rank <= 10; rank++) {
                    Map<String, Object> item = Json.asObject(items.get(rank - 1));
                    int sku = 13 - rank;
                    assertEquals(Set.of("sku", "name", "scanCount", "rank"), item.keySet());
                    assertEquals("sku" + sku, item.get("sku"));
                    assertEquals("Item " + sku, item.get("name"));
                    assertEquals(rank, number(item, "rank"));
                    assertEquals(sku == 12 ? 340 : sku * 10, number(item, "scanCount"));
                }
                assertEquals(items.subList(0, 3), Json.getList(get(client, base + "?limit=3"), "items"));
                assertEquals(items, Json.getList(get(client, base + "?limit=100"), "items"));
                assertTrue(Json.getList(get(client, base + "?limit=0"), "items").isEmpty());
                assertEquals(report, get(client, base));
                for (String invalid : new String[]{"-1", "abc", "1.5", "2147483648", ""}) {
                    HttpResponse<String> response = request(client, base + "?limit=" + invalid, "GET");
                    assertEquals(400, response.statusCode());
                    assertEquals(Set.of("error", "message"), Json.parseObject(response.body()).keySet());
                }
                assertEquals(404, request(client, base + "/extra", "GET").statusCode());
                assertEquals(405, request(client, base, "POST").statusCode());
            } finally {
                server.stop(0);
                recorder.shutdown();
            }
        }
    }

    private static Map<String, Object> get(HttpClient client, String url) throws Exception {
        HttpResponse<String> response = request(client, url, "GET");
        assertEquals(200, response.statusCode(), response.body());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
        return Json.parseObject(response.body());
    }

    private static HttpResponse<String> request(HttpClient client, String url, String method) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url)).timeout(java.time.Duration.ofSeconds(10))
            .method(method, HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static long number(Map<String, Object> row, String key) {
        return ((Number) row.get(key)).longValue();
    }
}
