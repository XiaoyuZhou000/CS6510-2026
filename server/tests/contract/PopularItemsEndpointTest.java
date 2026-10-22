package contract;

// Migration test inventory: GET /analytics/popular-items -> US3 operational reporting (1 of 7 retained routes).

import analytics.AnalyticsService;
import api.AnalyticsHttpHandler;
import api.json.Json;
import com.sun.net.httpserver.HttpServer;
import database.PopularWindowStore;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.InProcessAnalytics;

import java.net.*;
import java.net.http.*;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code GET /analytics/popular-items} over Analytics' own schema. Analytics is seeded through
 * {@code DeliverScanEvents}, so the names it reports are the catalog names captured at scan time and stored
 * in {@code product_name} (R12), never read from another service's tables.
 */
public class PopularItemsEndpointTest {
    @Test
    void latestMeansCommittedCheckpointAndNamesComeFromCatalog() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            assertTrue(analytics.windows.readLatest(10).isEmpty());
            analytics.deliver(List.of(InProcessAnalytics.event("tx-1", 1, "sku1", "Item 1"),
                    InProcessAnalytics.event("tx-1", 2, "sku2", "Item 2")));
            analytics.windows.writeWindow(snapshot(1, 1000, "sku2", 700));
            analytics.windows.writeWindow(snapshot(501, 1500, "sku1", 900));
            schema.update("UPDATE popular_window SET computed_at='2000-01-01 00:00:00' WHERE window_end=1000");
            var latest = analytics.windows.readLatest(10).orElseThrow();
            assertEquals(1500, latest.windowEnd(), "Select only the committed checkpoint boundary");
            assertEquals("Item 1", latest.ranks().getFirst().name());
            assertEquals(schema.productName("sku1"), latest.ranks().getFirst().name(), "the name captured at scan time");
            assertEquals(900, latest.ranks().getFirst().scanCount());
            assertTrue(analytics.windows.readLatest(0).orElseThrow().ranks().isEmpty());
            assertThrows(IllegalArgumentException.class, () -> analytics.windows.readLatest(-1));
        }
    }

    @Test
    void reportsPersistedWindowWithRankedTopTenAndRequestLocalLimit() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema);
             HttpClient client = HttpClient.newHttpClient()) {
            AnalyticsService recorder = analytics.startPipeline();
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/analytics/popular-items", new AnalyticsHttpHandler(recorder));
            server.start();
            String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/analytics/popular-items";
            try {
                Map<String, Object> empty = get(client, base);
                assertEquals(0, number(empty, "windowEnd"));
                assertEquals(0, number(empty, "windowStart"));
                assertTrue(Json.getList(empty, "items").isEmpty());
                Instant.parse((String) empty.get("computedAt"));

                List<String> scans = new ArrayList<>();
                for (int i = 1; i <= 12; i++) {
                    for (int n = 0; n < i * 10; n++) scans.add("sku" + i);
                }
                for (int n = 0; n < 220; n++) scans.add("sku12");
                analytics.deliverAll("tx-first", scans);
                awaitWindow(analytics, 1000);
                analytics.deliverAll("tx-next", Collections.nCopies(123, "sku1"));

                Map<String, Object> report = get(client, base);
                assertEquals(Set.of("windowSize", "slideInterval", "windowStart", "windowEnd", "computedAt", "items"), report.keySet());
                assertEquals(1000, number(report, "windowSize"));
                assertEquals(500, number(report, "slideInterval"));
                assertEquals(1, number(report, "windowStart"));
                assertEquals(1000, number(report, "windowEnd"));
                assertEquals(analytics.windows.readLatest(10).orElseThrow().computedAt(), Instant.parse((String) report.get("computedAt")));
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
                Map<String, Object> whileNextWindowIsInFlight = get(client, base);
                assertEquals(report, whileNextWindowIsInFlight,
                        "Queries must expose only the latest committed complete window");
                assertEquals(1000, number(whileNextWindowIsInFlight, "windowEnd"));
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

    @Test
    void publicPortPreservesPopularItemsShapeLimitAndErrors() throws Exception {
        String publicBase = System.getProperty("SERVER_BASE_URL", System.getenv("SERVER_BASE_URL"));
        org.junit.jupiter.api.Assumptions.assumeTrue(publicBase != null && !publicBase.isBlank(),
                "public router not configured");
        assertEquals(8080, URI.create(publicBase).getPort(), "contract checks must use the public port");
        String endpoint = publicBase + "/analytics/popular-items";
        try (HttpClient client = HttpClient.newHttpClient()) {
            Map<String, Object> report = get(client, endpoint);
            assertEquals(Set.of("windowSize", "slideInterval", "windowStart", "windowEnd", "computedAt", "items"),
                    report.keySet());
            assertEquals(1000, number(report, "windowSize"));
            assertEquals(500, number(report, "slideInterval"));
            Instant.parse((String) report.get("computedAt"));
            List<Object> items = Json.getList(report, "items");
            assertTrue(items.size() <= 10);
            assertEquals(items.subList(0, Math.min(3, items.size())),
                    Json.getList(get(client, endpoint + "?limit=3"), "items"));
            for (int index = 0; index < items.size(); index++) {
                Map<String, Object> item = Json.asObject(items.get(index));
                assertEquals(index + 1, number(item, "rank"));
            }
            assertEquals(400, request(client, endpoint + "?limit=-1", "GET").statusCode());
            assertEquals(404, request(client, endpoint + "/extra", "GET").statusCode());
            assertEquals(405, request(client, endpoint, "POST").statusCode());
        }
    }

    private static void awaitWindow(InProcessAnalytics analytics, long end) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (analytics.windows.readMaxWindowEnd() < end) {
            if (System.nanoTime() >= deadline) throw new AssertionError("Checkpoint did not persist: " + end);
            Thread.sleep(10);
        }
    }

    private static Map<String, Object> get(HttpClient client, String url) throws Exception {
        HttpResponse<String> response = request(client, url, "GET");
        assertEquals(200, response.statusCode(), response.body());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
        return Json.parseObject(response.body());
    }

    private static PopularWindowStore.PopularWindowSnapshot snapshot(
            long start, long end, String sku, long count) {
        return new PopularWindowStore.PopularWindowSnapshot(start, end,
                List.of(new PopularWindowStore.SnapshotRank(1, sku, count)));
    }

    private static HttpResponse<String> request(HttpClient client, String url, String method) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url)).timeout(java.time.Duration.ofSeconds(10))
            .method(method, HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static long number(Map<String, Object> row, String key) {
        return ((Number) row.get(key)).longValue();
    }
}
