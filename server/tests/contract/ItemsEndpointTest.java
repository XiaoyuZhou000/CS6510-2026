package contract;

import json.Json;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Contract test for GET /items (FR-001, FR-002; US1 acceptance scenarios 1–2).
 *
 * Requires the server to be running. Override the base URL via the
 * SERVER_BASE_URL system property (default: http://localhost:8080).
 */
public class ItemsEndpointTest {

    private static final String BASE_URL =
        System.getProperty("SERVER_BASE_URL", "http://localhost:8080");

    private static final int EXPECTED_CATALOG_SIZE = 2000;

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
            // Any other response (even 500) means the server is up; proceed.
        }
    }

    @Test
    void getItems_returnsOkWithCorrectShape() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/items"))
            .GET()
            .build();

        HttpResponse<String> response =
            client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode(),
            "GET /items should return 200 OK");

        String contentType = response.headers().firstValue("Content-Type").orElse("");
        assertTrue(contentType.contains("application/json"),
            "Content-Type should be application/json, got: " + contentType);
    }

    @Test
    void getItems_everyItemHasSkuNameAndPrice() throws Exception {
        List<Object> items = fetchItems();

        assertFalse(items.isEmpty(), "Catalog must not be empty");

        for (Object raw : items) {
            @SuppressWarnings("unchecked")
            Map<String, Object> item = (Map<String, Object>) raw;

            assertNotNull(item.get("sku"),   "Each item must have a 'sku' field");
            assertNotNull(item.get("name"),  "Each item must have a 'name' field");
            assertNotNull(item.get("price"), "Each item must have a 'price' field");

            String sku = item.get("sku").toString();
            assertFalse(sku.isBlank(), "SKU must not be blank");

            String name = item.get("name").toString();
            assertFalse(name.isBlank(), "Name must not be blank");

            double price = ((Number) item.get("price")).doubleValue();
            assertTrue(price >= 0, "Price must be non-negative, got: " + price);
        }
    }

    @Test
    void getItems_allSeedCatalogItemsPresentWithNoDuplicates() throws Exception {
        List<Object> items = fetchItems();

        assertEquals(EXPECTED_CATALOG_SIZE, items.size(),
            "Expected exactly " + EXPECTED_CATALOG_SIZE + " catalog items, got: " + items.size());

        Set<String> skus = new HashSet<>();
        for (Object raw : items) {
            @SuppressWarnings("unchecked")
            Map<String, Object> item = (Map<String, Object>) raw;
            String sku = item.get("sku").toString();
            assertTrue(skus.add(sku),
                "Duplicate SKU detected: " + sku);
        }

        assertEquals(EXPECTED_CATALOG_SIZE, skus.size(),
            "All " + EXPECTED_CATALOG_SIZE + " SKUs must be unique");
    }

    private List<Object> fetchItems() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/items"))
            .GET()
            .build();

        HttpResponse<String> response =
            client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode(), "Expected 200 from GET /items");

        Map<String, Object> body = Json.parseObject(response.body());
        assertNotNull(body.get("items"), "Response body must contain 'items' array");
        return Json.getList(body, "items");
    }
}
