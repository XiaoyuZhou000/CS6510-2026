package unit.api;

import api.CatalogHttpHandler;
import api.InventoryHttpHandler;
import database.StoreFailure;
import org.junit.jupiter.api.Test;
import transaction.CatalogOperations;
import transaction.InventoryOperations;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StoreQueryHttpHandlerTest {
    @Test
    void catalogResponseIsTranslatedUsingOnlyTheBusinessContract() throws Exception {
        CatalogOperations catalog = () -> List.of(
                new CatalogOperations.CatalogItemView("B", "Bread", new BigDecimal("2.50")),
                new CatalogOperations.CatalogItemView("A", "Apple", new BigDecimal("1.25")));
        TestExchange exchange = TestExchange.request("GET", "/items", "");

        new CatalogHttpHandler(catalog).handle(exchange);

        assertEquals(200, exchange.status());
        List<?> items = (List<?>) exchange.json().get("items");
        assertEquals(2, items.size());
        assertEquals("B", ((java.util.Map<?, ?>) items.getFirst()).get("sku"));
    }

    @Test
    void inventoryPassesAbsentOrExplicitThresholdAndSerializesAlerts() throws Exception {
        RecordingInventory inventory = new RecordingInventory();
        InventoryHttpHandler handler = new InventoryHttpHandler(inventory);

        TestExchange defaults = TestExchange.request("GET", "/inventory/low-stock", "");
        handler.handle(defaults);
        TestExchange override = TestExchange.request(
                "GET", "/inventory/low-stock?threshold=3", "");
        handler.handle(override);

        assertEquals(Arrays.asList(null, 3), inventory.thresholds);
        assertEquals(200, override.status());
        assertEquals(3, ((Number) override.json().get("threshold")).intValue());
        List<?> alerts = (List<?>) override.json().get("alerts");
        assertEquals("A", ((java.util.Map<?, ?>) alerts.getFirst()).get("sku"));
    }

    @Test
    void invalidThresholdAndMethodMismatchNeverReachInventory() throws Exception {
        RecordingInventory inventory = new RecordingInventory();
        InventoryHttpHandler handler = new InventoryHttpHandler(inventory);

        for (String query : List.of("-1", "abc", "")) {
            TestExchange invalid = TestExchange.request(
                    "GET", "/inventory/low-stock?threshold=" + query, "");
            handler.handle(invalid);
            assertEquals(400, invalid.status());
            assertEquals("INVALID_THRESHOLD", invalid.json().get("error"));
        }
        TestExchange wrongMethod = TestExchange.request("POST", "/inventory/low-stock", "");
        handler.handle(wrongMethod);

        assertEquals(405, wrongMethod.status());
        assertTrue(inventory.thresholds.isEmpty());
    }

    @Test
    void storeFailureIsSanitizedAtTheApiBoundary() throws Exception {
        CatalogOperations catalog = () -> { throw new StoreFailure("SQL host and credentials"); };
        TestExchange exchange = TestExchange.request("GET", "/items", "");

        new CatalogHttpHandler(catalog).handle(exchange);

        assertEquals(500, exchange.status());
        assertEquals("INTERNAL_ERROR", exchange.json().get("error"));
        assertFalse(exchange.body().contains("credentials"));
    }

    private static final class RecordingInventory implements InventoryOperations {
        private final List<Integer> thresholds = new ArrayList<>();

        @Override
        public LowStockView listLowStock(Integer thresholdOverride) {
            thresholds.add(thresholdOverride);
            int threshold = thresholdOverride == null ? 50 : thresholdOverride;
            Instant generated = Instant.parse("2026-09-23T12:00:00Z");
            return new LowStockView(threshold, generated, List.of(
                    new LowStockItemView("A", "Apple", 2, threshold, generated)));
        }
    }
}
