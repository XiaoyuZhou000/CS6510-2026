package unit.transaction;

import database.CatalogMetaStore;
import database.CatalogStore;
import database.InventoryStore;
import org.junit.jupiter.api.Test;
import inventory.CatalogCache;
import inventory.StoreQueryService;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class StoreQueryServiceTest {
    /** A fixed catalog version, so the cache never reloads; low stock still reads the store on every call. */
    private static final CatalogMetaStore META = () -> Optional.of(new CatalogMetaStore.CatalogMeta("v1", 2));

    @Test
    void mapsCatalogWithoutChangingStableStoreOrder() {
        AtomicInteger catalogReads = new AtomicInteger();
        CatalogStore catalog = () -> {
            catalogReads.incrementAndGet();
            return List.of(
                    new CatalogStore.CatalogItem("B", "Bread", new BigDecimal("2.50")),
                    new CatalogStore.CatalogItem("A", "Apple", new BigDecimal("1.25")));
        };
        RecordingInventoryStore inventory = new RecordingInventoryStore();

        StoreQueryService service = new StoreQueryService(new CatalogCache(catalog, META), inventory);

        assertEquals(List.of("B", "A"),
                service.listItems().stream().map(item -> item.sku()).toList());
        assertEquals(new BigDecimal("2.50"), service.listItems().getFirst().price());
        assertEquals(1, catalogReads.get(), "listItems is served from the catalog cache, loaded once");
    }

    @Test
    void resolvesDefaultsAndKeepsOverridesRequestLocal() {
        CatalogStore catalog = List::<CatalogStore.CatalogItem>of;
        RecordingInventoryStore inventory = new RecordingInventoryStore();
        StoreQueryService service = new StoreQueryService(new CatalogCache(catalog, META), inventory);

        var defaults = service.listLowStock(null);
        var override = service.listLowStock(3);
        var defaultsAgain = service.listLowStock(null);

        assertEquals(Arrays.asList(null, 3, null), inventory.requests);
        assertEquals(50, defaults.threshold());
        assertEquals(3, override.threshold());
        assertEquals(50, defaultsAgain.threshold());
        assertEquals(2, defaults.alerts().getFirst().currentStock());
        assertEquals(7, defaults.alerts().getFirst().threshold(),
                "the default response preserves each row's persisted threshold");
        assertEquals(3, override.alerts().getFirst().threshold(),
                "an override applies to this query only");
        assertEquals(defaults.generatedAt(), defaults.alerts().getFirst().triggeredAt());
    }

    @Test
    void rejectsNegativeBusinessThresholdsBeforeCallingTheStore() {
        RecordingInventoryStore inventory = new RecordingInventoryStore();
        StoreQueryService service = new StoreQueryService(
                new CatalogCache(List::<CatalogStore.CatalogItem>of, META), inventory);

        assertThrows(IllegalArgumentException.class, () -> service.listLowStock(-1));
        assertTrue(inventory.requests.isEmpty());
    }

    private static final class RecordingInventoryStore implements InventoryStore {
        private final List<Integer> requests = new ArrayList<>();

        @Override
        public List<LowStockRecord> findLowStock(Integer thresholdOverride) {
            requests.add(thresholdOverride);
            int threshold = thresholdOverride == null ? 7 : thresholdOverride;
            return List.of(new LowStockRecord("A", "Apple", 2, threshold));
        }
    }
}
