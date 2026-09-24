package transaction;

import database.CatalogStore;
import database.InventoryStore;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Business-layer catalog and committed-inventory read behavior. */
public final class StoreQueryService implements CatalogOperations, InventoryOperations {
    private final CatalogCache catalog;
    private final InventoryStore inventory;

    public StoreQueryService(CatalogStore catalog, InventoryStore inventory) {
        this(CatalogCache.load(Objects.requireNonNull(catalog, "catalog")), inventory);
    }

    public StoreQueryService(CatalogCache catalog, InventoryStore inventory) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.inventory = Objects.requireNonNull(inventory, "inventory");
    }

    @Override
    public List<CatalogItemView> listItems() {
        return catalog.items().stream()
                .map(item -> new CatalogItemView(item.sku(), item.name(), item.price()))
                .toList();
    }

    @Override
    public LowStockView listLowStock(Integer thresholdOverride) {
        if (thresholdOverride != null && thresholdOverride < 0) {
            throw new IllegalArgumentException("thresholdOverride must be non-negative");
        }
        int responseThreshold = LowStockThresholds.resolve(thresholdOverride);
        Instant generatedAt = Instant.now();
        List<LowStockItemView> alerts = inventory.findLowStock(thresholdOverride).stream()
                .map(row -> new LowStockItemView(row.sku(), row.name(), row.currentStock(),
                        row.threshold(), generatedAt))
                .toList();
        return new LowStockView(responseThreshold, generatedAt, alerts);
    }
}
