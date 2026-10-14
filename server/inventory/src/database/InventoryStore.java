package database;

import java.util.List;

/** Read-only database boundary for committed inventory and low-stock projections. */
public interface InventoryStore {

    /**
     * Returns current committed stock at or below the applicable threshold in SKU order.
     * A non-null override applies only to this call and must never update the persisted per-SKU
     * threshold.
     */
    List<LowStockRecord> findLowStock(Integer thresholdOverride) throws StoreFailure;

    record LowStockRecord(String sku, String name, int currentStock, int threshold) {
        public LowStockRecord {
            sku = requiredText(sku, 20, "sku");
            name = requiredText(name, 100, "name");
            if (currentStock < 0) {
                throw new IllegalArgumentException("currentStock must be non-negative");
            }
            if (threshold < 0) {
                throw new IllegalArgumentException("threshold must be non-negative");
            }
        }
    }

    private static String requiredText(String value, int maximumLength, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        if (value.length() > maximumLength) {
            throw new IllegalArgumentException(field + " must be at most " + maximumLength + " characters");
        }
        return value;
    }
}
