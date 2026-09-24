package transaction;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** API-facing low-stock query contract. */
public interface InventoryOperations {

    LowStockView listLowStock(Integer thresholdOverride);

    record LowStockView(int threshold, Instant generatedAt, List<LowStockItemView> alerts) {
        public LowStockView {
            if (threshold < 0) {
                throw new IllegalArgumentException("threshold must be non-negative");
            }
            Objects.requireNonNull(generatedAt, "generatedAt must not be null");
            alerts = List.copyOf(Objects.requireNonNull(alerts, "alerts must not be null"));
        }
    }

    record LowStockItemView(
            String sku,
            String name,
            int currentStock,
            int threshold,
            Instant triggeredAt) {

        public LowStockItemView {
            sku = requiredText(sku, 20, "sku");
            name = requiredText(name, 100, "name");
            if (currentStock < 0) {
                throw new IllegalArgumentException("currentStock must be non-negative");
            }
            if (threshold < 0) {
                throw new IllegalArgumentException("threshold must be non-negative");
            }
            if (currentStock > threshold) {
                throw new IllegalArgumentException("a low-stock alert must be at or below its threshold");
            }
            Objects.requireNonNull(triggeredAt, "triggeredAt must not be null");
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
