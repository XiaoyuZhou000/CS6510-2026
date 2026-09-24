package database;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;

/** Database boundary for the immutable catalog loaded at server startup. */
public interface CatalogStore {

    /**
     * Returns catalog rows in stable seed/load order.
     * Implementations must return one row per unique SKU.
     */
    List<CatalogItem> loadAll() throws StoreFailure;

    /** Catalog data whose price is captured by the transaction layer at scan time. */
    record CatalogItem(String sku, String name, BigDecimal price) {
        public CatalogItem {
            sku = requiredText(sku, 20, "sku");
            name = requiredText(name, 100, "name");
            price = decimal(price, 10, "price");
            if (price.signum() < 0) {
                throw new IllegalArgumentException("price must be non-negative");
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

    private static BigDecimal decimal(BigDecimal value, int precision, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        final BigDecimal scaled;
        try {
            scaled = value.setScale(2, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(field + " must have at most two decimal places", e);
        }
        if (scaled.precision() > precision) {
            throw new IllegalArgumentException(field + " exceeds DECIMAL(" + precision + ",2)");
        }
        return scaled;
    }
}
