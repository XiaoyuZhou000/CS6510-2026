package transaction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The immutable in-memory product snapshot that Scan prices from, with no SQL and no remote call (R4).
 * Its source of truth is {@code product_replica}: a new snapshot is installed only after the durable
 * replica committed it, and it is reloaded from there at startup.
 */
public final class ProductCatalogReplica {
    private volatile Snapshot snapshot;

    /** False until a snapshot is installed; Scan then answers 500 (FR-023). */
    public boolean usable() {
        return snapshot != null;
    }

    /** The installed catalog version, or null while unusable. */
    public String version() {
        Snapshot current = snapshot;
        return current == null ? null : current.version;
    }

    public int size() {
        Snapshot current = snapshot;
        return current == null ? 0 : current.products.size();
    }

    public Optional<Product> lookup(String sku) {
        Snapshot current = snapshot;
        return current == null ? Optional.empty() : Optional.ofNullable(current.products.get(sku));
    }

    /** Validates a snapshot without installing it, so a bad catalog never reaches the durable store either. */
    public static Snapshot snapshotOf(String version, List<Product> products) {
        if (version == null || version.isBlank()) throw new IllegalArgumentException("version must not be blank");
        Map<String, Product> bySku = new HashMap<>(Math.max(16, products.size() * 2));
        for (Product product : products) {
            Objects.requireNonNull(product, "products must not contain null");
            if (bySku.putIfAbsent(product.sku(), product) != null) {
                throw new IllegalArgumentException("Duplicate SKU in catalog snapshot: " + product.sku());
            }
        }
        return new Snapshot(version, Map.copyOf(bySku));
    }

    /** Atomically replaces the snapshot. */
    public void install(Snapshot next) {
        snapshot = Objects.requireNonNull(next, "next");
    }

    public void install(String version, List<Product> products) {
        install(snapshotOf(version, products));
    }

    /** A validated, immutable catalog version. */
    public static final class Snapshot {
        private final String version;
        private final Map<String, Product> products;

        private Snapshot(String version, Map<String, Product> products) {
            this.version = version;
            this.products = products;
        }

        public String version() {
            return version;
        }
    }

    public record Product(String sku, String name, BigDecimal price) {
        public Product {
            sku = requiredText(sku, 20, "sku");
            name = requiredText(name, 100, "name");
            Objects.requireNonNull(price, "price must not be null");
            try {
                price = price.setScale(2, RoundingMode.UNNECESSARY);
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException("price must have at most two decimal places", e);
            }
            if (price.signum() < 0 || price.precision() > 10) {
                throw new IllegalArgumentException("price must be a non-negative DECIMAL(10,2)");
            }
        }

        private static String requiredText(String value, int maximumLength, String field) {
            if (value == null || value.isBlank()) throw new IllegalArgumentException(field + " must not be blank");
            if (value.length() > maximumLength) {
                throw new IllegalArgumentException(field + " must be at most " + maximumLength + " characters");
            }
            return value;
        }
    }
}
