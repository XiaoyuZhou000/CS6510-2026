package database;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Transaction's durable copy of Inventory's catalog, replaced whole when the version changes (R4). */
public interface ProductReplicaStore {

    /** The stored replica, or empty when it was never loaded (a NULL meta version). */
    Optional<Replica> load() throws StoreFailure;

    /** Replaces every product row and the meta row in one local transaction. */
    void replace(String catalogVersion, List<ReplicaProduct> products) throws StoreFailure;

    record Replica(String catalogVersion, List<ReplicaProduct> products) {
        public Replica {
            Objects.requireNonNull(catalogVersion, "catalogVersion");
            products = List.copyOf(Objects.requireNonNull(products, "products"));
        }
    }

    /** One stored product; the column types are the constraint, so values pass through unvalidated. */
    record ReplicaProduct(String sku, String name, BigDecimal price) {
        public ReplicaProduct {
            Objects.requireNonNull(sku, "sku");
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(price, "price");
        }
    }
}
