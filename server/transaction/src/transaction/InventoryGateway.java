package transaction;

import java.util.List;
import java.util.Objects;

/**
 * Transaction's port to Inventory Service for catalog replication, until the catalog moves to events
 * (006 T110). The Scan path never calls it (FR-012), and checkout no longer does: the Saga runs over
 * domain events (D7). Failures without a definitive answer raise {@link DependencyUnavailableFailure}.
 */
public interface InventoryGateway {

    CatalogVersion catalogVersion();

    /** The complete catalog snapshot with the version it was loaded at, in ascending SKU order. */
    Catalog catalog();

    record CatalogVersion(String version, int productCount) {
        public CatalogVersion {
            if (version == null || version.isBlank()) throw new IllegalArgumentException("version must not be blank");
        }
    }

    record Catalog(String version, List<ProductCatalogReplica.Product> products) {
        public Catalog {
            if (version == null || version.isBlank()) throw new IllegalArgumentException("version must not be blank");
            products = List.copyOf(Objects.requireNonNull(products, "products"));
        }
    }
}
