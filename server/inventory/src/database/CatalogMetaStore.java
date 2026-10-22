package database;

import java.util.Optional;

/** Read-only boundary for the {@code catalog_meta} singleton that versions the catalog (R4, R8). */
public interface CatalogMetaStore {

    /** The current catalog version, or empty when the catalog was never seeded. */
    Optional<CatalogMeta> read() throws StoreFailure;

    /** A new {@code catalogVersion} UUID is written by every reset or reseed. */
    record CatalogMeta(String catalogVersion, int productCount) {
        public CatalogMeta {
            if (catalogVersion == null || catalogVersion.isBlank()) {
                throw new IllegalArgumentException("catalogVersion must not be blank");
            }
            if (productCount < 0) {
                throw new IllegalArgumentException("productCount must be non-negative");
            }
        }
    }
}
