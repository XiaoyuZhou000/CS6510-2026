package database;

import events.OutboxRow;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Boundary for the {@code catalog_meta} singleton that versions the catalog (R4, R8), and for the startup
 * publication that records the catalog's events (006 D9).
 */
public interface CatalogMetaStore {

    /** The current catalog version, or empty when the catalog was never seeded. */
    Optional<CatalogMeta> read() throws StoreFailure;

    /**
     * Inserts {@code outboxRows} into {@code outbox_event}, in the order given, and sets
     * {@code catalog_meta.published_at}, in one local transaction.
     */
    void publish(List<OutboxRow> outboxRows, Instant publishedAt) throws StoreFailure;

    /**
     * A new {@code catalogVersion} UUID is written by every reset or reseed; {@code publishedAt} is null until
     * the version's events were recorded.
     */
    record CatalogMeta(String catalogVersion, int productCount, Instant publishedAt) {
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
