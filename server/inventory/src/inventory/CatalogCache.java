package inventory;

import database.CatalogMetaStore;
import database.CatalogStore;
import database.StoreFailure;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Inventory's in-memory catalog snapshot and its version (R8), loaded once at startup. It serves
 * {@code GET /items} and is what {@link CatalogPublisher} publishes (006 D9). The catalog changes only through
 * reset and reseed while Inventory is stopped, so no version check runs afterwards. Stock is never cached here.
 */
public final class CatalogCache {
    private static final int LOAD_ATTEMPTS = 5;

    private final CatalogStore catalog;
    private final CatalogMetaStore meta;
    private final Snapshot snapshot;

    /** Loads the snapshot once; a database failure here fails the caller's startup. */
    public CatalogCache(CatalogStore catalog, CatalogMetaStore meta) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.meta = Objects.requireNonNull(meta, "meta");
        this.snapshot = load();
    }

    /** The cached snapshot; it never reads the database. */
    public Snapshot snapshot() {
        return snapshot;
    }

    /** Reads the version before and after the catalog, so a snapshot and its version come from one load. */
    private Snapshot load() {
        for (int attempt = 1; attempt <= LOAD_ATTEMPTS; attempt++) {
            Optional<String> before = meta.read().map(CatalogMetaStore.CatalogMeta::catalogVersion);
            List<CatalogOperations.CatalogItemView> products = catalog.loadAll().stream()
                    .map(item -> new CatalogOperations.CatalogItemView(item.sku(), item.name(), item.price()))
                    .toList();
            if (before.equals(meta.read().map(CatalogMetaStore.CatalogMeta::catalogVersion))) {
                return new Snapshot(before.orElse(null), products);
            }
        }
        throw new StoreFailure("The catalog changed during " + LOAD_ATTEMPTS + " consecutive loads");
    }

    /** Products in the store's SKU order; {@code version} is null when the catalog was never seeded. */
    public record Snapshot(String version, List<CatalogOperations.CatalogItemView> products) {
        public Snapshot {
            products = List.copyOf(Objects.requireNonNull(products, "products"));
        }
    }
}
