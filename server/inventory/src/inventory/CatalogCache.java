package inventory;

import database.CatalogMetaStore;
import database.CatalogStore;
import database.StoreFailure;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Inventory's in-memory catalog snapshot and its version (R8). It serves {@code GetCatalog} and
 * {@code GET /items}; {@link #currentVersion()} re-reads {@code catalog_meta} and reloads the snapshot
 * when the version changed. Stock is never cached here.
 */
public final class CatalogCache {
    private static final int LOAD_ATTEMPTS = 5;

    private final CatalogStore catalog;
    private final CatalogMetaStore meta;
    private volatile Snapshot snapshot;

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

    /** Reads the stored version, reloading the snapshot first when it differs from the cached one. */
    public Version currentVersion() {
        Optional<CatalogMetaStore.CatalogMeta> current = meta.read();
        String version = current.map(CatalogMetaStore.CatalogMeta::catalogVersion).orElse(null);
        if (!Objects.equals(version, snapshot.version())) reload(version);
        return new Version(version, current.map(CatalogMetaStore.CatalogMeta::productCount).orElse(0));
    }

    private synchronized void reload(String seenVersion) {
        if (Objects.equals(seenVersion, snapshot.version())) return;
        snapshot = load();
    }

    /** Reads the version before and after the catalog, so a snapshot and its version come from one load. */
    private Snapshot load() {
        for (int attempt = 1; attempt <= LOAD_ATTEMPTS; attempt++) {
            Optional<CatalogMetaStore.CatalogMeta> before = meta.read();
            List<CatalogOperations.CatalogItemView> products = catalog.loadAll().stream()
                    .map(item -> new CatalogOperations.CatalogItemView(item.sku(), item.name(), item.price()))
                    .toList();
            if (before.equals(meta.read())) {
                return new Snapshot(before.map(CatalogMetaStore.CatalogMeta::catalogVersion).orElse(null), products);
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

    /** The stored catalog version and product count; {@code catalogVersion} is null when never seeded. */
    public record Version(String catalogVersion, int productCount) { }
}
