package transaction;

import database.CatalogStore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Stable-order immutable catalog snapshot used for scan-time item and price capture. */
public final class CatalogCache {
    private final List<CatalogStore.CatalogItem> items;
    private final Map<String, CatalogStore.CatalogItem> bySku;

    private CatalogCache(List<CatalogStore.CatalogItem> items) {
        this.items = List.copyOf(items);
        Map<String, CatalogStore.CatalogItem> index = new LinkedHashMap<>();
        for (CatalogStore.CatalogItem item : items) {
            CatalogStore.CatalogItem previous = index.putIfAbsent(item.sku(), item);
            if (previous != null) throw new IllegalArgumentException("duplicate catalog SKU: " + item.sku());
        }
        this.bySku = Map.copyOf(index);
    }

    public static CatalogCache load(CatalogStore store) {
        Objects.requireNonNull(store, "store");
        return new CatalogCache(store.loadAll());
    }

    public CatalogStore.CatalogItem get(String sku) { return bySku.get(sku); }
    public List<CatalogStore.CatalogItem> items() { return items; }
    public int size() { return items.size(); }
}
