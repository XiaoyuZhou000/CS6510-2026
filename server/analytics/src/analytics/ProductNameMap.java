package analytics;

import database.ScanIngestStore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Analytics' in-memory copy of {@code product_name} (R12; data-model.md "Product name map"). It decides
 * which names an ingestion batch must write, so a warm service writes none, and it changes only after the
 * ingestion that wrote a name has committed. It is an optimization only: a lost or stale map causes at
 * most an idempotent rewrite of a name.
 */
public final class ProductNameMap {
    public static final int MAX_NAMES = 2_000;

    private final ConcurrentHashMap<String, String> names = new ConcurrentHashMap<>();

    /** Loads the stored names at startup (at most {@value #MAX_NAMES}). */
    public void loadFrom(ScanIngestStore store) {
        Map<String, String> stored = store.readProductNames(MAX_NAMES);
        names.clear();
        names.putAll(stored);
    }

    /**
     * The names of a batch that are absent from, or differ in, this map, keyed by SKU. When one SKU
     * appears with several names, the last one in batch order wins.
     */
    public Map<String, String> diff(Iterable<ScanIngestStore.ScanEvent> events) {
        Map<String, String> latest = new LinkedHashMap<>();
        for (ScanIngestStore.ScanEvent event : events) latest.put(event.sku(), event.productName());
        Map<String, String> changed = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : latest.entrySet()) {
            if (!entry.getValue().equals(names.get(entry.getKey()))) changed.put(entry.getKey(), entry.getValue());
        }
        return changed;
    }

    /** Records names that a committed ingestion wrote. */
    public void apply(Map<String, String> committed) {
        Objects.requireNonNull(committed, "committed");
        names.putAll(committed);
    }

    public String name(String sku) {
        return names.get(sku);
    }

    public int size() {
        return names.size();
    }
}
