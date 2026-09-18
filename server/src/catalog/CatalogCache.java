package catalog;

import persistence.CatalogDao;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class CatalogCache {

    public record CatalogItem(String sku, String name, BigDecimal price) {}

    private final Map<String, CatalogItem> bySku;

    private CatalogCache(Map<String, CatalogItem> bySku) {
        this.bySku = Collections.unmodifiableMap(bySku);
    }

    /**
     * Loads the entire catalog from the database once at startup.
     * The catalog is immutable during a run; this is the only load ever done.
     */
    public static CatalogCache load(CatalogDao dao) throws SQLException {
        Map<String, CatalogItem> map = new LinkedHashMap<>();
        for (CatalogDao.CatalogRow row : dao.loadAll()) {
            map.put(row.sku(), new CatalogItem(row.sku(), row.name(), row.price()));
        }
        return new CatalogCache(map);
    }

    /** Returns the CatalogItem for the given SKU, or null if not found. */
    public CatalogItem get(String sku) {
        return bySku.get(sku);
    }

    /** Returns all items in insertion order. */
    public Collection<CatalogItem> all() {
        return bySku.values();
    }

    public int size() {
        return bySku.size();
    }
}
