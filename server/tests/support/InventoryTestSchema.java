package support;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

/** Disposable Inventory schema ({@code inv_test_<uuid>}) from {@code db/inventory/schema.sql}. */
public final class InventoryTestSchema extends TestSchema {
    public static final int DEFAULT_THRESHOLD = 50;

    private InventoryTestSchema(Options options) throws Exception {
        super("inv", "inventory", options);
    }

    public static InventoryTestSchema create() throws Exception {
        return new InventoryTestSchema(Options.defaults());
    }

    public static InventoryTestSchema create(Options options) throws Exception {
        return new InventoryTestSchema(options);
    }

    /** The seed's SKU for product {@code n}: {@code SKU-000001}... */
    public static String sku(int n) {
        return String.format("SKU-%06d", n);
    }

    /** The seed's price for product {@code n}: {@code round(0.5 + (n mod 47) * 0.35, 2)}. */
    public static BigDecimal price(int n) {
        return new BigDecimal("0.5").add(new BigDecimal("0.35").multiply(BigDecimal.valueOf(n % 47)))
                .setScale(2, RoundingMode.HALF_UP);
    }

    /**
     * Seeds {@code products} deterministic products ({@code SKU-000001}.., "Item n", the seed's price),
     * each with {@code stock} units and threshold 50, plus a new {@code catalog_version}.
     */
    public String seed(int products, int stock) throws SQLException {
        try (PreparedStatement catalog = admin().prepareStatement(
                "INSERT INTO catalog_item (sku, name, price) VALUES (?, ?, ?)");
             PreparedStatement inventory = admin().prepareStatement(
                     "INSERT INTO inventory (sku, stock_quantity, low_stock_threshold) VALUES (?, ?, ?)")) {
            for (int n = 1; n <= products; n++) {
                catalog.setString(1, sku(n));
                catalog.setString(2, "Item " + n);
                catalog.setBigDecimal(3, price(n));
                catalog.addBatch();
                inventory.setString(1, sku(n));
                inventory.setInt(2, stock);
                inventory.setInt(3, DEFAULT_THRESHOLD);
                inventory.addBatch();
            }
            catalog.executeBatch();
            inventory.executeBatch();
        }
        return newCatalogVersion();
    }

    /** Adds one product with its own name, price, stock, and threshold, and a new catalog version. */
    public String addProduct(String sku, String name, BigDecimal price, int stock, int threshold) throws SQLException {
        update("INSERT INTO catalog_item (sku, name, price) VALUES (?, ?, ?)", sku, name, price);
        update("INSERT INTO inventory (sku, stock_quantity, low_stock_threshold) VALUES (?, ?, ?)", sku, stock, threshold);
        return newCatalogVersion();
    }

    /** Writes a fresh {@code catalog_version} UUID and the current product count, as a reseed does. */
    public String newCatalogVersion() throws SQLException {
        String version = UUID.randomUUID().toString();
        update("INSERT INTO catalog_meta (meta_key, catalog_version, product_count, seeded_at) "
                + "SELECT 1, ?, COUNT(*), NOW(3) FROM catalog_item "
                + "ON DUPLICATE KEY UPDATE catalog_version = VALUES(catalog_version), "
                + "product_count = VALUES(product_count), seeded_at = VALUES(seeded_at)", version);
        return version;
    }

    public String catalogVersion() throws SQLException {
        return queryString("SELECT catalog_version FROM catalog_meta WHERE meta_key = 1");
    }

    public int stock(String sku) throws SQLException {
        return (int) queryLong("SELECT stock_quantity FROM inventory WHERE sku = ?", sku);
    }

    public void setStock(String sku, int stock) throws SQLException {
        if (update("UPDATE inventory SET stock_quantity = ? WHERE sku = ?", stock, sku) != 1) {
            throw new IllegalArgumentException("Unknown SKU " + sku);
        }
    }

    public long totalUnits() throws SQLException {
        return queryLong("SELECT COALESCE(SUM(stock_quantity), 0) FROM inventory");
    }

    public long operationCount(String state) throws SQLException {
        return queryLong("SELECT COUNT(*) FROM checkout_operation WHERE state = ?", state);
    }
}
