package support;

import analytics.AnalyticsRecorder;
import analytics.AnalyticsWindowStore;
import catalog.CatalogCache;
import checkout.CheckoutService;
import persistence.CatalogDao;
import persistence.ConnectionPool;
import persistence.InventoryDao;
import persistence.PopularWindowDao;
import persistence.TransactionDao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

/** Isolated real-MySQL checkout fixture with a deliberately one-connection application pool. */
public final class CheckoutDatabase implements AutoCloseable {
    public static final String SKU = "race-sku";

    private final String name = "checkout_test_" + UUID.randomUUID().toString().replace("-", "");
    private final Connection admin;
    public final ConnectionPool pool;
    public final AnalyticsRecorder analytics;
    public final CheckoutService checkout;

    public CheckoutDatabase(int stock) throws Exception {
        String url = System.getProperty("DB_URL",
            "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
        String user = System.getProperty("DB_USER", "root");
        String password = System.getProperty("DB_PASS", "");
        admin = DriverManager.getConnection(url, user, password);
        try (Statement sql = admin.createStatement()) {
            sql.executeUpdate("CREATE DATABASE " + name);
            for (String table : new String[]{"catalog_item", "inventory", "transaction", "transaction_line"}) {
                sql.executeUpdate("CREATE TABLE " + name + ".`" + table + "` LIKE `" + table + "`");
            }
            sql.executeUpdate("INSERT INTO " + name + ".catalog_item VALUES ('" + SKU + "', 'Race Item', 2.50)");
            sql.executeUpdate("INSERT INTO " + name + ".inventory VALUES ('" + SKU + "', " + stock + ", 1)");
        }
        java.net.URI uri = java.net.URI.create(url.substring(5));
        pool = new ConnectionPool(uri.getHost(), uri.getPort() < 0 ? 3306 : uri.getPort(), name, user, password, 1);
        CatalogCache catalog = CatalogCache.load(new CatalogDao(pool));
        AnalyticsWindowStore noOpWindows = new AnalyticsWindowStore() {
            public long readMaxWindowEnd() { return 0; }
            public void writeWindow(long start, long end, List<PopularWindowDao.PopularEntry> entries) {}
        };
        analytics = new AnalyticsRecorder(sku -> "Race Item", noOpWindows);
        checkout = new CheckoutService(pool, new TransactionDao(pool), new InventoryDao(pool), catalog, analytics);
    }

    public int stock() throws Exception {
        try (PreparedStatement ps = admin.prepareStatement(
                "SELECT stock_quantity FROM " + name + ".inventory WHERE sku=?")) {
            ps.setString(1, SKU);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    public void setStock(int stock) throws Exception {
        try (PreparedStatement ps = admin.prepareStatement(
                "UPDATE " + name + ".inventory SET stock_quantity=? WHERE sku=?")) {
            ps.setInt(1, stock);
            ps.setString(2, SKU);
            ps.executeUpdate();
        }
    }

    @Override
    public void close() throws Exception {
        analytics.shutdown();
        pool.borrow().close();
        try (admin; Statement sql = admin.createStatement()) {
            sql.executeUpdate("DROP DATABASE " + name);
        }
    }
}
