package support;

import catalog.CatalogCache;
import persistence.CatalogDao;
import persistence.ConnectionPool;
import persistence.PopularWindowDao;

import java.sql.*;
import java.util.UUID;

/** Real MySQL fixture. Creates and removes only its uniquely named test database. */
public final class AnalyticsDatabase implements AutoCloseable {
    private final String name = "analytics_test_" + UUID.randomUUID().toString().replace("-", "");
    private final Connection admin;
    public final ConnectionPool pool;
    public final PopularWindowDao windows;
    public final CatalogCache catalog;

    public AnalyticsDatabase() throws Exception {
        String url = System.getProperty("DB_URL",
            "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
        String user = System.getProperty("DB_USER", "root");
        String password = System.getProperty("DB_PASS", "");
        admin = DriverManager.getConnection(url, user, password);
        try (Statement sql = admin.createStatement()) {
            sql.executeUpdate("CREATE DATABASE " + name);
            for (String table : new String[]{"catalog_item", "popular_window", "popular_item"}) {
                sql.executeUpdate("CREATE TABLE " + name + "." + table + " LIKE " + table);
            }
            for (int i = 1; i <= 12; i++) {
                sql.executeUpdate("INSERT INTO " + name + ".catalog_item VALUES ('sku" + i + "', 'Item " + i + "', 1.00)");
            }
        }
        java.net.URI uri = java.net.URI.create(url.substring(5));
        pool = new ConnectionPool(uri.getHost(), uri.getPort() < 0 ? 3306 : uri.getPort(), name, user, password, 2);
        windows = new PopularWindowDao(pool);
        catalog = CatalogCache.load(new CatalogDao(pool));
    }

    public void awaitWindow(long end) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (windows.readMaxWindowEnd() < end) {
            if (System.nanoTime() >= deadline) throw new AssertionError("Checkpoint did not persist: " + end);
            Thread.sleep(10);
        }
    }

    @Override
    public void close() throws Exception {
        for (int i = 0; i < 2; i++) pool.borrow().close();
        try (admin; Statement sql = admin.createStatement()) {
            sql.executeUpdate("DROP DATABASE " + name);
        }
    }
}
