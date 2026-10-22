package support;

import database.ConnectionPool;
import database.JdbcPopularWindowStore;

import java.sql.*;
import java.util.UUID;

/** Real MySQL fixture. Creates and removes only its uniquely named test database. */
public final class AnalyticsDatabase implements AutoCloseable {
    private final String name = "analytics_test_" + UUID.randomUUID().toString().replace("-", "");
    private final Connection admin;
    public final ConnectionPool pool;
    public final JdbcPopularWindowStore windows;

    public AnalyticsDatabase() throws Exception {
        String url = System.getProperty("DB_URL",
            "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
        String user = System.getProperty("DB_USER", System.getenv().getOrDefault("DB_USER", "root"));
        String password = System.getProperty("DB_PASS", System.getenv().getOrDefault("DB_PASSWORD", ""));
        admin = DriverManager.getConnection(url, user, password);
        try (Statement sql = admin.createStatement()) {
            sql.executeUpdate("CREATE DATABASE " + name);
            for (String table : new String[]{"catalog_item", "popular_window", "popular_item"}) {
                sql.executeUpdate("CREATE TABLE " + name + "." + table + " LIKE " + table);
            }
            sql.executeUpdate("CREATE TABLE " + name + ".scan_sequence (sequence_key TINYINT PRIMARY KEY, last_position BIGINT NOT NULL)");
            sql.executeUpdate("CREATE TABLE " + name + ".accepted_scan (event_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,position BIGINT NULL UNIQUE KEY,transaction_id VARCHAR(40),sku VARCHAR(20),unit_price DECIMAL(10,2),accepted_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),KEY idx_scan_pending(position,event_id))");
            sql.executeUpdate("CREATE TABLE " + name + ".analytics_checkpoint (checkpoint_key TINYINT PRIMARY KEY, last_window_end BIGINT NOT NULL)");
            sql.executeUpdate("INSERT INTO " + name + ".scan_sequence VALUES (1, 0)");
            sql.executeUpdate("INSERT INTO " + name + ".analytics_checkpoint VALUES (1, 0)");
            for (int i = 1; i <= 12; i++) {
                sql.executeUpdate("INSERT INTO " + name + ".catalog_item VALUES ('sku" + i + "', 'Item " + i + "', 1.00)");
            }
        }
        java.net.URI uri = java.net.URI.create(url.substring(5));
        pool = new ConnectionPool(uri.getHost(), uri.getPort() < 0 ? 3306 : uri.getPort(), name, user, password, 4);
        windows = new JdbcPopularWindowStore(pool);
    }

    /** Add fixture events in one commit, using the same contiguous positions as production. */
    public void appendScans(java.util.List<String> skus) throws Exception {
        Connection connection = pool.borrow();
        try {
            connection.setAutoCommit(false);
            long position;
            try (Statement sql = connection.createStatement();
                 ResultSet row = sql.executeQuery(
                         "SELECT last_position FROM scan_sequence WHERE sequence_key=1 FOR UPDATE")) {
                if (!row.next()) throw new SQLException("scan sequence missing");
                position = row.getLong(1);
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO accepted_scan (position,transaction_id,sku,unit_price) VALUES (?,?,?,1.00)")) {
                for (String sku : skus) {
                    insert.setLong(1, ++position);
                    insert.setString(2, "fixture-tx");
                    insert.setString(3, sku);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE scan_sequence SET last_position=? WHERE sequence_key=1")) {
                update.setLong(1, position);
                update.executeUpdate();
            }
            connection.commit();
        } catch (Exception failure) {
            connection.rollback();
            throw failure;
        } finally {
            connection.setAutoCommit(true);
            pool.release(connection);
        }
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
