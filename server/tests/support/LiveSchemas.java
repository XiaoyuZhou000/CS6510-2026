package support;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Connections to the running deployment's own schemas, each with that service's own credentials (R3),
 * for the live tests that run against {@code SERVER_BASE_URL}. Settings come from JVM properties, then
 * the environment: {@code DB_HOST}, {@code DB_PORT}, {@code <SERVICE>_DB_NAME}, {@code <SERVICE>_DB_USER},
 * and {@code <SERVICE>_DB_PASSWORD}. A missing password skips the test, which a strict live run reports.
 */
public final class LiveSchemas {
    private LiveSchemas() { }

    public static Connection inventory() throws SQLException {
        return connect("INVENTORY", "cs6510_inventory", "cs6510_inventory_svc");
    }

    public static Connection transaction() throws SQLException {
        return connect("TRANSACTION", "cs6510_transaction", "cs6510_transaction_svc");
    }

    /** One SKU's stock in Inventory's schema. */
    public static int stock(String sku) throws SQLException {
        try (Connection inventory = inventory();
             PreparedStatement select = inventory.prepareStatement("SELECT stock_quantity FROM inventory WHERE sku = ?")) {
            select.setString(1, sku);
            try (ResultSet row = select.executeQuery()) {
                assertTrue(row.next(), "SKU must exist in inventory: " + sku);
                return row.getInt(1);
            }
        }
    }

    public static void setStock(String sku, int stock) throws SQLException {
        try (Connection inventory = inventory();
             PreparedStatement update = inventory.prepareStatement("UPDATE inventory SET stock_quantity = ? WHERE sku = ?")) {
            update.setInt(1, stock);
            update.setString(2, sku);
            assertEquals(1, update.executeUpdate(), "SKU must exist in inventory: " + sku);
        }
    }

    private static Connection connect(String prefix, String schema, String user) throws SQLException {
        String password = setting(prefix + "_DB_PASSWORD", null);
        assumeTrue(password != null, prefix + "_DB_PASSWORD is needed to read " + schema + " with its own credentials");
        String url = "jdbc:mysql://" + setting("DB_HOST", "127.0.0.1") + ":" + setting("DB_PORT", "3307") + "/"
                + setting(prefix + "_DB_NAME", schema) + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        return DriverManager.getConnection(url, setting(prefix + "_DB_USER", user), password);
    }

    /** A JVM property, then the environment, then the fallback. */
    private static String setting(String key, String fallback) {
        String property = System.getProperty(key);
        if (property != null && !property.isBlank()) return property;
        String environment = System.getenv(key);
        return environment == null || environment.isBlank() ? fallback : environment;
    }
}
