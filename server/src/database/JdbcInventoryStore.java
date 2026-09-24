package database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** JDBC reader for current committed low-stock projections. */
public final class JdbcInventoryStore implements InventoryStore {
    private static final String QUERY = """
            SELECT i.sku, c.name, i.stock_quantity,
                   CASE WHEN ? IS NULL THEN i.low_stock_threshold ELSE ? END AS threshold
            FROM inventory i
            JOIN catalog_item c ON c.sku = i.sku
            WHERE i.stock_quantity <=
                  CASE WHEN ? IS NULL THEN i.low_stock_threshold ELSE ? END
            ORDER BY i.sku
            """;

    private final ConnectionPool pool;

    public JdbcInventoryStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public List<LowStockRecord> findLowStock(Integer thresholdOverride) {
        if (thresholdOverride != null && thresholdOverride < 0) {
            throw new IllegalArgumentException("thresholdOverride must be non-negative");
        }
        Connection connection = pool.borrow();
        try (PreparedStatement statement = connection.prepareStatement(QUERY)) {
            for (int index = 1; index <= 4; index++) bind(statement, index, thresholdOverride);
            try (ResultSet results = statement.executeQuery()) {
                List<LowStockRecord> alerts = new ArrayList<>();
                while (results.next()) {
                    alerts.add(new LowStockRecord(results.getString("sku"), results.getString("name"),
                            results.getInt("stock_quantity"), results.getInt("threshold")));
                }
                return List.copyOf(alerts);
            }
        } catch (SQLException e) {
            throw new StoreFailure("Failed to read low-stock inventory", e);
        } finally {
            pool.release(connection);
        }
    }

    private static void bind(PreparedStatement statement, int index, Integer threshold)
            throws SQLException {
        if (threshold == null) statement.setNull(index, Types.INTEGER);
        else statement.setInt(index, threshold);
    }
}
