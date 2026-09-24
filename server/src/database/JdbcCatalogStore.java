package database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** JDBC catalog reader used by the layered composition root. */
public final class JdbcCatalogStore implements CatalogStore {
    private final ConnectionPool pool;

    public JdbcCatalogStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public List<CatalogItem> loadAll() {
        Connection connection = pool.borrow();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT sku,name,price FROM catalog_item ORDER BY sku");
             ResultSet results = statement.executeQuery()) {
            List<CatalogItem> items = new ArrayList<>();
            while (results.next()) {
                items.add(new CatalogItem(results.getString("sku"), results.getString("name"),
                        results.getBigDecimal("price")));
            }
            return List.copyOf(items);
        } catch (SQLException e) {
            throw new StoreFailure("Failed to load catalog", e);
        } finally {
            pool.release(connection);
        }
    }
}
