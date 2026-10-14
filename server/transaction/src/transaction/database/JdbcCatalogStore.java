package transaction.database;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/** Transaction-owned catalog validation within the caller's scan transaction. */
public final class JdbcCatalogStore {
    public Optional<CatalogItem> findBySku(Connection connection, String sku) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT name,price FROM catalog_item WHERE sku=?")) {
            statement.setString(1, sku);
            try (ResultSet results = statement.executeQuery()) {
                if (!results.next()) return Optional.empty();
                return Optional.of(new CatalogItem(results.getString(1), results.getBigDecimal(2)));
            }
        }
    }

    public record CatalogItem(String name, BigDecimal price) { }
}
