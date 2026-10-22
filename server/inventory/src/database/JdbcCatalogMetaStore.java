package database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/** JDBC reader for the {@code catalog_meta} singleton: one primary-key read. */
public final class JdbcCatalogMetaStore implements CatalogMetaStore {
    private final ConnectionPool pool;

    public JdbcCatalogMetaStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public Optional<CatalogMeta> read() {
        Connection connection = pool.borrow();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT catalog_version, product_count FROM catalog_meta WHERE meta_key = 1");
             ResultSet results = statement.executeQuery()) {
            if (!results.next()) return Optional.empty();
            return Optional.of(new CatalogMeta(results.getString(1), results.getInt(2)));
        } catch (SQLException e) {
            try {
                JdbcUnitOfWork.abort(connection);
            } catch (SQLException rollback) {
                e.addSuppressed(rollback);
            }
            throw new StoreFailure("Failed to read catalog version", e);
        } finally {
            pool.release(connection);
        }
    }
}
