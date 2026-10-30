package database;

import events.OutboxRow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC adapter for the {@code catalog_meta} singleton: one primary-key read, and the catalog publication
 * (006 D9), which is one batched {@code outbox_event} INSERT and the {@code published_at} UPDATE in one commit.
 */
public final class JdbcCatalogMetaStore implements CatalogMetaStore {
    private static final String MARK_PUBLISHED = "UPDATE catalog_meta SET published_at = ? WHERE meta_key = 1";

    private final ConnectionPool pool;

    public JdbcCatalogMetaStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public Optional<CatalogMeta> read() {
        Connection connection = pool.borrow();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT catalog_version, product_count, published_at FROM catalog_meta WHERE meta_key = 1");
             ResultSet results = statement.executeQuery()) {
            if (!results.next()) return Optional.empty();
            Timestamp published = results.getTimestamp(3);
            return Optional.of(new CatalogMeta(results.getString(1), results.getInt(2),
                    published == null ? null : published.toInstant()));
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

    @Override
    public void publish(List<OutboxRow> outboxRows, Instant publishedAt) {
        Objects.requireNonNull(publishedAt, "publishedAt");
        List<OutboxRow> rows = List.copyOf(outboxRows);
        JdbcUnitOfWork.inTransaction(pool, "inventory.catalog.publish", connection -> {
            JdbcOutboxStore.insertRows(connection, rows);
            markPublished(connection, publishedAt);
            return null;
        });
    }

    /** Sets {@code published_at} inside the caller's open unit of work. */
    public static void markPublished(Connection unitOfWork, Instant publishedAt) throws SQLException {
        try (PreparedStatement update = unitOfWork.prepareStatement(MARK_PUBLISHED)) {
            update.setTimestamp(1, Timestamp.from(publishedAt));
            if (update.executeUpdate() != 1) throw new SQLException("The catalog_meta singleton is missing");
        }
    }
}
