package database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** JDBC adapter for durable transaction creation and lookup. */
public final class JdbcTransactionStore implements TransactionStore {
    private final ConnectionPool pool;

    public JdbcTransactionStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public void insertOpen(String transactionId, String stationId) {
        Connection connection = pool.borrow();
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO `transaction` (transaction_id, station_id, status, total_amount, started_at) " +
                        "VALUES (?, ?, 'OPEN', 0.00, NOW(3))")) {
            statement.setString(1, transactionId);
            statement.setString(2, stationId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new StoreFailure("Failed to create transaction", e);
        } finally {
            pool.release(connection);
        }
    }

    @Override
    public Optional<TransactionRecord> findById(String transactionId) {
        Connection connection = pool.borrow();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT t.station_id, t.status, t.total_amount, t.started_at, t.completed_at, " +
                        "COALESCE(SUM(tl.quantity), 0) AS item_count " +
                        "FROM `transaction` t LEFT JOIN transaction_line tl " +
                        "ON tl.transaction_id=t.transaction_id WHERE t.transaction_id=? " +
                        "GROUP BY t.transaction_id,t.station_id,t.status,t.total_amount,t.started_at,t.completed_at")) {
            statement.setString(1, transactionId);
            try (ResultSet results = statement.executeQuery()) {
                if (!results.next()) return Optional.empty();
                Timestamp completed = results.getTimestamp("completed_at");
                return Optional.of(new TransactionRecord(transactionId,
                        results.getString("station_id"),
                        TransactionStatus.valueOf(results.getString("status")),
                        results.getBigDecimal("total_amount"),
                        instant(results.getTimestamp("started_at")),
                        completed == null ? null : completed.toInstant(),
                        results.getInt("item_count")));
            }
        } catch (SQLException e) {
            throw new StoreFailure("Failed to read transaction", e);
        } finally {
            pool.release(connection);
        }
    }

    private static Instant instant(Timestamp value) {
        if (value == null) throw new StoreFailure("Transaction is missing started_at");
        return value.toInstant();
    }
}
