package database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;

/** One-connection JDBC implementation of the atomic checkout commit. */
public final class JdbcCheckoutCompletionStore implements CheckoutCompletionStore {
    private final ConnectionPool pool;

    public JdbcCheckoutCompletionStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public CompletionResult completeAtomically(CompletionCommand command) {
        Connection connection = pool.borrow();
        boolean committed = false;
        try {
            connection.setAutoCommit(false);
            Instant completedAt = guardOpen(connection, command);
            if (completedAt == null) {
                connection.rollback();
                return new NotOpen();
            }
            insertLines(connection, command);
            for (CompletionLine line : command.lines()) {
                if (!decrement(connection, line)) {
                    connection.rollback();
                    return new InsufficientStock(line.sku());
                }
            }
            connection.commit();
            committed = true;
            return new Completed(completedAt);
        } catch (SQLException e) {
            rollback(connection, e);
            throw new StoreFailure("Failed to complete transaction", e);
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException resetFailure) {
                if (committed) {
                    System.err.println("[database] Failed to reset connection: " + resetFailure.getMessage());
                }
            }
            pool.release(connection);
        }
    }

    private static Instant guardOpen(Connection connection, CompletionCommand command) throws SQLException {
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE `transaction` SET status='COMPLETED',completed_at=NOW(3),total_amount=? " +
                        "WHERE transaction_id=? AND status='OPEN'")) {
            update.setBigDecimal(1, command.totalAmount());
            update.setString(2, command.transactionId());
            if (update.executeUpdate() == 0) return null;
        }
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT completed_at FROM `transaction` WHERE transaction_id=?")) {
            query.setString(1, command.transactionId());
            try (ResultSet results = query.executeQuery()) {
                if (!results.next()) throw new SQLException("Completed transaction disappeared");
                Timestamp timestamp = results.getTimestamp(1);
                if (timestamp == null) throw new SQLException("Completed transaction has no completion time");
                return timestamp.toInstant();
            }
        }
    }

    private static void insertLines(Connection connection, CompletionCommand command) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO transaction_line (transaction_id,sku,quantity,unit_price) VALUES (?,?,?,?)")) {
            for (CompletionLine line : command.lines()) {
                insert.setString(1, command.transactionId());
                insert.setString(2, line.sku());
                insert.setInt(3, line.quantity());
                insert.setBigDecimal(4, line.unitPrice());
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    private static boolean decrement(Connection connection, CompletionLine line) throws SQLException {
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE inventory SET stock_quantity=stock_quantity-? " +
                        "WHERE sku=? AND stock_quantity>=?")) {
            update.setInt(1, line.quantity());
            update.setString(2, line.sku());
            update.setInt(3, line.quantity());
            return update.executeUpdate() == 1;
        }
    }

    private static void rollback(Connection connection, SQLException original) {
        try { connection.rollback(); } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
    }
}
