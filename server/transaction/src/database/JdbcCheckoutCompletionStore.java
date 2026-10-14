package database;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Locks the transaction first, then consumes committed lines and updates stock in SKU order. */
public final class JdbcCheckoutCompletionStore implements CheckoutCompletionStore {
    private final ConnectionPool pool;
    public JdbcCheckoutCompletionStore(ConnectionPool pool) { this.pool = Objects.requireNonNull(pool); }

    @Override
    public CompletionResult completeAtomically(String id) {
        Connection c = pool.borrow();
        try {
            c.setAutoCommit(false);
            try (PreparedStatement s = c.prepareStatement(
                    "SELECT status FROM `transaction` WHERE transaction_id=? FOR UPDATE")) {
                s.setString(1, id);
                try (ResultSet r = s.executeQuery()) {
                    if (!r.next()) { c.rollback(); return new MissingTransaction(); }
                    if (!"OPEN".equals(r.getString(1))) { c.rollback(); return new NotOpen(); }
                }
            }
            List<Line> lines = new ArrayList<>();
            try (PreparedStatement s = c.prepareStatement(
                    "SELECT sku,quantity FROM transaction_line WHERE transaction_id=? ORDER BY sku")) {
                s.setString(1, id);
                try (ResultSet r = s.executeQuery()) {
                    while (r.next()) lines.add(new Line(r.getString(1),r.getInt(2)));
                }
            }
            if (lines.isEmpty()) { c.rollback(); return new EmptyBasket(); }
            for (Line line : lines) {
                try (PreparedStatement s = c.prepareStatement(
                        "UPDATE inventory SET stock_quantity=stock_quantity-? " +
                        "WHERE sku=? AND stock_quantity>=?")) {
                    s.setInt(1, line.quantity);
                    s.setString(2, line.sku);
                    s.setInt(3, line.quantity);
                    if (s.executeUpdate() != 1) {
                        c.rollback();
                        return new InsufficientStock(line.sku);
                    }
                }
            }
            Instant completedAt;
            try (PreparedStatement s = c.prepareStatement(
                    "UPDATE `transaction` SET status='COMPLETED',completed_at=NOW(3) " +
                    "WHERE transaction_id=? AND status='OPEN'")) {
                s.setString(1, id);
                if (s.executeUpdate() != 1) throw new SQLException("OPEN transaction disappeared");
            }
            try (PreparedStatement s = c.prepareStatement(
                    "SELECT completed_at FROM `transaction` WHERE transaction_id=?")) {
                s.setString(1, id);
                try (ResultSet r = s.executeQuery()) {
                    if (!r.next()) throw new SQLException("Completed transaction disappeared");
                    completedAt = r.getTimestamp(1).toInstant();
                }
            }
            c.commit();
            return new Completed(completedAt);
        } catch (SQLException e) {
            try { c.rollback(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
            throw new StoreFailure("Failed to complete transaction", e);
        } catch (RuntimeException e) {
            try { c.rollback(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
            throw e;
        } finally {
            try { c.setAutoCommit(true); }
            catch (SQLException reset) { System.err.println("[transaction] Connection reset failed: " + reset); }
            pool.release(c);
        }
    }

    private record Line(String sku, int quantity) { }
}


