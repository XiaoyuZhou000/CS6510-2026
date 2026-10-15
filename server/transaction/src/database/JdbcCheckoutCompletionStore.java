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
        long storeStart = System.nanoTime();
        Connection c = pool.borrow();
        ScanProfiler.record(ScanProfiler.Phase.COMPLETE_POOL, storeStart);
        boolean completed = false;
        try {
            long phaseStart = System.nanoTime();
            c.setAutoCommit(false);
            ScanProfiler.record(ScanProfiler.Phase.COMPLETE_BEGIN, phaseStart);
            phaseStart = System.nanoTime();
            try (PreparedStatement s = c.prepareStatement(
                    "SELECT status FROM `transaction` WHERE transaction_id=? FOR UPDATE")) {
                s.setString(1, id);
                try (ResultSet r = s.executeQuery()) {
                    if (!r.next()) { rollbackProfiled(c); return new MissingTransaction(); }
                    if (!"OPEN".equals(r.getString(1))) { rollbackProfiled(c); return new NotOpen(); }
                }
            }
            ScanProfiler.record(ScanProfiler.Phase.COMPLETE_LOCK, phaseStart);
            phaseStart = System.nanoTime();
            List<Line> lines = new ArrayList<>();
            try (PreparedStatement s = c.prepareStatement(
                    "SELECT sku,quantity FROM transaction_line WHERE transaction_id=? ORDER BY sku")) {
                s.setString(1, id);
                try (ResultSet r = s.executeQuery()) {
                    while (r.next()) lines.add(new Line(r.getString(1),r.getInt(2)));
                }
            }
            ScanProfiler.record(ScanProfiler.Phase.COMPLETE_LINES, phaseStart);
            if (lines.isEmpty()) { rollbackProfiled(c); return new EmptyBasket(); }
            phaseStart = System.nanoTime();
            for (Line line : lines) {
                try (PreparedStatement s = c.prepareStatement(
                        "UPDATE inventory SET stock_quantity=stock_quantity-? " +
                        "WHERE sku=? AND stock_quantity>=?")) {
                    s.setInt(1, line.quantity);
                    s.setString(2, line.sku);
                    s.setInt(3, line.quantity);
                    if (s.executeUpdate() != 1) {
                        ScanProfiler.record(ScanProfiler.Phase.COMPLETE_INVENTORY, phaseStart);
                        rollbackProfiled(c);
                        return new InsufficientStock(line.sku);
                    }
                }
            }
            ScanProfiler.record(ScanProfiler.Phase.COMPLETE_INVENTORY, phaseStart);
            phaseStart = System.nanoTime();
            Instant completedAt;
            try (PreparedStatement s = c.prepareStatement(
                    "UPDATE `transaction` SET status='COMPLETED',completed_at=NOW(3) " +
                    "WHERE transaction_id=? AND status='OPEN'")) {
                s.setString(1, id);
                if (s.executeUpdate() != 1) throw new SQLException("OPEN transaction disappeared");
            }
            ScanProfiler.record(ScanProfiler.Phase.COMPLETE_STATUS_UPDATE, phaseStart);
            phaseStart = System.nanoTime();
            try (PreparedStatement s = c.prepareStatement(
                    "SELECT completed_at FROM `transaction` WHERE transaction_id=?")) {
                s.setString(1, id);
                try (ResultSet r = s.executeQuery()) {
                    if (!r.next()) throw new SQLException("Completed transaction disappeared");
                    completedAt = r.getTimestamp(1).toInstant();
                }
            }
            ScanProfiler.record(ScanProfiler.Phase.COMPLETE_TIMESTAMP, phaseStart);
            phaseStart = System.nanoTime();
            c.commit();
            ScanProfiler.record(ScanProfiler.Phase.COMPLETE_COMMIT, phaseStart);
            completed = true;
            return new Completed(completedAt);
        } catch (SQLException e) {
            try { rollbackProfiled(c); } catch (SQLException rollback) { e.addSuppressed(rollback); }
            throw new StoreFailure("Failed to complete transaction", e);
        } catch (RuntimeException e) {
            try { rollbackProfiled(c); } catch (SQLException rollback) { e.addSuppressed(rollback); }
            throw e;
        } finally {
            long returnStart = System.nanoTime();
            try { c.setAutoCommit(true); }
            catch (SQLException reset) { System.err.println("[transaction] Connection reset failed: " + reset); }
            pool.release(c);
            ScanProfiler.record(ScanProfiler.Phase.COMPLETE_POOL_RETURN, returnStart);
            ScanProfiler.record(completed ? ScanProfiler.Phase.COMPLETE_SUCCESS_STORE
                    : ScanProfiler.Phase.COMPLETE_REJECTED_STORE, storeStart);
        }
    }

    private static void rollbackProfiled(Connection c) throws SQLException {
        long start = System.nanoTime();
        c.rollback();
        ScanProfiler.record(ScanProfiler.Phase.COMPLETE_ROLLBACK, start);
    }

    private record Line(String sku, int quantity) { }
}


