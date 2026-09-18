package persistence;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.util.List;

public final class TransactionDao {

    public record TransactionLine(String sku, int quantity, BigDecimal unitPrice) {}

    /**
     * Fetched result for a completed transaction (used by GET /transactions/{id} fallback).
     */
    public record TransactionRow(
        String transactionId, String stationId, String status,
        BigDecimal totalAmount, Instant startedAt, Instant completedAt,
        int itemCount
    ) {}

    private final ConnectionPool pool;

    public TransactionDao(ConnectionPool pool) {
        this.pool = pool;
    }

    /** Inserts a new OPEN transaction row. */
    public void insertOpen(String transactionId, String stationId) throws SQLException {
        Connection conn = pool.borrow();
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO `transaction` (transaction_id, station_id, status, total_amount, started_at) " +
                "VALUES (?, ?, 'OPEN', 0.00, NOW(3))")) {
            ps.setString(1, transactionId);
            ps.setString(2, stationId);
            ps.executeUpdate();
        } finally {
            pool.release(conn);
        }
    }

    /**
     * Within an already-open (autoCommit=false) connection, attempts to transition the
     * transaction from OPEN to COMPLETED and record the total amount.
     *
     * Returns the DB-written completed_at Instant if 1 row was affected,
     * or null if 0 rows (already completed/cancelled, or not found).
     */
    public Instant completeIfOpen(Connection conn, String transactionId, BigDecimal totalAmount)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE `transaction` SET status='COMPLETED', completed_at=NOW(3), total_amount=? " +
                "WHERE transaction_id=? AND status='OPEN'")) {
            ps.setBigDecimal(1, totalAmount);
            ps.setString(2, transactionId);
            if (ps.executeUpdate() == 0) return null;
        }
        // Read the DB-written completedAt within the same transaction
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT completed_at FROM `transaction` WHERE transaction_id=?")) {
            ps.setString(1, transactionId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Timestamp ts = rs.getTimestamp(1);
                    return ts != null ? ts.toInstant() : Instant.now();
                }
            }
        }
        return Instant.now();
    }

    /** Inserts one transaction_line row per entry in the list (within an existing connection). */
    public void insertLines(Connection conn, String transactionId, List<TransactionLine> lines)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO transaction_line (transaction_id, sku, quantity, unit_price) VALUES (?, ?, ?, ?)")) {
            for (TransactionLine line : lines) {
                ps.setString(1, transactionId);
                ps.setString(2, line.sku());
                ps.setInt(3, line.quantity());
                ps.setBigDecimal(4, line.unitPrice());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /**
     * Reads a completed transaction's metadata plus the aggregate item count from
     * transaction_line. Used by GET /transactions/{id} once a basket has been evicted.
     * Returns null if the transaction is not found.
     */
    public TransactionRow readCompleted(String transactionId) throws SQLException {
        Connection conn = pool.borrow();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT t.station_id, t.status, t.total_amount, t.started_at, t.completed_at, " +
                "       COALESCE(SUM(tl.quantity), 0) AS item_count " +
                "FROM `transaction` t " +
                "LEFT JOIN transaction_line tl ON tl.transaction_id = t.transaction_id " +
                "WHERE t.transaction_id = ? " +
                "GROUP BY t.transaction_id, t.station_id, t.status, t.total_amount, t.started_at, t.completed_at")) {
            ps.setString(1, transactionId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                Timestamp started = rs.getTimestamp("started_at");
                Timestamp completed = rs.getTimestamp("completed_at");
                return new TransactionRow(
                    transactionId,
                    rs.getString("station_id"),
                    rs.getString("status"),
                    rs.getBigDecimal("total_amount"),
                    started != null ? started.toInstant() : Instant.now(),
                    completed != null ? completed.toInstant() : null,
                    rs.getInt("item_count")
                );
            }
        } finally {
            pool.release(conn);
        }
    }
}
