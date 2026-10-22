package database;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Committed checkout state; the timestamps it stores are supplied by the service, so nothing is read back. */
public final class JdbcTransactionStore implements TransactionStore {
    private static final int LOAD_ATTEMPTS = 3;
    private static final String ROW = "SELECT station_id, status, item_count, total_amount, attempt_count, "
            + "active_attempt_number, started_at, completed_at FROM `transaction` WHERE transaction_id = ?";
    private static final String LINES = "SELECT sku, COUNT(*), MIN(unit_price), MIN(product_name) "
            + "FROM accepted_scan WHERE transaction_id = ? GROUP BY sku ORDER BY sku";

    private final ConnectionPool pool;
    public JdbcTransactionStore(ConnectionPool pool) { this.pool = Objects.requireNonNull(pool); }

    @Override
    public void insertOpen(String transactionId, String stationId, Instant startedAt) {
        JdbcUnitOfWork.inTransaction(pool, "transaction.store.start", connection -> {
            try (PreparedStatement s = connection.prepareStatement(
                    "INSERT INTO `transaction` (transaction_id, station_id, status, item_count, total_amount, " +
                    "attempt_count, started_at) VALUES (?, ?, 'OPEN', 0, 0.00, 0, ?)")) {
                s.setString(1, transactionId);
                s.setString(2, stationId);
                s.setTimestamp(3, Timestamp.from(startedAt));
                s.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public Optional<TransactionRecord> findById(String id) {
        Connection c = pool.borrow();
        try (PreparedStatement s = c.prepareStatement(ROW)) {
            s.setString(1, id);
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? Optional.of(record(id, r)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw failed(c, "Failed to read transaction", e);
        } finally { pool.release(c); }
    }

    /**
     * Two reads on one connection. A Scan committed by another writer between them shows up as more
     * scan rows than {@code item_count}; the load then repeats, so it never returns a torn state.
     */
    @Override
    public Optional<TransactionState> loadState(String id) {
        Connection c = pool.borrow();
        try {
            for (int attempt = 1; attempt <= LOAD_ATTEMPTS; attempt++) {
                TransactionRecord row;
                int attemptCount;
                Integer activeAttempt;
                try (PreparedStatement s = c.prepareStatement(ROW)) {
                    s.setString(1, id);
                    try (ResultSet r = s.executeQuery()) {
                        if (!r.next()) return Optional.empty();
                        row = record(id, r);
                        attemptCount = r.getInt("attempt_count");
                        int active = r.getInt("active_attempt_number");
                        activeAttempt = r.wasNull() ? null : active;
                    }
                }
                List<TransactionLine> lines = new ArrayList<>();
                int units = 0;
                try (PreparedStatement s = c.prepareStatement(LINES)) {
                    s.setString(1, id);
                    try (ResultSet r = s.executeQuery()) {
                        while (r.next()) {
                            lines.add(new TransactionLine(r.getString(1), r.getString(4), r.getInt(2), r.getBigDecimal(3)));
                            units += r.getInt(2);
                        }
                    }
                }
                if (units == row.itemCount()) return Optional.of(new TransactionState(row, attemptCount, activeAttempt, lines));
            }
            throw new StoreFailure("Transaction " + id + " kept changing while its state was loaded");
        } catch (SQLException e) {
            throw failed(c, "Failed to load transaction state", e);
        } finally { pool.release(c); }
    }

    /** Ends the session's transaction explicitly, since a failed first statement leaves one the driver missed. */
    private static StoreFailure failed(Connection c, String message, SQLException e) {
        try { JdbcUnitOfWork.abort(c); } catch (SQLException rollback) { e.addSuppressed(rollback); }
        return new StoreFailure(message, e);
    }

    private static TransactionRecord record(String id, ResultSet r) throws SQLException {
        Timestamp completed = r.getTimestamp("completed_at");
        return new TransactionRecord(id, r.getString("station_id"),
            TransactionStatus.valueOf(r.getString("status")), r.getBigDecimal("total_amount"),
            r.getTimestamp("started_at").toInstant(), completed == null ? null : completed.toInstant(),
            r.getInt("item_count"));
    }
}
