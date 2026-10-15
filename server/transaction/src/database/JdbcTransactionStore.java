package database;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Reads committed checkout state; OPEN baskets are never reconstructed from process memory. */
public final class JdbcTransactionStore implements TransactionStore {
    private final ConnectionPool pool;
    public JdbcTransactionStore(ConnectionPool pool) { this.pool = Objects.requireNonNull(pool); }

    @Override
    public void insertOpen(String transactionId, String stationId) {
        long storeStart = System.nanoTime();
        Connection c = pool.borrow();
        ScanProfiler.record(ScanProfiler.Phase.START_POOL, storeStart);
        long insertStart = System.nanoTime();
        try (PreparedStatement s = c.prepareStatement(
                "INSERT INTO `transaction` (transaction_id,station_id,status,total_amount,started_at) " +
                "VALUES (?,?,'OPEN',0.00,NOW(3))")) {
            s.setString(1, transactionId);
            s.setString(2, stationId);
            s.executeUpdate();
            ScanProfiler.record(ScanProfiler.Phase.START_INSERT, insertStart);
        } catch (SQLException e) {
            throw new StoreFailure("Failed to create transaction", e);
        } finally {
            pool.release(c);
            ScanProfiler.record(ScanProfiler.Phase.START_STORE, storeStart);
        }
    }

    @Override
    public Optional<TransactionRecord> findById(String id) {
        Connection c = pool.borrow();
        try (PreparedStatement s = c.prepareStatement(
                "SELECT t.station_id,t.status,t.total_amount,t.started_at,t.completed_at," +
                "COALESCE(SUM(l.quantity),0) AS item_count FROM `transaction` t " +
                "LEFT JOIN transaction_line l ON l.transaction_id=t.transaction_id " +
                "WHERE t.transaction_id=? GROUP BY t.transaction_id,t.station_id,t.status," +
                "t.total_amount,t.started_at,t.completed_at")) {
            s.setString(1, id);
            try (ResultSet r = s.executeQuery()) {
                if (!r.next()) return Optional.empty();
                Timestamp completed = r.getTimestamp("completed_at");
                return Optional.of(new TransactionRecord(id, r.getString("station_id"),
                    TransactionStatus.valueOf(r.getString("status")),
                    r.getBigDecimal("total_amount"), r.getTimestamp("started_at").toInstant(),
                    completed == null ? null : completed.toInstant(), r.getInt("item_count")));
            }
        } catch (SQLException e) {
            throw new StoreFailure("Failed to read transaction", e);
        } finally { pool.release(c); }
    }

    @Override
    public List<TransactionLine> findLines(String id) {
        Connection c = pool.borrow();
        try (PreparedStatement s = c.prepareStatement(
                "SELECT l.sku,c.name,l.quantity,l.unit_price FROM transaction_line l " +
                "JOIN catalog_item c ON c.sku=l.sku WHERE l.transaction_id=? ORDER BY l.sku")) {
            s.setString(1, id);
            List<TransactionLine> lines = new ArrayList<>();
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) lines.add(new TransactionLine(r.getString(1),r.getString(2),
                    r.getInt(3),r.getBigDecimal(4)));
            }
            return List.copyOf(lines);
        } catch (SQLException e) {
            throw new StoreFailure("Failed to read transaction lines", e);
        } finally { pool.release(c); }
    }
}

