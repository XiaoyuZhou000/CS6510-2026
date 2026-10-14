package database;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Objects;
import transaction.database.JdbcCatalogStore;

/** A scan, basket update, sequence advance, and event are one InnoDB commit. */
public final class JdbcAcceptedScanStore implements AcceptedScanStore {
    private static final int MAX_ATTEMPTS = 3;
    private final ConnectionPool pool;
    private final JdbcCatalogStore catalog = new JdbcCatalogStore();
    public JdbcAcceptedScanStore(ConnectionPool pool) { this.pool = Objects.requireNonNull(pool); }

    @Override
    public ScanResult scanAtomically(String transactionId, String sku) {
        for (int attempt = 1; ; attempt++) {
            Connection c = pool.borrow();
            boolean commitAttempted = false;
            boolean retry = false;
            try {
                c.setAutoCommit(false);
                try (PreparedStatement s = c.prepareStatement(
                        "SELECT status FROM `transaction` WHERE transaction_id=? FOR UPDATE")) {
                    s.setString(1, transactionId);
                    try (ResultSet r = s.executeQuery()) {
                        if (!r.next()) { c.rollback(); return new MissingTransaction(); }
                        if (!"OPEN".equals(r.getString(1))) { c.rollback(); return new NotOpen(); }
                    }
                }
                var catalogItem = catalog.findBySku(c, sku);
                if (catalogItem.isEmpty()) { c.rollback(); return new UnknownSku(); }
                String name = catalogItem.get().name();
                BigDecimal catalogPrice = catalogItem.get().price();
                try (PreparedStatement s = c.prepareStatement(
                        "INSERT INTO transaction_line (transaction_id,sku,quantity,unit_price) " +
                        "VALUES (?,?,1,?) ON DUPLICATE KEY UPDATE quantity=quantity+1")) {
                    s.setString(1, transactionId);
                    s.setString(2, sku);
                    s.setBigDecimal(3, catalogPrice);
                    s.executeUpdate();
                }
                BigDecimal unitPrice;
                try (PreparedStatement s = c.prepareStatement(
                        "SELECT unit_price FROM transaction_line WHERE transaction_id=? AND sku=?")) {
                    s.setString(1, transactionId);
                    s.setString(2, sku);
                    try (ResultSet r = s.executeQuery()) {
                        if (!r.next()) throw new SQLException("Inserted line disappeared");
                        unitPrice = r.getBigDecimal(1);
                    }
                }
                int count;
                BigDecimal total;
                try (PreparedStatement s = c.prepareStatement(
                        "SELECT COALESCE(SUM(quantity),0),COALESCE(SUM(quantity*unit_price),0) " +
                        "FROM transaction_line WHERE transaction_id=?")) {
                    s.setString(1, transactionId);
                    try (ResultSet r = s.executeQuery()) {
                        r.next();
                        count = Math.toIntExact(r.getLong(1));
                        total = r.getBigDecimal(2);
                    }
                }
                try (PreparedStatement s = c.prepareStatement(
                        "UPDATE `transaction` SET total_amount=? WHERE transaction_id=?")) {
                    s.setBigDecimal(1, total);
                    s.setString(2, transactionId);
                    if (s.executeUpdate() != 1) throw new SQLException("Transaction disappeared");
                }
                long position;
                try (PreparedStatement s = c.prepareStatement(
                        "SELECT last_position FROM scan_sequence WHERE sequence_key=1 FOR UPDATE");
                     ResultSet r = s.executeQuery()) {
                    if (!r.next()) throw new SQLException("Scan sequence singleton missing");
                    position = Math.addExact(r.getLong(1), 1L);
                }
                try (PreparedStatement s = c.prepareStatement(
                        "UPDATE scan_sequence SET last_position=? WHERE sequence_key=1")) {
                    s.setLong(1, position);
                    if (s.executeUpdate() != 1) throw new SQLException("Scan sequence singleton missing");
                }
                try (PreparedStatement s = c.prepareStatement(
                        "INSERT INTO accepted_scan (position,transaction_id,sku,unit_price) " +
                        "VALUES (?,?,?,?)")) {
                    s.setLong(1, position);
                    s.setString(2, transactionId);
                    s.setString(3, sku);
                    s.setBigDecimal(4, unitPrice);
                    s.executeUpdate();
                }
                // A commit error is ambiguous: never retry or acknowledge it as success.
                commitAttempted = true;
                c.commit();
                return new Accepted(position, name, unitPrice, count, total);
            } catch (SQLException e) {
                try { c.rollback(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
                retry = !commitAttempted && isDeadlock(e) && attempt < MAX_ATTEMPTS;
                if (!retry) throw new StoreFailure("Failed to accept scan", e);
            } catch (RuntimeException e) {
                try { c.rollback(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
                throw e;
            } finally {
                try { c.setAutoCommit(true); }
                catch (SQLException reset) { System.err.println("[transaction] Connection reset failed: " + reset); }
                pool.release(c);
            }
            if (retry) {
                try { Thread.sleep(10L * attempt); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new StoreFailure("Scan retry interrupted", interrupted);
                }
            }
        }
    }

    private static boolean isDeadlock(SQLException e) {
        return e.getErrorCode() == 1213 || "40001".equals(e.getSQLState());
    }
}


