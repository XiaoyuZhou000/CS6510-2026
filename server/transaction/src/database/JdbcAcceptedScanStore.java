package database;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Objects;
import transaction.database.JdbcCatalogStore;
import database.ScanProfiler.Phase;

/** A basket update and unpositioned durable event are one InnoDB commit. */
public final class JdbcAcceptedScanStore implements AcceptedScanStore {
    private static final int MAX_ATTEMPTS = 3;
    private final ConnectionPool pool;
    private final JdbcCatalogStore catalog = new JdbcCatalogStore();
    public JdbcAcceptedScanStore(ConnectionPool pool) { this.pool = Objects.requireNonNull(pool); }

    @Override
    public ScanResult scanAtomically(String transactionId, String sku) {
        for (int attempt = 1; ; attempt++) {
            long storeStart = System.nanoTime();
            long phaseStart = storeStart;
            Connection c = pool.borrow();
            ScanProfiler.record(Phase.POOL_ACQUIRE, phaseStart);
            long transactionStart = System.nanoTime();
            boolean commitAttempted = false;
            boolean retry = false;
            try {
                phaseStart = System.nanoTime();
                c.setAutoCommit(false);
                ScanProfiler.record(Phase.BEGIN, phaseStart);
                phaseStart = System.nanoTime();
                try (PreparedStatement s = c.prepareStatement(
                        "SELECT status FROM `transaction` WHERE transaction_id=? FOR UPDATE")) {
                    s.setString(1, transactionId);
                    try (ResultSet r = s.executeQuery()) {
                        if (!r.next()) { c.rollback(); return new MissingTransaction(); }
                        if (!"OPEN".equals(r.getString(1))) { c.rollback(); return new NotOpen(); }
                    }
                }
                ScanProfiler.record(Phase.TRANSACTION_LOCK, phaseStart);
                phaseStart = System.nanoTime();
                var catalogItem = catalog.findBySku(c, sku);
                ScanProfiler.record(Phase.CATALOG_LOOKUP, phaseStart);
                if (catalogItem.isEmpty()) { c.rollback(); return new UnknownSku(); }
                String name = catalogItem.get().name();
                BigDecimal catalogPrice = catalogItem.get().price();
                phaseStart = System.nanoTime();
                try (PreparedStatement s = c.prepareStatement(
                        "INSERT INTO transaction_line (transaction_id,sku,quantity,unit_price) " +
                        "VALUES (?,?,1,?) ON DUPLICATE KEY UPDATE quantity=quantity+1")) {
                    s.setString(1, transactionId);
                    s.setString(2, sku);
                    s.setBigDecimal(3, catalogPrice);
                    s.executeUpdate();
                }
                ScanProfiler.record(Phase.LINE_UPSERT, phaseStart);
                BigDecimal unitPrice;
                phaseStart = System.nanoTime();
                try (PreparedStatement s = c.prepareStatement(
                        "SELECT unit_price FROM transaction_line WHERE transaction_id=? AND sku=?")) {
                    s.setString(1, transactionId);
                    s.setString(2, sku);
                    try (ResultSet r = s.executeQuery()) {
                        if (!r.next()) throw new SQLException("Inserted line disappeared");
                        unitPrice = r.getBigDecimal(1);
                    }
                }
                ScanProfiler.record(Phase.LINE_PRICE, phaseStart);
                int count;
                BigDecimal total;
                phaseStart = System.nanoTime();
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
                ScanProfiler.record(Phase.BASKET_AGGREGATE, phaseStart);
                phaseStart = System.nanoTime();
                try (PreparedStatement s = c.prepareStatement(
                        "UPDATE `transaction` SET total_amount=? WHERE transaction_id=?")) {
                    s.setBigDecimal(1, total);
                    s.setString(2, transactionId);
                    if (s.executeUpdate() != 1) throw new SQLException("Transaction disappeared");
                }
                ScanProfiler.record(Phase.TOTAL_UPDATE, phaseStart);
                long eventId;
                phaseStart = System.nanoTime();
                try (PreparedStatement s = c.prepareStatement(
                        "INSERT INTO accepted_scan (transaction_id,sku,unit_price) " +
                        "VALUES (?,?,?)", Statement.RETURN_GENERATED_KEYS)) {
                    s.setString(1, transactionId);
                    s.setString(2, sku);
                    s.setBigDecimal(3, unitPrice);
                    if (s.executeUpdate() != 1) throw new SQLException("Event insert failed");
                    try (ResultSet keys = s.getGeneratedKeys()) {
                        if (!keys.next()) throw new SQLException("Event identity missing");
                        eventId = keys.getLong(1);
                    }
                }
                ScanProfiler.record(Phase.EVENT_INSERT, phaseStart);
                // A commit error is ambiguous: never retry or acknowledge it as success.
                commitAttempted = true;
                phaseStart = System.nanoTime();
                c.commit();
                ScanProfiler.record(Phase.COMMIT, phaseStart);
                ScanProfiler.record(Phase.TRANSACTION_EXECUTION, transactionStart);
                return new Accepted(eventId, name, unitPrice, count, total);
            } catch (SQLException e) {
                try { c.rollback(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
                retry = !commitAttempted && isDeadlock(e) && attempt < MAX_ATTEMPTS;
                if (!retry) throw new StoreFailure("Failed to accept scan", e);
            } catch (RuntimeException e) {
                try { c.rollback(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
                throw e;
            } finally {
                phaseStart = System.nanoTime();
                try { c.setAutoCommit(true); }
                catch (SQLException reset) { System.err.println("[transaction] Connection reset failed: " + reset); }
                pool.release(c);
                ScanProfiler.record(Phase.POOL_RETURN, phaseStart);
                ScanProfiler.record(Phase.STORE_TOTAL, storeStart);
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


