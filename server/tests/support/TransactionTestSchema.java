package support;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Disposable Transaction schema ({@code txn_test_<uuid>}) from {@code db/transaction/schema.sql}. */
public final class TransactionTestSchema extends TestSchema {
    private final Set<String> unavailable = new HashSet<>();

    private TransactionTestSchema(Options options) throws Exception {
        super("txn", "transaction", options);
    }

    public static TransactionTestSchema create() throws Exception {
        return new TransactionTestSchema(Options.defaults());
    }

    public static TransactionTestSchema create(Options options) throws Exception {
        return new TransactionTestSchema(options);
    }

    public record TransactionRow(String status, int itemCount, BigDecimal totalAmount, int attemptCount,
                                 Integer activeAttemptNumber, boolean completed) { }

    public record ScanRow(long scanId, int scanSequence, String sku, String productName, BigDecimal unitPrice,
                          boolean delivered) { }

    public record AttemptRow(int attemptNumber, String state, String rejectionReason, String rejectedSku,
                             String lines, String clientOutcome, int retryCount) { }

    public Optional<TransactionRow> transaction(String transactionId) throws SQLException {
        try (PreparedStatement sql = admin().prepareStatement(
                "SELECT status, item_count, total_amount, attempt_count, active_attempt_number, completed_at "
                        + "FROM `transaction` WHERE transaction_id = ?")) {
            sql.setString(1, transactionId);
            try (ResultSet row = sql.executeQuery()) {
                if (!row.next()) return Optional.empty();
                int active = row.getInt(5);
                // wasNull() describes the last column read, so it is checked before any other read.
                Integer activeAttempt = row.wasNull() ? null : active;
                return Optional.of(new TransactionRow(row.getString(1), row.getInt(2), row.getBigDecimal(3),
                        row.getInt(4), activeAttempt, row.getTimestamp(6) != null));
            }
        }
    }

    /** Scan rows of one transaction in {@code scan_sequence} order. */
    public List<ScanRow> scans(String transactionId) throws SQLException {
        List<ScanRow> scans = new ArrayList<>();
        try (PreparedStatement sql = admin().prepareStatement(
                "SELECT scan_id, scan_sequence, sku, product_name, unit_price, delivered_at FROM accepted_scan "
                        + "WHERE transaction_id = ? ORDER BY scan_sequence")) {
            sql.setString(1, transactionId);
            try (ResultSet row = sql.executeQuery()) {
                while (row.next()) {
                    scans.add(new ScanRow(row.getLong(1), row.getInt(2), row.getString(3), row.getString(4),
                            row.getBigDecimal(5), row.getTimestamp(6) != null));
                }
            }
        }
        return scans;
    }

    public long scanCount() throws SQLException {
        return queryLong("SELECT COUNT(*) FROM accepted_scan");
    }

    /** Outbox backlog: accepted scans not yet acknowledged by Analytics. */
    public long undeliveredCount() throws SQLException {
        return queryLong("SELECT COUNT(*) FROM accepted_scan WHERE delivered_at IS NULL");
    }

    public List<AttemptRow> attempts(String transactionId) throws SQLException {
        List<AttemptRow> attempts = new ArrayList<>();
        try (PreparedStatement sql = admin().prepareStatement(
                "SELECT attempt_number, state, rejection_reason, rejected_sku, `lines`, client_outcome, retry_count "
                        + "FROM checkout_attempt WHERE transaction_id = ? ORDER BY attempt_number")) {
            sql.setString(1, transactionId);
            try (ResultSet row = sql.executeQuery()) {
                while (row.next()) {
                    attempts.add(new AttemptRow(row.getInt(1), row.getString(2), row.getString(3), row.getString(4),
                            row.getString(5), row.getString(6), row.getInt(7)));
                }
            }
        }
        return attempts;
    }

    public long pendingAttemptCount() throws SQLException {
        return queryLong("SELECT COUNT(*) FROM checkout_attempt WHERE state = 'PENDING'");
    }

    /**
     * Loads a deterministic product replica ({@code SKU-000001}.. with the seed's names and prices)
     * under {@code version}, as a committed replica refresh would leave it.
     */
    public void seedReplica(int products, String version) throws SQLException {
        update("DELETE FROM product_replica");
        try (PreparedStatement insert = admin().prepareStatement(
                "INSERT INTO product_replica (sku, name, price, catalog_version) VALUES (?, ?, ?, ?)")) {
            for (int n = 1; n <= products; n++) {
                insert.setString(1, InventoryTestSchema.sku(n));
                insert.setString(2, "Item " + n);
                insert.setBigDecimal(3, InventoryTestSchema.price(n));
                insert.setString(4, version);
                insert.addBatch();
            }
            insert.executeBatch();
        }
        update("UPDATE product_replica_meta SET catalog_version = ?, product_count = ?, refreshed_at = NOW(3) "
                + "WHERE meta_key = 1", version, products);
    }

    /**
     * Simulates an outage of one of this schema's tables by renaming it away, without losing data.
     * {@link #close()} restores nothing; the schema is dropped anyway.
     */
    public synchronized void setTableUnavailable(String table, boolean makeUnavailable) throws SQLException {
        if (!table.matches("[a-z_]+")) throw new IllegalArgumentException("Unexpected table name: " + table);
        if (unavailable.contains(table) == makeUnavailable) return;
        try (Statement sql = admin().createStatement()) {
            sql.executeUpdate(makeUnavailable
                    ? "RENAME TABLE `" + name() + "`.`" + table + "` TO `" + name() + "`.`" + table + "_outage`"
                    : "RENAME TABLE `" + name() + "`.`" + table + "_outage` TO `" + name() + "`.`" + table + "`");
        }
        if (makeUnavailable) unavailable.add(table);
        else unavailable.remove(table);
    }
}
