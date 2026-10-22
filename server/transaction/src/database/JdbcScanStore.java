package database;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Objects;

/** Two statements and one commit per Scan, with no read (contracts/scan-event-delivery.md section 1). */
public final class JdbcScanStore implements ScanStore {
    private static final String GUARDED_UPDATE = "UPDATE `transaction` SET item_count = ?, total_amount = ? "
            + "WHERE transaction_id = ? AND item_count = ? AND status = 'OPEN' AND active_attempt_number IS NULL";
    private static final String INSERT_EVENT = "INSERT INTO accepted_scan "
            + "(transaction_id, scan_sequence, sku, product_name, unit_price, accepted_at) VALUES (?, ?, ?, ?, ?, ?)";

    private final ConnectionPool pool;

    public JdbcScanStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public AppendResult appendScan(String transactionId, int expectedItemCount, BigDecimal newTotal, AcceptedScan scan) {
        if (scan.scanSequence() != expectedItemCount + 1) {
            throw new IllegalArgumentException("scanSequence must be the expected item count plus one");
        }
        return JdbcUnitOfWork.inTransaction(pool, "transaction.store.scan", connection -> {
            try (PreparedStatement update = connection.prepareStatement(GUARDED_UPDATE)) {
                update.setInt(1, scan.scanSequence());
                update.setBigDecimal(2, newTotal);
                update.setString(3, transactionId);
                update.setInt(4, expectedItemCount);
                if (update.executeUpdate() == 0) throw JdbcUnitOfWork.rollbackAndReturn(AppendResult.GUARD_MISS);
            }
            try (PreparedStatement insert = connection.prepareStatement(INSERT_EVENT)) {
                insert.setString(1, transactionId);
                insert.setInt(2, scan.scanSequence());
                insert.setString(3, scan.sku());
                insert.setString(4, scan.productName());
                insert.setBigDecimal(5, scan.unitPrice());
                insert.setTimestamp(6, Timestamp.from(scan.acceptedAt()));
                insert.executeUpdate();
            }
            return AppendResult.OK;
        });
    }
}
