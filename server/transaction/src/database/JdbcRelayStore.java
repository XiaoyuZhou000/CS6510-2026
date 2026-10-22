package database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * One SELECT per round, and after an acknowledged delivery one UPDATE and one commit (R22). The SELECT uses
 * the {@code (delivered_at, scan_id)} index and ends its unit with a rollback.
 */
public final class JdbcRelayStore implements RelayStore {
    private static final String SELECT_UNDELIVERED = "SELECT scan_id, transaction_id, scan_sequence, sku, product_name, "
            + "accepted_at FROM accepted_scan WHERE delivered_at IS NULL ORDER BY scan_id LIMIT ?";

    private final ConnectionPool pool;

    public JdbcRelayStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public List<UndeliveredScan> selectUndelivered(int limit) {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        return JdbcUnitOfWork.read(pool, "transaction.relay.select", connection -> {
            List<UndeliveredScan> scans = new ArrayList<>(Math.min(limit, 512));
            try (PreparedStatement select = connection.prepareStatement(SELECT_UNDELIVERED)) {
                select.setInt(1, limit);
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        scans.add(new UndeliveredScan(rows.getLong(1), rows.getString(2), rows.getInt(3),
                                rows.getString(4), rows.getString(5), rows.getTimestamp(6).toInstant()));
                    }
                }
            }
            return scans;
        });
    }

    @Override
    public int markDelivered(List<Long> scanIds, Instant deliveredAt) {
        Objects.requireNonNull(deliveredAt, "deliveredAt");
        if (scanIds == null || scanIds.isEmpty()) throw new IllegalArgumentException("scanIds must not be empty");
        StringBuilder sql = new StringBuilder(80 + scanIds.size() * 3)
                .append("UPDATE accepted_scan SET delivered_at = ? WHERE delivered_at IS NULL AND scan_id IN (");
        for (int i = 0; i < scanIds.size(); i++) sql.append(i == 0 ? "?" : ", ?");
        sql.append(')');
        return JdbcUnitOfWork.inTransaction(pool, "transaction.relay.mark", connection -> {
            try (PreparedStatement update = connection.prepareStatement(sql.toString())) {
                update.setTimestamp(1, Timestamp.from(deliveredAt));
                int parameter = 2;
                for (long scanId : scanIds) update.setLong(parameter++, scanId);
                return update.executeUpdate();
            }
        });
    }

    @Override
    public long countUndelivered() {
        return JdbcUnitOfWork.read(pool, "transaction.relay.count", connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT COUNT(*) FROM accepted_scan WHERE delivered_at IS NULL");
                 ResultSet row = select.executeQuery()) {
                row.next();
                return row.getLong(1);
            }
        });
    }
}
