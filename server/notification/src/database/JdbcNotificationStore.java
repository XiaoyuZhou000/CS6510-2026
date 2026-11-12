package database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link NotificationStore} on Notification's pool (007 T056). The dispatcher's batches are one UPDATE each plus
 * one commit (plan "Key flows"): marking per ≤ 200 IDs, failures per distinct (charged, error) outcome, and the
 * park check in the failure unit. Every UPDATE is guarded by {@code state = 'PENDING'}, so a delivered row never
 * changes back.
 */
public final class JdbcNotificationStore implements NotificationStore {
    private static final int MAX_ERROR = 255;
    private static final String COLUMNS = "n.notification_id, n.sale_id, n.sku, n.subscriber_id, n.quantity, "
            + "n.completed_at, n.created_at, s.subscription_version, n.charged_failures, n.uncharged_failures";

    private final ConnectionPool pool;

    public JdbcNotificationStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public int markDelivered(Collection<String> notificationIds, Instant at) {
        if (notificationIds.isEmpty()) return 0;
        String sql = "UPDATE notification SET state = 'DELIVERED', delivered_at = ?, attempt_count = attempt_count + 1 "
                + "WHERE notification_id IN (" + placeholders(notificationIds.size()) + ") AND state = 'PENDING'";
        return JdbcUnitOfWork.inTransaction(pool, "notification.delivery.mark", connection -> {
            try (PreparedStatement update = connection.prepareStatement(sql)) {
                update.setTimestamp(1, timestamp(at));
                int parameter = 2;
                for (String id : notificationIds) update.setString(parameter++, id);
                return update.executeUpdate();
            }
        });
    }

    @Override
    public int recordFailures(List<Failure> failures, Instant at, int parkAfterCharged) {
        if (failures.isEmpty()) return 0;
        Map<String, List<String>> charged = new LinkedHashMap<>();
        Map<String, List<String>> uncharged = new LinkedHashMap<>();
        List<String> chargedIds = new ArrayList<>();
        for (Failure failure : failures) {
            String error = truncate(failure.error());
            (failure.charged() ? charged : uncharged).computeIfAbsent(error, ignored -> new ArrayList<>())
                    .add(failure.notificationId());
            if (failure.charged()) chargedIds.add(failure.notificationId());
        }
        return JdbcUnitOfWork.inTransaction(pool, "notification.delivery.failures", connection -> {
            Timestamp now = timestamp(at);
            for (boolean isCharged : new boolean[] {true, false}) {
                String counter = isCharged ? "charged_failures" : "uncharged_failures";
                for (Map.Entry<String, List<String>> group : (isCharged ? charged : uncharged).entrySet()) {
                    // MySQL evaluates single-table assignments left to right, so the backoff sees the new counts.
                    String sql = "UPDATE notification SET attempt_count = attempt_count + 1, " + counter + " = " + counter
                            + " + 1, last_error = ?, next_attempt_at = TIMESTAMPADD(MICROSECOND, LEAST(200000 << "
                            + "LEAST(charged_failures + uncharged_failures, 10), 5000000), ?) "
                            + "WHERE notification_id IN (" + placeholders(group.getValue().size()) + ") AND state = 'PENDING'";
                    try (PreparedStatement update = connection.prepareStatement(sql)) {
                        update.setString(1, group.getKey());
                        update.setTimestamp(2, now);
                        int parameter = 3;
                        for (String id : group.getValue()) update.setString(parameter++, id);
                        update.executeUpdate();
                    }
                }
            }
            if (chargedIds.isEmpty()) return 0;
            String park = "UPDATE notification SET state = 'PARKED', parked_at = ? WHERE notification_id IN ("
                    + placeholders(chargedIds.size()) + ") AND state = 'PENDING' AND charged_failures >= ?";
            try (PreparedStatement update = connection.prepareStatement(park)) {
                update.setTimestamp(1, now);
                int parameter = 2;
                for (String id : chargedIds) update.setString(parameter++, id);
                update.setInt(parameter, parkAfterCharged);
                return update.executeUpdate();
            }
        });
    }

    @Override
    public int park(Collection<String> notificationIds, Instant at) {
        if (notificationIds.isEmpty()) return 0;
        String sql = "UPDATE notification SET state = 'PARKED', parked_at = ? WHERE notification_id IN ("
                + placeholders(notificationIds.size()) + ") AND state = 'PENDING'";
        return JdbcUnitOfWork.inTransaction(pool, "notification.delivery.park", connection -> {
            try (PreparedStatement update = connection.prepareStatement(sql)) {
                update.setTimestamp(1, timestamp(at));
                int parameter = 2;
                for (String id : notificationIds) update.setString(parameter++, id);
                return update.executeUpdate();
            }
        });
    }

    @Override
    public List<Pending> selectDue(Instant now, int limit, Collection<String> skipSubscribers) {
        if (limit <= 0) return List.of();
        String sql = "SELECT " + COLUMNS + " FROM notification n JOIN sale_selection s ON s.sale_id = n.sale_id "
                + "WHERE n.state = 'PENDING' AND n.next_attempt_at <= ?"
                + (skipSubscribers.isEmpty() ? "" : " AND n.subscriber_id NOT IN (" + placeholders(skipSubscribers.size()) + ")")
                + " ORDER BY n.next_attempt_at LIMIT ?";
        return JdbcUnitOfWork.read(pool, "notification.delivery.sweep", connection -> {
            try (PreparedStatement select = connection.prepareStatement(sql)) {
                select.setTimestamp(1, timestamp(now));
                int parameter = 2;
                for (String subscriber : skipSubscribers) select.setString(parameter++, subscriber);
                select.setInt(parameter, limit);
                List<Pending> due = new ArrayList<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) due.add(pending(rows));
                }
                return due;
            }
        });
    }

    @Override
    public int retryParked(Optional<String> subscriberId, Instant now) {
        String sql = "UPDATE notification SET state = 'PENDING', parked_at = NULL, charged_failures = 0, "
                + "operator_retries = operator_retries + 1, next_attempt_at = ? WHERE state = 'PARKED'"
                + (subscriberId.isPresent() ? " AND subscriber_id = ?" : "");
        return JdbcUnitOfWork.inTransaction(pool, "notification.delivery.retryParked", connection -> {
            try (PreparedStatement update = connection.prepareStatement(sql)) {
                update.setTimestamp(1, timestamp(now));
                if (subscriberId.isPresent()) update.setString(2, subscriberId.get());
                return update.executeUpdate();
            }
        });
    }

    @Override
    public Counts counts() {
        return JdbcUnitOfWork.read(pool, "notification.delivery.counts", connection -> {
            long pending = 0, delivered = 0, parked = 0, charged = 0, uncharged = 0;
            try (PreparedStatement select = connection.prepareStatement("SELECT state, COUNT(*), "
                    + "COALESCE(SUM(charged_failures), 0), COALESCE(SUM(uncharged_failures), 0) FROM notification GROUP BY state");
                 ResultSet rows = select.executeQuery()) {
                while (rows.next()) {
                    long count = rows.getLong(2);
                    switch (rows.getString(1)) {
                        case "PENDING" -> pending = count;
                        case "DELIVERED" -> delivered = count;
                        case "PARKED" -> parked = count;
                        default -> throw new IllegalStateException("Unknown notification state " + rows.getString(1));
                    }
                    charged += rows.getLong(3);
                    uncharged += rows.getLong(4);
                }
            }
            return new Counts(pending, delivered, parked, charged, uncharged);
        });
    }

    static Pending pending(ResultSet row) throws java.sql.SQLException {
        return new Pending(row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getInt(5),
                row.getTimestamp(6).toInstant(), row.getTimestamp(7).toInstant(), row.getLong(8), row.getInt(9),
                row.getInt(10));
    }

    static Timestamp timestamp(Instant at) {
        return Timestamp.from(at.truncatedTo(ChronoUnit.MILLIS));
    }

    static String placeholders(int count) {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < count; i++) text.append(i == 0 ? "?" : ", ?");
        return text.toString();
    }

    private static String truncate(String error) {
        if (error == null) return null;
        return error.length() <= MAX_ERROR ? error : error.substring(0, MAX_ERROR);
    }
}
