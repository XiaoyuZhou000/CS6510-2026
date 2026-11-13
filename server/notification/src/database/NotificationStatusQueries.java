package database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Notification's durable status (FR-039, contracts/notification-http.md "Operational endpoints"): the subscription
 * history version, the active subscribers, the selected sales, the notifications by state, the charged and
 * uncharged failure totals, and the parked records. The running components add the index version, the catalog
 * state, the consumer lag, the alarms, and the last confirmed send ({@code notification.NotificationStatus}).
 */
public final class NotificationStatusQueries {
    private static final String STATUS = "SELECT c.version, "
            + "(SELECT COUNT(*) FROM subscriber WHERE status = 'ACTIVE'), "
            + "(SELECT COUNT(*) FROM sale_selection), "
            + "(SELECT COUNT(*) FROM notification WHERE state = 'PENDING'), "
            + "(SELECT COUNT(*) FROM notification WHERE state = 'DELIVERED'), "
            + "(SELECT COUNT(*) FROM notification WHERE state = 'PARKED'), "
            + "(SELECT COUNT(*) FROM dead_letter_event), "
            + "(SELECT COALESCE(SUM(charged_failures), 0) FROM notification), "
            + "(SELECT COALESCE(SUM(uncharged_failures), 0) FROM notification) "
            + "FROM subscription_clock c WHERE c.id = 1";

    private final ConnectionPool pool;

    public NotificationStatusQueries(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    public record Status(long subscriptionVersion, long activeSubscribers, long saleSelections, long pending,
                         long delivered, long parked, long parkedEvents, long chargedFailures, long unchargedFailures) {
        public boolean pendingWork() {
            return pending > 0;
        }

        public String line() {
            return "status service=notification pending_work=" + (pendingWork() ? "yes" : "no")
                    + " subscription_version=" + subscriptionVersion + " active_subscribers=" + activeSubscribers
                    + " sale_selections=" + saleSelections + " notifications_pending=" + pending
                    + " notifications_delivered=" + delivered + " notifications_parked=" + parked
                    + " parked_events=" + parkedEvents + " charged_failures=" + chargedFailures
                    + " uncharged_failures=" + unchargedFailures;
        }
    }

    /** A record Notification's consumers parked in {@code dead_letter_event} (FR-016), without its bytes. */
    public record ParkedEvent(String eventId, String topic, int partition, long offset, String eventType,
                              Integer schemaVersion, String reason, String detail, Instant firstParkedAt,
                              Instant lastParkedAt, int parkCount) { }

    private static final String PARKED_EVENTS = "SELECT event_id, topic, partition_no, record_offset, event_type, "
            + "schema_version, reason, detail, first_parked_at, last_parked_at, park_count FROM dead_letter_event "
            + "ORDER BY first_parked_at, dead_letter_id LIMIT ?";

    /** Up to {@code limit} parked records, oldest first, for {@code GET /notifications/ops/parked}. */
    public List<ParkedEvent> parkedEvents(int limit) {
        if (limit <= 0) return List.of();
        return JdbcUnitOfWork.read(pool, "notification.parkedEvents", connection -> {
            try (PreparedStatement select = connection.prepareStatement(PARKED_EVENTS)) {
                select.setInt(1, limit);
                List<ParkedEvent> parked = new ArrayList<>();
                try (ResultSet row = select.executeQuery()) {
                    while (row.next()) {
                        int version = row.getInt(6);
                        Integer schemaVersion = row.wasNull() ? null : version;
                        parked.add(new ParkedEvent(row.getString(1), row.getString(2), row.getInt(3), row.getLong(4),
                                row.getString(5), schemaVersion, row.getString(7), row.getString(8),
                                row.getTimestamp(9).toInstant(), row.getTimestamp(10).toInstant(), row.getInt(11)));
                    }
                }
                return parked;
            }
        });
    }

    public Status read() {
        return JdbcUnitOfWork.read(pool, "notification.status", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(STATUS);
                 ResultSet row = statement.executeQuery()) {
                if (!row.next()) throw new IllegalStateException("subscription_clock has no row");
                return new Status(row.getLong(1), row.getLong(2), row.getLong(3), row.getLong(4), row.getLong(5),
                        row.getLong(6), row.getLong(7), row.getLong(8), row.getLong(9));
            }
        });
    }
}
