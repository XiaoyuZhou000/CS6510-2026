package database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;

/**
 * Notification's operational status line (FR-039): the subscription history version, the active subscribers,
 * the selected sales, the notifications by state, and the parked records. Later stories add the consumer lag
 * and the delivery lanes, which only the running components know.
 */
public final class NotificationStatusQueries {
    private static final String STATUS = "SELECT c.version, "
            + "(SELECT COUNT(*) FROM subscriber WHERE status = 'ACTIVE'), "
            + "(SELECT COUNT(*) FROM sale_selection), "
            + "(SELECT COUNT(*) FROM notification WHERE state = 'PENDING'), "
            + "(SELECT COUNT(*) FROM notification WHERE state = 'DELIVERED'), "
            + "(SELECT COUNT(*) FROM notification WHERE state = 'PARKED'), "
            + "(SELECT COUNT(*) FROM dead_letter_event) "
            + "FROM subscription_clock c WHERE c.id = 1";

    private final ConnectionPool pool;

    public NotificationStatusQueries(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    public record Status(long subscriptionVersion, long activeSubscribers, long saleSelections, long pending,
                         long delivered, long parked, long parkedEvents) {
        public boolean pendingWork() {
            return pending > 0;
        }

        public String line() {
            return "status service=notification pending_work=" + (pendingWork() ? "yes" : "no")
                    + " subscription_version=" + subscriptionVersion + " active_subscribers=" + activeSubscribers
                    + " sale_selections=" + saleSelections + " notifications_pending=" + pending
                    + " notifications_delivered=" + delivered + " notifications_parked=" + parked
                    + " parked_events=" + parkedEvents;
        }
    }

    public Status read() {
        return JdbcUnitOfWork.read(pool, "notification.status", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(STATUS);
                 ResultSet row = statement.executeQuery()) {
                if (!row.next()) throw new IllegalStateException("subscription_clock has no row");
                return new Status(row.getLong(1), row.getLong(2), row.getLong(3), row.getLong(4), row.getLong(5),
                        row.getLong(6), row.getLong(7));
            }
        });
    }
}
