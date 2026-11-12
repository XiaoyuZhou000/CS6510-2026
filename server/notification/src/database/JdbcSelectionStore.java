package database;

import faults.FaultHooks;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * {@link SelectionStore} on Notification's pool (007 T053), in one unit of work:
 *
 * <ol>
 *   <li>{@code SELECT version FROM subscription_clock WHERE id = 1 FOR SHARE} gives {@code V};</li>
 *   <li>the planner plans against {@code V}, reading {@code subscription_change} rows on this connection when its
 *       index is behind;</li>
 *   <li>{@code INSERT INTO sale_selection}; a duplicate key rolls back and answers {@link AlreadySelected};</li>
 *   <li>{@code INSERT INTO notification} in multi-row statements of at most {@value #CHUNK} rows, {@code PENDING}
 *       and due at once;</li>
 *   <li>{@code FaultHooks.hit("notification.selection.beforeCommit")}, commit, then
 *       {@code FaultHooks.hit("notification.selection.afterCommit")}.</li>
 * </ol>
 *
 * <p>Budget: 2 + ⌈n/500⌉ statements and 1 commit per sale on the warm path ({@code notification.select.*}).</p>
 */
public final class JdbcSelectionStore implements SelectionStore {
    public static final int CHUNK = 500;
    private static final int DUPLICATE_KEY = 1062;
    private static final String SHARE_CLOCK = "SELECT version FROM subscription_clock WHERE id = 1 FOR SHARE";
    private static final String INSERT_SELECTION = "INSERT INTO sale_selection (sale_id, sale_event_id, completed_at, "
            + "`lines`, subscription_version, recipient_count, notification_count, selected_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String INSERT_NOTIFICATIONS = "INSERT INTO notification (notification_id, sale_id, sku, "
            + "subscriber_id, quantity, completed_at, state, next_attempt_at, created_at) VALUES ";
    private static final String ROW = "(?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)";

    private final ConnectionPool pool;
    private final Clock clock;

    public JdbcSelectionStore(ConnectionPool pool, Clock clock) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public Result select(Sale sale, Planner planner) {
        Objects.requireNonNull(sale, "sale");
        Objects.requireNonNull(planner, "planner");
        Result result = JdbcUnitOfWork.inTransaction(pool, "notification.select", connection -> {
            long version = lockedVersion(connection);
            Plan plan = planner.plan(version, (after, upTo) -> {
                try {
                    return JdbcSubscriptionStore.readChanges(connection, after, upTo);
                } catch (SQLException failure) {
                    throw new StoreFailure("Reading the subscription history failed", failure);
                }
            });
            Instant selectedAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);
            Timestamp at = Timestamp.from(selectedAt);
            Timestamp completedAt = Timestamp.from(sale.completedAt().truncatedTo(ChronoUnit.MILLIS));
            try (PreparedStatement insert = connection.prepareStatement(INSERT_SELECTION)) {
                insert.setString(1, sale.saleId());
                insert.setString(2, sale.saleEventId());
                insert.setTimestamp(3, completedAt);
                insert.setString(4, NotificationJson.lines(sale.lines()));
                insert.setLong(5, version);
                insert.setInt(6, plan.recipientCount());
                insert.setInt(7, plan.notifications().size());
                insert.setTimestamp(8, at);
                insert.executeUpdate();
            } catch (SQLException failure) {
                if (failure.getErrorCode() != DUPLICATE_KEY) throw failure;
                JdbcUnitOfWork.abort(connection);
                throw JdbcUnitOfWork.rollbackAndReturn(new AlreadySelected());
            }
            List<PlannedNotification> planned = plan.notifications();
            for (int from = 0; from < planned.size(); from += CHUNK) {
                List<PlannedNotification> chunk = planned.subList(from, Math.min(planned.size(), from + CHUNK));
                StringBuilder sql = new StringBuilder(INSERT_NOTIFICATIONS);
                for (int i = 0; i < chunk.size(); i++) sql.append(i == 0 ? ROW : ", " + ROW);
                try (PreparedStatement insert = connection.prepareStatement(sql.toString())) {
                    int parameter = 1;
                    for (PlannedNotification notification : chunk) {
                        insert.setString(parameter++, notification.notificationId());
                        insert.setString(parameter++, sale.saleId());
                        insert.setString(parameter++, notification.sku());
                        insert.setString(parameter++, notification.subscriberId());
                        insert.setInt(parameter++, notification.quantity());
                        insert.setTimestamp(parameter++, completedAt);
                        insert.setTimestamp(parameter++, at);
                        insert.setTimestamp(parameter++, at);
                    }
                    insert.executeUpdate();
                }
            }
            FaultHooks.hit("notification.selection.beforeCommit");
            List<NotificationStore.Pending> rows = new ArrayList<>(planned.size());
            for (PlannedNotification notification : planned) {
                rows.add(new NotificationStore.Pending(notification.notificationId(), sale.saleId(), notification.sku(),
                        notification.subscriberId(), notification.quantity(), completedAt.toInstant(), selectedAt,
                        version, 0, 0));
            }
            return new Selected(version, selectedAt, rows);
        });
        if (result instanceof Selected) FaultHooks.hit("notification.selection.afterCommit");
        return result;
    }

    private static long lockedVersion(Connection connection) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(SHARE_CLOCK); ResultSet row = select.executeQuery()) {
            if (!row.next()) throw new SQLException("subscription_clock has no row");
            return row.getLong(1);
        }
    }
}
