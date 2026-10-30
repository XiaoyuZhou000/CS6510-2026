package database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Inventory's operational status (FR-039, FR-046; contracts/service-deployment.md): the processed checkout
 * operations by state, the outcome events the broker has not confirmed yet, and the parked events of its
 * consumer. Every operation state is final, so Inventory's only pending work is unpublished outbox rows.
 */
public final class InventoryStatusQueries {
    private static final List<String> STATES = List.of("APPLIED", "REJECTED", "RESTORED", "BLOCKED");
    private static final String STATUS = "SELECT state, COUNT(*) FROM checkout_operation GROUP BY state";
    private static final String EVENTS = "SELECT (SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL), "
            + "(SELECT COUNT(*) FROM dead_letter_event)";

    private final ConnectionPool pool;

    public InventoryStatusQueries(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    /**
     * @param operations        every state, in APPLIED, REJECTED, RESTORED, BLOCKED order, with zero counts kept
     * @param unpublishedOutbox {@code outbox_event} rows the broker has not confirmed yet
     * @param parkedEvents      {@code dead_letter_event} rows of the checkout consumer
     */
    public record Status(Map<String, Long> operations, long unpublishedOutbox, long parkedEvents) {
        public String line() {
            StringBuilder line = new StringBuilder("status service=inventory");
            operations.forEach((state, count) -> line.append(' ').append(state.toLowerCase(Locale.ROOT))
                    .append('=').append(count));
            return line.append(" unpublished_outbox=").append(unpublishedOutbox)
                    .append(" parked_events=").append(parkedEvents).toString();
        }
    }

    public Status read() {
        return JdbcUnitOfWork.read(pool, "inventory.status", connection -> {
            Map<String, Long> operations = new LinkedHashMap<>();
            STATES.forEach(state -> operations.put(state, 0L));
            try (PreparedStatement statement = connection.prepareStatement(STATUS);
                 ResultSet row = statement.executeQuery()) {
                while (row.next()) operations.put(row.getString(1), row.getLong(2));
            }
            try (PreparedStatement statement = connection.prepareStatement(EVENTS);
                 ResultSet row = statement.executeQuery()) {
                row.next();
                return new Status(Collections.unmodifiableMap(operations), row.getLong(1), row.getLong(2));
            }
        });
    }
}
