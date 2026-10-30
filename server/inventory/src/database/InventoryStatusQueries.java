package database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Inventory's operational status (FR-039, FR-046; contracts/service-deployment.md): the processed checkout
 * operations by state, the outbox events the broker has not confirmed yet, the parked events of its
 * consumer, and the catalog version with the time its events were recorded (D9). Every operation state is
 * final, so Inventory's only pending work is unpublished outbox rows.
 */
public final class InventoryStatusQueries {
    private static final List<String> STATES = List.of("APPLIED", "REJECTED", "RESTORED", "BLOCKED");
    private static final String STATUS = "SELECT state, COUNT(*) FROM checkout_operation GROUP BY state";
    private static final String EVENTS = "SELECT (SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL), "
            + "(SELECT COUNT(*) FROM dead_letter_event), "
            + "(SELECT catalog_version FROM catalog_meta WHERE meta_key = 1), "
            + "(SELECT published_at FROM catalog_meta WHERE meta_key = 1)";

    private final ConnectionPool pool;

    public InventoryStatusQueries(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    /**
     * @param operations         every state, in APPLIED, REJECTED, RESTORED, BLOCKED order, with zero counts kept
     * @param unpublishedOutbox  {@code outbox_event} rows the broker has not confirmed yet
     * @param parkedEvents       {@code dead_letter_event} rows of the checkout consumer
     * @param catalogVersion     null while the catalog was never seeded
     * @param catalogPublishedAt when the startup publication recorded the catalog's events; null before that
     */
    public record Status(Map<String, Long> operations, long unpublishedOutbox, long parkedEvents,
                         String catalogVersion, Instant catalogPublishedAt) {
        public String line() {
            StringBuilder line = new StringBuilder("status service=inventory");
            operations.forEach((state, count) -> line.append(' ').append(state.toLowerCase(Locale.ROOT))
                    .append('=').append(count));
            return line.append(" unpublished_outbox=").append(unpublishedOutbox)
                    .append(" parked_events=").append(parkedEvents)
                    .append(" catalog_version=").append(catalogVersion == null ? "none" : catalogVersion)
                    .append(" catalog_published_at=").append(catalogPublishedAt == null ? "none" : catalogPublishedAt)
                    .toString();
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
                Timestamp published = row.getTimestamp(4);
                return new Status(Collections.unmodifiableMap(operations), row.getLong(1), row.getLong(2),
                        row.getString(3), published == null ? null : published.toInstant());
            }
        });
    }
}
