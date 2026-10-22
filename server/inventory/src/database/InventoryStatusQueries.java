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
 * Inventory's operational status (FR-039; contracts/service-deployment.md): the processed checkout
 * operations by state. Every state is final, so Inventory itself holds no pending recovery work.
 */
public final class InventoryStatusQueries {
    private static final List<String> STATES = List.of("APPLIED", "REJECTED", "RESTORED", "BLOCKED");
    private static final String STATUS = "SELECT state, COUNT(*) FROM checkout_operation GROUP BY state";

    private final ConnectionPool pool;

    public InventoryStatusQueries(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    /** @param operations every state, in APPLIED, REJECTED, RESTORED, BLOCKED order, with zero counts kept */
    public record Status(Map<String, Long> operations) {
        public String line() {
            StringBuilder line = new StringBuilder("status service=inventory");
            operations.forEach((state, count) -> line.append(' ').append(state.toLowerCase(Locale.ROOT))
                    .append('=').append(count));
            return line.toString();
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
            return new Status(Collections.unmodifiableMap(operations));
        });
    }
}
