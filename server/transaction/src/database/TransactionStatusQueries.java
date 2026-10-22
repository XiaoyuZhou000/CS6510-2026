package database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * Transaction's operational status (FR-039; contracts/service-deployment.md): pending and alarmed checkout
 * attempts, undelivered scan events, the FR-054 completion counts, and the product replica's version and
 * age. One read-only statement on Transaction's own pool; the pending-attempt and undelivered counts use
 * their indexes.
 */
public final class TransactionStatusQueries {
    private static final String STATUS = "SELECT "
            + "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'PENDING'), "
            + "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'PENDING' AND (last_error LIKE 'FAILED\\_PRECONDITION:%' "
            + "OR last_error LIKE 'INVALID\\_ARGUMENT:%' OR last_error LIKE 'INTEGRITY:%')), "
            + "(SELECT COUNT(*) FROM accepted_scan WHERE delivered_at IS NULL), "
            + "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'PENDING_ERROR'), "
            + "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'NONE'), "
            + "catalog_version, product_count, refreshed_at FROM product_replica_meta WHERE meta_key = 1";

    private final ConnectionPool pool;
    private final Clock clock;

    public TransactionStatusQueries(ConnectionPool pool) {
        this(pool, Clock.systemUTC());
    }

    public TransactionStatusQueries(ConnectionPool pool, Clock clock) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * @param alarmedAttempts pending attempts whose last error is an integrity alarm: FAILED_PRECONDITION,
     *                        INVALID_ARGUMENT, or INTEGRITY
     * @param replicaVersion  null while the replica was never loaded
     * @param replicaAge      time since the installed replica was last replaced, in whole seconds; null while
     *                        never loaded. A refresh that finds the same catalog version keeps the replica.
     */
    public record Status(long pendingAttempts, long alarmedAttempts, long undeliveredScans, long backgroundCompletions,
                         long completionsWithoutResponse, String replicaVersion, int replicaProducts, Duration replicaAge) {
        /** Recovery or delivery work remains while an attempt is pending or an event is undelivered. */
        public boolean pendingWork() {
            return pendingAttempts > 0 || undeliveredScans > 0;
        }

        public String line() {
            return "status service=transaction pending_work=" + (pendingWork() ? "yes" : "no")
                    + " pending_attempts=" + pendingAttempts + " alarmed_attempts=" + alarmedAttempts
                    + " undelivered_scans=" + undeliveredScans + " background_completions=" + backgroundCompletions
                    + " completions_without_response=" + completionsWithoutResponse
                    + " replica_version=" + (replicaVersion == null ? "none" : replicaVersion)
                    + " replica_products=" + replicaProducts
                    + " replica_age_seconds=" + (replicaAge == null ? "none" : Long.toString(replicaAge.toSeconds()));
        }
    }

    public Status read() {
        return JdbcUnitOfWork.read(pool, "transaction.status", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(STATUS);
                 ResultSet row = statement.executeQuery()) {
                if (!row.next()) throw new IllegalStateException("product_replica_meta has no row");
                Timestamp refreshed = row.getTimestamp(8);
                Duration age = refreshed == null ? null
                        : Duration.ofSeconds(Duration.between(refreshed.toInstant(), clock.instant()).toSeconds());
                return new Status(row.getLong(1), row.getLong(2), row.getLong(3), row.getLong(4), row.getLong(5),
                        row.getString(6), row.getInt(7), age);
            }
        });
    }
}
