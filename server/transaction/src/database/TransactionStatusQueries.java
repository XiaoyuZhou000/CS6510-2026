package database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * Transaction's operational status (FR-039, FR-046; contracts/service-deployment.md): pending, alarmed, and
 * stuck checkout attempts, unpublished scan events and outbox rows, parked events, the FR-054 completion
 * counts, and the product replica's version and age. One read-only statement on Transaction's own pool; the
 * pending-attempt and unpublished counts use their indexes. An attempt is stuck once it has been PENDING for
 * {@code CHECKOUT_STUCK_ALARM_SECONDS} (60) (FR-038).
 */
public final class TransactionStatusQueries {
    private static final String STATUS = "SELECT "
            + "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'PENDING'), "
            + "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'PENDING' AND (last_error LIKE 'FAILED\\_PRECONDITION:%' "
            + "OR last_error LIKE 'INVALID\\_ARGUMENT:%' OR last_error LIKE 'INTEGRITY:%')), "
            + "(SELECT COUNT(*) FROM accepted_scan WHERE published_at IS NULL), "
            + "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'PENDING_ERROR'), "
            + "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'NONE'), "
            + "catalog_version, product_count, refreshed_at, "
            + "(SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL), "
            + "(SELECT COUNT(*) FROM dead_letter_event), "
            + "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'PENDING' AND created_at < ?) "
            + "FROM product_replica_meta WHERE meta_key = 1";
    public static final Duration DEFAULT_STUCK_AFTER = Duration.ofSeconds(60);

    private final ConnectionPool pool;
    private final Clock clock;
    private final Duration stuckAfter;

    public TransactionStatusQueries(ConnectionPool pool) {
        this(pool, Clock.systemUTC());
    }

    public TransactionStatusQueries(ConnectionPool pool, Clock clock) {
        this(pool, clock, DEFAULT_STUCK_AFTER);
    }

    public TransactionStatusQueries(ConnectionPool pool, Clock clock, Duration stuckAfter) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.stuckAfter = Objects.requireNonNull(stuckAfter, "stuckAfter");
    }

    /**
     * @param alarmedAttempts pending attempts whose last error is an integrity alarm: FAILED_PRECONDITION,
     *                        INVALID_ARGUMENT, or INTEGRITY
     * @param replicaVersion  null while the replica was never loaded
     * @param replicaAge      time since the installed replica was last replaced, in whole seconds; null while
     *                        never loaded. A refresh that finds the same catalog version keeps the replica.
     * @param unpublishedOutbox {@code outbox_event} rows the broker has not confirmed yet
     * @param parkedEvents    {@code dead_letter_event} rows of Transaction's consumers
     * @param stuckAttempts   attempts PENDING for {@code CHECKOUT_STUCK_ALARM_SECONDS} or longer
     */
    public record Status(long pendingAttempts, long alarmedAttempts, long unpublishedScans, long backgroundCompletions,
                         long completionsWithoutResponse, String replicaVersion, int replicaProducts, Duration replicaAge,
                         long unpublishedOutbox, long parkedEvents, long stuckAttempts) {
        /** Recovery or delivery work remains while an attempt is pending or an event is unpublished. */
        public boolean pendingWork() {
            return pendingAttempts > 0 || unpublishedScans > 0 || unpublishedOutbox > 0;
        }

        public String line() {
            return "status service=transaction pending_work=" + (pendingWork() ? "yes" : "no")
                    + " pending_attempts=" + pendingAttempts + " alarmed_attempts=" + alarmedAttempts
                    + " unpublished_scans=" + unpublishedScans + " background_completions=" + backgroundCompletions
                    + " completions_without_response=" + completionsWithoutResponse
                    + " replica_version=" + (replicaVersion == null ? "none" : replicaVersion)
                    + " replica_products=" + replicaProducts
                    + " replica_age_seconds=" + (replicaAge == null ? "none" : Long.toString(replicaAge.toSeconds()))
                    + " unpublished_outbox=" + unpublishedOutbox + " parked_events=" + parkedEvents
                    + " stuck_attempts=" + stuckAttempts;
        }
    }

    public Status read() {
        return JdbcUnitOfWork.read(pool, "transaction.status", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(STATUS)) {
                statement.setTimestamp(1, Timestamp.from(clock.instant().minus(stuckAfter)));
                try (ResultSet row = statement.executeQuery()) {
                    if (!row.next()) throw new IllegalStateException("product_replica_meta has no row");
                    Timestamp refreshed = row.getTimestamp(8);
                    Duration age = refreshed == null ? null
                            : Duration.ofSeconds(Duration.between(refreshed.toInstant(), clock.instant()).toSeconds());
                    return new Status(row.getLong(1), row.getLong(2), row.getLong(3), row.getLong(4), row.getLong(5),
                            row.getString(6), row.getInt(7), age, row.getLong(9), row.getLong(10), row.getLong(11));
                }
            }
        });
    }
}
