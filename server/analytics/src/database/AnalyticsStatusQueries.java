package database;

import analytics.WindowMath;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;

/**
 * Analytics' operational status (FR-039; contracts/service-deployment.md): the highest stored position,
 * the published checkpoint, and the highest boundary those positions make eligible. Windows are pending
 * while the checkpoint is below that boundary.
 */
public final class AnalyticsStatusQueries {
    private static final String STATUS = "SELECT s.last_position, c.last_window_end "
            + "FROM scan_sequence s JOIN analytics_checkpoint c ON c.checkpoint_key = 1 WHERE s.sequence_key = 1";

    private final ConnectionPool pool;

    public AnalyticsStatusQueries(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    public record Status(long lastPosition, long lastWindowEnd) {
        /** 0 below 1,000 positions; otherwise the highest of 1,000, 1,500, ... at or below the last position. */
        public long highestEligibleBoundary() {
            if (lastPosition < WindowMath.WINDOW_SIZE) return 0;
            return WindowMath.WINDOW_SIZE
                    + (lastPosition - WindowMath.WINDOW_SIZE) / WindowMath.SLIDE_INTERVAL * WindowMath.SLIDE_INTERVAL;
        }

        /** Eligible windows above the checkpoint, not yet published. */
        public long unpublishedWindows() {
            long highest = highestEligibleBoundary();
            if (highest <= lastWindowEnd) return 0;
            long first = lastWindowEnd == 0 ? WindowMath.WINDOW_SIZE : lastWindowEnd + WindowMath.SLIDE_INTERVAL;
            return (highest - first) / WindowMath.SLIDE_INTERVAL + 1;
        }

        public boolean pendingWork() {
            return unpublishedWindows() > 0;
        }

        public String line() {
            return "status service=analytics pending_work=" + (pendingWork() ? "yes" : "no")
                    + " last_position=" + lastPosition + " last_window_end=" + lastWindowEnd
                    + " highest_eligible_boundary=" + highestEligibleBoundary()
                    + " unpublished_windows=" + unpublishedWindows();
        }
    }

    public Status read() {
        return JdbcUnitOfWork.read(pool, "analytics.status", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(STATUS);
                 ResultSet row = statement.executeQuery()) {
                if (!row.next()) throw new IllegalStateException("scan_sequence or analytics_checkpoint has no row");
                return new Status(row.getLong(1), row.getLong(2));
            }
        });
    }
}
