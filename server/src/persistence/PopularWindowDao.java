package persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

public final class PopularWindowDao {

    public record PopularEntry(String sku, String name, long scanCount) {}

    public record WindowRow(
        long windowId,
        long windowStart,
        long windowEnd,
        Instant computedAt,
        List<RankedItem> items
    ) {}

    public record RankedItem(int rank, String sku, String name, long scanCount) {}

    private final persistence.ConnectionPool pool;

    public PopularWindowDao(persistence.ConnectionPool pool) {
        this.pool = pool;
    }

    /**
     * Reads MAX(window_end) from popular_window, or 0 if the table is empty.
     * Called once at startup to seed the globalScanCounter in AnalyticsRecorder.
     */
    public long readMaxWindowEnd() throws SQLException {
        Connection conn = pool.borrow();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COALESCE(MAX(window_end), 0) FROM popular_window")) {
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } finally {
            pool.release(conn);
        }
    }

    /**
     * Persists one popular_window row and up to 10 popular_item rows in a single JDBC transaction.
     * entries must be in descending scan-count order (rank 1 = most scanned).
     */
    public void writeWindow(long windowStart, long windowEnd, List<PopularEntry> entries)
            throws SQLException {
        Connection conn = pool.borrow();
        boolean committed = false;
        try {
            conn.setAutoCommit(false);

            long windowId;
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO popular_window (window_start, window_end, computed_at) VALUES (?,?,?)",
                    PreparedStatement.RETURN_GENERATED_KEYS)) {
                ps.setLong(1, windowStart);
                ps.setLong(2, windowEnd);
                ps.setTimestamp(3, Timestamp.from(Instant.now()));
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    keys.next();
                    windowId = keys.getLong(1);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO popular_item (window_id, rank_pos, sku, scan_count) VALUES (?,?,?,?)")) {
                int rank = 1;
                for (PopularEntry e : entries) {
                    if (rank > 10) break;
                    ps.setLong(1, windowId);
                    ps.setInt(2, rank++);
                    ps.setString(3, e.sku());
                    ps.setLong(4, e.scanCount());
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            conn.commit();
            committed = true;
        } finally {
            if (!committed) {
                try { conn.rollback(); } catch (SQLException ignored) {}
            }
            try { conn.setAutoCommit(true); } catch (SQLException ignored) {}
            pool.release(conn);
        }
    }

    /**
     * Reads the latest window and its items (joined to catalog_item for name), capped at limit.
     * Returns null if no window has been persisted yet.
     */
    public WindowRow readLatestWindow(int limit) throws SQLException {
        if (limit < 0) throw new IllegalArgumentException("limit must be non-negative");
        Connection conn = pool.borrow();
        try {
            long windowId;
            long windowStart;
            long windowEnd;
            Instant computedAt;

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT window_id, window_start, window_end, computed_at " +
                    "FROM popular_window ORDER BY window_id DESC LIMIT 1")) {
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return null;
                    windowId    = rs.getLong("window_id");
                    windowStart = rs.getLong("window_start");
                    windowEnd   = rs.getLong("window_end");
                    computedAt  = rs.getTimestamp("computed_at").toInstant();
                }
            }

            List<RankedItem> items = new java.util.ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT pi.rank_pos, pi.sku, ci.name, pi.scan_count " +
                    "FROM popular_item pi JOIN catalog_item ci ON pi.sku = ci.sku " +
                    "WHERE pi.window_id = ? ORDER BY pi.rank_pos LIMIT ?")) {
                ps.setLong(1, windowId);
                ps.setInt(2, Math.min(limit, 10));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        items.add(new RankedItem(
                            rs.getInt("rank_pos"),
                            rs.getString("sku"),
                            rs.getString("name"),
                            rs.getLong("scan_count")
                        ));
                    }
                }
            }

            return new WindowRow(windowId, windowStart, windowEnd, computedAt, items);
        } finally {
            pool.release(conn);
        }
    }
}
