package support;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Disposable Analytics schema ({@code ana_test_<uuid>}) from {@code db/analytics/schema.sql}. */
public final class AnalyticsTestSchema extends TestSchema {
    private AnalyticsTestSchema(Options options) throws Exception {
        super("ana", "analytics", options);
    }

    public static AnalyticsTestSchema create() throws Exception {
        return new AnalyticsTestSchema(Options.defaults());
    }

    public static AnalyticsTestSchema create(Options options) throws Exception {
        return new AnalyticsTestSchema(options);
    }

    public record StoredEvent(long position, String sourceTransactionId, int sourceScanSequence, String sku) { }

    public record Rank(int rankPos, String sku, long scanCount) { }

    public long lastPosition() throws SQLException {
        return queryLong("SELECT last_position FROM scan_sequence WHERE sequence_key = 1");
    }

    public long storedEvents() throws SQLException {
        return queryLong("SELECT COUNT(*) FROM accepted_scan");
    }

    /** Every stored event in position order. */
    public List<StoredEvent> events() throws SQLException {
        List<StoredEvent> events = new ArrayList<>();
        try (PreparedStatement sql = admin().prepareStatement(
                "SELECT position, source_transaction_id, source_scan_sequence, sku FROM accepted_scan ORDER BY position");
             ResultSet row = sql.executeQuery()) {
            while (row.next()) events.add(new StoredEvent(row.getLong(1), row.getString(2), row.getInt(3), row.getString(4)));
        }
        return events;
    }

    public long checkpoint() throws SQLException {
        return queryLong("SELECT last_window_end FROM analytics_checkpoint WHERE checkpoint_key = 1");
    }

    /** Published window ends in ascending order. */
    public List<Long> windowEnds() throws SQLException {
        List<Long> ends = new ArrayList<>();
        try (PreparedStatement sql = admin().prepareStatement("SELECT window_end FROM popular_window ORDER BY window_end");
             ResultSet row = sql.executeQuery()) {
            while (row.next()) ends.add(row.getLong(1));
        }
        return ends;
    }

    /** The persisted ranks of the window ending at {@code windowEnd}, in rank order. */
    public List<Rank> ranks(long windowEnd) throws SQLException {
        List<Rank> ranks = new ArrayList<>();
        try (PreparedStatement sql = admin().prepareStatement(
                "SELECT pi.rank_pos, pi.sku, pi.scan_count FROM popular_item pi "
                        + "JOIN popular_window pw ON pw.window_id = pi.window_id WHERE pw.window_end = ? ORDER BY pi.rank_pos")) {
            sql.setLong(1, windowEnd);
            try (ResultSet row = sql.executeQuery()) {
                while (row.next()) ranks.add(new Rank(row.getInt(1), row.getString(2), row.getLong(3)));
            }
        }
        return ranks;
    }

    public String productName(String sku) throws SQLException {
        return queryString("SELECT name FROM product_name WHERE sku = ?", sku);
    }
}
