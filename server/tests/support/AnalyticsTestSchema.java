package support;

import org.junit.jupiter.api.Assertions;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    /**
     * Fixture rows written directly, bypassing ingestion: positions {@code from..to} with one SKU, then
     * {@code scan_sequence} at least {@code to}. No {@code product_name} row is written.
     */
    public void insertPositioned(long from, long to, String sku) throws SQLException {
        try (PreparedStatement insert = admin().prepareStatement("INSERT INTO accepted_scan "
                + "(position, source_transaction_id, source_scan_sequence, sku) VALUES (?, ?, ?, ?)")) {
            for (long position = from; position <= to; position++) {
                insert.setLong(1, position);
                insert.setString(2, "fixture-" + position);
                insert.setInt(3, 1);
                insert.setString(4, sku);
                insert.addBatch();
            }
            insert.executeBatch();
        }
        update("UPDATE scan_sequence SET last_position = GREATEST(last_position, ?) WHERE sequence_key = 1", to);
    }

    /** Writes one name directly, as an earlier ingestion would have. */
    public void seedName(String sku, String name) throws SQLException {
        update("INSERT INTO product_name (sku, name) VALUES (?, ?) AS n ON DUPLICATE KEY UPDATE name = n.name", sku, name);
    }

    /** Every stored event's source identity, {@code transaction_id#scan_sequence}. */
    public Set<String> identities() throws SQLException {
        Set<String> identities = new HashSet<>();
        for (StoredEvent event : events()) identities.add(event.sourceTransactionId() + "#" + event.sourceScanSequence());
        return identities;
    }

    /**
     * The top ten of positions {@code end - 999 .. end}, computed independently from the stored positions:
     * count descending, then SKU ascending, as {@code sku:count}.
     */
    public List<String> expectedRanks(long end) throws SQLException {
        List<String> expected = new ArrayList<>();
        try (PreparedStatement sql = admin().prepareStatement("SELECT sku, COUNT(*) AS n FROM accepted_scan "
                + "WHERE position BETWEEN ? AND ? GROUP BY sku ORDER BY n DESC, sku ASC LIMIT 10")) {
            sql.setLong(1, end - 999);
            sql.setLong(2, end);
            try (ResultSet row = sql.executeQuery()) {
                while (row.next()) expected.add(row.getString(1) + ":" + row.getLong(2));
            }
        }
        return expected;
    }

    /** The persisted ranks of a window as {@code sku:count}, in rank order. */
    public List<String> storedRanks(long end) throws SQLException {
        return ranks(end).stream().map(rank -> rank.sku() + ":" + rank.scanCount()).toList();
    }

    /**
     * Asserts that windows 1,000, 1,500, ... up to {@code highest} each exist exactly once, with rank
     * positions 1..n, that each equals a recomputation over the stored positions, and that the checkpoint
     * is {@code highest}.
     */
    public void assertWindowsMatchPositions(long highest) throws SQLException {
        List<Long> expectedEnds = new ArrayList<>();
        for (long end = 1000; end <= highest; end += 500) expectedEnds.add(end);
        Assertions.assertEquals(expectedEnds, windowEnds(), "published window ends");
        for (long end : expectedEnds) {
            Assertions.assertEquals(end - 999, queryLong("SELECT window_start FROM popular_window WHERE window_end = ?", end));
            List<Rank> ranks = ranks(end);
            for (int i = 0; i < ranks.size(); i++) Assertions.assertEquals(i + 1, ranks.get(i).rankPos());
            Assertions.assertEquals(expectedRanks(end), storedRanks(end), "independent ranks at " + end);
        }
        Assertions.assertEquals(highest, checkpoint());
    }

    /** Positions are exactly 1..n, and the scan_sequence checkpoint is n. */
    public void assertPositionsContiguous() throws SQLException {
        List<StoredEvent> events = events();
        for (int i = 0; i < events.size(); i++) {
            Assertions.assertEquals(i + 1, events.get(i).position(), "positions must be contiguous from 1");
        }
        Assertions.assertEquals(events.size(), lastPosition());
    }
}
