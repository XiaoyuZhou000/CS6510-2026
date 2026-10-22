package database;

import metrics.LatencyHistogram;
import metrics.PerfMetrics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Atomically publishes complete ranks and advances the durable checkpoint, on Analytics' managed pool
 * (R20): sessions stay in manual-commit mode, and every unit ends in a commit or a rollback. Names come
 * from {@code product_name}, captured at scan time (R12). {@code analytics.window.publishLag} records the
 * time from the boundary event's ingestion to its window's commit.
 */
public final class JdbcPopularWindowStore implements PopularWindowStore {
    private static final LatencyHistogram PUBLISH_LAG = PerfMetrics.timer("analytics.window.publishLag");
    private final ConnectionPool pool;

    public JdbcPopularWindowStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public long readMaxWindowEnd() {
        Connection connection = pool.borrow();
        try {
            long checkpoint = readCheckpoint(connection, false);
            connection.rollback();
            return checkpoint;
        } catch (SQLException failure) {
            abortQuietly(connection, failure);
            throw new StoreFailure("Failed to read analytics checkpoint", failure);
        } finally {
            pool.release(connection);
        }
    }

    @Override
    public void writeWindow(PopularWindowSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        Connection connection = pool.borrow();
        try {
            long checkpoint = readCheckpoint(connection, true);
            if (snapshot.windowEnd() <= checkpoint) {
                verifyEquivalent(connection, snapshot);
                connection.commit();
                return;
            }
            long next = checkpoint == 0 ? 1000 : checkpoint + 500;
            if (snapshot.windowEnd() != next) {
                throw new IllegalStateException("window boundary is out of checkpoint order: " + snapshot.windowEnd());
            }
            long id = insertHeader(connection, snapshot);
            insertRanks(connection, id, snapshot.ranks());
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE analytics_checkpoint SET last_window_end=? WHERE checkpoint_key=1 AND last_window_end=?")) {
                update.setLong(1, snapshot.windowEnd());
                update.setLong(2, checkpoint);
                if (update.executeUpdate() != 1) throw new SQLException("analytics checkpoint changed unexpectedly");
            }
            long lagMicros = publishLagMicros(connection, snapshot.windowEnd());
            connection.commit();
            PerfMetrics.increment("analytics.window.published");
            if (lagMicros >= 0) PUBLISH_LAG.record(lagMicros * 1_000);
        } catch (SQLException failure) {
            abortQuietly(connection, failure);
            throw new StoreFailure("Failed to persist popular window at " + snapshot.windowEnd(), failure);
        } catch (RuntimeException failure) {
            abortQuietly(connection, failure);
            throw failure;
        } finally {
            pool.release(connection);
        }
    }

    @Override
    public Optional<PopularWindowView> readLatest(int limit) {
        if (limit < 0) throw new IllegalArgumentException("limit must be non-negative");
        Connection connection = pool.borrow();
        try {
            // One READ COMMITTED snapshot per statement; the checkpoint commits with its header and ranks.
            long checkpoint = readCheckpoint(connection, false);
            if (checkpoint == 0) {
                connection.rollback();
                return Optional.empty();
            }
            Header header = readHeader(connection, checkpoint);
            if (header == null) throw new SQLException("checkpoint has no complete window at " + checkpoint);
            List<StoredRank> ranks = readRanks(connection, header.id(), Math.min(limit, 10));
            connection.rollback();
            return Optional.of(new PopularWindowView(header.id(), header.start(), header.end(),
                    header.computedAt(), ranks));
        } catch (SQLException failure) {
            abortQuietly(connection, failure);
            throw new StoreFailure("Failed to read latest popular window", failure);
        } finally {
            pool.release(connection);
        }
    }

    private static long readCheckpoint(Connection connection, boolean lock) throws SQLException {
        String query = "SELECT last_window_end FROM analytics_checkpoint WHERE checkpoint_key=1"
                + (lock ? " FOR UPDATE" : "");
        try (PreparedStatement statement = connection.prepareStatement(query);
             ResultSet rows = statement.executeQuery()) {
            if (!rows.next()) throw new SQLException("analytics checkpoint singleton is missing");
            return rows.getLong(1);
        }
    }

    private static long insertHeader(Connection connection, PopularWindowSnapshot snapshot)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO popular_window (window_start,window_end,computed_at) VALUES (?,?,?)",
                Statement.RETURN_GENERATED_KEYS)) {
            statement.setLong(1, snapshot.windowStart());
            statement.setLong(2, snapshot.windowEnd());
            statement.setTimestamp(3, Timestamp.from(Instant.now()));
            statement.executeUpdate();
            try (ResultSet keys = statement.getGeneratedKeys()) {
                if (!keys.next()) throw new SQLException("popular window ID missing");
                return keys.getLong(1);
            }
        }
    }

    private static void insertRanks(Connection connection, long id, List<SnapshotRank> ranks)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO popular_item (window_id,rank_pos,sku,scan_count) VALUES (?,?,?,?)")) {
            for (SnapshotRank rank : ranks) {
                statement.setLong(1, id);
                statement.setInt(2, rank.rank());
                statement.setString(3, rank.sku());
                statement.setLong(4, rank.scanCount());
                statement.addBatch();
            }
            if (!ranks.isEmpty()) statement.executeBatch();
        }
    }

    private static void verifyEquivalent(Connection connection, PopularWindowSnapshot expected)
            throws SQLException {
        Header stored = readHeader(connection, expected.windowEnd());
        if (stored == null || stored.start() != expected.windowStart()) {
            throw new IllegalStateException("committed window boundary is missing or conflicts");
        }
        List<SnapshotRank> ranks = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT rank_pos,sku,scan_count FROM popular_item WHERE window_id=? ORDER BY rank_pos")) {
            statement.setLong(1, stored.id());
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) ranks.add(new SnapshotRank(rows.getInt(1), rows.getString(2), rows.getLong(3)));
            }
        }
        if (!ranks.equals(expected.ranks())) throw new IllegalStateException("committed window ranks conflict");
    }

    private static Header readHeader(Connection connection, long end) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT window_id,window_start,window_end,computed_at FROM popular_window WHERE window_end=?")) {
            statement.setLong(1, end);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) return null;
                return new Header(rows.getLong(1), rows.getLong(2), rows.getLong(3),
                        rows.getTimestamp(4).toInstant());
            }
        }
    }

    private static List<StoredRank> readRanks(Connection connection, long id, int limit)
            throws SQLException {
        List<StoredRank> ranks = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT pi.rank_pos,pi.sku,pn.name,pi.scan_count "
                + "FROM popular_item pi JOIN product_name pn ON pn.sku=pi.sku "
                + "WHERE pi.window_id=? ORDER BY pi.rank_pos LIMIT ?")) {
            statement.setLong(1, id);
            statement.setInt(2, limit);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) ranks.add(new StoredRank(rows.getInt(1), rows.getString(2),
                        rows.getString(3), rows.getLong(4)));
            }
        }
        return List.copyOf(ranks);
    }

    /**
     * Microseconds since the boundary event was ingested, or -1 when no event holds that position
     * (fixtures). Computed by the server, because {@code received_at} is a server-generated timestamp.
     */
    private static long publishLagMicros(Connection connection, long position) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT TIMESTAMPDIFF(MICROSECOND, received_at, CURRENT_TIMESTAMP(3)) FROM accepted_scan WHERE position=?")) {
            statement.setLong(1, position);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Math.max(0, rows.getLong(1)) : -1;
            }
        }
    }

    /** Explicit ROLLBACK statement, as {@link JdbcUnitOfWork#abort} explains. */
    private static void abortQuietly(Connection connection, Throwable failure) {
        try {
            JdbcUnitOfWork.abort(connection);
        } catch (SQLException rollback) {
            failure.addSuppressed(rollback);
        }
    }

    private record Header(long id, long start, long end, Instant computedAt) { }
}
