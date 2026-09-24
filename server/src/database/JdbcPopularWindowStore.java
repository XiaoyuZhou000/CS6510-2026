package database;

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

/** JDBC persistence for atomic popular-window headers and ranks. */
public final class JdbcPopularWindowStore implements PopularWindowStore {
    private final ConnectionPool pool;

    public JdbcPopularWindowStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public long readMaxWindowEnd() {
        Connection connection = pool.borrow();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COALESCE(MAX(window_end),0) FROM popular_window");
             ResultSet results = statement.executeQuery()) {
            results.next();
            return results.getLong(1);
        } catch (SQLException e) {
            throw new StoreFailure("Failed to read popular-window recovery point", e);
        } finally {
            pool.release(connection);
        }
    }

    @Override
    public void writeWindow(PopularWindowSnapshot snapshot) {
        Connection connection = pool.borrow();
        try {
            connection.setAutoCommit(false);
            long windowId = insertHeader(connection, snapshot);
            insertRanks(connection, windowId, snapshot.ranks());
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, e);
            throw new StoreFailure("Failed to persist popular window", e);
        } finally {
            try { connection.setAutoCommit(true); }
            catch (SQLException resetFailure) {
                System.err.println("[database] Failed to reset connection: " + resetFailure.getMessage());
            }
            pool.release(connection);
        }
    }

    @Override
    public Optional<PopularWindowView> readLatest(int limit) {
        if (limit < 0) throw new IllegalArgumentException("limit must be non-negative");
        Connection connection = pool.borrow();
        try {
            Header header = readHeader(connection);
            if (header == null) return Optional.empty();
            List<StoredRank> ranks = readRanks(connection, header.windowId(), Math.min(limit, 10));
            return Optional.of(new PopularWindowView(header.windowId(), header.windowStart(),
                    header.windowEnd(), header.computedAt(), ranks));
        } catch (SQLException e) {
            throw new StoreFailure("Failed to read latest popular window", e);
        } finally {
            pool.release(connection);
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
                if (!keys.next()) throw new SQLException("Popular window did not return an ID");
                return keys.getLong(1);
            }
        }
    }

    private static void insertRanks(Connection connection, long windowId, List<SnapshotRank> ranks)
            throws SQLException {
        if (ranks.isEmpty()) return;
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO popular_item (window_id,rank_pos,sku,scan_count) VALUES (?,?,?,?)")) {
            for (SnapshotRank rank : ranks) {
                statement.setLong(1, windowId);
                statement.setInt(2, rank.rank());
                statement.setString(3, rank.sku());
                statement.setLong(4, rank.scanCount());
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private static Header readHeader(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT window_id,window_start,window_end,computed_at "
                        + "FROM popular_window ORDER BY window_id DESC LIMIT 1");
             ResultSet results = statement.executeQuery()) {
            if (!results.next()) return null;
            return new Header(results.getLong("window_id"), results.getLong("window_start"),
                    results.getLong("window_end"), results.getTimestamp("computed_at").toInstant());
        }
    }

    private static List<StoredRank> readRanks(Connection connection, long windowId, int limit)
            throws SQLException {
        List<StoredRank> ranks = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT pi.rank_pos,pi.sku,ci.name,pi.scan_count "
                        + "FROM popular_item pi JOIN catalog_item ci ON ci.sku=pi.sku "
                        + "WHERE pi.window_id=? ORDER BY pi.rank_pos LIMIT ?")) {
            statement.setLong(1, windowId);
            statement.setInt(2, limit);
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    ranks.add(new StoredRank(results.getInt("rank_pos"), results.getString("sku"),
                            results.getString("name"), results.getLong("scan_count")));
                }
            }
        }
        return List.copyOf(ranks);
    }

    private static void rollback(Connection connection, SQLException original) {
        try { connection.rollback(); }
        catch (SQLException rollbackFailure) { original.addSuppressed(rollbackFailure); }
    }

    private record Header(long windowId, long windowStart, long windowEnd, Instant computedAt) { }
}
