package database;

import analytics.PipelineMessage;
import analytics.WindowMath;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Fresh committed reads of the immutable accepted-scan log. */
public final class JdbcAcceptedScanReader {
    private final ConnectionPool pool;
    private final JdbcScanMaterializer materializer;

    public JdbcAcceptedScanReader(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.materializer = new JdbcScanMaterializer(pool);
    }

    public int materializeBatch(int batchSize) {
        return materializer.materializeBatch(batchSize);
    }

    public List<PipelineMessage.AcceptedScan> readAfter(long cursor, int batchSize) {
        if (cursor < 0 || batchSize <= 0) throw new IllegalArgumentException("invalid cursor or batch size");
        Connection connection = pool.borrow();
        try {
            connection.setAutoCommit(true);
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT position,sku FROM accepted_scan WHERE position>? ORDER BY position LIMIT ?")) {
                statement.setLong(1, cursor);
                statement.setInt(2, batchSize);
                try (ResultSet rows = statement.executeQuery()) {
                    List<PipelineMessage.AcceptedScan> scans = readContiguous(rows, cursor + 1);
                    return List.copyOf(scans);
                }
            }
        } catch (SQLException failure) {
            throw new StoreFailure("Failed to poll accepted scans", failure);
        } finally {
            pool.release(connection);
        }
    }

    public List<PipelineMessage.AcceptedScan> prewarm(long checkpoint) {
        if (checkpoint == 0) return List.of();
        if (!WindowMath.isCheckpointBoundary(checkpoint)) {
            throw new IllegalArgumentException("invalid checkpoint");
        }
        long first = checkpoint - WindowMath.WINDOW_SIZE + 1;
        Connection connection = pool.borrow();
        try {
            connection.setAutoCommit(true);
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT position,sku FROM accepted_scan WHERE position BETWEEN ? AND ? ORDER BY position")) {
                statement.setLong(1, first);
                statement.setLong(2, checkpoint);
                try (ResultSet rows = statement.executeQuery()) {
                    List<PipelineMessage.AcceptedScan> scans = readContiguous(rows, first);
                    if (scans.size() != WindowMath.WINDOW_SIZE) {
                        throw new IllegalStateException("checkpoint overlap is incomplete at " + checkpoint);
                    }
                    return List.copyOf(scans);
                }
            }
        } catch (SQLException failure) {
            throw new StoreFailure("Failed to prewarm accepted scans", failure);
        } finally {
            pool.release(connection);
        }
    }

    private static List<PipelineMessage.AcceptedScan> readContiguous(ResultSet rows, long expected)
            throws SQLException {
        List<PipelineMessage.AcceptedScan> scans = new ArrayList<>();
        while (rows.next()) {
            long position = rows.getLong("position");
            if (position != expected) {
                throw new IllegalStateException("missing accepted-scan position " + expected);
            }
            scans.add(new PipelineMessage.AcceptedScan(position, rows.getString("sku")));
            expected++;
        }
        return scans;
    }
}
