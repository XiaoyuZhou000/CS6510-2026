package database;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Assigns a committed pending batch consecutive analytics positions atomically. */
public final class JdbcScanMaterializer {
    private final ConnectionPool pool;
    private static final boolean PROFILE = Boolean.parseBoolean(System.getenv("SCAN_PROFILE"));
    private static final AtomicLong BATCHES = new AtomicLong();
    private static final LongAdder EVENTS = new LongAdder();
    private static final LongAdder TOTAL_NANOS = new LongAdder();
    private static final LongAdder LOCK_NANOS = new LongAdder();
    private static final LongAdder UPDATE_NANOS = new LongAdder();
    private static final LongAdder COMMIT_NANOS = new LongAdder();

    public JdbcScanMaterializer(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool);
    }

    public int materializeBatch(int batchSize) {
        return materializeBatch(batchSize, () -> {}, () -> {});
    }

    // Hooks permit exact crash-boundary tests; production uses no-op hooks.
    int materializeBatch(int batchSize, Runnable beforeCommit, Runnable afterCommit) {
        if (batchSize <= 0 || batchSize > 4096) throw new IllegalArgumentException("batch size");
        long batchStart = System.nanoTime();
        Connection c = pool.borrow();
        boolean commitAttempted = false;
        try {
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            c.setAutoCommit(false);
            long last;
            long phaseStart = System.nanoTime();
            try (PreparedStatement s = c.prepareStatement(
                    "SELECT last_position FROM scan_sequence WHERE sequence_key=1 FOR UPDATE");
                 ResultSet r = s.executeQuery()) {
                if (!r.next()) throw new SQLException("Materializer checkpoint missing");
                last = r.getLong(1);
            }
            long lockNanos = System.nanoTime() - phaseStart;
            List<Long> pending = new ArrayList<>(batchSize);
            try (PreparedStatement s = c.prepareStatement(
                    "SELECT event_id FROM accepted_scan WHERE position IS NULL " +
                    "ORDER BY event_id LIMIT ? FOR UPDATE SKIP LOCKED")) {
                s.setInt(1, batchSize);
                try (ResultSet r = s.executeQuery()) {
                    while (r.next()) pending.add(r.getLong(1));
                }
            }
            if (pending.isEmpty()) {
                c.rollback();
                return 0;
            }
            phaseStart = System.nanoTime();
            try (PreparedStatement s = c.prepareStatement(
                    "UPDATE accepted_scan SET position=? WHERE event_id=? AND position IS NULL")) {
                for (long id : pending) {
                    s.setLong(1, ++last);
                    s.setLong(2, id);
                    if (s.executeUpdate() != 1) throw new SQLException("Pending event changed");
                }
            }
            long updateNanos = System.nanoTime() - phaseStart;
            try (PreparedStatement s = c.prepareStatement(
                    "UPDATE scan_sequence SET last_position=? WHERE sequence_key=1")) {
                s.setLong(1, last);
                if (s.executeUpdate() != 1) throw new SQLException("Materializer checkpoint missing");
            }
            beforeCommit.run();
            commitAttempted = true;
            phaseStart = System.nanoTime();
            c.commit();
            long commitNanos = System.nanoTime() - phaseStart;
            if (PROFILE) recordBatch(pending.size(), System.nanoTime() - batchStart,
                lockNanos, updateNanos, commitNanos);
            afterCommit.run();
            return pending.size();
        } catch (SQLException e) {
            try { c.rollback(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
            throw new StoreFailure("Failed to materialize accepted scans", e);
        } catch (RuntimeException e) {
            if (!commitAttempted) {
                try { c.rollback(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
            }
            throw e;
        } finally {
            try {
                c.setAutoCommit(true);
                c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            } catch (SQLException reset) {
                System.err.println("[analytics] Materializer connection reset failed: " + reset);
            }
            pool.release(c);
        }
    }

    private static void recordBatch(int events, long totalNanos, long lockNanos,
            long updateNanos, long commitNanos) {
        EVENTS.add(events);
        TOTAL_NANOS.add(totalNanos);
        LOCK_NANOS.add(lockNanos);
        UPDATE_NANOS.add(updateNanos);
        COMMIT_NANOS.add(commitNanos);
        long batches = BATCHES.incrementAndGet();
        if (batches % 1000 == 0) {
            System.err.printf("[materializer-profile] batches=%d events=%d avg_batch=%.2f " +
                "avg_total_ms=%.3f avg_checkpoint_lock_ms=%.3f avg_row_updates_ms=%.3f " +
                "avg_commit_ms=%.3f%n", batches, EVENTS.sum(), EVENTS.sum() / (double) batches,
                TOTAL_NANOS.sum() / (batches * 1_000_000.0),
                LOCK_NANOS.sum() / (batches * 1_000_000.0),
                UPDATE_NANOS.sum() / (batches * 1_000_000.0),
                COMMIT_NANOS.sum() / (batches * 1_000_000.0));
        }
    }
}
