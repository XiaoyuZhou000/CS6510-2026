package database;

import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.AnalyticsDatabase;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class JdbcScanMaterializerTest {
    @Test
    void lowerEventIdCommittingLateIsStillDiscovered() throws Exception {
        try (AnalyticsDatabase db = open()) {
            JdbcScanMaterializer materializer = new JdbcScanMaterializer(db.pool);
            Connection low = db.pool.borrow();
            try {
                low.setAutoCommit(false);
                long lowId = insert(low, "sku1");
                Connection high = db.pool.borrow();
                long highId;
                try { highId = insert(high, "sku2"); }
                finally { db.pool.release(high); }
                assertTrue(lowId < highId);
                assertEquals(1, materializer.materializeBatch(10));
                assertEquals(1L, position(db, highId));
                low.commit();
                assertNull(position(db, lowId));
                assertEquals(1, materializer.materializeBatch(10));
                assertEquals(2L, position(db, lowId));
                assertEquals(2, checkpoint(db));
            } finally {
                low.rollback();
                low.setAutoCommit(true);
                db.pool.release(low);
            }
        }
    }

    @Test
    void rollbackLeavesNoPositionAndNoCheckpointAdvance() throws Exception {
        try (AnalyticsDatabase db = open()) {
            Connection writer = db.pool.borrow();
            try {
                writer.setAutoCommit(false);
                insert(writer, "sku1");
                writer.rollback();
            } finally {
                writer.setAutoCommit(true);
                db.pool.release(writer);
            }
            assertEquals(0, new JdbcScanMaterializer(db.pool).materializeBatch(10));
            assertEquals(0, checkpoint(db));
            assertEquals(0, count(db, "SELECT COUNT(*) FROM accepted_scan"));
        }
    }

    @Test
    void crashBeforeCommitRollsBackWholeBatchAndRetryKeepsItGapless() throws Exception {
        try (AnalyticsDatabase db = open()) {
            insertPending(db, 5);
            JdbcScanMaterializer materializer = new JdbcScanMaterializer(db.pool);
            assertThrows(IllegalStateException.class, () ->
                materializer.materializeBatch(3, () -> { throw new IllegalStateException("crash"); }, () -> {}));
            assertEquals(0, checkpoint(db));
            assertEquals(5, count(db, "SELECT COUNT(*) FROM accepted_scan WHERE position IS NULL"));
            assertEquals(3, materializer.materializeBatch(3));
            assertEquals(2, materializer.materializeBatch(3));
            assertEquals(0, materializer.materializeBatch(3));
            assertEquals(5, checkpoint(db));
            assertEquals(5, count(db, "SELECT COUNT(DISTINCT position) FROM accepted_scan"));
            assertEquals(1, count(db, "SELECT MIN(position) FROM accepted_scan"));
        }
    }

    @Test
    void crashAfterCommitAndConcurrentRetriesNeverReassignPositions() throws Exception {
        try (AnalyticsDatabase db = open(); ExecutorService workers = Executors.newFixedThreadPool(2)) {
            insertPending(db, 12);
            JdbcScanMaterializer materializer = new JdbcScanMaterializer(db.pool);
            assertThrows(IllegalStateException.class, () ->
                materializer.materializeBatch(4, () -> {}, () -> { throw new IllegalStateException("crash"); }));
            assertEquals(4, checkpoint(db));
            List<Long> firstIds = ids(db, 4);
            List<Long> firstPositions = new ArrayList<>();
            for (long id : firstIds) firstPositions.add(position(db, id));
            List<Callable<Integer>> calls = List.of(
                () -> materializer.materializeBatch(4),
                () -> materializer.materializeBatch(4));
            int processed = 0;
            for (Future<Integer> result : workers.invokeAll(calls)) processed += result.get();
            assertEquals(8, processed);
            assertEquals(0, materializer.materializeBatch(4));
            assertEquals(12, checkpoint(db));
            for (int i = 0; i < firstIds.size(); i++)
                assertEquals(firstPositions.get(i), position(db, firstIds.get(i)));
            assertEquals(12, count(db, "SELECT COUNT(DISTINCT position) FROM accepted_scan"));
            assertEquals(12, count(db, "SELECT MAX(position) FROM accepted_scan"));
        }
    }

    private static AnalyticsDatabase open() throws Exception {
        try { return new AnalyticsDatabase(); }
        catch (SQLException e) { throw new TestAbortedException("MySQL unavailable", e); }
    }
    private static long insert(Connection c, String sku) throws Exception {
        try (PreparedStatement s = c.prepareStatement(
                "INSERT INTO accepted_scan(transaction_id,sku,unit_price) VALUES ('tx',?,1.00)",
                Statement.RETURN_GENERATED_KEYS)) {
            s.setString(1, sku);
            s.executeUpdate();
            try (ResultSet keys = s.getGeneratedKeys()) {
                assertTrue(keys.next());
                return keys.getLong(1);
            }
        }
    }
    private static void insertPending(AnalyticsDatabase db, int count) throws Exception {
        Connection c = db.pool.borrow();
        try { for (int i = 0; i < count; i++) insert(c, "sku1"); }
        finally { db.pool.release(c); }
    }
    private static Long position(AnalyticsDatabase db, long id) throws Exception {
        Connection c = db.pool.borrow();
        try (PreparedStatement s = c.prepareStatement(
                "SELECT position FROM accepted_scan WHERE event_id=?")) {
            s.setLong(1, id);
            try (ResultSet r = s.executeQuery()) {
                assertTrue(r.next());
                long value = r.getLong(1);
                return r.wasNull() ? null : value;
            }
        } finally { db.pool.release(c); }
    }
    private static int checkpoint(AnalyticsDatabase db) throws Exception {
        return count(db, "SELECT last_position FROM scan_sequence WHERE sequence_key=1");
    }
    private static int count(AnalyticsDatabase db, String sql) throws Exception {
        Connection c = db.pool.borrow();
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            assertTrue(r.next());
            return r.getInt(1);
        } finally { db.pool.release(c); }
    }
    private static List<Long> ids(AnalyticsDatabase db, int count) throws Exception {
        Connection c = db.pool.borrow();
        try (PreparedStatement s = c.prepareStatement(
                "SELECT event_id FROM accepted_scan WHERE position IS NOT NULL ORDER BY position LIMIT ?")) {
            s.setInt(1,count);
            try (ResultSet r = s.executeQuery()) {
                List<Long> ids = new ArrayList<>();
                while (r.next()) ids.add(r.getLong(1));
                return ids;
            }
        } finally { db.pool.release(c); }
    }
}
