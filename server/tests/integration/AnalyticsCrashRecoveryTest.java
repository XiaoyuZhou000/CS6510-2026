package integration;

import analytics.AnalyticsService;
import database.JdbcAcceptedScanReader;
import database.PopularWindowStore;
import database.StoreFailure;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.AnalyticsDatabase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class AnalyticsCrashRecoveryTest {
    @Test
    void outageBeforeReadReplaysEveryEligibleWindow() throws Exception {
        try (AnalyticsDatabase db = open()) {
            db.appendScans(stream(1500));
            catchUp(db);
            assertWindowsMatchAcceptedScans(db, 1500);
        }
    }

    @Test
    void interruptionDuringPartialAssemblyReplaysFromCheckpoint() throws Exception {
        try (AnalyticsDatabase db = open()) {
            db.appendScans(stream(750));
            AnalyticsService first = start(db, db.windows);
            Thread.sleep(200);
            first.shutdown();
            assertEquals(0, db.windows.readMaxWindowEnd());
            db.appendScans(stream(750));
            catchUp(db);
            assertWindowsMatchAcceptedScans(db, 1500);
        }
    }

    @Test
    void failureBeforeWindowCommitReplaysCandidate() throws Exception {
        try (AnalyticsDatabase db = open()) {
            db.appendScans(stream(1500));
            InterruptingStore failed = new InterruptingStore(db.windows, false);
            AnalyticsService first = start(db, failed);
            assertTrue(failed.firstWrite.await(5, TimeUnit.SECONDS));
            first.shutdown();
            assertEquals(0, db.windows.readMaxWindowEnd());
            catchUp(db);
            assertWindowsMatchAcceptedScans(db, 1500);
        }
    }

    @Test
    void failureAfterCommitBeforeAcknowledgementDoesNotDuplicateWindow() throws Exception {
        try (AnalyticsDatabase db = open()) {
            db.appendScans(stream(1500));
            InterruptingStore failed = new InterruptingStore(db.windows, true);
            AnalyticsService first = start(db, failed);
            assertTrue(failed.firstWrite.await(5, TimeUnit.SECONDS));
            db.awaitWindow(1000);
            first.shutdown();
            catchUp(db);
            assertWindowsMatchAcceptedScans(db, 1500);
        }
    }

    private static AnalyticsService start(AnalyticsDatabase db, PopularWindowStore windows) {
        return new AnalyticsService(windows, new JdbcAcceptedScanReader(db.pool));
    }

    private static void catchUp(AnalyticsDatabase db) throws Exception {
        AnalyticsService service = start(db, db.windows);
        try { db.awaitWindow(1500); }
        finally { service.shutdown(); }
    }

    private static List<String> stream(int size) {
        List<String> skus = new ArrayList<>();
        for (int i = 0; i < size; i++) skus.add("sku" + (1 + (i * 7) % 12));
        return skus;
    }

    private static void assertWindowsMatchAcceptedScans(AnalyticsDatabase db, long highest)
            throws Exception {
        Connection connection = db.pool.borrow();
        try (Statement statement = connection.createStatement();
             ResultSet windows = statement.executeQuery(
                     "SELECT window_id,window_start,window_end FROM popular_window ORDER BY window_end")) {
            long expectedEnd = 1000;
            while (windows.next()) {
                long end = windows.getLong("window_end");
                assertEquals(expectedEnd, end);
                assertEquals(end - 999, windows.getLong("window_start"));
                long id = windows.getLong("window_id");
                List<String> expected = new ArrayList<>();
                try (PreparedStatement counts = connection.prepareStatement(
                        "SELECT sku,COUNT(*) AS n FROM accepted_scan WHERE position BETWEEN ? AND ? "
                        + "GROUP BY sku ORDER BY n DESC,sku ASC LIMIT 10")) {
                    counts.setLong(1, end - 999);
                    counts.setLong(2, end);
                    try (ResultSet rows = counts.executeQuery()) {
                        while (rows.next()) expected.add(rows.getString(1) + ":" + rows.getLong(2));
                    }
                }
                List<String> actual = new ArrayList<>();
                try (PreparedStatement ranks = connection.prepareStatement(
                        "SELECT rank_pos,sku,scan_count FROM popular_item WHERE window_id=? ORDER BY rank_pos")) {
                    ranks.setLong(1, id);
                    try (ResultSet rows = ranks.executeQuery()) {
                        while (rows.next()) {
                            assertEquals(actual.size() + 1, rows.getInt(1));
                            actual.add(rows.getString(2) + ":" + rows.getLong(3));
                        }
                    }
                }
                assertEquals(expected, actual, "independent ranks at " + end);
                expectedEnd += 500;
            }
            assertEquals(highest + 500, expectedEnd);
            assertEquals(highest, db.windows.readMaxWindowEnd());
        } finally { db.pool.release(connection); }
    }

    private static AnalyticsDatabase open() throws Exception {
        try { return new AnalyticsDatabase(); }
        catch (SQLException unavailable) { throw new TestAbortedException("MySQL unavailable", unavailable); }
    }

    private static final class InterruptingStore implements PopularWindowStore {
        private final PopularWindowStore delegate;
        private final boolean afterCommit;
        private final CountDownLatch firstWrite = new CountDownLatch(1);

        InterruptingStore(PopularWindowStore delegate, boolean afterCommit) {
            this.delegate = delegate;
            this.afterCommit = afterCommit;
        }

        @Override public long readMaxWindowEnd() { return delegate.readMaxWindowEnd(); }
        @Override public Optional<PopularWindowView> readLatest(int limit) { return delegate.readLatest(limit); }
        @Override public void writeWindow(PopularWindowSnapshot window) {
            if (afterCommit) delegate.writeWindow(window);
            firstWrite.countDown();
            throw new StoreFailure("injected persistence interruption");
        }
    }
}
