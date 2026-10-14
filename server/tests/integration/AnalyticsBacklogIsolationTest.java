package integration;

import analytics.AnalyticsService;
import database.JdbcAcceptedScanReader;
import database.JdbcPopularWindowStore;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.CheckoutDatabase;
import transaction.TransactionOperations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class AnalyticsBacklogIsolationTest {
    @Test
    void transactionContinuesDuringAnalyticsOutageAndPrewarmRestoresOverlap() throws Exception {
        try (CheckoutDatabase db = open()) {
            String id = db.checkout.start(new TransactionOperations.StartCommand("outage-station"))
                    .transactionId();
            for (int position = 1; position <= 1000; position++) {
                String sku = position % 4 == 0 ? CheckoutDatabase.OTHER_SKU : CheckoutDatabase.SKU;
                db.checkout.scan(new TransactionOperations.ScanCommand(id, sku));
            }
            assertEquals(1000, db.position());
            assertEquals(1000, db.checkout.get(id).itemCount());
            assertEquals(2000, db.stock(), "Analytics outage cannot deduct inventory");

            JdbcPopularWindowStore windows = new JdbcPopularWindowStore(db.pool);
            AnalyticsService first = new AnalyticsService(windows, new JdbcAcceptedScanReader(db.pool));
            try { await(windows, 1000); }
            finally { first.shutdown(); }
            assertWindow(db, 1000);
            for (int position = 1001; position <= 1500; position++) {
                String sku = position % 4 == 0 ? CheckoutDatabase.OTHER_SKU : CheckoutDatabase.SKU;
                db.checkout.scan(new TransactionOperations.ScanCommand(id, sku));
            }
            assertEquals(1500, db.position());
            assertEquals(1500, db.checkout.get(id).itemCount());

            AnalyticsService restarted = new AnalyticsService(windows, new JdbcAcceptedScanReader(db.pool));
            try { await(windows, 1500); }
            finally { restarted.shutdown(); }
            assertWindow(db, 1500);
            assertEquals(1500, windows.readLatest(10).orElseThrow().windowEnd());
        }
    }

    private static void await(JdbcPopularWindowStore windows, long end) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (windows.readMaxWindowEnd() < end) {
            if (System.nanoTime() >= deadline) throw new AssertionError("Analytics did not catch up to " + end);
            Thread.sleep(20);
        }
    }

    private static void assertWindow(CheckoutDatabase db, long end) throws Exception {
        Connection connection = db.pool.borrow();
        try {
            List<String> expected = new ArrayList<>();
            try (PreparedStatement query = connection.prepareStatement(
                    "SELECT sku,COUNT(*) AS n FROM accepted_scan WHERE position BETWEEN ? AND ? "
                    + "GROUP BY sku ORDER BY n DESC,sku ASC LIMIT 10")) {
                query.setLong(1, end - 999);
                query.setLong(2, end);
                try (ResultSet rows = query.executeQuery()) {
                    while (rows.next()) expected.add(rows.getString(1) + ":" + rows.getLong(2));
                }
            }
            List<String> actual = new ArrayList<>();
            try (PreparedStatement query = connection.prepareStatement(
                    "SELECT pi.rank_pos,pi.sku,pi.scan_count FROM popular_item pi "
                    + "JOIN popular_window pw ON pw.window_id=pi.window_id "
                    + "WHERE pw.window_end=? ORDER BY pi.rank_pos")) {
                query.setLong(1, end);
                try (ResultSet rows = query.executeQuery()) {
                    while (rows.next()) {
                        assertEquals(actual.size() + 1, rows.getInt(1));
                        actual.add(rows.getString(2) + ":" + rows.getLong(3));
                    }
                }
            }
            assertEquals(expected, actual);
        } finally { db.pool.release(connection); }
    }

    private static CheckoutDatabase open() throws Exception {
        try { return new CheckoutDatabase(2000); }
        catch (SQLException unavailable) { throw new TestAbortedException("MySQL unavailable", unavailable); }
    }
}
