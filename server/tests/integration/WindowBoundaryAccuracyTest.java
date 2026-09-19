package integration;

import analytics.AnalyticsRecorder;
import org.junit.jupiter.api.Test;
import support.AnalyticsDatabase;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class WindowBoundaryAccuracyTest {
    @Test
    void concurrentBurstsPersistExactOverlappingWindows() throws Exception {
        try (AnalyticsDatabase db = new AnalyticsDatabase()) {
            AnalyticsRecorder recorder = new AnalyticsRecorder(db.catalog, db.windows);
            try (ExecutorService workers = Executors.newFixedThreadPool(8)) {
                // Each burst has a known SKU but arbitrary internal thread ordering. Boundaries
                // fall inside bursts, so no test lock serializes recordScan or its snapshot.
                for (int phase = 1; phase <= 3; phase++) {
                    String sku = "sku" + phase;
                    CountDownLatch start = new CountDownLatch(1);
                    List<Future<?>> scans = new ArrayList<>();
                    for (int thread = 0; thread < 8; thread++) {
                        scans.add(workers.submit(() -> {
                            start.await();
                            for (int scan = 0; scan < 100; scan++) recorder.recordScan(sku);
                            return null;
                        }));
                    }
                    start.countDown();
                    for (Future<?> scan : scans) scan.get(10, TimeUnit.SECONDS);
                }
                db.awaitWindow(2000);
                Connection conn = db.pool.borrow();
                try (Statement sql = conn.createStatement(); ResultSet windows = sql.executeQuery(
                        "SELECT window_id, window_start, window_end FROM popular_window ORDER BY window_id")) {
                    int seen = 0;
                    while (windows.next()) {
                        long end = windows.getLong("window_end");
                        assertEquals(++seen * 500L, end);
                        assertEquals(end - 999, windows.getLong("window_start"));
                        Map<String, Long> expected = new HashMap<>();
                        for (long position = Math.max(1, end - 999); position <= end; position++) {
                            expected.merge("sku" + ((position - 1) / 800 + 1), 1L, Long::sum);
                        }
                        Map<String, Long> actual = new HashMap<>();
                        try (PreparedStatement items = conn.prepareStatement(
                                "SELECT sku, scan_count FROM popular_item WHERE window_id=?")) {
                            items.setLong(1, windows.getLong("window_id"));
                            try (ResultSet rows = items.executeQuery()) {
                                while (rows.next()) actual.put(rows.getString(1), rows.getLong(2));
                            }
                        }
                        assertEquals(expected, actual, "Exact counts for boundary " + end);
                    }
                    assertEquals(4, seen);
                    assertEquals(2000, db.windows.readLatestWindow(10).windowEnd());
                } finally {
                    db.pool.release(conn);
                }
            } finally {
                recorder.shutdown();
            }
        }
    }
}
