package integration;

import analytics.AnalyticsService;
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
            AnalyticsService recorder = new AnalyticsService(db.windows);
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
                            for (int scan = 0; scan < 100; scan++) recorder.recordAcceptedScan(sku);
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
                        assertEquals(1000L + seen++ * 500L, end);
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

                        List<Map.Entry<String, Long>> expectedRanks = expected.entrySet().stream()
                                .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                                        .thenComparing(Map.Entry.comparingByKey()))
                                .toList();
                        try (PreparedStatement items = conn.prepareStatement(
                                "SELECT rank_pos, sku, scan_count FROM popular_item "
                                        + "WHERE window_id=? ORDER BY rank_pos")) {
                            items.setLong(1, windows.getLong("window_id"));
                            try (ResultSet rows = items.executeQuery()) {
                                int rank = 0;
                                while (rows.next()) {
                                    Map.Entry<String, Long> expectedRank = expectedRanks.get(rank++);
                                    assertEquals(rank, rows.getInt("rank_pos"));
                                    assertEquals(expectedRank.getKey(), rows.getString("sku"));
                                    assertEquals(expectedRank.getValue(), rows.getLong("scan_count"));
                                }
                                assertEquals(expectedRanks.size(), rank);
                            }
                        }
                    }
                    assertEquals(3, seen);
                    assertEquals(2000, db.windows.readLatest(10).orElseThrow().windowEnd());
                } finally {
                    db.pool.release(conn);
                }
            } finally {
                recorder.shutdown();
            }
        }
    }

    @Test
    void failedRankInsertNeverMakesItsWindowVisible() throws Exception {
        try (AnalyticsDatabase db = new AnalyticsDatabase()) {
            Connection setup = db.pool.borrow();
            try (Statement sql = setup.createStatement()) {
                sql.executeUpdate("ALTER TABLE popular_item ADD CONSTRAINT "
                        + "fk_boundary_popular_sku FOREIGN KEY (sku) REFERENCES catalog_item(sku)");
            } finally {
                db.pool.release(setup);
            }

            AnalyticsService recorder = new AnalyticsService(db.windows, 500);
            try {
                for (int scan = 0; scan < 1000; scan++) recorder.recordAcceptedScan("sku1");
                db.awaitWindow(1000);

                for (int scan = 0; scan < 500; scan++) recorder.recordAcceptedScan("missing-sku");
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (System.nanoTime() < deadline) {
                    if (db.windows.readMaxWindowEnd() > 1000) {
                        fail("A window with failed rank inserts became visible");
                    }
                    Thread.sleep(20);
                }

                var visible = db.windows.readLatest(10).orElseThrow();
                assertEquals(1, visible.windowStart());
                assertEquals(1000, visible.windowEnd());
                assertEquals(List.of(new database.PopularWindowStore.StoredRank(
                        1, "sku1", "Item 1", 1000)), visible.ranks());
            } finally {
                recorder.shutdown();
            }
        }
    }
}
