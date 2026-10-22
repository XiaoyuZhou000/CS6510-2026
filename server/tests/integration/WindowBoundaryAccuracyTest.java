package integration;

import analytics.AnalyticsOperations;
import analytics.AnalyticsService;
import database.PopularWindowStore;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.InProcessAnalytics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exact overlapping windows (FR-044, FR-045) over an {@link AnalyticsTestSchema}. Bursts arrive through
 * concurrent {@code DeliverScanEvents} calls with arbitrary interleaving; boundaries fall inside bursts.
 */
public class WindowBoundaryAccuracyTest {
    @Test
    void concurrentBurstsPersistExactOverlappingWindows() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(AnalyticsTestSchema.Options.defaults().pool(8, 12));
             InProcessAnalytics analytics = InProcessAnalytics.over(schema);
             ExecutorService workers = Executors.newFixedThreadPool(8)) {
            AnalyticsService recorder = analytics.startPipeline();
            try {
                AtomicInteger transactions = new AtomicInteger();
                // Each burst has one known SKU but arbitrary internal ordering: 8 callers x 10 batches of 10.
                for (int phase = 1; phase <= 3; phase++) {
                    String sku = "sku" + phase;
                    CountDownLatch start = new CountDownLatch(1);
                    List<Future<?>> deliveries = new ArrayList<>();
                    for (int thread = 0; thread < 8; thread++) {
                        deliveries.add(workers.submit(() -> {
                            start.await();
                            for (int batch = 0; batch < 10; batch++) {
                                String id = "tx-" + transactions.incrementAndGet();
                                analytics.deliver(InProcessAnalytics.events(id, 1, java.util.Collections.nCopies(10, sku)));
                            }
                            return null;
                        }));
                    }
                    start.countDown();
                    for (Future<?> delivery : deliveries) delivery.get(30, TimeUnit.SECONDS);
                    if (phase == 1) {
                        Thread.sleep(300);
                        AnalyticsOperations.PopularItemsView empty = recorder.latestPopularItems(10);
                        assertEquals(0, empty.windowEnd(), "under 1,000 scans the established empty response is served");
                        assertTrue(empty.items().isEmpty());
                    }
                }
                awaitWindow(analytics, 2000);
                List<Long> ends = schema.windowEnds();
                assertEquals(List.of(1000L, 1500L, 2000L), ends);
                for (long end : ends) {
                    assertEquals(end - 999, schema.queryLong("SELECT window_start FROM popular_window WHERE window_end = ?", end));
                    Map<String, Long> expected = new HashMap<>();
                    for (long position = end - 999; position <= end; position++) {
                        expected.merge("sku" + ((position - 1) / 800 + 1), 1L, Long::sum);
                    }
                    Map<String, Long> actual = new HashMap<>();
                    for (AnalyticsTestSchema.Rank rank : schema.ranks(end)) actual.put(rank.sku(), rank.scanCount());
                    assertEquals(expected, actual, "Exact counts for boundary " + end);

                    List<Map.Entry<String, Long>> expectedRanks = expected.entrySet().stream()
                            .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                                    .thenComparing(Map.Entry.comparingByKey()))
                            .toList();
                    List<AnalyticsTestSchema.Rank> stored = schema.ranks(end);
                    assertEquals(expectedRanks.size(), stored.size());
                    for (int rank = 0; rank < stored.size(); rank++) {
                        assertEquals(rank + 1, stored.get(rank).rankPos());
                        assertEquals(expectedRanks.get(rank).getKey(), stored.get(rank).sku());
                        assertEquals(expectedRanks.get(rank).getValue(), stored.get(rank).scanCount());
                    }
                    assertEquals(schema.expectedRanks(end), schema.storedRanks(end), "independent recomputation at " + end);
                }
                assertEquals(2000, analytics.windows.readLatest(10).orElseThrow().windowEnd());
            } finally {
                recorder.shutdown();
            }
        }
    }

    @Test
    void tiesBreakByAscendingSkuAndAtMostTenItemsArePublished() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            List<String> skus = new ArrayList<>();
            // 13 SKUs: sku13..sku1 each 70 times, then 90 more of sku7, in reverse SKU order of first arrival.
            for (int sku = 13; sku >= 1; sku--) for (int n = 0; n < 70; n++) skus.add("sku" + sku);
            for (int n = 0; n < 90; n++) skus.add("sku7");
            analytics.deliverAll("tx-ties", skus);
            AnalyticsService recorder = analytics.startPipeline();
            try {
                awaitWindow(analytics, 1000);
            } finally {
                recorder.shutdown();
            }
            List<String> stored = schema.storedRanks(1000);
            assertEquals(10, stored.size());
            assertEquals(List.of("sku7:160", "sku1:70", "sku10:70", "sku11:70", "sku12:70", "sku13:70", "sku2:70",
                    "sku3:70", "sku4:70", "sku5:70"), stored);
            assertEquals(schema.expectedRanks(1000), stored);
        }
    }

    @Test
    void failedRankInsertNeverMakesItsWindowVisible() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            AnalyticsService recorder = analytics.startPipeline();
            try {
                analytics.deliverAll("tx-good", java.util.Collections.nCopies(1000, "sku1"));
                awaitWindow(analytics, 1000);

                // Positions 1,001..1,500 name a SKU no ingestion ever named, so popular_item's foreign key
                // to product_name fails every rank insert of window 1,500.
                schema.insertPositioned(1001, 1500, "missing-sku");
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (System.nanoTime() < deadline) {
                    if (analytics.windows.readMaxWindowEnd() > 1000) {
                        fail("A window with failed rank inserts became visible");
                    }
                    Thread.sleep(20);
                }

                var visible = analytics.windows.readLatest(10).orElseThrow();
                assertEquals(1, visible.windowStart());
                assertEquals(1000, visible.windowEnd());
                assertEquals(List.of(new PopularWindowStore.StoredRank(1, "sku1", "Item 1", 1000)), visible.ranks());
                assertEquals(List.of(1000L), schema.windowEnds());
            } finally {
                recorder.shutdown();
            }
        }
    }

    private static void awaitWindow(InProcessAnalytics analytics, long end) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (analytics.windows.readMaxWindowEnd() < end) {
            if (System.nanoTime() >= deadline) throw new AssertionError("Checkpoint did not persist: " + end);
            Thread.sleep(10);
        }
    }
}
