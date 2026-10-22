package database;

import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Atomic window publication over an {@link AnalyticsTestSchema}. Ranks reference {@code product_name}, so the
 * fixture names {@code sku1..sku12} ("Item n") are seeded as ingestion would have written them.
 */
class JdbcPopularWindowStoreTest {
    @Test
    void recoversMaximumEndAndReadsLatestRanksInStoredOrderWithALimit() throws Exception {
        try (AnalyticsTestSchema schema = seeded()) {
            JdbcPopularWindowStore windows = new JdbcPopularWindowStore(schema.pool());
            assertEquals(0, windows.readMaxWindowEnd());
            assertTrue(windows.readLatest(10).isEmpty());

            windows.writeWindow(snapshot(1, 1000, rank(1, "sku2", 60), rank(2, "sku1", 40)));
            windows.writeWindow(snapshot(501, 1500, rank(1, "sku3", 70), rank(2, "sku1", 30)));

            assertEquals(1500, windows.readMaxWindowEnd());
            var latest = windows.readLatest(1).orElseThrow();
            assertEquals(1500, latest.windowEnd());
            assertEquals(List.of("sku3"), latest.ranks().stream().map(r -> r.sku()).toList());
            assertEquals("Item 3", latest.ranks().getFirst().name(), "names come from product_name");
            assertEquals(List.of("Item 3", "Item 1"),
                    windows.readLatest(10).orElseThrow().ranks().stream().map(r -> r.name()).toList());
            assertTrue(windows.readLatest(0).orElseThrow().ranks().isEmpty());
        }
    }

    @Test
    void headerAndRanksRollBackTogetherWhenAnyRankFails() throws Exception {
        try (AnalyticsTestSchema schema = seeded()) {
            JdbcPopularWindowStore windows = new JdbcPopularWindowStore(schema.pool());
            // popular_item.sku references product_name; a SKU no ingestion ever named fails the rank insert.
            PopularWindowStore.PopularWindowSnapshot invalidForeignKey = snapshot(1, 1000, rank(1, "missing-sku", 1000));

            assertThrows(StoreFailure.class, () -> windows.writeWindow(invalidForeignKey));
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM popular_window"));
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM popular_item"));
            assertEquals(0, schema.checkpoint());
        }
    }

    @Test
    void equivalentAlreadyCommittedWindowIsRecognizedWithoutDuplicatingItsBoundary() throws Exception {
        try (AnalyticsTestSchema schema = seeded()) {
            JdbcPopularWindowStore windows = new JdbcPopularWindowStore(schema.pool());
            PopularWindowStore.PopularWindowSnapshot window = snapshot(1, 1000, rank(1, "sku2", 60), rank(2, "sku1", 40));

            windows.writeWindow(window);
            assertDoesNotThrow(() -> windows.writeWindow(window));

            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM popular_window"));
            assertEquals(2, schema.queryLong("SELECT COUNT(*) FROM popular_item"));
            assertThrows(IllegalStateException.class, () -> windows.writeWindow(snapshot(1, 1000, rank(1, "sku1", 1000))));
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM popular_window"));
        }
    }

    @Test
    void checkpointRejectsOutOfOrderBoundariesAndTracksOnlyCompleteWrites() throws Exception {
        try (AnalyticsTestSchema schema = seeded()) {
            JdbcPopularWindowStore windows = new JdbcPopularWindowStore(schema.pool());
            assertEquals(0, schema.checkpoint());
            assertThrows(IllegalStateException.class, () -> windows.writeWindow(snapshot(501, 1500, rank(1, "sku1", 1000))));
            assertEquals(0, schema.checkpoint());
            assertTrue(windows.readLatest(10).isEmpty());

            windows.writeWindow(snapshot(1, 1000, rank(1, "sku1", 1000)));
            assertEquals(1000, schema.checkpoint());
            assertThrows(IllegalStateException.class, () -> windows.writeWindow(snapshot(1001, 2000, rank(1, "sku1", 1000))));
            assertEquals(1000, windows.readLatest(10).orElseThrow().windowEnd());
            windows.writeWindow(snapshot(501, 1500, rank(1, "sku2", 1000)));
            assertEquals(1500, schema.checkpoint());
        }
    }

    /** Constitution II: two Analytics instances publish the same windows concurrently over one schema. */
    @Test
    void twoStoresWritingTheSameWindowsPublishEachExactlyOnceAndOnlyMoveForward() throws Exception {
        try (AnalyticsTestSchema schema = seeded(); ExecutorService workers = Executors.newFixedThreadPool(3)) {
            JdbcPopularWindowStore one = new JdbcPopularWindowStore(schema.newPool("test.windows.one", 2, 2, null));
            JdbcPopularWindowStore two = new JdbcPopularWindowStore(schema.newPool("test.windows.two", 2, 2, null));
            List<PopularWindowStore.PopularWindowSnapshot> sequence = new ArrayList<>();
            for (int n = 0; n < 12; n++) {
                long end = 1000 + n * 500L;
                sequence.add(snapshot(end - 999, end, rank(1, "sku" + (n % 11 + 1), 600), rank(2, "sku12", 400)));
            }
            CountDownLatch go = new CountDownLatch(1);
            AtomicBoolean writing = new AtomicBoolean(true);
            List<Long> observed = Collections.synchronizedList(new ArrayList<>());
            Future<?> sampler = workers.submit(() -> {
                JdbcPopularWindowStore reader = new JdbcPopularWindowStore(schema.pool());
                while (writing.get()) observed.add(reader.readMaxWindowEnd());
                return null;
            });
            List<Callable<Void>> writers = new ArrayList<>();
            for (JdbcPopularWindowStore store : List.of(one, two)) {
                writers.add(() -> {
                    go.await();
                    for (PopularWindowStore.PopularWindowSnapshot window : sequence) store.writeWindow(window);
                    return null;
                });
            }
            List<Future<Void>> results = new ArrayList<>();
            for (Callable<Void> writer : writers) results.add(workers.submit(writer));
            go.countDown();
            for (Future<Void> result : results) assertDoesNotThrow(() -> result.get(), "a published boundary is a verified no-op");
            writing.set(false);
            sampler.get();

            assertEquals(12, schema.queryLong("SELECT COUNT(*) FROM popular_window"));
            assertEquals(24, schema.queryLong("SELECT COUNT(*) FROM popular_item"));
            assertEquals(6_500, schema.checkpoint());
            for (PopularWindowStore.PopularWindowSnapshot window : sequence) {
                assertEquals(List.of(window.ranks().get(0).sku() + ":600", "sku12:400"), schema.storedRanks(window.windowEnd()));
            }
            for (int i = 1; i < observed.size(); i++) {
                assertTrue(observed.get(i) >= observed.get(i - 1), "the checkpoint only moves forward: " + observed);
            }
        }
    }

    private static AnalyticsTestSchema seeded() throws Exception {
        AnalyticsTestSchema schema = AnalyticsTestSchema.create();
        for (int i = 1; i <= 12; i++) schema.seedName("sku" + i, "Item " + i);
        return schema;
    }

    private static PopularWindowStore.PopularWindowSnapshot snapshot(
            long start, long end, PopularWindowStore.SnapshotRank... ranks) {
        return new PopularWindowStore.PopularWindowSnapshot(start, end, List.of(ranks));
    }

    private static PopularWindowStore.SnapshotRank rank(int rank, String sku, long count) {
        return new PopularWindowStore.SnapshotRank(rank, sku, count);
    }
}
