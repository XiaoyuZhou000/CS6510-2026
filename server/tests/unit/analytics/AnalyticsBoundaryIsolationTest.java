package unit.analytics;

import analytics.AnalyticsOperations;
import analytics.AnalyticsService;
import database.PopularWindowStore;
import database.StoreFailure;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class AnalyticsBoundaryIsolationTest {
    @Test
    void analyticsContractExposesNoApiOrJdbcTypes() {
        for (Method method : AnalyticsOperations.class.getDeclaredMethods()) {
            assertBoundaryType(method.getReturnType());
            for (Class<?> parameter : method.getParameterTypes()) assertBoundaryType(parameter);
        }
    }

    @Test
    void serviceOwnsWindowBoundsCountsAndDeterministicRanking() throws Exception {
        FakeWindowStore store = new FakeWindowStore();
        AnalyticsService service = new AnalyticsService(store);
        try {
            for (int scan = 0; scan < 400; scan++) service.recordAcceptedScan("B");
            for (int scan = 0; scan < 400; scan++) service.recordAcceptedScan("A");
            for (int scan = 0; scan < 200; scan++) service.recordAcceptedScan("C");

            assertTrue(store.awaitWrites(1));
            PopularWindowStore.PopularWindowSnapshot window = store.writes.getFirst();
            assertEquals(1, window.windowStart());
            assertEquals(1000, window.windowEnd());
            assertEquals(List.of(
                    new PopularWindowStore.SnapshotRank(1, "A", 400),
                    new PopularWindowStore.SnapshotRank(2, "B", 400),
                    new PopularWindowStore.SnapshotRank(3, "C", 200)), window.ranks());
        } finally {
            service.shutdown();
        }
    }

    @Test
    void failedOlderWindowRetriesBeforeNewerWindowAndOnlyStoreStateIsVisible() throws Exception {
        FakeWindowStore store = new FakeWindowStore();
        store.failWrites.set(true);
        AnalyticsService service = new AnalyticsService(store);
        try {
            for (int scan = 0; scan < 1500; scan++) service.recordAcceptedScan("A");
            assertTrue(store.awaitAttempts());
            assertEquals(AnalyticsOperations.PopularItemsView.empty(), service.latestPopularItems(10));

            store.failWrites.set(false);
            assertTrue(store.awaitWrites(2));
            assertEquals(List.of(1000L, 1500L), store.writes.stream()
                    .map(PopularWindowStore.PopularWindowSnapshot::windowEnd).toList());

            store.latest = Optional.of(new PopularWindowStore.PopularWindowView(
                    2, 501, 1500, Instant.parse("2026-09-23T12:00:00Z"), List.of(
                    new PopularWindowStore.StoredRank(1, "A", "Apple", 1000))));
            AnalyticsOperations.PopularItemsView visible = service.latestPopularItems(1);
            assertEquals(1500, visible.windowEnd());
            assertEquals("Apple", visible.items().getFirst().name());
        } finally {
            service.shutdown();
        }
    }

    private static void assertBoundaryType(Class<?> type) {
        String name = type.getName();
        assertFalse(name.startsWith("api."), name);
        assertFalse(name.startsWith("com.sun.net.httpserver"), name);
        assertFalse(name.startsWith("java.sql"), name);
    }

    private static final class FakeWindowStore implements PopularWindowStore {
        private final List<PopularWindowSnapshot> writes =
                Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean failWrites = new AtomicBoolean();
        private volatile int attempts;
        private volatile Optional<PopularWindowView> latest = Optional.empty();

        @Override public long readMaxWindowEnd() { return 0; }

        @Override
        public void writeWindow(PopularWindowSnapshot snapshot) {
            attempts++;
            if (failWrites.get()) throw new StoreFailure("injected failure");
            writes.add(snapshot);
        }

        @Override
        public Optional<PopularWindowView> readLatest(int limit) {
            return latest.map(view -> new PopularWindowView(view.windowId(), view.windowStart(),
                    view.windowEnd(), view.computedAt(),
                    view.ranks().subList(0, Math.min(limit, view.ranks().size()))));
        }

        boolean awaitAttempts() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (attempts == 0 && System.nanoTime() < deadline) Thread.sleep(10);
            return attempts > 0;
        }

        boolean awaitWrites(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(6);
            while (writes.size() < count && System.nanoTime() < deadline) Thread.sleep(10);
            return writes.size() >= count;
        }
    }
}
