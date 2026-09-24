package unit.analytics;

import analytics.AnalyticsOperations;
import analytics.AnalyticsService;
import database.PopularWindowStore;
import database.StoreFailure;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class AnalyticsServiceTest {
    @Test
    void firstFiveHundredScansRemainAPartialUnpublishedWindow() {
        FakeStore store = new FakeStore(0);
        AnalyticsService service = new AnalyticsService(store);
        try {
            for (int scan = 0; scan < 500; scan++) service.recordAcceptedScan("A");
            assertTrue(store.snapshots.isEmpty());
            assertEquals(AnalyticsOperations.PopularItemsView.empty(), service.latestPopularItems(10));
        } finally {
            service.shutdown();
        }
    }

    @Test
    void exactRingWindowAndDeterministicRankingArePersistedEveryFiveHundred() throws Exception {
        FakeStore store = new FakeStore(0);
        AnalyticsService service = new AnalyticsService(store);
        try {
            for (int scan = 0; scan < 500; scan++) service.recordAcceptedScan("B");
            for (int scan = 0; scan < 500; scan++) service.recordAcceptedScan("A");
            assertTrue(store.awaitSnapshots(1));
            assertEquals(1, store.snapshots.get(0).windowStart());
            assertEquals(1000, store.snapshots.get(0).windowEnd());
            assertEquals(List.of(
                    new PopularWindowStore.SnapshotRank(1, "A", 500),
                    new PopularWindowStore.SnapshotRank(2, "B", 500)), store.snapshots.get(0).ranks());

            for (int scan = 0; scan < 500; scan++) service.recordAcceptedScan("C");
            assertTrue(store.awaitSnapshots(2));
            assertEquals(501, store.snapshots.get(1).windowStart());
            assertEquals(1500, store.snapshots.get(1).windowEnd());
            assertEquals(List.of(
                    new PopularWindowStore.SnapshotRank(1, "A", 500),
                    new PopularWindowStore.SnapshotRank(2, "C", 500)), store.snapshots.get(1).ranks());
        } finally {
            service.shutdown();
        }
    }

    @Test
    void restartSkipsTheFirstInvalidBoundaryThenPersistsInOrder() throws Exception {
        FakeStore store = new FakeStore(1000);
        AnalyticsService service = new AnalyticsService(store);
        try {
            for (int scan = 0; scan < 500; scan++) service.recordAcceptedScan("A");
            assertTrue(store.snapshots.isEmpty());
            for (int scan = 0; scan < 500; scan++) service.recordAcceptedScan("B");
            assertTrue(store.awaitSnapshots(1));
            assertEquals(1001, store.snapshots.getFirst().windowStart());
            assertEquals(2000, store.snapshots.getFirst().windowEnd());
        } finally {
            service.shutdown();
        }
    }

    @Test
    void failedCheckpointRetriesInPlaceAndNeverLetsANewerWindowOvertakeIt() throws Exception {
        FakeStore store = new FakeStore(0);
        store.failWrites.set(true);
        AnalyticsService service = new AnalyticsService(store);
        try {
            for (int scan = 0; scan < 1500; scan++) service.recordAcceptedScan("A");
            assertTrue(store.firstAttempt.await(2, TimeUnit.SECONDS));
            assertTrue(store.snapshots.isEmpty());
            store.failWrites.set(false);
            assertTrue(store.awaitSnapshots(2));
            assertEquals(List.of(1000L, 1500L),
                    store.snapshots.stream().map(PopularWindowStore.PopularWindowSnapshot::windowEnd).toList());
            assertTrue(store.attempts.get() >= 3);
        } finally {
            service.shutdown();
        }
    }

    @Test
    void latestReadsOnlyCommittedStoreStateAndShutdownRejectsNewScans() {
        FakeStore store = new FakeStore(0);
        store.latest = Optional.of(new PopularWindowStore.PopularWindowView(7, 501, 1500,
                Instant.parse("2026-09-23T00:00:00Z"), List.of(
                new PopularWindowStore.StoredRank(1, "A", "Apple", 700))));
        AnalyticsService service = new AnalyticsService(store);

        var latest = service.latestPopularItems(3);
        assertEquals(1500, latest.windowEnd());
        assertEquals("Apple", latest.items().getFirst().name());
        assertEquals(3, store.lastReadLimit);

        service.shutdown();
        assertThrows(IllegalStateException.class, () -> service.recordAcceptedScan("A"));
    }

    private static final class FakeStore implements PopularWindowStore {
        private final long recoveredEnd;
        private final List<PopularWindowSnapshot> snapshots =
                Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean failWrites = new AtomicBoolean();
        private final AtomicInteger attempts = new AtomicInteger();
        private final CountDownLatch firstAttempt = new CountDownLatch(1);
        private volatile Optional<PopularWindowView> latest = Optional.empty();
        private volatile int lastReadLimit = -1;

        FakeStore(long recoveredEnd) {
            this.recoveredEnd = recoveredEnd;
        }

        @Override
        public long readMaxWindowEnd() {
            return recoveredEnd;
        }

        @Override
        public void writeWindow(PopularWindowSnapshot snapshot) {
            attempts.incrementAndGet();
            firstAttempt.countDown();
            if (failWrites.get()) throw new StoreFailure("injected failure");
            snapshots.add(snapshot);
        }

        @Override
        public Optional<PopularWindowView> readLatest(int limit) {
            lastReadLimit = limit;
            return latest.map(window -> new PopularWindowView(window.windowId(), window.windowStart(),
                    window.windowEnd(), window.computedAt(),
                    window.ranks().subList(0, Math.min(limit, window.ranks().size()))));
        }

        boolean awaitSnapshots(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(6);
            while (snapshots.size() < count && System.nanoTime() < deadline) Thread.sleep(10);
            return snapshots.size() >= count;
        }
    }
}
