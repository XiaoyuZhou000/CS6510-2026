package unit;

import analytics.AnalyticsService;
import database.PopularWindowStore;
import database.StoreFailure;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class AnalyticsRecorderRecoveryTest {
    @Test
    void freshStartAndRestartBothWaitForACompleteThousandScanBuffer() throws Exception {
        RecoveringStore fresh = new RecoveringStore(0, 0);
        AnalyticsService freshRecorder = new AnalyticsService(fresh);
        try {
            for (int i = 0; i < 999; i++) freshRecorder.recordAcceptedScan("sku-a");
            assertEquals(0, fresh.attempts.get());
            freshRecorder.recordAcceptedScan("sku-a");
            assertTrue(fresh.firstPersist.await(2, TimeUnit.SECONDS));
            assertEquals(List.of(1L), fresh.persistedStarts);
            assertEquals(List.of(1000L), fresh.persistedEnds);
        } finally {
            freshRecorder.shutdown();
        }

        RecoveringStore restarted = new RecoveringStore(0, 1000);
        AnalyticsService restartedRecorder = new AnalyticsService(restarted);
        try {
            for (int i = 0; i < 500; i++) restartedRecorder.recordAcceptedScan("sku-b");
            assertEquals(0, restarted.attempts.get(),
                "the first post-restart half-window must be skipped");
            for (int i = 0; i < 500; i++) restartedRecorder.recordAcceptedScan("sku-b");
            assertTrue(restarted.firstPersist.await(2, TimeUnit.SECONDS));
            assertEquals(List.of(1001L), restarted.persistedStarts);
            assertEquals(List.of(2000L), restarted.persistedEnds);
        } finally {
            restartedRecorder.shutdown();
        }
    }

    @Test
    void transientFailuresBeyondFastRetryBudgetAreEventuallyPersisted() throws Exception {
        RecoveringStore store = new RecoveringStore(5, 0);
        AnalyticsService recorder = new AnalyticsService(store);
        try {
            for (int i = 0; i < 1000; i++) recorder.recordAcceptedScan("sku-a");
            assertTrue(store.firstPersist.await(8, TimeUnit.SECONDS));
            assertEquals(List.of(1000L), store.persistedEnds);
            assertEquals(6, store.attempts.get());
        } finally {
            recorder.shutdown();
        }
    }

    @Test
    void prolongedFailureRetainsOldestCheckpointWithoutBlockingScans() throws Exception {
        RecoveringStore store = new RecoveringStore(Integer.MAX_VALUE, 0);
        AnalyticsService recorder = new AnalyticsService(store);
        try {
            long started = System.nanoTime();
            for (int i = 0; i < 1500; i++) recorder.recordAcceptedScan("sku-a");
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue(elapsedMillis < 1000, "scan recording must not wait for persistence");
            assertTrue(store.firstAttempt.await(2, TimeUnit.SECONDS));
            assertTrue(store.persistedEnds.isEmpty());

            store.recover.set(true);
            assertTrue(store.twoPersists.await(5, TimeUnit.SECONDS));
            assertEquals(List.of(1000L, 1500L), store.persistedEnds,
                "a newer checkpoint must never overtake the oldest failed checkpoint");
        } finally {
            recorder.shutdown();
        }
    }

    private static final class RecoveringStore implements PopularWindowStore {
        private final AtomicInteger failuresRemaining;
        private final long initialWindowEnd;
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicBoolean recover = new AtomicBoolean();
        final CountDownLatch firstAttempt = new CountDownLatch(1);
        final CountDownLatch firstPersist = new CountDownLatch(1);
        final CountDownLatch twoPersists = new CountDownLatch(2);
        final List<Long> persistedStarts = java.util.Collections.synchronizedList(new ArrayList<>());
        final List<Long> persistedEnds = java.util.Collections.synchronizedList(new ArrayList<>());

        RecoveringStore(int failures, long initialWindowEnd) {
            failuresRemaining = new AtomicInteger(failures);
            this.initialWindowEnd = initialWindowEnd;
        }

        @Override
        public long readMaxWindowEnd() {
            return initialWindowEnd;
        }

        @Override
        public void writeWindow(PopularWindowSnapshot snapshot) {
            attempts.incrementAndGet();
            firstAttempt.countDown();
            if (!recover.get() && failuresRemaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                throw new StoreFailure("injected persistence failure");
            }
            persistedStarts.add(snapshot.windowStart());
            persistedEnds.add(snapshot.windowEnd());
            firstPersist.countDown();
            twoPersists.countDown();
        }

        @Override
        public Optional<PopularWindowView> readLatest(int limit) {
            return Optional.empty();
        }
    }
}
