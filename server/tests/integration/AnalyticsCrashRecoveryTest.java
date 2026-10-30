package integration;

import analytics.AnalyticsService;
import database.PopularWindowStore;
import database.StoreFailure;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.FaultInjection;
import support.InProcessAnalytics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Analytics recovery from {@code analytics_checkpoint} and the 1,000-position overlap (FR-048), over an
 * {@link AnalyticsTestSchema} seeded through {@code ScanAccepted} records that its scan consumer ingests. Every
 * published window must appear exactly once and equal a recomputation over the durable positions; no partial or
 * duplicate window is ever visible. An ingestion that crashes before or after its commit is read again from the
 * broker until it is stored once (US5 scenarios 3-5).
 */
class AnalyticsCrashRecoveryTest {
    @Test
    void outageBeforeReadReplaysEveryEligibleWindow() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            analytics.deliverAll("tx-a", stream(0, 1500));
            catchUp(schema, analytics);
            schema.assertWindowsMatchPositions(1500);
        }
    }

    @Test
    void interruptionDuringPartialAssemblyReplaysFromCheckpoint() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            analytics.deliverAll("tx-a", stream(0, 750));
            AnalyticsService first = analytics.startPipeline();
            Thread.sleep(200);
            first.shutdown();
            assertEquals(0, analytics.windows.readMaxWindowEnd());
            analytics.deliverAll("tx-b", stream(750, 750));
            catchUp(schema, analytics);
            schema.assertWindowsMatchPositions(1500);
        }
    }

    @Test
    void failureBeforeWindowCommitReplaysCandidate() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            analytics.deliverAll("tx-a", stream(0, 1500));
            InterruptingStore failed = new InterruptingStore(analytics.windows, false);
            AnalyticsService first = analytics.startPipeline(failed);
            assertTrue(failed.firstWrite.await(5, TimeUnit.SECONDS));
            first.shutdown();
            assertEquals(0, analytics.windows.readMaxWindowEnd());
            assertTrue(schema.windowEnds().isEmpty(), "no partial window is visible");
            catchUp(schema, analytics);
            schema.assertWindowsMatchPositions(1500);
        }
    }

    @Test
    void failureAfterCommitBeforeAcknowledgementDoesNotDuplicateWindow() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            analytics.deliverAll("tx-a", stream(0, 1500));
            InterruptingStore failed = new InterruptingStore(analytics.windows, true);
            AnalyticsService first = analytics.startPipeline(failed);
            assertTrue(failed.firstWrite.await(5, TimeUnit.SECONDS));
            awaitWindow(analytics, 1000);
            first.shutdown();
            catchUp(schema, analytics);
            schema.assertWindowsMatchPositions(1500);
        }
    }

    @Test
    void ingestionCrashBeforeCommitIsRedeliveredWithoutGapsOrDuplicateWindows() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            AnalyticsService pipeline = analytics.startPipeline();
            try {
                analytics.deliver(InProcessAnalytics.events("tx-a", 1, stream(0, 800)));
                List<String> second = stream(800, 700);
                long storedBefore = analytics.stored();
                try (FaultInjection.Armed armed = FaultInjection.spec().fail("analytics.ingest.beforeCommit").armInThisJvm()) {
                    analytics.publish(InProcessAnalytics.events("tx-a", 801, second));
                    analytics.awaitFailures(2, Duration.ofSeconds(10));
                    assertEquals(800, schema.lastPosition(), "the crashed batch left no position");
                    Thread.sleep(300);
                    assertTrue(schema.windowEnds().isEmpty(), "no window exists before 1,000 positions");
                    assertEquals(0, analytics.consumer.parked(), "a crashed ingestion is read again, never parked");
                }
                analytics.awaitConsumed(Duration.ofSeconds(30));
                assertEquals(700, analytics.stored() - storedBefore, "the records are read again and stored once");
                awaitWindow(analytics, 1500);
            } finally {
                pipeline.shutdown();
            }
            schema.assertPositionsContiguous();
            schema.assertWindowsMatchPositions(1500);
        }
    }

    @Test
    void ingestionCrashAfterCommitIsRedeliveredAsDuplicatesWithoutDuplicateWindows() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            AnalyticsService pipeline = analytics.startPipeline();
            try {
                List<String> first = stream(0, 1000);
                long committed;
                try (FaultInjection.Armed armed = FaultInjection.spec().fail("analytics.ingest.afterCommit").armInThisJvm()) {
                    analytics.publish(InProcessAnalytics.events("tx-a", 1, first));
                    analytics.awaitFailures(2, Duration.ofSeconds(10));
                    // A poll returns at most 500 records, all of which the crashed batch committed.
                    committed = schema.lastPosition();
                    assertTrue(committed >= 1 && committed <= 500, "the batch committed before the crash: " + committed);
                }
                analytics.awaitConsumed(Duration.ofSeconds(30));
                assertEquals(1000, schema.lastPosition(), "the committed records were read again without new positions");
                assertTrue(analytics.duplicates() >= committed, "every committed record read again is a duplicate");
                awaitWindow(analytics, 1000);
                // The relay re-sends a batch whose marking crashed: every record is a duplicate.
                var redelivered = analytics.deliver(InProcessAnalytics.events("tx-a", 1, first));
                assertEquals(0, redelivered.stored());
                assertEquals(1000, redelivered.duplicates());
                assertEquals(500, analytics.deliver(InProcessAnalytics.events("tx-a", 1001, stream(1000, 500))).stored());
                awaitWindow(analytics, 1500);
            } finally {
                pipeline.shutdown();
            }
            schema.assertPositionsContiguous();
            schema.assertWindowsMatchPositions(1500);
        }
    }

    private static void catchUp(AnalyticsTestSchema schema, InProcessAnalytics analytics) throws Exception {
        AnalyticsService service = analytics.startPipeline();
        try { awaitWindow(analytics, 1500); }
        finally { service.shutdown(); }
    }

    private static void awaitWindow(InProcessAnalytics analytics, long end) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (analytics.windows.readMaxWindowEnd() < end) {
            if (System.nanoTime() >= deadline) throw new AssertionError("Checkpoint did not persist: " + end);
            Thread.sleep(10);
        }
    }

    /** {@code size} SKUs of the legacy fixture stream, starting at stream index {@code from}. */
    private static List<String> stream(int from, int size) {
        List<String> skus = new ArrayList<>();
        for (int i = from; i < from + size; i++) skus.add("sku" + (1 + (i * 7) % 12));
        return skus;
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
