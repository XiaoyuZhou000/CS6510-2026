package integration;

import analytics.AnalyticsService;
import database.JdbcRelayStore;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.InProcessAnalytics;
import support.TransactionTestSchema;
import transaction.ScanEventRelay;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Popular-items visibility delay over broker delivery (FR-055, SC-008; contracts/scan-event-delivery.md section 6).
 * A steady healthy stream at 800 scans per second, above the default workload's measured 765 (CP1: 45,877 scans in
 * 60 s), is committed into Transaction's {@code accepted_scan}; its real relay publishes through
 * {@code KafkaScanEventSink} onto the bus, and Analytics' real consumer and window pipeline publish the windows.
 * From the commit of each boundary scan to the publication of its window takes under 5 s at p99, and once the
 * stream stops the highest eligible boundary is published within 60 s.
 */
class AnalyticsVisibilityLagTest {
    private static final int SCANS_PER_SECOND = 800;
    private static final int ROWS_PER_INSERT = 8;
    private static final int SCANS = 12_400;
    private static final long P99_LIMIT_NANOS = TimeUnit.SECONDS.toNanos(5);

    @Test
    void boundaryScansBecomeVisibleWithinFiveSecondsAtP99AndTheLastBoundaryWithinSixtySeconds() throws Exception {
        try (TransactionTestSchema transactions = TransactionTestSchema.create();
             AnalyticsTestSchema schema = AnalyticsTestSchema.create(AnalyticsTestSchema.Options.defaults().pool(4, 8));
             InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            AnalyticsService pipeline = analytics.startPipeline();
            ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(transactions.pool()), analytics.sink(),
                    ScanEventRelay.Settings.defaults());
            long[] committedAt = new long[SCANS + 1];
            long[] publishedAt = new long[SCANS + 1];
            AtomicBoolean watching = new AtomicBoolean(true);
            AtomicLong published = new AtomicLong();
            Thread watcher = Thread.ofPlatform().daemon().start(() -> {
                while (watching.get()) {
                    long end = analytics.windows.readMaxWindowEnd();
                    long now = System.nanoTime();
                    for (long boundary = published.get() + 1; boundary <= end; boundary++) {
                        if (publishedAt[(int) boundary] == 0) publishedAt[(int) boundary] = now;
                    }
                    published.set(Math.max(published.get(), end));
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException interrupted) {
                        return;
                    }
                }
            });
            relay.start();
            try {
                long started = System.nanoTime();
                long interval = TimeUnit.SECONDS.toNanos(1) * ROWS_PER_INSERT / SCANS_PER_SECOND;
                for (int first = 1; first <= SCANS; first += ROWS_PER_INSERT) {
                    long due = started + (first - 1) / ROWS_PER_INSERT * interval;
                    long wait = due - System.nanoTime();
                    if (wait > 0) TimeUnit.NANOSECONDS.sleep(wait);
                    int rows = Math.min(ROWS_PER_INSERT, SCANS - first + 1);
                    String transactionId = "tx-lag-" + (first - 1) / 400;
                    int sequence = (first - 1) % 400 + 1;
                    if (sequence == 1) transactions.insertOpenTransaction(transactionId);
                    transactions.insertScans(transactionId, sequence, rows, n -> "sku" + (1 + n % 12));
                    long committed = System.nanoTime();
                    for (int scan = first; scan < first + rows; scan++) committedAt[scan] = committed;
                }
                long stopped = System.nanoTime();
                long highest = 1_000 + (SCANS - 1_000) / 500 * 500;
                while (published.get() < highest) {
                    if (System.nanoTime() - stopped > TimeUnit.SECONDS.toNanos(60)) {
                        fail("The highest eligible boundary " + highest + " was not published within 60 s; at "
                                + published.get());
                    }
                    Thread.sleep(20);
                }
                List<Long> lags = new ArrayList<>();
                for (long boundary = 1_000; boundary <= highest; boundary += 500) {
                    lags.add(publishedAt[(int) boundary] - committedAt[(int) boundary]);
                }
                Collections.sort(lags);
                long p99 = lags.get((int) Math.ceil(lags.size() * 0.99) - 1);
                assertTrue(p99 < P99_LIMIT_NANOS, "boundary visibility p99 " + TimeUnit.NANOSECONDS.toMillis(p99)
                        + " ms over " + lags.size() + " boundaries; all: " + lags.stream().map(TimeUnit.NANOSECONDS::toMillis).toList());
                assertTrue(stopped - started < TimeUnit.SECONDS.toNanos(30), "the stream kept its steady rate");
                System.out.println("[visibility] boundary p99 " + TimeUnit.NANOSECONDS.toMillis(p99) + " ms, max "
                        + TimeUnit.NANOSECONDS.toMillis(lags.getLast()) + " ms over " + lags.size() + " boundaries");
                // The scans after the last boundary are counted too, though they complete no window.
                while (schema.storedEvents() < SCANS) {
                    if (System.nanoTime() - stopped > TimeUnit.SECONDS.toNanos(60)) fail("Analytics did not ingest every scan");
                    Thread.sleep(20);
                }
            } finally {
                watching.set(false);
                watcher.join(1_000);
                relay.close();
                pipeline.shutdown();
            }
            schema.assertPositionsContiguous();
            assertEquals(transactions.identities(), schema.identities(), "every scan was counted once");
        }
    }
}
