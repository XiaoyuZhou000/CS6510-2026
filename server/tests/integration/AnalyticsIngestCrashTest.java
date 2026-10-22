package integration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import support.AnalyticsTestSchema;
import support.CheckoutDeployment;
import support.FaultInjection;
import support.ServiceHttp;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end crashes of Analytics between receiving a batch and acknowledging it (FR-041 to FR-043;
 * contracts/scan-event-delivery.md sections 3 and 8), one crash point per run, with real Transaction,
 * Inventory, and Analytics processes over disposable schemas.
 */
class AnalyticsIngestCrashTest {
    private static final int SCANS = 1_100;

    @ParameterizedTest
    @ValueSource(strings = {"analytics.ingest.received", "analytics.ingest.beforeCommit", "analytics.ingest.afterCommit"})
    void crashBetweenReceptionAndAcknowledgementLosesAndDuplicatesNothing(String hook) throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(5, 10_000, null)) {
            scanInParallel(deployment.transactionHttp, SCANS);
            // Stop Transaction so the first delivery happens only once Analytics is up with the hook armed.
            deployment.stopTransaction();
            assertEquals(SCANS, deployment.transactionSchema.undeliveredCount());
            deployment.startAnalytics(FaultInjection.spec().halt(hook).build());
            deployment.startTransaction();
            assertEquals(137, deployment.analyticsProcess().awaitExit(Duration.ofSeconds(30)), "Analytics halts at " + hook);

            List<AnalyticsTestSchema.StoredEvent> beforeRestart = deployment.analyticsSchema.events();
            boolean committed = hook.equals("analytics.ingest.afterCommit");
            assertEquals(committed ? 500 : 0, beforeRestart.size(), "only a committed batch is durable at " + hook);
            assertEquals(SCANS, deployment.transactionSchema.undeliveredCount(), "delivered_at is set only after an OK reply");

            deployment.startAnalytics();
            deployment.awaitAnalyticsHoldsEveryScan(Duration.ofSeconds(60));
            List<AnalyticsTestSchema.StoredEvent> after = deployment.analyticsSchema.events();
            assertEquals(beforeRestart, after.subList(0, beforeRestart.size()), "every position recorded before the crash keeps its event");
            if (committed) {
                ScanEventDeliveryTest.awaitCounter(deployment, "analytics.ingest.duplicates", 500);
            } else {
                ScanEventDeliveryTest.awaitCounter(deployment, "analytics.ingest.stored", SCANS);
                assertEquals(0, deployment.analyticsProcess().latestCounter("analytics.ingest.duplicates"),
                        "after a crash before the commit the batch is stored fresh");
            }

            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (deployment.analyticsSchema.checkpoint() < 1000) {
                if (System.nanoTime() > deadline) fail("window 1,000 was not published" + System.lineSeparator() + deployment.logs());
                Thread.sleep(100);
            }
            deployment.analyticsSchema.assertWindowsMatchPositions(1000);
        }
    }

    private static void scanInParallel(ServiceHttp http, int scans) throws Exception {
        int stations = 10;
        try (ExecutorService workers = Executors.newFixedThreadPool(stations)) {
            List<Future<?>> work = new ArrayList<>();
            for (int station = 0; station < stations; station++) {
                String id = http.start("crash-" + station);
                int share = scans / stations;
                int offset = station;
                work.add(workers.submit(() -> {
                    for (int n = 0; n < share; n++) {
                        assertEquals(200, http.scan(id, "SKU-00000" + ((n + offset) % 5 + 1)).statusCode());
                    }
                    return null;
                }));
            }
            for (Future<?> done : work) done.get();
        }
    }
}
