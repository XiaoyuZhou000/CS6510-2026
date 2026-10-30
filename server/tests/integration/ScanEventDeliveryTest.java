package integration;

import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.FaultInjection;
import support.ServiceHttp;
import support.ServiceProcesses;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * At-least-once delivery of accepted scans from Transaction's outbox through the broker to Analytics across real
 * processes (US5 scenarios 3, 4, 6, and 8; US2 scenario 3; contracts/scan-event-delivery.md sections 2 and 7). The
 * relay publishes whether or not Analytics runs; the records wait in the scan topic. Each test ends with every
 * accepted scan stored in Analytics exactly once, at contiguous positions.
 */
class ScanEventDeliveryTest {
    private static final Duration DRAIN = Duration.ofSeconds(60);

    @Test
    void scansSucceedWhileAnalyticsIsDownAndEveryScanIsCountedOnceAfterItRestarts() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(5, 1_000, null)) {
            String id = deployment.transactionHttp.start("outage-1");
            scan(deployment.transactionHttp, id, 30);
            String paid = deployment.transactionHttp.start("outage-2");
            scan(deployment.transactionHttp, paid, 4);
            assertEquals(200, deployment.transactionHttp.complete(paid).statusCode(), "Complete never waits for Analytics");
            deployment.awaitScansPublished(DRAIN);
            assertEquals(34, deployment.scanTopicEnd(), "the scans wait durably in the topic while Analytics is down");

            deployment.startAnalytics();
            deployment.awaitAnalyticsHoldsEveryScan(DRAIN);
            assertEquals("Item 1", deployment.analyticsSchema.productName("SKU-000001"), "names captured at scan time");

            deployment.stopAnalytics();
            scan(deployment.transactionHttp, id, 20);
            deployment.awaitScansPublished(DRAIN);
            assertEquals(54, deployment.scanTopicEnd());
            assertEquals(34, deployment.analyticsSchema.storedEvents(), "nothing is consumed while Analytics is down");
            deployment.startAnalytics();
            deployment.awaitAnalyticsHoldsEveryScan(DRAIN);
            assertEquals(54, deployment.analyticsSchema.storedEvents());
        }
    }

    @Test
    void haltAfterConfirmationBeforeMarkingIsResentAndCountedOnce() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(5, 1_000, null)) {
            deployment.startAnalytics();
            deployment.stopTransaction();
            // Forty scans committed while Transaction was down, so one relay batch carries them all.
            deployment.transactionSchema.insertOpenTransaction("confirm-halt");
            deployment.transactionSchema.insertScans("confirm-halt", 1, 40, n -> "SKU-00000" + (n % 5 + 1));
            deployment.launchTransaction(FaultInjection.spec().halt("relay.afterConfirm").build());
            assertEquals(137, deployment.transactionProcess().awaitExit(Duration.ofSeconds(60)),
                    "Transaction halts after the broker confirmed the batch");
            assertEquals(40, deployment.transactionSchema.undeliveredCount(), "published_at was never committed");
            awaitStored(deployment, 40);
            assertEquals(40, deployment.scanTopicEnd(), "the broker holds the confirmed batch");

            deployment.startTransaction();
            deployment.awaitAnalyticsHoldsEveryScan(DRAIN);
            assertEquals(80, deployment.scanTopicEnd(), "the restarted relay sent the batch again");
            assertEquals(40, deployment.analyticsSchema.lastPosition(), "the re-sent records added no position");
            awaitCounter(deployment, "analytics.ingest.duplicates", 40);
        }
    }

    @Test
    void transactionCrashAfterScanCommitsBeforePublicationPublishesAfterRestart() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(5, 1_000, null)) {
            deployment.killTransaction();
            // Twenty-five scans whose commit preceded the crash; the crashed relay never published them.
            deployment.transactionSchema.insertOpenTransaction("crash-before-publication");
            deployment.transactionSchema.insertScans("crash-before-publication", 1, 25, n -> "SKU-00000" + (n % 5 + 1));
            deployment.startAnalytics();
            Thread.sleep(500);
            assertEquals(0, deployment.analyticsSchema.storedEvents());
            assertEquals(0, deployment.scanTopicEnd());
            assertEquals(25, deployment.transactionSchema.undeliveredCount());
            deployment.startTransaction();
            deployment.awaitAnalyticsHoldsEveryScan(DRAIN);
            assertEquals(25, deployment.analyticsSchema.storedEvents());
        }
    }

    @Test
    void scansOfFailedAndNeverCompletedCheckoutsAreCounted() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(5, 1_000, null)) {
            deployment.startAnalytics();
            deployment.inventorySchema.setStock("SKU-000002", 1);
            ServiceHttp http = deployment.transactionHttp;
            String completed = http.start("counted-completed");
            for (int i = 0; i < 3; i++) assertEquals(200, http.scan(completed, "SKU-000001").statusCode());
            assertEquals(200, http.complete(completed).statusCode());
            String rejected = http.start("counted-rejected");
            for (int i = 0; i < 2; i++) assertEquals(200, http.scan(rejected, "SKU-000002").statusCode());
            HttpResponse<String> refusal = http.complete(rejected);
            assertEquals(409, refusal.statusCode(), refusal.body());
            assertEquals("OPEN", ServiceHttp.json(http.lookup(rejected)).get("status"), "a rejected checkout reopens");
            String abandoned = http.start("counted-abandoned");
            for (int i = 0; i < 4; i++) assertEquals(200, http.scan(abandoned, "SKU-000003").statusCode());

            deployment.awaitAnalyticsHoldsEveryScan(DRAIN);
            assertEquals(9, deployment.analyticsSchema.storedEvents());
            assertEquals(2, deployment.analyticsSchema.queryLong("SELECT COUNT(*) FROM accepted_scan WHERE sku = 'SKU-000002'"));
            assertEquals(4, deployment.analyticsSchema.queryLong("SELECT COUNT(*) FROM accepted_scan WHERE sku = 'SKU-000003'"));
        }
    }

    /** Constitution II: two Transaction processes on one schema; both relays may publish the same rows. */
    @Test
    void twoTransactionProcessesOnOneSchemaStillCountEveryScanOnce() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(5, 1_000, null)) {
            int secondPort = ServiceProcesses.freePort();
            deployment.startExtraTransaction(secondPort);
            ServiceHttp second = new ServiceHttp(secondPort);
            List<String> ids = new ArrayList<>();
            try (ExecutorService stations = Executors.newFixedThreadPool(4)) {
                List<Future<?>> work = new ArrayList<>();
                for (int station = 0; station < 4; station++) {
                    ServiceHttp http = station % 2 == 0 ? deployment.transactionHttp : second;
                    String id = http.start("relay-pair-" + station);
                    ids.add(id);
                    work.add(stations.submit(() -> {
                        // Alternate processes for one basket, as a balanced router would.
                        for (int n = 0; n < 60; n++) {
                            ServiceHttp target = n % 2 == 0 ? deployment.transactionHttp : second;
                            assertEquals(200, target.scan(id, "SKU-00000" + (n % 5 + 1)).statusCode());
                        }
                        return null;
                    }));
                }
                for (Future<?> station : work) station.get();
            }
            // Both relays poll the same undelivered rows; a row either selects may be published twice.
            deployment.awaitScansPublished(DRAIN);
            assertTrue(deployment.scanTopicEnd() >= 240, "every scan is in the topic at least once");
            deployment.startAnalytics();
            deployment.awaitAnalyticsHoldsEveryScan(DRAIN);
            assertEquals(240, deployment.analyticsSchema.storedEvents());
            assertEquals(240, deployment.analyticsSchema.lastPosition());
            for (String id : ids) {
                assertEquals(60, ((Number) ServiceHttp.json(deployment.transactionHttp.lookup(id)).get("itemCount")).intValue());
            }
        }
    }

    private static void scan(ServiceHttp http, String id, int count) throws Exception {
        for (int n = 0; n < count; n++) {
            HttpResponse<String> response = http.scan(id, "SKU-00000" + (n % 5 + 1));
            assertEquals(200, response.statusCode(), response.body());
        }
    }

    private static void awaitStored(CheckoutDeployment deployment, long events) throws Exception {
        long deadline = System.nanoTime() + DRAIN.toNanos();
        while (deployment.analyticsSchema.storedEvents() < events) {
            if (System.nanoTime() > deadline) fail("Analytics stored only " + deployment.analyticsSchema.storedEvents()
                    + System.lineSeparator() + deployment.logs());
            Thread.sleep(100);
        }
    }

    /** Waits until Analytics' printed metrics show {@code counter} at least {@code minimum}. */
    static void awaitCounter(CheckoutDeployment deployment, String counter, long minimum) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        long value;
        while ((value = deployment.analyticsProcess().latestCounter(counter)) < minimum) {
            if (System.nanoTime() > deadline) {
                fail(counter + " stayed at " + value + ", expected at least " + minimum + System.lineSeparator()
                        + deployment.logs());
            }
            Thread.sleep(200);
        }
    }
}
