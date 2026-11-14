import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** T067: the notification report (contracts/subscriber-client-and-reconciliation.md, research N9, SC-007). */
class NotificationReportTest {

    private static Map<String, Object> report(Path run) throws Exception {
        RunData data = RunData.load(run);
        return NotificationReport.build(data, Reconciler.reconcile(data, Reconciler.FIXED));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> section(Map<String, Object> map, String... path) {
        Map<String, Object> current = map;
        for (String key : path) current = (Map<String, Object>) current.get(key);
        return current;
    }

    @Test
    void nearestRankPercentiles() {
        long[] hundred = new long[100];
        for (int i = 0; i < 100; i++) hundred[i] = i + 1;
        assertEquals(50, NotificationReport.nearestRank(hundred, 50));
        assertEquals(95, NotificationReport.nearestRank(hundred, 95));
        assertEquals(99, NotificationReport.nearestRank(hundred, 99));
        assertEquals(100, NotificationReport.nearestRank(hundred, 100));
        long[] four = {10, 20, 30, 40};
        assertEquals(20, NotificationReport.nearestRank(four, 50));
        assertEquals(40, NotificationReport.nearestRank(four, 95));
        assertEquals(7, NotificationReport.nearestRank(new long[] {7}, 99));
    }

    @Test
    void endToEndLatencyUsesEachNotificationsFirstReceipt(@TempDir Path dir) throws Exception {
        Map<String, Object> report = report(SyntheticRun.create(dir));
        Map<String, Object> endToEnd = section(report, "latencyMs", "endToEnd");
        // K1 is received at +10 ms and again at +500 ms; only the first receipt counts.
        assertEquals(20L, endToEnd.get("p50"));
        assertEquals(40L, endToEnd.get("p95"));
        assertEquals(40L, endToEnd.get("p99"));
        assertEquals(40L, endToEnd.get("max"));
        assertEquals(4L, endToEnd.get("n"));
        Map<String, Object> toDelivered = section(report, "latencyMs", "toDelivered");
        assertEquals(15L, toDelivered.get("p50"));
        assertEquals(35L, toDelivered.get("p95"));
        assertNull(section(report, "latencyMs").get("endToEndExcluding"), "only for isolation runs");
    }

    @Test
    void neverReceivedNotificationsAreExcludedAndCountedAsMissing(@TempDir Path dir) throws Exception {
        Path run = SyntheticRun.create(dir);
        SyntheticRun.removeLine(SyntheticRun.receipts(run, "sub-0001"), "n-txn-1-SKU-000002-sub-0001");
        Map<String, Object> report = report(run);
        Map<String, Object> endToEnd = section(report, "latencyMs", "endToEnd");
        assertEquals(3L, endToEnd.get("n"));
        assertEquals(30L, endToEnd.get("p50"));
        assertEquals(40L, endToEnd.get("p95"));
        Map<String, Object> notifications = section(report, "notifications");
        assertEquals(4L, notifications.get("owed"));
        assertEquals(4L, notifications.get("delivered"));
        assertEquals(3L, notifications.get("received"));
        assertEquals("FAIL", section(report, "reconciliation").get("verdict"));
        assertEquals(1L, section(report, "reconciliation", "counts").get("missingDeliveredNotReceived"));
        assertEquals(Boolean.FALSE, section(report, "drain").get("deadlineMet"), "a missing notification never drains");
    }

    @Test
    void activeWindowRates(@TempDir Path dir) throws Exception {
        Map<String, Object> report = report(SyntheticRun.create(dir));
        Map<String, Object> sales = section(report, "sales");
        assertEquals(3L, sales.get("completed"));
        assertEquals(2.0, ((Number) sales.get("activeWindowSeconds")).doubleValue(), 1e-9);
        assertEquals(1.5, ((Number) sales.get("perSecond")).doubleValue(), 1e-9);
        assertEquals(1L, sales.get("backgroundCompletions"));
        Map<String, Object> notifications = section(report, "notifications");
        assertEquals(1L, notifications.get("redeliveries"));
        assertEquals(2.0, ((Number) notifications.get("deliveredPerSecond")).doubleValue(), 1e-9);
        assertEquals(2.0, ((Number) notifications.get("receivedPerSecond")).doubleValue(), 1e-9);
    }

    @Test
    void drainIsMeasuredFromTheFinalCompletionToTheLastFirstReceipt(@TempDir Path dir) throws Exception {
        Path run = SyntheticRun.create(dir);
        Map<String, Object> drain = section(report(run), "drain");
        // Every first receipt precedes txn-3, the final completion, which has no recipient.
        assertEquals(Instant.ofEpochMilli(SyntheticRun.T0 + 2_000).toString(), drain.get("finalCompletionAt"));
        assertEquals(Instant.ofEpochMilli(SyntheticRun.T0 + 1_040).toString(), drain.get("allReceivedAt"));
        assertEquals(0.0, ((Number) drain.get("drainSeconds")).doubleValue(), 1e-9);
        assertEquals(60L, drain.get("deadlineSeconds"));
        assertEquals(Boolean.TRUE, drain.get("deadlineMet"));

        SyntheticRun.editLine(SyntheticRun.receipts(run, "sub-0003"), "n-txn-2-SKU-000003-sub-0003",
                line -> line.replace("\"receivedAtMs\":" + (SyntheticRun.T0 + 1_040),
                        "\"receivedAtMs\":" + (SyntheticRun.T0 + 63_500)));
        drain = section(report(run), "drain");
        assertEquals(61.5, ((Number) drain.get("drainSeconds")).doubleValue(), 1e-9);
        assertEquals(Boolean.FALSE, drain.get("deadlineMet"));
    }

    @Test
    void isolationRunsReportLatencyExcludingTheFaultedSubscriber(@TempDir Path dir) throws Exception {
        Map<String, Object> report = report(SyntheticRun.create(dir, "reject:sub-0001"));
        Map<String, Object> excluding = section(report, "latencyMs", "endToEndExcluding");
        assertEquals("sub-0001", excluding.get("subscriberId"));
        assertEquals(30L, excluding.get("p50"));
        assertEquals(40L, excluding.get("p95"));
        assertEquals(40L, excluding.get("p99"));
        assertEquals("reject:sub-0001", section(report, "run").get("faults"));
    }

    /** A healthy isolation run (T125) names the same subscriber, so its other subscribers are measured the same way. */
    @Test
    void healthyIsolationRunsReportLatencyExcludingTheNamedSubscriber(@TempDir Path dir) throws Exception {
        Map<String, Object> report = report(SyntheticRun.create(dir, "none", "sub-0001"));
        Map<String, Object> excluding = section(report, "latencyMs", "endToEndExcluding");
        assertEquals("sub-0001", excluding.get("subscriberId"));
        assertEquals(30L, excluding.get("p50"));
        assertEquals(40L, excluding.get("p95"));
        assertEquals("none", section(report, "run").get("faults"));
    }

    @Test
    void backlogAndDeliveryFailures(@TempDir Path dir) throws Exception {
        Path run = SyntheticRun.create(dir);
        Path rows = SyntheticRun.exports(run, "notifications.tsv");
        SyntheticRun.setField(rows, "n-txn-1-SKU-000001-sub-0001", 8, "2");
        SyntheticRun.setField(rows, "n-txn-1-SKU-000001-sub-0001", 9, "1");
        SyntheticRun.setField(rows, "n-txn-2-SKU-000003-sub-0003", 9, "3");
        SyntheticRun.setField(rows, "n-txn-2-SKU-000003-sub-0003", 10, "1");
        Map<String, Object> report = report(run);
        Map<String, Object> failures = section(report, "deliveryFailures");
        assertEquals(2L, failures.get("chargedFailures"));
        assertEquals(4L, failures.get("unchargedFailures"));
        assertEquals(0L, failures.get("parked"));
        assertEquals(1L, failures.get("operatorRetried"));
        Map<String, Object> max = section(report, "backlog", "max");
        assertEquals(9L, max.get("saleEventsUnpublished"));
        assertEquals(5L, max.get("saleConsumerLag"));
        assertEquals(7L, max.get("notificationsPending"));
        assertEquals(11L, max.get("channelLag"));
        Map<String, Object> last = section(report, "backlog", "final");
        assertEquals(0L, last.get("saleEventsUnpublished"));
        assertEquals(0L, last.get("channelLag"));
    }

    @Test
    void fieldNamesMatchTheContract(@TempDir Path dir) throws Exception {
        Path run = SyntheticRun.create(dir, "stall:sub-0002");
        Map<String, Object> report = report(run);
        assertEquals(List.of("run", "sales", "notifications", "latencyMs", "deliveryFailures", "backlog", "drain",
                "reconciliation"), List.copyOf(report.keySet()));
        assertEquals(List.of("id", "startedAt", "deploymentCommit", "subscribers", "seed", "skusPerSubscriber",
                "skuDistribution", "loadClient", "faults"), List.copyOf(section(report, "run").keySet()));
        assertEquals(List.of("stations", "durationSeconds"), List.copyOf(section(report, "run", "loadClient").keySet()));
        assertEquals(List.of("completed", "activeWindowSeconds", "perSecond", "backgroundCompletions"),
                List.copyOf(section(report, "sales").keySet()));
        assertEquals(List.of("owed", "delivered", "received", "redeliveries", "deliveredPerSecond", "receivedPerSecond"),
                List.copyOf(section(report, "notifications").keySet()));
        assertEquals(List.of("endToEnd", "toDelivered", "endToEndExcluding"), List.copyOf(section(report, "latencyMs").keySet()));
        assertEquals(List.of("p50", "p95", "p99", "max", "n"), List.copyOf(section(report, "latencyMs", "endToEnd").keySet()));
        assertEquals(List.of("p50", "p95", "p99"), List.copyOf(section(report, "latencyMs", "toDelivered").keySet()));
        assertEquals(List.of("subscriberId", "p50", "p95", "p99"),
                List.copyOf(section(report, "latencyMs", "endToEndExcluding").keySet()));
        assertEquals(List.of("chargedFailures", "unchargedFailures", "parked", "operatorRetried"),
                List.copyOf(section(report, "deliveryFailures").keySet()));
        assertEquals(List.of("max", "final"), List.copyOf(section(report, "backlog").keySet()));
        List<String> backlogFields = List.of("saleEventsUnpublished", "saleConsumerLag", "notificationsPending", "channelLag");
        assertEquals(backlogFields, List.copyOf(section(report, "backlog", "max").keySet()));
        assertEquals(backlogFields, List.copyOf(section(report, "backlog", "final").keySet()));
        assertEquals(List.of("finalCompletionAt", "allReceivedAt", "drainSeconds", "deadlineSeconds", "deadlineMet"),
                List.copyOf(section(report, "drain").keySet()));
        assertEquals(List.of("verdict", "counts"), List.copyOf(section(report, "reconciliation").keySet()));
        assertEquals(Reconciler.CATEGORIES, List.copyOf(section(report, "reconciliation", "counts").keySet()));

        Map<String, Object> runSection = section(report, "run");
        assertEquals("synthetic", runSection.get("id"));
        assertEquals(3L, runSection.get("subscribers"));
        assertEquals(42L, runSection.get("seed"));
        assertEquals(2L, runSection.get("skusPerSubscriber"));
        assertEquals("uniform", runSection.get("skuDistribution"));

        // ReconcileMain --report-out writes the JSON and its Markdown rendering next to it.
        Path out = run.resolve("notification-report.json");
        assertEquals(0, ReconcileMain.run(new String[] {"--run-dir=" + run, "--mode=fixed", "--report-out=" + out}));
        assertEquals(Json.compact(report), Json.compact(Json.parseObject(Files.readString(out))));
        String markdown = Files.readString(run.resolve("notification-report.md"));
        assertTrue(markdown.contains("PASS"), markdown);
        assertTrue(markdown.contains("p95"), markdown);
    }
}
