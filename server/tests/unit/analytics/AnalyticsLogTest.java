package unit.analytics;

import analytics.AnalyticsLog;
import analytics.AnalyticsService;
import org.junit.jupiter.api.Test;
import support.PipelineTestSupport.FakePopularWindowStore;
import support.PipelineTestSupport.OperationalLogCollector;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class AnalyticsLogTest {
    private static final Instant NOW = Instant.parse("2026-09-30T20:00:00Z");

    @Test
    void writesUtcJsonLinesWithRequiredEventFieldsAndEscaping() {
        var records = new OperationalLogCollector();
        var log = new AnalyticsLog(Clock.fixed(NOW, ZoneOffset.UTC), records);

        log.backlog(AnalyticsLog.Stage.INGRESS, "in\"gress", 8);
        log.persistenceRetry(3, 501, 1500, new IllegalStateException("ignored"));
        log.ingestionRejected("FORCED", "closed\nnow");
        log.drainStarted(3, 2, 1);
        log.shutdownCompleted();

        assertEquals(5, records.records().size());
        assertTrue(records.records().stream().allMatch(line ->
                line.startsWith("{\"timestamp\":\"2026-09-30T20:00:00Z\"")
                        && line.contains("\"stage\":") && !line.contains("\n")));
        assertTrue(records.records().getFirst().contains("\"queue\":\"in\\\"gress\""));
        assertTrue(records.records().get(1).contains("\"retryCount\":3"));
        assertTrue(records.records().get(2).contains("\"reason\":\"closed now\""));
        assertTrue(records.records().get(3).contains("\"ingressQueueDepth\":3"));
        assertTrue(records.records().get(3).contains("\"windowQueueDepth\":2"));
        assertTrue(records.records().get(3).contains("\"rankingQueueDepth\":1"));
        assertTrue(records.records().get(4).contains("\"shutdownOutcome\":\"DRAINED\""));
    }

    @Test
    void sanitizesCredentialsDatabaseSqlStackTraceAndBasketData() {
        var records = new OperationalLogCollector();
        var log = new AnalyticsLog(Clock.fixed(NOW, ZoneOffset.UTC), records);

        List<String> sensitiveReasons = List.of(
                "password=hunter2",
                "user=root",
                "apiKey=client-secret-value",
                "access_token=bearer-secret-value",
                "jdbc:mysql://root:secret@host/db?useSSL=false",
                "SELECT sku, scan_count FROM popular_item",
                "java.lang.IllegalStateException: boom\n\tat x.y.Worker.run(Worker.java:42)",
                "basketItems=[SKU-1, SKU-2]");

        for (String reason : sensitiveReasons) {
            log.workerFailed(AnalyticsLog.Stage.PERSISTENCE, new RuntimeException(reason));
        }

        assertEquals(sensitiveReasons.size(), records.records().size());
        assertSensitiveDataAbsent(records.records());
        assertTrue(records.records().stream().allMatch(record ->
                record.contains("\"errorType\":\"RuntimeException\"")));
        assertTrue(records.records().stream().allMatch(record -> !record.contains("\n")));
    }

    @Test
    void sanitizesEveryFreeTextOperationalField() {
        var records = new OperationalLogCollector();
        var log = new AnalyticsLog(Clock.fixed(NOW, ZoneOffset.UTC), records);

        log.backlog(AnalyticsLog.Stage.INGRESS, "basketContents=[SKU-9]", 4);
        log.ingestionRejected("password=state-secret", "DELETE FROM popular_item");
        log.workerFailed(AnalyticsLog.Stage.WINDOW,
                new IllegalStateException("jdbc:mysql://host/db\n at x.y.Window.run(Window.java:7)"));

        assertSensitiveDataAbsent(records.records());
        assertTrue(records.records().getFirst().contains("[REDACTED_BASKET_DATA]"));
        assertTrue(records.records().get(1).contains("password=[REDACTED]"));
        assertTrue(records.records().get(1).contains("[REDACTED_SQL]"));
        assertTrue(records.records().get(2).contains("[REDACTED_DATABASE_URL]"));
    }

    private static void assertSensitiveDataAbsent(List<String> records) {
        String output = String.join("\n", records).toLowerCase();
        assertAll(
                () -> assertFalse(output.contains("hunter2")),
                () -> assertFalse(output.contains("user=root")),
                () -> assertFalse(output.contains("client-secret-value")),
                () -> assertFalse(output.contains("bearer-secret-value")),
                () -> assertFalse(output.contains("jdbc:mysql")),
                () -> assertFalse(output.contains("select sku")),
                () -> assertFalse(output.contains("delete from")),
                () -> assertFalse(output.contains("worker.java:42")),
                () -> assertFalse(output.contains("window.java:7")),
                () -> assertFalse(output.contains("basketitems")),
                () -> assertFalse(output.contains("basketcontents")),
                () -> assertFalse(output.contains("sku-1")),
                () -> assertFalse(output.contains("sku-9")));
    }

    @Test
    void validatesEventSpecificRequiredFields() {
        assertThrows(NullPointerException.class, () -> new AnalyticsLog.Event(
                NOW, AnalyticsLog.EventType.BACKLOG, AnalyticsLog.Stage.INGRESS,
                "ingress", null, null, null, null, null, null, null, null,
                null, null, null));
        assertThrows(IllegalArgumentException.class, () -> AnalyticsLog.Event.shutdown(
                NOW, AnalyticsLog.EventType.SHUTDOWN_COMPLETED, AnalyticsLog.ShutdownOutcome.TIMEOUT));
    }

    @Test
    void emitsIncreasingBacklogHighWatersAndExactlyOneTerminalOutcome() {
        var records = new OperationalLogCollector();
        var releaseWindow = new CountDownLatch(1);
        AnalyticsService.WorkerDecorator holdWindow = (stage, worker) ->
                stage == AnalyticsLog.Stage.WINDOW ? () -> {
                    try {
                        releaseWindow.await();
                        worker.run();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                } : worker;
        var service = new AnalyticsService(new FakePopularWindowStore(),
                new AnalyticsLog(Clock.fixed(NOW, ZoneOffset.UTC), records),
                2, 1_000, holdWindow);

        for (int scan = 0; scan < 8; scan++) service.recordAcceptedScan("A");
        assertTrue(records.records().stream().anyMatch(line -> line.contains("\"queueDepth\":2")));
        assertTrue(records.records().stream().anyMatch(line -> line.contains("\"queueDepth\":4")));
        assertTrue(records.records().stream().anyMatch(line -> line.contains("\"queueDepth\":8")));

        releaseWindow.countDown();
        service.shutdown();
        assertEquals(1, records.count(line -> line.contains("\"eventType\":\"SHUTDOWN_COMPLETED\"")
                || line.contains("\"eventType\":\"SHUTDOWN_FORCED\"")));
    }
}
