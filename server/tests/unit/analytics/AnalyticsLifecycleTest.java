package unit.analytics;

import analytics.AnalyticsLog;
import analytics.AnalyticsService;
import org.junit.jupiter.api.Test;
import support.PipelineTestSupport.FakePopularWindowStore;
import support.PipelineTestSupport.OperationalLogCollector;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static support.PipelineTestSupport.awaitOrFail;

class AnalyticsLifecycleTest {
    @Test
    void drainsCompleteWorkSuppressesPartialWindowAndTerminatesNamedWorkers() throws Exception {
        var store = new FakePopularWindowStore();
        var records = new OperationalLogCollector();
        var service = new AnalyticsService(store, new AnalyticsLog(records), 1_000, 2_000);

        assertEquals(AnalyticsService.State.RUNNING, service.state());
        for (int scan = 0; scan < 1_999; scan++) service.recordAcceptedScan("A");
        service.shutdown();

        assertEquals(AnalyticsService.State.TERMINATED, service.state());
        assertEquals(2, store.committedWindows().size());
        assertEquals(1, records.count(line -> line.contains("\"eventType\":\"DRAIN_STARTED\"")));
        assertEquals(1, records.count(line -> line.contains("\"eventType\":\"SHUTDOWN_COMPLETED\"")));
        assertEquals(0, records.count(line -> line.contains("\"eventType\":\"SHUTDOWN_FORCED\"")));
        assertFalse(workerAlive("analytics-window"));
        assertFalse(workerAlive("analytics-ranking"));
        assertFalse(workerAlive("analytics-persistence"));

        int recordCount = records.records().size();
        service.shutdown();
        assertEquals(recordCount, records.records().size(), "repeated shutdown must be idempotent");
        assertThrows(IllegalStateException.class, () -> service.recordAcceptedScan("A"));
        assertEquals(1, records.count(line -> line.contains("\"eventType\":\"INGESTION_REJECTED\"")));
        assertEquals(1, records.count(line -> line.contains("\"eventType\":\"SHUTDOWN_COMPLETED\"")
                || line.contains("\"eventType\":\"SHUTDOWN_FORCED\"")),
                "shutdown must emit exactly one terminal outcome");
    }

    @Test
    void shutdownMarkerCannotBeOvertakenByConcurrentIngestion() throws Exception {
        var store = new FakePopularWindowStore();
        var service = new AnalyticsService(store, new AnalyticsLog(line -> { }), 100, 2_000);
        Thread producer = new Thread(() -> {
            for (int scan = 0; scan < 20_000; scan++) {
                try {
                    service.recordAcceptedScan("A");
                } catch (IllegalStateException closed) {
                    return;
                }
            }
        });

        producer.start();
        awaitOrFail(Duration.ofSeconds(1), "producer admission", () -> producer.getState() != Thread.State.NEW);
        service.shutdown();
        producer.join(1_000);

        assertEquals(AnalyticsService.State.TERMINATED, service.state());
        assertFalse(producer.isAlive());
        assertThrows(IllegalStateException.class, () -> service.recordAcceptedScan("A"));
    }

    private static boolean workerAlive(String name) {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.getName().equals(name) && thread.isAlive());
    }
}
