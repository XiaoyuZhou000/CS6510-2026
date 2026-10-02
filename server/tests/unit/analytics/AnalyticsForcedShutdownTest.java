package unit.analytics;

import analytics.AnalyticsLog;
import analytics.AnalyticsService;
import org.junit.jupiter.api.Test;
import support.PipelineTestSupport.FakePopularWindowStore;
import support.PipelineTestSupport.OperationalLogCollector;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static support.PipelineTestSupport.awaitOrFail;

class AnalyticsForcedShutdownTest {
    @Test
    void permanentStoreFailureUsesOneDeadlineAndReportsForcedNotCompleted() throws Exception {
        var store = new FakePopularWindowStore();
        store.writeGate().fail();
        var records = new OperationalLogCollector();
        var service = new AnalyticsService(store, new AnalyticsLog(records), 1_000, 120);
        for (int scan = 0; scan < 1_000; scan++) service.recordAcceptedScan("A");
        awaitOrFail("persistence attempt", () -> store.writeAttemptCount() > 0);

        long started = System.nanoTime();
        service.shutdown();
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - started).toMillis();

        assertEquals(AnalyticsService.State.FORCED, service.state());
        assertTrue(elapsedMillis < 600, "three workers must share one timeout, not receive one each");
        assertEquals(1, records.count(line -> line.contains("\"eventType\":\"SHUTDOWN_FORCED\"")));
        assertTrue(records.records().stream().anyMatch(line -> line.contains("\"shutdownOutcome\":\"TIMEOUT\"")));
        assertEquals(0, records.count(line -> line.contains("\"eventType\":\"SHUTDOWN_COMPLETED\"")));
    }

    @Test
    void callerInterruptionForcesWorkersRestoresFlagAndReportsInterrupted() throws Exception {
        var store = new FakePopularWindowStore();
        store.writeGate().block();
        var records = new OperationalLogCollector();
        var service = new AnalyticsService(store, new AnalyticsLog(records), 1_000, 5_000);
        for (int scan = 0; scan < 1_000; scan++) service.recordAcceptedScan("A");
        awaitOrFail("blocked persistence attempt", () -> store.writeAttemptCount() > 0);
        var callerWasInterrupted = new AtomicBoolean();
        Thread caller = new Thread(() -> {
            service.shutdown();
            callerWasInterrupted.set(Thread.currentThread().isInterrupted());
        });

        caller.start();
        awaitOrFail("draining state", () -> service.state() == AnalyticsService.State.DRAINING);
        caller.interrupt();
        caller.join(1_000);

        assertFalse(caller.isAlive());
        assertTrue(callerWasInterrupted.get());
        assertEquals(AnalyticsService.State.FORCED, service.state());
        assertTrue(records.records().stream().anyMatch(line -> line.contains("\"shutdownOutcome\":\"INTERRUPTED\"")));
        assertEquals(0, records.count(line -> line.contains("\"eventType\":\"SHUTDOWN_COMPLETED\"")));
    }
}
