package unit.analytics;

import analytics.AnalyticsLog;
import analytics.AnalyticsService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import support.PipelineTestSupport.FakePopularWindowStore;
import support.PipelineTestSupport.OperationalLogCollector;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static support.PipelineTestSupport.awaitOrFail;

class AnalyticsWorkerFailureTest {
    @ParameterizedTest
    @EnumSource(value = AnalyticsLog.Stage.class, names = {"WINDOW", "RANKING", "PERSISTENCE"})
    void firstWorkerFailureWinsInterruptsPeersRejectsIngressAndPreservesReads(AnalyticsLog.Stage failedStage)
            throws Exception {
        var store = new FakePopularWindowStore();
        store.setLatest(new database.PopularWindowStore.PopularWindowView(
                1, 1, 1000, Instant.parse("2026-09-30T00:00:00Z"), List.of(
                new database.PopularWindowStore.StoredRank(1, "A", "Apple", 1000))));
        var records = new OperationalLogCollector();
        AnalyticsService.WorkerDecorator failure = (stage, worker) -> stage == failedStage
                ? () -> { throw new IllegalStateException(
                        "password=secret SELECT * FROM basketItems stack trace"); }
                : worker;
        var service = new AnalyticsService(
                store, new AnalyticsLog(records), 1_000, 1_000, failure);

        awaitOrFail("failed lifecycle", () -> service.state() == AnalyticsService.State.FAILED);

        assertThrows(IllegalStateException.class, () -> service.recordAcceptedScan("A"));
        assertEquals(1000, service.latestPopularItems(10).windowEnd());
        assertEquals(1, records.count(line -> line.contains("\"eventType\":\"INGESTION_REJECTED\"")));
        assertEquals(1, records.count(line -> line.contains("\"eventType\":\"WORKER_FAILED\"")));
        String failureRecord = records.records().stream()
                .filter(line -> line.contains("\"eventType\":\"WORKER_FAILED\""))
                .findFirst().orElseThrow();
        assertTrue(failureRecord.contains("\"stage\":\"" + failedStage + "\""));
        assertFalse(failureRecord.contains("secret"));
        assertFalse(failureRecord.toLowerCase().contains("select *"));
        assertFalse(failureRecord.toLowerCase().contains("basketitems"));
        service.shutdown();
        assertEquals(AnalyticsService.State.FAILED, service.state());
    }
}
