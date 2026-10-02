package unit.analytics;

import analytics.AnalyticsLog;
import analytics.AnalyticsService;
import database.PopularWindowStore;
import org.junit.jupiter.api.Test;
import support.PipelineTestSupport.FakePopularWindowStore;
import support.PipelineTestSupport.OperationalLogCollector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static support.PipelineTestSupport.awaitOrFail;

class ConcurrentAnalyticsIngestionTest {
    @Test
    void concurrentProducersAreAdmittedExactlyOnceIntoOneFifoWindowOrder() throws Exception {
        FakePopularWindowStore store = new FakePopularWindowStore();
        OperationalLogCollector records = new OperationalLogCollector();
        AnalyticsService service = new AnalyticsService(store, new AnalyticsLog(records), 1);
        int producers = 10;
        int scansPerProducer = 150;
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> submissions = new ArrayList<>();
        long began = System.nanoTime();
        try (var executor = Executors.newFixedThreadPool(producers)) {
            for (int producer = 0; producer < producers; producer++) {
                String sku = "S" + producer;
                submissions.add(executor.submit(() -> {
                    start.await();
                    for (int scan = 0; scan < scansPerProducer; scan++) {
                        service.recordAcceptedScan(sku);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> submission : submissions) submission.get();
        }
        Duration admissionTime = Duration.ofNanos(System.nanoTime() - began);

        awaitOrFail(Duration.ofSeconds(4), "two persisted complete windows",
                () -> store.committedWindows().size() == 2);
        List<PopularWindowStore.PopularWindowSnapshot> windows = store.committedWindows();
        assertEquals(List.of(1L, 501L), windows.stream().map(w -> w.windowStart()).toList());
        assertEquals(List.of(1000L, 1500L), windows.stream().map(w -> w.windowEnd()).toList());
        assertTrue(windows.stream().allMatch(window ->
                window.ranks().stream().mapToLong(PopularWindowStore.SnapshotRank::scanCount).sum()
                        == 1000));
        assertEquals(1_500, producers * scansPerProducer);
        assertTrue(admissionTime.compareTo(Duration.ofSeconds(3)) < 0,
                "unbounded offer-based admission must not wait for pipeline capacity");
        List<Integer> backlogDepths = records.records().stream()
                .filter(line -> line.contains("\"eventType\":\"BACKLOG\""))
                .map(ConcurrentAnalyticsIngestionTest::queueDepth)
                .toList();
        assertFalse(backlogDepths.isEmpty());
        for (int index = 1; index < backlogDepths.size(); index++) {
            assertTrue(backlogDepths.get(index) > backlogDepths.get(index - 1));
        }
        assertTrue(records.records().stream().allMatch(line -> line.contains("\"queue\":\"ingress\"")));
        service.shutdown();
    }

    private static int queueDepth(String line) {
        String marker = "\"queueDepth\":";
        int start = line.indexOf(marker) + marker.length();
        int end = line.indexOf(',', start);
        if (end < 0) end = line.indexOf('}', start);
        return Integer.parseInt(line.substring(start, end));
    }
}
