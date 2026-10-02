package unit.analytics;

import analytics.AnalyticsLog;
import analytics.PersistenceFilter;
import analytics.PipelineMessage;
import org.junit.jupiter.api.Test;
import support.PipelineTestSupport.FakePopularWindowStore;
import support.PipelineTestSupport.OperationalLogCollector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;
import static support.PipelineTestSupport.awaitOrFail;

class PersistenceFilterTest {
    @Test
    void retriesOldestInPlaceWithFastThenCappedDelaysAndOneLogPerFailure() {
        var input = new LinkedBlockingQueue<PipelineMessage.RankingMessage>();
        var store = new FakePopularWindowStore();
        store.writeGate().failNext(4);
        var records = new OperationalLogCollector();
        var delays = new ArrayList<Long>();
        input.add(window(1, 1000));
        input.add(window(501, 1500));
        input.add(PipelineMessage.RankingEnd.INSTANCE);

        new PersistenceFilter(input, store, new AnalyticsLog(records), delays::add).run();

        assertEquals(List.of(50L, 200L, 1_000L, 1_000L), delays);
        assertEquals(List.of(1000L, 1000L, 1000L, 1000L, 1000L, 1500L),
                store.attemptedWindows().stream().map(w -> w.windowEnd()).toList());
        assertEquals(List.of(1000L, 1500L),
                store.committedWindows().stream().map(w -> w.windowEnd()).toList());
        assertEquals(4, records.count(line -> line.contains("\"eventType\":\"PERSISTENCE_RETRY\"")));
        assertTrue(records.records().getFirst().contains("\"retryCount\":1"));
        assertTrue(records.records().getLast().contains("\"retryCount\":4"));
        assertTrue(records.records().stream().allMatch(line ->
                line.contains("\"windowStart\":1") && line.contains("\"windowEnd\":1000")
                        && line.contains("\"errorType\":\"StoreFailure\"")));
    }

    @Test
    void retryWaitIsInterruptibleAndDoesNotDequeueANewerWindow() throws Exception {
        var input = new LinkedBlockingQueue<PipelineMessage.RankingMessage>();
        var store = new FakePopularWindowStore();
        store.writeGate().fail();
        var records = new OperationalLogCollector();
        input.add(window(1, 1000));
        input.add(window(501, 1500));
        Thread worker = new Thread(
                new PersistenceFilter(input, store, new AnalyticsLog(records), Thread::sleep));

        worker.start();
        awaitOrFail("the first retry record", () -> !records.records().isEmpty());
        worker.interrupt();
        worker.join(1_000);

        assertFalse(worker.isAlive());
        assertEquals(List.of(1000L),
                store.attemptedWindows().stream().map(w -> w.windowEnd()).toList());
        assertTrue(store.committedWindows().isEmpty());
        assertEquals(1, input.size(), "the newer data window must remain queued");
    }

    private static PipelineMessage.RankedWindow window(long start, long end) {
        return new PipelineMessage.RankedWindow(start, end,
                List.of(new PipelineMessage.RankedItem(1, "A", 1000)));
    }
}
