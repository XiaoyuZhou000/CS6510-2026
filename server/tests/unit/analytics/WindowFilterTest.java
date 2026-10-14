package unit.analytics;

import analytics.PipelineMessage;
import analytics.WindowFilter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;
import static support.PipelineTestSupport.awaitOrFail;

class WindowFilterTest {
    @Test
    void emitsOnlyCompleteWindowsWithChronologicalFiveHundredEventOverlap() throws Exception {
        BlockingQueue<PipelineMessage.IngressMessage> input = new LinkedBlockingQueue<>();
        BlockingQueue<PipelineMessage.WindowMessage> output = new LinkedBlockingQueue<>();
        Thread worker = new Thread(new WindowFilter(input, output, 0));
        worker.start();

        for (int i = 1; i <= 999; i++) input.add(new PipelineMessage.AcceptedScan(sku(i)));
        awaitOrFail(Duration.ofSeconds(1), "999 scans to be consumed", () -> input.isEmpty());
        assertTrue(output.isEmpty());
        input.add(new PipelineMessage.AcceptedScan(sku(1000)));
        awaitOrFail("first snapshot", () -> output.size() == 1);
        for (int i = 1001; i <= 1500; i++) input.add(new PipelineMessage.AcceptedScan(sku(i)));
        input.add(PipelineMessage.IngressEnd.INSTANCE);
        worker.join(2000);

        List<PipelineMessage.WindowMessage> messages = new ArrayList<>();
        output.drainTo(messages);
        assertEquals(3, messages.size());
        var first = assertInstanceOf(PipelineMessage.WindowSnapshot.class, messages.get(0));
        var second = assertInstanceOf(PipelineMessage.WindowSnapshot.class, messages.get(1));
        assertEquals(List.of(1L, 1000L), List.of(first.windowStart(), first.windowEnd()));
        assertEquals(List.of(501L, 1500L), List.of(second.windowStart(), second.windowEnd()));
        assertEquals(1000, first.skus().size());
        assertEquals(first.skus().subList(500, 1000), second.skus().subList(0, 500));
        assertEquals("S0001", first.skus().getFirst());
        assertEquals("S1500", second.skus().getLast());
        assertInstanceOf(PipelineMessage.WindowEnd.class, messages.get(2));
    }

    @Test
    void suppressesPartialDrainAndWarmsUpFullyAfterRestart() throws Exception {
        BlockingQueue<PipelineMessage.IngressMessage> input = new LinkedBlockingQueue<>();
        BlockingQueue<PipelineMessage.WindowMessage> output = new LinkedBlockingQueue<>();
        Thread worker = new Thread(new WindowFilter(input, output, 1000));
        worker.start();
        for (int i = 0; i < 999; i++) input.add(new PipelineMessage.AcceptedScan("A"));
        input.add(PipelineMessage.IngressEnd.INSTANCE);
        worker.join(2000);
        assertEquals(List.of(PipelineMessage.WindowEnd.INSTANCE), new ArrayList<>(output));

        input = new LinkedBlockingQueue<>();
        output = new LinkedBlockingQueue<>();
        worker = new Thread(new WindowFilter(input, output, 1000));
        worker.start();
        for (int i = 0; i < 1000; i++) input.add(new PipelineMessage.AcceptedScan("A"));
        input.add(PipelineMessage.IngressEnd.INSTANCE);
        worker.join(2000);
        var snapshot = assertInstanceOf(PipelineMessage.WindowSnapshot.class, output.take());
        assertEquals(List.of(1001L, 2000L), List.of(snapshot.windowStart(), snapshot.windowEnd()));
        assertInstanceOf(PipelineMessage.WindowEnd.class, output.take());
    }

    private static String sku(int number) {
        return "S" + String.format("%04d", number);
    }
    @Test
    void positionedReplayPrewarmsOverlapAndIgnoresExactDuplicate() throws Exception {
        BlockingQueue<PipelineMessage.IngressMessage> input = new LinkedBlockingQueue<>();
        BlockingQueue<PipelineMessage.WindowMessage> output = new LinkedBlockingQueue<>();
        List<PipelineMessage.AcceptedScan> prewarm = new ArrayList<>();
        for (int position = 1; position <= 1000; position++) {
            prewarm.add(new PipelineMessage.AcceptedScan(position, "A"));
        }
        Thread worker = new Thread(new WindowFilter(input, output, 1000, prewarm));
        worker.start();
        input.add(new PipelineMessage.AcceptedScan(1000, "A"));
        for (int position = 1001; position <= 1500; position++) {
            input.add(new PipelineMessage.AcceptedScan(position, "B"));
        }
        input.add(PipelineMessage.IngressEnd.INSTANCE);
        worker.join(2000);
        assertFalse(worker.isAlive());
        var window = assertInstanceOf(PipelineMessage.WindowSnapshot.class, output.take());
        assertEquals(501, window.windowStart());
        assertEquals(1500, window.windowEnd());
        assertEquals(500, window.skus().stream().filter("A"::equals).count());
        assertEquals(500, window.skus().stream().filter("B"::equals).count());
        assertInstanceOf(PipelineMessage.WindowEnd.class, output.take());
    }

    @Test
    void positionedReplayRejectsGapAndConflictingDuplicate() throws Exception {
        for (PipelineMessage.AcceptedScan bad : List.of(
                new PipelineMessage.AcceptedScan(2, "A"),
                new PipelineMessage.AcceptedScan(1, "B"))) {
            BlockingQueue<PipelineMessage.IngressMessage> input = new LinkedBlockingQueue<>();
            BlockingQueue<PipelineMessage.WindowMessage> output = new LinkedBlockingQueue<>();
            java.util.concurrent.atomic.AtomicReference<Throwable> failure =
                    new java.util.concurrent.atomic.AtomicReference<>();
            Thread worker = new Thread(new WindowFilter(input, output, 0, List.of()));
            worker.setUncaughtExceptionHandler((thread, thrown) -> failure.set(thrown));
            worker.start();
            if (bad.position() == 1) input.add(new PipelineMessage.AcceptedScan(1, "A"));
            input.add(bad);
            worker.join(2000);
            assertInstanceOf(IllegalStateException.class, failure.get());
            assertTrue(output.isEmpty());
        }
    }
    @Test
    void positionedStreamEmitsExactlyTheAbsoluteThousandFiveHundredAndTwoThousandEnds()
            throws Exception {
        BlockingQueue<PipelineMessage.IngressMessage> input = new LinkedBlockingQueue<>();
        BlockingQueue<PipelineMessage.WindowMessage> output = new LinkedBlockingQueue<>();
        Thread worker = new Thread(new WindowFilter(input, output, 0, List.of()));
        worker.start();
        for (int position = 1; position <= 2000; position++) {
            input.add(new PipelineMessage.AcceptedScan(position, sku(position)));
        }
        input.add(PipelineMessage.IngressEnd.INSTANCE);
        worker.join(2000);
        assertFalse(worker.isAlive());
        List<PipelineMessage.WindowMessage> messages = new ArrayList<>(output);
        assertEquals(4, messages.size());
        var first = (PipelineMessage.WindowSnapshot) messages.get(0);
        var second = (PipelineMessage.WindowSnapshot) messages.get(1);
        var third = (PipelineMessage.WindowSnapshot) messages.get(2);
        assertEquals(List.of(1000L, 1500L, 2000L),
                List.of(first.windowEnd(), second.windowEnd(), third.windowEnd()));
        assertEquals(first.skus().subList(500, 1000), second.skus().subList(0, 500));
        assertEquals(second.skus().subList(500, 1000), third.skus().subList(0, 500));
    }
}
