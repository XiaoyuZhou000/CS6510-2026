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
}
