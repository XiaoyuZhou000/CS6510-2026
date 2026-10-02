package unit.analytics;

import analytics.PipelineMessage;
import analytics.RankingFilter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;

class RankingFilterTest {
    @Test
    void ranksByCountThenSkuTruncatesToTenAndForwardsEnd() throws Exception {
        BlockingQueue<PipelineMessage.WindowMessage> input = new LinkedBlockingQueue<>();
        BlockingQueue<PipelineMessage.RankingMessage> output = new LinkedBlockingQueue<>();
        Thread worker = new Thread(new RankingFilter(input, output));
        worker.start();

        List<String> skus = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            for (int occurrence = 0; occurrence < 50 + i; occurrence++) {
                skus.add(String.format("S%02d", i));
            }
        }
        while (skus.size() < 1000) skus.add("TOP");
        input.add(new PipelineMessage.WindowSnapshot(1, 1000, skus));
        input.add(PipelineMessage.WindowEnd.INSTANCE);
        worker.join(2000);

        var ranked = assertInstanceOf(PipelineMessage.RankedWindow.class, output.take());
        assertEquals(10, ranked.ranks().size());
        assertEquals("TOP", ranked.ranks().getFirst().sku());
        assertEquals(List.of(1,2,3,4,5,6,7,8,9,10),
                ranked.ranks().stream().map(PipelineMessage.RankedItem::rank).toList());
        assertThrows(UnsupportedOperationException.class,
                () -> ranked.ranks().add(new PipelineMessage.RankedItem(10, "X", 1)));
        assertInstanceOf(PipelineMessage.RankingEnd.class, output.take());
    }

    @Test
    void supportsFewerThanTenAndUsesSkuAscendingForTies() throws Exception {
        BlockingQueue<PipelineMessage.WindowMessage> input = new LinkedBlockingQueue<>();
        BlockingQueue<PipelineMessage.RankingMessage> output = new LinkedBlockingQueue<>();
        Thread worker = new Thread(new RankingFilter(input, output));
        worker.start();
        List<String> skus = new ArrayList<>();
        for (int i = 0; i < 500; i++) skus.add("B");
        for (int i = 0; i < 500; i++) skus.add("A");
        input.add(new PipelineMessage.WindowSnapshot(1, 1000, skus));
        input.add(PipelineMessage.WindowEnd.INSTANCE);
        worker.join(2000);
        var ranked = assertInstanceOf(PipelineMessage.RankedWindow.class, output.take());
        assertEquals(List.of("A", "B"),
                ranked.ranks().stream().map(PipelineMessage.RankedItem::sku).toList());
        assertEquals(List.of(500L, 500L),
                ranked.ranks().stream().map(PipelineMessage.RankedItem::scanCount).toList());
    }
}
