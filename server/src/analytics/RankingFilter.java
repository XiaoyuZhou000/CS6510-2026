package analytics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/** Converts complete immutable windows into deterministic top-ten rankings. */
public final class RankingFilter implements Runnable {
    private final BlockingQueue<PipelineMessage.WindowMessage> input;
    private final BlockingQueue<PipelineMessage.RankingMessage> output;

    public RankingFilter(
            BlockingQueue<PipelineMessage.WindowMessage> input,
            BlockingQueue<PipelineMessage.RankingMessage> output) {
        this.input = Objects.requireNonNull(input, "input must not be null");
        this.output = Objects.requireNonNull(output, "output must not be null");
    }

    @Override
    public void run() {
        try {
            while (true) {
                PipelineMessage.WindowMessage message = input.take();
                if (message instanceof PipelineMessage.WindowEnd) {
                    output.put(PipelineMessage.RankingEnd.INSTANCE);
                    return;
                }
                PipelineMessage.WindowSnapshot snapshot = (PipelineMessage.WindowSnapshot) message;
                output.put(rank(snapshot));
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static PipelineMessage.RankedWindow rank(PipelineMessage.WindowSnapshot snapshot) {
        Map<String, Long> counts = new HashMap<>();
        for (String sku : snapshot.skus()) counts.merge(sku, 1L, Long::sum);
        List<Map.Entry<String, Long>> ordered = new ArrayList<>(counts.entrySet());
        ordered.sort(Map.Entry.<String, Long>comparingByValue(Comparator.reverseOrder())
                .thenComparing(Map.Entry.comparingByKey()));

        List<PipelineMessage.RankedItem> ranks = new ArrayList<>();
        for (int index = 0; index < Math.min(10, ordered.size()); index++) {
            Map.Entry<String, Long> item = ordered.get(index);
            ranks.add(new PipelineMessage.RankedItem(index + 1, item.getKey(), item.getValue()));
        }
        return new PipelineMessage.RankedWindow(
                snapshot.windowStart(), snapshot.windowEnd(), ranks);
    }
}
