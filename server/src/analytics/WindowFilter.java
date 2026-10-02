package analytics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/** Owns scan ordering and the 1,000-event hopping-window ring. */
public final class WindowFilter implements Runnable {
    private final BlockingQueue<PipelineMessage.IngressMessage> input;
    private final BlockingQueue<PipelineMessage.WindowMessage> output;
    private final long persistedMaximumEnd;
    private final String[] ring = new String[WindowMath.WINDOW_SIZE];

    public WindowFilter(
            BlockingQueue<PipelineMessage.IngressMessage> input,
            BlockingQueue<PipelineMessage.WindowMessage> output,
            long persistedMaximumEnd) {
        this.input = Objects.requireNonNull(input, "input must not be null");
        this.output = Objects.requireNonNull(output, "output must not be null");
        if (persistedMaximumEnd < 0) {
            throw new IllegalArgumentException("persistedMaximumEnd must not be negative");
        }
        this.persistedMaximumEnd = persistedMaximumEnd;
    }

    @Override
    public void run() {
        long acceptedSinceStart = 0;
        try {
            while (true) {
                PipelineMessage.IngressMessage message = input.take();
                if (message instanceof PipelineMessage.IngressEnd) {
                    output.put(PipelineMessage.WindowEnd.INSTANCE);
                    return;
                }

                PipelineMessage.AcceptedScan scan = (PipelineMessage.AcceptedScan) message;
                acceptedSinceStart++;
                ring[WindowMath.ringIndex(acceptedSinceStart)] = scan.sku();
                if (WindowMath.isEmissionEligible(acceptedSinceStart)) {
                    WindowMath.WindowBounds bounds =
                            WindowMath.emissionBounds(persistedMaximumEnd, acceptedSinceStart);
                    output.put(new PipelineMessage.WindowSnapshot(
                            bounds.windowStart(), bounds.windowEnd(), snapshot(acceptedSinceStart)));
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private List<String> snapshot(long acceptedSinceStart) {
        long firstFreshPosition = acceptedSinceStart - WindowMath.WINDOW_SIZE + 1;
        List<String> result = new ArrayList<>(WindowMath.WINDOW_SIZE);
        for (long position = firstFreshPosition; position <= acceptedSinceStart; position++) {
            result.add(ring[WindowMath.ringIndex(position)]);
        }
        return List.copyOf(result);
    }
}
