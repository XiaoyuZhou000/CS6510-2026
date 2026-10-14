package analytics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/** Orders durable scans and maintains the 1,000-position hopping-window ring. */
public final class WindowFilter implements Runnable {
    private final BlockingQueue<PipelineMessage.IngressMessage> input;
    private final BlockingQueue<PipelineMessage.WindowMessage> output;
    private final String[] ring = new String[WindowMath.WINDOW_SIZE];
    private final boolean transitionalInput;
    private long lastApplied;
    private long acceptedSinceStart;

    /** Transitional constructor retained for existing single-process pipeline tests. */
    public WindowFilter(BlockingQueue<PipelineMessage.IngressMessage> input,
            BlockingQueue<PipelineMessage.WindowMessage> output, long persistedMaximumEnd) {
        this.input = Objects.requireNonNull(input, "input");
        this.output = Objects.requireNonNull(output, "output");
        if (persistedMaximumEnd < 0) throw new IllegalArgumentException("negative checkpoint");
        this.lastApplied = persistedMaximumEnd;
        this.transitionalInput = true;
    }

    /** Durable mode: reconstruct the exact overlap at the committed checkpoint before polling. */
    public WindowFilter(BlockingQueue<PipelineMessage.IngressMessage> input,
            BlockingQueue<PipelineMessage.WindowMessage> output, long checkpoint,
            List<PipelineMessage.AcceptedScan> prewarm) {
        this.input = Objects.requireNonNull(input, "input");
        this.output = Objects.requireNonNull(output, "output");
        Objects.requireNonNull(prewarm, "prewarm");
        if (checkpoint < 0 || (checkpoint != 0 && !WindowMath.isCheckpointBoundary(checkpoint))) {
            throw new IllegalArgumentException("invalid checkpoint");
        }
        if (prewarm.size() != (checkpoint == 0 ? 0 : WindowMath.WINDOW_SIZE)) {
            throw new IllegalArgumentException("prewarm must contain the complete checkpoint window");
        }
        long expected = checkpoint == 0 ? 1 : checkpoint - WindowMath.WINDOW_SIZE + 1;
        for (PipelineMessage.AcceptedScan scan : prewarm) {
            if (scan.position() != expected) throw new IllegalArgumentException("prewarm has a missing position");
            ring[WindowMath.ringIndex(expected++)] = scan.sku();
        }
        this.lastApplied = checkpoint;
        this.transitionalInput = false;
    }

    @Override
    public void run() {
        try {
            while (true) {
                PipelineMessage.IngressMessage message = input.take();
                if (message instanceof PipelineMessage.IngressEnd) {
                    output.put(PipelineMessage.WindowEnd.INSTANCE);
                    return;
                }
                PipelineMessage.AcceptedScan scan = (PipelineMessage.AcceptedScan) message;
                if (transitionalInput) {
                    long position = ++acceptedSinceStart + lastApplied;
                    ring[WindowMath.ringIndex(acceptedSinceStart)] = scan.sku();
                    if (WindowMath.isEmissionEligible(acceptedSinceStart)) {
                        output.put(new PipelineMessage.WindowSnapshot(position - 999, position,
                                transitionalSnapshot(acceptedSinceStart)));
                    }
                } else {
                    acceptPositioned(scan);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void acceptPositioned(PipelineMessage.AcceptedScan scan) throws InterruptedException {
        long position = scan.position();
        if (position <= lastApplied) {
            if (position <= lastApplied - WindowMath.WINDOW_SIZE
                    || !scan.sku().equals(ring[WindowMath.ringIndex(position)])) {
                throw new IllegalStateException("conflicting or unverifiable duplicate position " + position);
            }
            return;
        }
        if (position != lastApplied + 1) {
            throw new IllegalStateException("missing accepted-scan position after " + lastApplied);
        }
        ring[WindowMath.ringIndex(position)] = scan.sku();
        lastApplied = position;
        if (WindowMath.isCheckpointBoundary(position)) {
            output.put(new PipelineMessage.WindowSnapshot(WindowMath.windowStart(position), position,
                    positionedSnapshot(position)));
        }
    }

    private List<String> positionedSnapshot(long end) {
        List<String> result = new ArrayList<>(WindowMath.WINDOW_SIZE);
        for (long position = end - WindowMath.WINDOW_SIZE + 1; position <= end; position++) {
            result.add(ring[WindowMath.ringIndex(position)]);
        }
        return List.copyOf(result);
    }

    private List<String> transitionalSnapshot(long count) {
        List<String> result = new ArrayList<>(WindowMath.WINDOW_SIZE);
        for (long position = count - WindowMath.WINDOW_SIZE + 1; position <= count; position++) {
            result.add(ring[WindowMath.ringIndex(position)]);
        }
        return List.copyOf(result);
    }
}
