package analytics;

/** Pure, recovery-safe hopping-window calculations shared by pipeline stages and tests. */
public final class WindowMath {
    public static final int WINDOW_SIZE = 1000;
    public static final int SLIDE_INTERVAL = 500;

    private WindowMath() {}

    public static int ringIndex(long scanNumber) {
        if (scanNumber < 1) throw new IllegalArgumentException("scanNumber must be positive");
        return (int) ((scanNumber - 1) % WINDOW_SIZE);
    }

    /**
     * Returns the position assigned to a fresh event after recovery from the maximum persisted
     * window end. Fresh event counts are one-based; no prior ring contents are assumed.
     */
    public static long scanPosition(long persistedMaximumEnd, long acceptedSinceStart) {
        if (persistedMaximumEnd < 0) {
            throw new IllegalArgumentException("persistedMaximumEnd must not be negative");
        }
        if (acceptedSinceStart < 1) {
            throw new IllegalArgumentException("acceptedSinceStart must be positive");
        }
        try {
            return Math.addExact(persistedMaximumEnd, acceptedSinceStart);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("scan position exceeds the supported range", overflow);
        }
    }

    /**
     * True only when this process owns a complete 1,000-event window and is at a 500-event hop.
     * This deliberately depends on fresh events rather than on the recovered absolute position.
     */
    public static boolean isEmissionEligible(long acceptedSinceStart) {
        return acceptedSinceStart >= WINDOW_SIZE
                && (acceptedSinceStart - WINDOW_SIZE) % SLIDE_INTERVAL == 0;
    }

    /** Returns the complete eligible bounds, seeded after the persisted maximum end. */
    public static WindowBounds emissionBounds(long persistedMaximumEnd, long acceptedSinceStart) {
        if (!isEmissionEligible(acceptedSinceStart)) {
            throw new IllegalArgumentException(
                    "acceptedSinceStart must identify a complete 1,000-event window boundary");
        }
        long end = scanPosition(persistedMaximumEnd, acceptedSinceStart);
        return new WindowBounds(end - WINDOW_SIZE + 1, end);
    }

    public static boolean isCheckpointBoundary(long scanNumber) {
        return scanNumber >= WINDOW_SIZE && scanNumber % SLIDE_INTERVAL == 0;
    }

    public static long windowStart(long windowEnd) {
        if (!isCheckpointBoundary(windowEnd)) {
            throw new IllegalArgumentException("windowEnd must be a positive checkpoint boundary");
        }
        return windowEnd - WINDOW_SIZE + 1;
    }

    /**
     * Determines what to do at a scan count and carries the one-shot restart skip forward.
     * A non-boundary never consumes the skip; the first boundary does.
     */
    public static CheckpointDecision checkpointDecision(long scanNumber, boolean skipNextCheckpoint) {
        if (!isCheckpointBoundary(scanNumber)) {
            return new CheckpointDecision(false, skipNextCheckpoint);
        }
        if (skipNextCheckpoint) {
            return new CheckpointDecision(false, false);
        }
        return new CheckpointDecision(true, false);
    }

    public record CheckpointDecision(boolean checkpoint, boolean skipNextCheckpoint) {}

    public record WindowBounds(long windowStart, long windowEnd) {
        public WindowBounds {
            if (windowStart <= 0 || windowEnd - windowStart + 1 != WINDOW_SIZE) {
                throw new IllegalArgumentException("bounds must describe one complete 1,000-event window");
            }
        }
    }
}
