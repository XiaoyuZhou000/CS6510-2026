package analytics;

/** Pure hopping-window calculations shared by the recorder and its unit tests. */
public final class WindowMath {
    public static final int WINDOW_SIZE = 1000;
    public static final int SLIDE_INTERVAL = 500;

    private WindowMath() {}

    public static int ringIndex(long scanNumber) {
        if (scanNumber < 1) throw new IllegalArgumentException("scanNumber must be positive");
        return (int) ((scanNumber - 1) % WINDOW_SIZE);
    }

    public static boolean isCheckpointBoundary(long scanNumber) {
        return scanNumber > 0 && scanNumber % SLIDE_INTERVAL == 0;
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
}
