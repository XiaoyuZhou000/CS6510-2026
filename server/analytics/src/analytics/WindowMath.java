package analytics;

/** Absolute positions and bounds for 1,000-scan windows with a 500-position hop. */
public final class WindowMath {
    public static final int WINDOW_SIZE = 1000;
    public static final int SLIDE_INTERVAL = 500;

    private WindowMath() { }

    public static int ringIndex(long position) {
        if (position < 1) throw new IllegalArgumentException("position must be positive");
        return (int) ((position - 1) % WINDOW_SIZE);
    }

    public static boolean isCheckpointBoundary(long position) {
        return position >= WINDOW_SIZE && (position - WINDOW_SIZE) % SLIDE_INTERVAL == 0;
    }

    public static long windowStart(long end) {
        if (!isCheckpointBoundary(end)) throw new IllegalArgumentException("invalid window end");
        return end - WINDOW_SIZE + 1;
    }

    /** Transitional single-process ingestion uses a fresh count, not a durable cursor. */
    public static boolean isEmissionEligible(long freshCount) {
        return isCheckpointBoundary(freshCount);
    }
}
