package rpc;

import java.time.Duration;
import java.util.function.LongSupplier;

/** A fixed response budget measured on a monotonic clock from handler entry (R6). */
public final class ResponseBudget {
    private final long startNanos;
    private final long totalNanos;
    private final LongSupplier clock;

    private ResponseBudget(long startNanos, Duration total, LongSupplier clock) {
        if (total == null || total.isNegative()) throw new IllegalArgumentException("budget must not be negative");
        this.startNanos = startNanos;
        this.totalNanos = total.toNanos();
        this.clock = clock;
    }

    public static ResponseBudget start(Duration total) {
        return start(total, System::nanoTime);
    }

    /** Starts a budget on the given nanosecond clock; tests pass a controllable clock. */
    public static ResponseBudget start(Duration total, LongSupplier nanoClock) {
        return new ResponseBudget(nanoClock.getAsLong(), total, nanoClock);
    }

    public long startNanos() { return startNanos; }

    public Duration total() { return Duration.ofNanos(totalNanos); }

    /** Time left, never negative. */
    public Duration remaining() {
        long left = totalNanos - (clock.getAsLong() - startNanos);
        return left <= 0 ? Duration.ZERO : Duration.ofNanos(left);
    }

    public boolean expired() {
        return remaining().isZero();
    }

    /** The deadline for the next call: the smaller of {@code perCall} and the time left. */
    public Duration nextDeadline(Duration perCall) {
        Duration left = remaining();
        return perCall.compareTo(left) < 0 ? perCall : left;
    }
}
