package events;

import database.StoreFailure;

import java.time.Duration;
import java.util.Map;

/**
 * How a consumer treats a failed record (D6, FR-013).
 *
 * <ul>
 *   <li><b>Transient</b>: every {@link StoreFailure} (database unavailable, contention exhausted, or commit
 *       outcome unknown) and every {@link TransientEventFailure}. Retried indefinitely with backoff from
 *       100 ms doubling to 5 s; only the record's partition waits.</li>
 *   <li><b>Permanent</b>: {@link EventDecodeException}, {@link PermanentEventFailure}, and any other
 *       {@code RuntimeException}, which is parked as {@link ParkReason#HANDLER_ERROR} with its class and
 *       message, so an unexpected bug never blocks a partition indefinitely. Attempted
 *       {@code CONSUMER_MAX_ATTEMPTS} (5) times, 200 ms apart, then parked.</li>
 *   <li><b>Stall alarm</b>: a record still in transient retry after 60 s is reported as stalled.</li>
 * </ul>
 */
public record ParkingPolicy(int maxAttempts, Duration permanentRetryDelay, Duration minBackoff, Duration maxBackoff,
                            Duration stallAfter) {
    public static final int DEFAULT_MAX_ATTEMPTS = 5;

    /** The verdict for one failure. */
    public record Verdict(boolean transientFailure, ParkReason reason, String detail) { }

    public ParkingPolicy {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be positive");
        for (Duration value : new Duration[] {permanentRetryDelay, minBackoff, maxBackoff, stallAfter}) {
            if (value == null || value.isNegative()) throw new IllegalArgumentException("durations must not be negative");
        }
        if (maxBackoff.compareTo(minBackoff) < 0) throw new IllegalArgumentException("maxBackoff < minBackoff");
    }

    public static ParkingPolicy defaults() {
        return new ParkingPolicy(DEFAULT_MAX_ATTEMPTS, Duration.ofMillis(200), Duration.ofMillis(100),
                Duration.ofSeconds(5), Duration.ofSeconds(60));
    }

    /** {@code CONSUMER_MAX_ATTEMPTS} (5); the delays are fixed by D6. */
    public static ParkingPolicy fromEnvironment(Map<String, String> environment) {
        ParkingPolicy defaults = defaults();
        String attempts = environment.get("CONSUMER_MAX_ATTEMPTS");
        if (attempts == null || attempts.isBlank()) return defaults;
        return new ParkingPolicy(Integer.parseInt(attempts.trim()), defaults.permanentRetryDelay(),
                defaults.minBackoff(), defaults.maxBackoff(), defaults.stallAfter());
    }

    public Verdict classify(RuntimeException failure) {
        if (failure instanceof StoreFailure || failure instanceof TransientEventFailure) {
            return new Verdict(true, null, describe(failure));
        }
        if (failure instanceof EventDecodeException decode) return new Verdict(false, decode.reason(), describe(failure));
        if (failure instanceof PermanentEventFailure permanent) {
            return new Verdict(false, permanent.reason(), describe(failure));
        }
        return new Verdict(false, ParkReason.HANDLER_ERROR, describe(failure));
    }

    /** The wait before transient retry number {@code failures} (1-based): 100 ms, 200 ms, ... up to 5 s. */
    public Duration transientBackoff(int failures) {
        long nanos = minBackoff.toNanos() << Math.min(Math.max(failures - 1, 0), 20);
        return Duration.ofNanos(Math.min(nanos, maxBackoff.toNanos()));
    }

    private static String describe(RuntimeException failure) {
        String message = failure.getMessage();
        return failure.getClass().getName() + (message == null ? "" : ": " + message);
    }
}
