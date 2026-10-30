package events;

/**
 * A failure that retrying cannot fix, such as an integrity fault. The consumer retries the record
 * {@code CONSUMER_MAX_ATTEMPTS} times, then parks it with {@link #reason()} (D6).
 */
public final class PermanentEventFailure extends RuntimeException {
    private final ParkReason reason;

    public PermanentEventFailure(ParkReason reason, String message) {
        this(reason, message, null);
    }

    public PermanentEventFailure(ParkReason reason, String message, Throwable cause) {
        super(message, cause);
        if (reason == null) throw new IllegalArgumentException("reason is required");
        this.reason = reason;
    }

    public ParkReason reason() { return reason; }
}
