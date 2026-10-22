package database;

/**
 * Transport-neutral failure raised by database adapters.
 *
 * <p>Adapters wrap infrastructure-specific exceptions (for example {@code SQLException}) so
 * callers never need to depend on JDBC. The API layer must not expose this failure's message to
 * clients because it may contain database details.</p>
 *
 * <p>{@link #commitOutcomeUnknown()} is true when {@code commit()} itself failed: the work may or
 * may not be durable, so callers must reconcile from the database instead of retrying blindly
 * (R19, R20).</p>
 *
 * <p>{@link #contended()} is true when deadlock or lock-wait retries were exhausted before commit:
 * nothing is durable, and the caller may retry later (R8).</p>
 */
public final class StoreFailure extends RuntimeException {
    private final boolean commitOutcomeUnknown;
    private final boolean contended;

    public StoreFailure(String message) {
        this(message, null, false);
    }

    public StoreFailure(String message, Throwable cause) {
        this(message, cause, false);
    }

    public StoreFailure(String message, Throwable cause, boolean commitOutcomeUnknown) {
        this(message, cause, commitOutcomeUnknown, false);
    }

    private StoreFailure(String message, Throwable cause, boolean commitOutcomeUnknown, boolean contended) {
        super(requireMessage(message), cause);
        this.commitOutcomeUnknown = commitOutcomeUnknown;
        this.contended = contended;
    }

    /** Contention persisted through every retry; the unit rolled back. */
    public static StoreFailure contended(String message, Throwable cause) {
        return new StoreFailure(message, cause, false, true);
    }

    public boolean commitOutcomeUnknown() {
        return commitOutcomeUnknown;
    }

    public boolean contended() {
        return contended;
    }

    private static String requireMessage(String message) {
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("message must not be blank");
        }
        return message;
    }
}
