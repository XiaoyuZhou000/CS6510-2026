package events;

/**
 * A failure that may clear by itself, such as a peer state that is not ready yet. The consumer keeps the
 * offset, pauses only this record's partition, and retries the same record with backoff indefinitely (D6).
 */
public final class TransientEventFailure extends RuntimeException {
    public TransientEventFailure(String message) {
        super(message);
    }

    public TransientEventFailure(String message, Throwable cause) {
        super(message, cause);
    }
}
