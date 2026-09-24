package database;

/**
 * Transport-neutral failure raised by database adapters.
 *
 * <p>Adapters wrap infrastructure-specific exceptions (for example {@code SQLException}) so
 * callers never need to depend on JDBC. The API layer must not expose this failure's message to
 * clients because it may contain database details.</p>
 */
public final class StoreFailure extends RuntimeException {

    public StoreFailure(String message) {
        super(requireMessage(message));
    }

    public StoreFailure(String message, Throwable cause) {
        super(requireMessage(message), cause);
    }

    private static String requireMessage(String message) {
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("message must not be blank");
        }
        return message;
    }
}
