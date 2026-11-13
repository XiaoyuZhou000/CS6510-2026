package notification;

/**
 * A subscription request that was not applied (contracts/notification-http.md), with its error code. The API
 * layer maps each code to its HTTP status; the message never names Kafka, an offset, or SQL.
 */
public final class SubscriptionFailure extends RuntimeException {
    public enum Code {
        INVALID_REQUEST, INVALID_SUBSCRIBER_ID, MISSING_SKUS, UNKNOWN_SKU, SUBSCRIBER_EXISTS, SUBSCRIBER_NOT_FOUND,
        SUBSCRIBER_CANCELLED, LAST_SKU,
        /** Catalog not loaded, database unavailable, or a commit that failed or whose outcome is unknown. */
        INTERNAL_ERROR
    }

    private final Code code;

    public SubscriptionFailure(Code code, String message) {
        this(code, message, null);
    }

    public SubscriptionFailure(Code code, String message, Throwable cause) {
        super(message, cause);
        if (code == null) throw new IllegalArgumentException("code is required");
        this.code = code;
    }

    public Code code() { return code; }
}
