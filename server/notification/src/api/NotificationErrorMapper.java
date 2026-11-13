package api;

import notification.SubscriptionFailure;

/**
 * Maps Notification's business failures to the HTTP contract's statuses and codes (007 contracts/notification-http.md).
 * Every other failure is {@code 500 INTERNAL_ERROR} with a fixed message, so no response names Kafka, an offset,
 * or SQL, and an internal exception's message is never echoed.
 */
public final class NotificationErrorMapper {
    public static final String INTERNAL_ERROR = "INTERNAL_ERROR";
    public static final String INVALID_REQUEST = "INVALID_REQUEST";

    public record ApiError(int httpStatus, String error, String message) { }

    private NotificationErrorMapper() { }

    public static ApiError map(RuntimeException failure) {
        if (failure instanceof SubscriptionFailure subscription) {
            return switch (subscription.code()) {
                case INVALID_REQUEST, INVALID_SUBSCRIBER_ID, MISSING_SKUS, UNKNOWN_SKU ->
                        new ApiError(400, subscription.code().name(), subscription.getMessage());
                case SUBSCRIBER_NOT_FOUND -> new ApiError(404, subscription.code().name(), subscription.getMessage());
                case SUBSCRIBER_EXISTS, SUBSCRIBER_CANCELLED, LAST_SKU ->
                        new ApiError(409, subscription.code().name(), subscription.getMessage());
                case INTERNAL_ERROR -> new ApiError(500, INTERNAL_ERROR, subscription.getMessage());
            };
        }
        return new ApiError(500, INTERNAL_ERROR, "The request could not be completed; it was not applied or its outcome "
                + "is unknown, and repeating it is safe");
    }
}
