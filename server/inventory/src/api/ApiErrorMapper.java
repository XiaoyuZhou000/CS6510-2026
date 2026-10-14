package api;

/** Inventory handlers expose the established sanitized database failure shape. */
public final class ApiErrorMapper {
    private ApiErrorMapper() { }

    public static ApiError map(Throwable failure) {
        return new ApiError(500, "INTERNAL_ERROR", "Internal server error");
    }

    public record ApiError(int httpStatus, String error, String message) { }
}
