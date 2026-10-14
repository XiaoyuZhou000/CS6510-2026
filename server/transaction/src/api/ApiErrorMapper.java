package api;

import transaction.TransactionFailure;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/** The single mapping from transport-neutral failures to public HTTP error responses. */
public final class ApiErrorMapper {

    public static final String INTERNAL_ERROR = "INTERNAL_ERROR";
    public static final String INTERNAL_MESSAGE = "Internal server error";

    private static final Map<TransactionFailure.Code, Mapping> DOMAIN_MAPPINGS;

    static {
        EnumMap<TransactionFailure.Code, Mapping> mappings =
                new EnumMap<>(TransactionFailure.Code.class);
        mappings.put(TransactionFailure.Code.NOT_FOUND,
                new Mapping(404, "TRANSACTION_NOT_FOUND"));
        mappings.put(TransactionFailure.Code.UNKNOWN_SKU,
                new Mapping(404, "SKU_NOT_FOUND"));
        mappings.put(TransactionFailure.Code.NOT_OPEN,
                new Mapping(409, "TRANSACTION_NOT_OPEN"));
        mappings.put(TransactionFailure.Code.EMPTY_BASKET,
                new Mapping(409, "EMPTY_BASKET"));
        mappings.put(TransactionFailure.Code.INSUFFICIENT_STOCK,
                new Mapping(409, "INSUFFICIENT_STOCK"));
        DOMAIN_MAPPINGS = Map.copyOf(mappings);
    }

    private ApiErrorMapper() { }

    public static ApiError map(Throwable failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        if (failure instanceof TransactionFailure transactionFailure) {
            Mapping mapping = DOMAIN_MAPPINGS.get(transactionFailure.code());
            return new ApiError(mapping.httpStatus(), mapping.errorCode(),
                    transactionFailure.getMessage());
        }
        return internalError();
    }

    /** Read-only mapping table used by handlers and mapping-focused tests. */
    public static Map<TransactionFailure.Code, Mapping> domainMappings() {
        return DOMAIN_MAPPINGS;
    }

    public static ApiError internalError() {
        return new ApiError(500, INTERNAL_ERROR, INTERNAL_MESSAGE);
    }

    public record Mapping(int httpStatus, String errorCode) {
        public Mapping {
            if (httpStatus < 400 || httpStatus > 599) {
                throw new IllegalArgumentException("httpStatus must be an error status");
            }
            errorCode = required(errorCode, "errorCode");
        }
    }

    /** Exact public response fields; serialization remains an API-layer concern. */
    public record ApiError(int httpStatus, String error, String message) {
        public ApiError {
            if (httpStatus < 400 || httpStatus > 599) {
                throw new IllegalArgumentException("httpStatus must be an error status");
            }
            error = required(error, "error");
            message = required(message, "message");
        }
    }

    private static String required(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
