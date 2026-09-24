package transaction;

import java.util.Objects;

/** Stable, transport-neutral transaction-domain failure. */
public final class TransactionFailure extends RuntimeException {

    public enum Code {
        NOT_FOUND,
        NOT_OPEN,
        EMPTY_BASKET,
        UNKNOWN_SKU,
        INSUFFICIENT_STOCK
    }

    private final Code code;

    public TransactionFailure(Code code, String message) {
        super(requireMessage(message));
        this.code = Objects.requireNonNull(code, "code must not be null");
    }

    public Code code() {
        return code;
    }

    private static String requireMessage(String message) {
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("message must not be blank");
        }
        return message;
    }
}
