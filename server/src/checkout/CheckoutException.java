package checkout;

public final class CheckoutException extends RuntimeException {

    public final int httpStatus;
    public final String errorCode;

    public CheckoutException(int httpStatus, String errorCode, String message) {
        super(message);
        this.httpStatus = httpStatus;
        this.errorCode  = errorCode;
    }
}
