package transaction;

/**
 * Inventory definitively refused a checkout attempt for a reason the public contract has no code for:
 * INVALID_REQUEST (a SKU it does not know) or STOCK_RESTORED. The basket was reopened, nothing was
 * deducted, and the request answers 500 (contracts/checkout-saga.md "Response budget and mapping").
 */
public final class CheckoutRejectedFailure extends RuntimeException {
    private final String reason;
    private final String rejectedSku;

    public CheckoutRejectedFailure(String transactionId, String reason, String rejectedSku) {
        super("Checkout of transaction " + transactionId + " was refused: " + reason
                + (rejectedSku == null ? "" : " (SKU " + rejectedSku + ")"));
        this.reason = reason;
        this.rejectedSku = rejectedSku;
    }

    public String reason() {
        return reason;
    }

    public String rejectedSku() {
        return rejectedSku;
    }
}
