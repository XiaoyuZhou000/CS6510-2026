package transaction;

/**
 * A checkout attempt for the transaction is pending, so its basket is frozen and nothing may join it
 * (FR-035). It maps to 500; the bounded wait for the attempt to settle builds on this.
 */
public final class CheckoutPendingFailure extends RuntimeException {
    public CheckoutPendingFailure(String transactionId) {
        super("A checkout is pending for transaction " + transactionId);
    }
}
