package support;

import events.DecodedEvent;
import events.EnvelopeCodec;
import events.EventTypes;
import selfcheckout.events.v1.AttemptId;
import selfcheckout.events.v1.CheckoutLine;
import selfcheckout.events.v1.CheckoutRequested;
import selfcheckout.events.v1.StockRestorationRequested;

import java.util.UUID;

/**
 * Checkout events as Transaction's producer records them, for tests that hand them to Inventory's handlers
 * directly or publish them as the owning producer would. Lines are sent in the order given.
 */
public final class TestEvents {
    private TestEvents() { }

    /** One line of a request. */
    public record Line(String sku, int quantity) { }

    public static Line line(String sku, int quantity) {
        return new Line(sku, quantity);
    }

    /** The serialized envelope of a {@code CheckoutRequested} with a fresh {@code event_id}. */
    public static byte[] requested(String transactionId, int attemptNumber, Line... lines) {
        return requested(UUID.randomUUID().toString(), transactionId, attemptNumber, lines);
    }

    public static byte[] requested(String eventId, String transactionId, int attemptNumber, Line... lines) {
        CheckoutRequested.Builder payload = CheckoutRequested.newBuilder().setAttempt(attempt(transactionId, attemptNumber));
        for (Line line : lines) payload.addLines(CheckoutLine.newBuilder().setSku(line.sku()).setQuantity(line.quantity()));
        return EnvelopeCodec.encode(eventId, EventTypes.CHECKOUT_REQUESTED, transactionId, transactionId,
                EventTypes.TRANSACTION, System.currentTimeMillis(), payload.build());
    }

    /** The decoded {@code CheckoutRequested}, as Inventory's consumer hands it to its handler. */
    public static DecodedEvent request(String transactionId, int attemptNumber, Line... lines) {
        return EnvelopeCodec.decode(requested(transactionId, attemptNumber, lines), transactionId);
    }

    /** The serialized envelope of the reserved {@code StockRestorationRequested}. */
    public static byte[] restorationRequested(String eventId, String transactionId, int attemptNumber) {
        return EnvelopeCodec.encode(eventId, EventTypes.STOCK_RESTORATION_REQUESTED, transactionId, transactionId,
                EventTypes.TRANSACTION, System.currentTimeMillis(),
                StockRestorationRequested.newBuilder().setAttempt(attempt(transactionId, attemptNumber)).build());
    }

    private static AttemptId attempt(String transactionId, int attemptNumber) {
        return AttemptId.newBuilder().setTransactionId(transactionId).setAttemptNumber(attemptNumber).build();
    }
}
