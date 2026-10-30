package inventory;

import database.CheckoutOperationStore.Attempt;
import events.DecodedEvent;
import events.EventHandler;
import events.EventTypes;
import events.ParkReason;
import events.PermanentEventFailure;

import java.util.Objects;

/**
 * Consumes the reserved {@code StockRestorationRequested} (FR-039, D8). No production code produces it;
 * direct tests record it in Transaction's outbox and send it as the owning producer would. The restoration
 * is idempotent per attempt, and its outcome event is identified by the request's own {@code event_id}, so a
 * redelivered request re-sends the event it recorded.
 */
public final class StockRestorationHandler implements EventHandler {
    private final CheckoutStockService stock;

    public StockRestorationHandler(CheckoutStockService stock) {
        this.stock = Objects.requireNonNull(stock, "stock");
    }

    @Override
    public void handle(DecodedEvent event) {
        if (event.type() != EventTypes.STOCK_RESTORATION_REQUESTED) {
            throw new PermanentEventFailure(ParkReason.UNKNOWN_TYPE, event.type().typeName() + " is not a restoration request");
        }
        Attempt attempt = StockOutcomeEvents.restorationRequest(event);
        try {
            stock.restore(attempt.transactionId(), attempt.attemptNumber(), event.eventId());
        } catch (CheckoutStockService.InvalidCheckoutRequest invalid) {
            throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, invalid.getMessage(), invalid);
        }
    }
}
