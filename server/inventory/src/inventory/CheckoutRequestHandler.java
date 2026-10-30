package inventory;

import database.StoreFailure;
import events.DecodedEvent;
import events.EventHandler;
import events.EventTypes;
import events.ParkReason;
import events.PermanentEventFailure;
import events.TransientEventFailure;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Consumes {@code CheckoutRequested} (D8): checks the lines Transaction froze, which are sorted by SKU with
 * one positive line per SKU, and hands them to {@link CheckoutStockService}. A request that breaks those
 * rules is a programming fault, parked as {@link ParkReason#INVALID_PAYLOAD} and never answered.
 */
public final class CheckoutRequestHandler implements EventHandler {
    private final CheckoutStockService stock;

    public CheckoutRequestHandler(CheckoutStockService stock) {
        this.stock = Objects.requireNonNull(stock, "stock");
    }

    @Override
    public void handle(DecodedEvent event) {
        if (event.type() != EventTypes.CHECKOUT_REQUESTED) {
            throw new PermanentEventFailure(ParkReason.UNKNOWN_TYPE, event.type().typeName() + " is not a checkout request");
        }
        StockOutcomeEvents.Request request = StockOutcomeEvents.request(event);
        validate(request.lines());
        try {
            stock.apply(request.attempt().transactionId(), request.attempt().attemptNumber(), request.lines());
        } catch (CheckoutStockService.InvalidCheckoutRequest invalid) {
            throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, invalid.getMessage(), invalid);
        } catch (StoreFailure failure) {
            // Contention that outlasted the store's own retries, or an unavailable database: never a
            // business outcome such as INSUFFICIENT_STOCK (FR-033). The same record is retried.
            throw new TransientEventFailure("Checkout request " + request.attempt() + " could not be applied yet", failure);
        }
    }

    private static void validate(List<CheckoutStockService.Line> lines) {
        if (lines.isEmpty()) throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, "CheckoutRequested has no lines");
        Set<String> skus = new HashSet<>();
        String previous = null;
        for (CheckoutStockService.Line line : lines) {
            if (line.quantity() < 1) {
                throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, "quantity of " + line.sku() + " is not positive");
            }
            if (!skus.add(line.sku())) {
                throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, "sku " + line.sku() + " appears more than once");
            }
            if (previous != null && previous.compareTo(line.sku()) > 0) {
                throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, "lines are not sorted by SKU");
            }
            previous = line.sku();
        }
    }
}
