package notification;

import database.NotificationJson;
import database.NotificationStore;
import database.SelectionStore;
import database.StoreFailure;
import events.DecodedEvent;
import events.EventHandler;
import events.EventTypes;
import events.ParkReason;
import events.PermanentEventFailure;
import events.TransientEventFailure;
import metrics.PerfMetrics;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.SoldLine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Selects the recipients of one {@code SaleCompleted} (007 FR-010–FR-013, research N5), on a
 * {@code notification-sale-completed} consumer thread:
 *
 * <ol>
 *   <li>The payload is checked again: lines non-empty, unique, and ascending, quantities ≥ 1, and
 *       {@code sale_id = transaction_id = routing_key}; anything else is a {@link PermanentEventFailure}.</li>
 *   <li>The selection transaction locks the clock {@code FOR SHARE}, catches the index up to that version, plans one
 *       notification per (SKU, subscriber), and records the selection with its rows.</li>
 *   <li>A sale already selected is a no-op, so a redelivered event changes nothing (FR-012).</li>
 *   <li>After the commit only, the new rows go to the dispatcher; the consumer then commits the offset.</li>
 * </ol>
 *
 * <p>A {@link StoreFailure} is a {@link TransientEventFailure}: the record is retried, and only its partition waits.</p>
 */
public final class SaleSelectionHandler implements EventHandler {
    private final SelectionStore store;
    private final SubscriptionIndex index;
    private final Consumer<List<NotificationStore.Pending>> dispatcher;

    public SaleSelectionHandler(SelectionStore store, SubscriptionIndex index,
                                Consumer<List<NotificationStore.Pending>> dispatcher) {
        this.store = Objects.requireNonNull(store, "store");
        this.index = Objects.requireNonNull(index, "index");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    }

    @Override
    public void handle(DecodedEvent event) {
        if (event.type() != EventTypes.SALE_COMPLETED) {
            throw new PermanentEventFailure(ParkReason.UNKNOWN_TYPE,
                    event.type().typeName() + " does not belong on the sale-completed topic");
        }
        SaleCompleted sale = event.payload(SaleCompleted.class);
        SelectionStore.Sale selection = validated(sale, event);
        SelectionStore.Result result;
        try {
            result = store.select(selection, (lockedVersion, changes) ->
                    SelectionPlan.plan(selection.saleId(), selection.lines(), snapshot(lockedVersion, changes)));
        } catch (StoreFailure failure) {
            throw new TransientEventFailure("selection of sale " + selection.saleId() + " failed: " + failure.getMessage(),
                    failure);
        }
        switch (result) {
            case SelectionStore.Selected selected -> {
                PerfMetrics.increment("notification.selection.selected");
                PerfMetrics.add("notification.selection.notifications", selected.notifications().size());
                if (!selected.notifications().isEmpty()) dispatcher.accept(selected.notifications());
            }
            case SelectionStore.AlreadySelected ignored -> PerfMetrics.increment("notification.selection.duplicates");
        }
    }

    private SubscriptionIndex.Snapshot snapshot(long lockedVersion, SelectionStore.ChangeSource changes) {
        try {
            return index.atVersion(lockedVersion, changes);
        } catch (IllegalStateException inconsistent) {
            // Committed history and the clock lock rule this out; waiting is safer than parking a sale.
            throw new TransientEventFailure(inconsistent.getMessage(), inconsistent);
        }
    }

    private static SelectionStore.Sale validated(SaleCompleted sale, DecodedEvent event) {
        String saleId = sale.getSaleId();
        if (saleId.isEmpty() || !saleId.equals(sale.getTransactionId()) || !saleId.equals(event.routingKey())) {
            throw new PermanentEventFailure(ParkReason.PAYLOAD_MISMATCH, "sale_id " + saleId + ", transaction_id "
                    + sale.getTransactionId() + ", and routing_key " + event.routingKey() + " must be equal");
        }
        if (sale.getLinesCount() == 0) throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, "sale " + saleId + " has no lines");
        List<NotificationJson.Line> lines = new ArrayList<>(sale.getLinesCount());
        String previous = null;
        for (SoldLine line : sale.getLinesList()) {
            if (line.getSku().isEmpty() || line.getQuantity() < 1) {
                throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, "sale " + saleId + " has an invalid line "
                        + line.getSku() + " x " + line.getQuantity());
            }
            if (previous != null && previous.compareTo(line.getSku()) >= 0) {
                throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, "sale " + saleId
                        + " lines are not unique and ascending at " + line.getSku());
            }
            previous = line.getSku();
            lines.add(new NotificationJson.Line(line.getSku(), line.getQuantity()));
        }
        return new SelectionStore.Sale(saleId, event.eventId(), Instant.ofEpochMilli(sale.getCompletedAtMs()), lines);
    }
}
