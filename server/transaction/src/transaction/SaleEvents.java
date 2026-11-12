package transaction;

import events.EnvelopeCodec;
import events.EventIds;
import events.EventTypes;
import events.OutboxRow;
import events.TopicNames;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.SoldLine;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Builds the {@code SaleCompleted} outbox row that T3 records together with the completion (007 FR-001 to
 * FR-005, contracts/sale-completed-event.md).
 *
 * <p>The row's {@code event_id} is {@link EventIds#saleEventId} of the transaction ID, so a sale has exactly one
 * possible identity; {@code sale_id}, {@code transaction_id}, the record key, the routing key, and the correlation
 * ID are all the transaction ID, the producer is {@code transaction}, and the aggregate key is
 * {@code sale:<transaction_id>}. The lines are the completed attempt's basket: non-empty, one per SKU, in
 * ascending SKU order, each with a quantity of at least 1. Who is notified is decided only by Notification
 * Service; the event names nobody (FR-004).</p>
 */
public final class SaleEvents {
    public static final String AGGREGATE_PREFIX = "sale:";

    private final TopicNames names;

    public SaleEvents(TopicNames names) {
        this.names = Objects.requireNonNull(names, "names");
    }

    /** The outbox {@code aggregate_key} of a sale's {@code SaleCompleted}: {@code sale:<transaction_id>}. */
    public static String aggregateKey(String transactionId) {
        return AGGREGATE_PREFIX + transactionId;
    }

    /**
     * The {@code SaleCompleted} row of transaction {@code transactionId}, completed by attempt
     * {@code attemptNumber} at {@code completedAt}.
     *
     * @throws IllegalArgumentException for an empty basket, a duplicate SKU, or a missing identity
     */
    public OutboxRow saleCompleted(String transactionId, int attemptNumber, Instant completedAt, String stationId,
                                   List<CheckoutEvents.Line> lines) {
        if (transactionId == null || transactionId.isEmpty()) throw new IllegalArgumentException("transactionId is required");
        if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be at least 1");
        Objects.requireNonNull(completedAt, "completedAt");
        Objects.requireNonNull(lines, "lines");
        if (lines.isEmpty()) throw new IllegalArgumentException("a completed sale has at least one line");
        SaleCompleted.Builder payload = SaleCompleted.newBuilder()
                .setSaleId(transactionId)
                .setTransactionId(transactionId)
                .setAttemptNumber(attemptNumber)
                .setCompletedAtMs(completedAt.toEpochMilli())
                .setStationId(stationId == null ? "" : stationId);
        for (CheckoutEvents.Line line : CheckoutEvents.sortedUnique(lines)) {
            payload.addLines(SoldLine.newBuilder().setSku(line.sku()).setQuantity(line.quantity()));
        }
        String eventId = EventIds.saleEventId(transactionId);
        byte[] envelope = EnvelopeCodec.encode(eventId, EventTypes.SALE_COMPLETED, transactionId, transactionId,
                EventTypes.TRANSACTION, completedAt.toEpochMilli(), payload.build());
        return new OutboxRow(eventId, names.saleCompleted(), transactionId, EventTypes.SALE_COMPLETED.typeName(),
                aggregateKey(transactionId), envelope, completedAt);
    }
}
