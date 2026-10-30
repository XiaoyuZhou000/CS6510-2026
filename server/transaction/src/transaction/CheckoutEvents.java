package transaction;

import database.CheckoutAttemptStore.AttemptKey;
import database.CheckoutAttemptStore.RejectionReason;
import events.DecodedEvent;
import events.EnvelopeCodec;
import events.EventTypes;
import events.OutboxRow;
import events.ParkReason;
import events.PermanentEventFailure;
import events.TopicNames;
import selfcheckout.events.v1.AttemptId;
import selfcheckout.events.v1.CheckoutLine;
import selfcheckout.events.v1.CheckoutRequested;
import selfcheckout.events.v1.RestorationNotNeeded;
import selfcheckout.events.v1.StockDeducted;
import selfcheckout.events.v1.StockDeductionRefused;
import selfcheckout.events.v1.StockRejected;
import selfcheckout.events.v1.StockRestored;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Transaction's checkout events (contracts/event-catalog.md, D7): builds the {@code CheckoutRequested}
 * envelope and outbox row of an attempt, and decodes Inventory's outcome events into business records.
 *
 * <p>A request has a random UUID {@code event_id}, the transaction ID as record key and correlation ID,
 * {@code <transaction_id>:<attempt_number>} as aggregate key, producer {@code transaction}, and its lines
 * sorted by SKU with one line per SKU; it is recorded once by T1 and re-sent with the same bytes.</p>
 */
public final class CheckoutEvents {
    private final TopicNames names;

    public CheckoutEvents(TopicNames names) {
        this.names = Objects.requireNonNull(names, "names");
    }

    /** One basket line of an attempt: a SKU and its positive quantity. */
    public record Line(String sku, int quantity) {
        public Line {
            Objects.requireNonNull(sku, "sku");
            if (quantity < 1) throw new IllegalArgumentException("quantity must be at least 1");
        }
    }

    /** A decoded outcome event of Inventory, for one attempt. */
    public sealed interface Outcome permits Deducted, Rejected, Refused, Restored, NotNeeded {
        AttemptKey key();
    }

    public record Deducted(AttemptKey key) implements Outcome { }

    /** {@code reason} is INSUFFICIENT_STOCK or INVALID_REQUEST; {@code sku} names the SKU. */
    public record Rejected(AttemptKey key, RejectionReason reason, String sku) implements Outcome { }

    /** The attempt's stock was restored or blocked earlier, so nothing was deducted. */
    public record Refused(AttemptKey key) implements Outcome { }

    /** Reserved: Inventory returned the attempt's deducted units. */
    public record Restored(AttemptKey key) implements Outcome { }

    /** Reserved: Inventory recorded a restoration request without returning units. */
    public record NotNeeded(AttemptKey key) implements Outcome { }

    /** The attempt's {@code CheckoutRequested} outbox row, created at {@code createdAt}. */
    public OutboxRow request(AttemptKey key, List<Line> lines, Instant createdAt) {
        List<Line> sorted = sortedUnique(lines);
        CheckoutRequested.Builder payload = CheckoutRequested.newBuilder().setAttempt(attemptId(key));
        for (Line line : sorted) payload.addLines(CheckoutLine.newBuilder().setSku(line.sku()).setQuantity(line.quantity()));
        String eventId = UUID.randomUUID().toString();
        String transactionId = key.transactionId();
        byte[] envelope = EnvelopeCodec.encode(eventId, EventTypes.CHECKOUT_REQUESTED, transactionId, transactionId,
                EventTypes.TRANSACTION, createdAt.toEpochMilli(), payload.build());
        return new OutboxRow(eventId, names.checkout(), transactionId, EventTypes.CHECKOUT_REQUESTED.typeName(),
                EventTypes.attemptKey(transactionId, key.attemptNumber()), envelope, createdAt);
    }

    /** The business meaning of an outcome event that passed the envelope checks. */
    public static Outcome outcome(DecodedEvent event) {
        return switch (event.type()) {
            case STOCK_DEDUCTED -> new Deducted(key(event.payload(StockDeducted.class).getAttempt()));
            case STOCK_REJECTED -> {
                StockRejected rejected = event.payload(StockRejected.class);
                RejectionReason reason = switch (rejected.getReason()) {
                    case INSUFFICIENT_STOCK -> RejectionReason.INSUFFICIENT_STOCK;
                    case INVALID_REQUEST -> RejectionReason.INVALID_REQUEST;
                    default -> throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD,
                            "StockRejected has no rejection reason");
                };
                yield new Rejected(key(rejected.getAttempt()), reason, rejected.getSku().isEmpty() ? null : rejected.getSku());
            }
            case STOCK_DEDUCTION_REFUSED -> new Refused(key(event.payload(StockDeductionRefused.class).getAttempt()));
            case STOCK_RESTORED -> new Restored(key(event.payload(StockRestored.class).getAttempt()));
            case RESTORATION_NOT_NEEDED -> new NotNeeded(key(event.payload(RestorationNotNeeded.class).getAttempt()));
            default -> throw new PermanentEventFailure(ParkReason.UNKNOWN_TYPE,
                    event.type().typeName() + " does not belong on the outcome topic");
        };
    }

    /** The lines in SKU order; a second line for a SKU is a programming fault. */
    static List<Line> sortedUnique(List<Line> lines) {
        Set<String> skus = new HashSet<>();
        for (Line line : lines) {
            if (!skus.add(line.sku())) throw new IllegalArgumentException("SKU " + line.sku() + " appears twice");
        }
        return lines.stream().sorted(Comparator.comparing(Line::sku)).toList();
    }

    private static AttemptKey key(AttemptId id) {
        if (id.getAttemptNumber() < 1) {
            throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, "attempt_number must be at least 1");
        }
        return new AttemptKey(id.getTransactionId(), id.getAttemptNumber());
    }

    private static AttemptId attemptId(AttemptKey key) {
        return AttemptId.newBuilder().setTransactionId(key.transactionId()).setAttemptNumber(key.attemptNumber()).build();
    }
}
