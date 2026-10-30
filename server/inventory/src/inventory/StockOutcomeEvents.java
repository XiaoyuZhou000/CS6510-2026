package inventory;

import com.google.protobuf.Message;
import database.CheckoutOperationStore.ApplyOutcome;
import database.CheckoutOperationStore.Attempt;
import database.CheckoutOperationStore.RestoreOutcome;
import events.DecodedEvent;
import events.EnvelopeCodec;
import events.EventTypes;
import events.OutboxRow;
import events.TopicNames;
import selfcheckout.events.v1.AttemptId;
import selfcheckout.events.v1.CheckoutLine;
import selfcheckout.events.v1.CheckoutRequested;
import selfcheckout.events.v1.RejectionReason;
import selfcheckout.events.v1.RestorationNotNeeded;
import selfcheckout.events.v1.RestorationResult;
import selfcheckout.events.v1.StockDeducted;
import selfcheckout.events.v1.StockDeductionRefused;
import selfcheckout.events.v1.StockRejected;
import selfcheckout.events.v1.StockRestorationRequested;
import selfcheckout.events.v1.StockRestored;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Inventory's checkout events (contracts/event-catalog.md, D8): builds the outcome envelopes and their
 * outbox rows, and decodes the requests Transaction records. Every outcome has producer {@code inventory},
 * the request's key (the transaction ID) as routing key and correlation ID, and the attempt's
 * {@code <transaction_id>:<attempt_number>} as aggregate key, on the outcome topic.
 *
 * <p>A fresh outcome gets a random {@code event_id}. Two are deterministic, so that a redelivered request
 * finds the row it recorded before: the {@code StockDeductionRefused} of an attempt, and the outcome of a
 * restoration, derived from the restoration request's own {@code event_id}.</p>
 */
public final class StockOutcomeEvents {
    private final TopicNames names;
    private final Clock clock;

    public StockOutcomeEvents(TopicNames names, Clock clock) {
        this.names = Objects.requireNonNull(names, "names");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** A decoded {@code CheckoutRequested}: the attempt and its lines in the order they were sent. */
    public record Request(Attempt attempt, List<CheckoutStockService.Line> lines) { }

    /** The outcome event of a freshly decided apply: {@code StockDeducted} or {@code StockRejected}. */
    public OutboxRow outcome(Attempt attempt, ApplyOutcome outcome, String rejectedSku) {
        AttemptId id = attemptId(attempt);
        return switch (outcome) {
            case APPLIED -> row(UUID.randomUUID().toString(), EventTypes.STOCK_DEDUCTED, attempt,
                    StockDeducted.newBuilder().setAttempt(id).build());
            case INSUFFICIENT_STOCK, INVALID_REQUEST -> row(UUID.randomUUID().toString(), EventTypes.STOCK_REJECTED,
                    attempt, StockRejected.newBuilder().setAttempt(id)
                            .setReason(outcome == ApplyOutcome.INSUFFICIENT_STOCK ? RejectionReason.INSUFFICIENT_STOCK
                                    : RejectionReason.INVALID_REQUEST)
                            .setSku(rejectedSku == null ? "" : rejectedSku).build());
            case STOCK_RESTORED, PAYLOAD_MISMATCH -> throw new IllegalArgumentException(
                    outcome + " is never the decision of a fresh apply");
        };
    }

    /** The attempt's {@code StockDeductionRefused}, with the same {@code event_id} every time. */
    public OutboxRow refused(Attempt attempt) {
        return row(deterministicId("StockDeductionRefused:" + attempt.transactionId() + ":" + attempt.attemptNumber()),
                EventTypes.STOCK_DEDUCTION_REFUSED, attempt,
                StockDeductionRefused.newBuilder().setAttempt(attemptId(attempt)).build());
    }

    /** The reserved restoration's outcome, identified by the request {@code requestEventId} that caused it. */
    public OutboxRow restoration(Attempt attempt, String requestEventId, RestoreOutcome outcome) {
        String eventId = deterministicId("restoration:" + requestEventId);
        AttemptId id = attemptId(attempt);
        return switch (outcome) {
            case RESTORED -> row(eventId, EventTypes.STOCK_RESTORED, attempt,
                    StockRestored.newBuilder().setAttempt(id).build());
            case ALREADY_RESTORED, NOTHING_TO_RESTORE -> row(eventId, EventTypes.RESTORATION_NOT_NEEDED, attempt,
                    RestorationNotNeeded.newBuilder().setAttempt(id)
                            .setResult(outcome == RestoreOutcome.ALREADY_RESTORED ? RestorationResult.ALREADY_RESTORED
                                    : RestorationResult.NOTHING_TO_RESTORE).build());
        };
    }

    /** The attempt and lines of a {@code CheckoutRequested} that passed the envelope checks. */
    public static Request request(DecodedEvent event) {
        CheckoutRequested requested = event.payload(CheckoutRequested.class);
        List<CheckoutStockService.Line> lines = new ArrayList<>(requested.getLinesCount());
        for (CheckoutLine line : requested.getLinesList()) {
            lines.add(new CheckoutStockService.Line(line.getSku(), line.getQuantity()));
        }
        return new Request(attempt(requested.getAttempt()), lines);
    }

    /** The attempt of a {@code StockRestorationRequested} that passed the envelope checks. */
    public static Attempt restorationRequest(DecodedEvent event) {
        return attempt(event.payload(StockRestorationRequested.class).getAttempt());
    }

    private static Attempt attempt(AttemptId id) {
        return new Attempt(id.getTransactionId(), id.getAttemptNumber());
    }

    private OutboxRow row(String eventId, EventTypes type, Attempt attempt, Message payload) {
        Instant now = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        String key = attempt.transactionId();
        byte[] envelope = EnvelopeCodec.encode(eventId, type, key, key, EventTypes.INVENTORY, now.toEpochMilli(), payload);
        return new OutboxRow(eventId, names.name(type.topic()), key, type.typeName(),
                EventTypes.attemptKey(attempt.transactionId(), attempt.attemptNumber()), envelope, now);
    }

    private static AttemptId attemptId(Attempt attempt) {
        return AttemptId.newBuilder().setTransactionId(attempt.transactionId())
                .setAttemptNumber(attempt.attemptNumber()).build();
    }

    private static String deterministicId(String name) {
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
