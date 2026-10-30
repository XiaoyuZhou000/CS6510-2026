package inventory;

import api.json.Json;
import database.CheckoutOperationStore;
import events.OutboxRow;
import events.OutboxSignal;
import events.ParkReason;
import events.PermanentEventFailure;
import faults.FaultHooks;
import metrics.PerfMetrics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

/**
 * Inventory's side of the checkout Saga (R5, R8; 006 D8): request validation, the canonical payload, and
 * the outcome events. Each attempt identifier is applied at most once; stock is never cached, and every
 * decision reads the locked stock rows.
 *
 * <ul>
 *   <li>A fresh apply records its outcome event in the apply transaction; after the commit it hits the
 *       fault hook {@code inventory.apply.afterCommit} and signals the dispatcher.</li>
 *   <li>A redelivered request rolls back and re-sends the recorded outcome by {@code outcome_event_id}. A
 *       RESTORED or BLOCKED attempt answers its {@code StockDeductionRefused}, recorded once.</li>
 *   <li>A reused attempt key with other lines, and a second deduction for one transaction, are integrity
 *       faults: {@link PermanentEventFailure}, parked, never answered (FR-038).</li>
 *   <li>Contention keeps the 005 deadlock retry inside the store; a fault that persists surfaces as a
 *       {@code StoreFailure}, which the consumer retries as transient, never as INSUFFICIENT_STOCK (FR-033).</li>
 * </ul>
 */
public final class CheckoutStockService {
    public static final int MAX_LINES = 2000;
    private static final LongAdder INTEGRITY_ALARMS = PerfMetrics.counter("inventory.apply.integrityAlarms");
    private static final LongAdder RESENT = PerfMetrics.counter("inventory.apply.resentOutcomes");

    private final CheckoutOperationStore operations;
    private final StockOutcomeEvents events;
    private final OutboxSignal signal;

    public CheckoutStockService(CheckoutOperationStore operations, StockOutcomeEvents events, OutboxSignal signal) {
        this.operations = Objects.requireNonNull(operations, "operations");
        this.events = Objects.requireNonNull(events, "events");
        this.signal = Objects.requireNonNull(signal, "signal");
    }

    public enum Outcome { APPLIED, INSUFFICIENT_STOCK, INVALID_REQUEST, STOCK_RESTORED, NOT_PROCESSED }

    public enum RestoreOutcome { RESTORED, ALREADY_RESTORED, NOTHING_TO_RESTORE }

    /** One requested line; quantities arrive unsigned, so anything below 1 is malformed. */
    public record Line(String sku, int quantity) { }

    /** {@code rejectedSku} is set for INSUFFICIENT_STOCK and INVALID_REQUEST; {@code eventId} is the answer sent. */
    public record ApplyResult(Outcome outcome, String rejectedSku, boolean replayed, String eventId) { }

    public record RecordedOutcome(Outcome outcome, String rejectedSku) { }

    /** {@code lines} are the restored units for RESTORED and ALREADY_RESTORED, else empty. */
    public record RestoreResult(RestoreOutcome outcome, List<Line> lines, boolean replayed, String eventId) {
        public RestoreResult {
            lines = List.copyOf(lines);
        }
    }

    /**
     * Applies the attempt's lines atomically and records the outcome event, or re-sends the recorded one.
     *
     * @throws InvalidCheckoutRequest a malformed request; nothing is recorded
     * @throws PermanentEventFailure  {@link ParkReason#PAYLOAD_MISMATCH} for a reused identifier with another
     *                                payload, {@link ParkReason#INTEGRITY} for a second applied operation
     */
    public ApplyResult apply(String transactionId, int attemptNumber, List<Line> lines) {
        CheckoutOperationStore.Attempt attempt = attempt(transactionId, attemptNumber);
        List<CheckoutOperationStore.Line> ordered = validated(lines);
        CheckoutOperationStore.ApplyResult result;
        try {
            result = operations.apply(attempt, ordered, canonical(ordered),
                    (outcome, rejectedSku) -> events.outcome(attempt, outcome, rejectedSku));
        } catch (CheckoutOperationStore.IntegrityFailure integrity) {
            throw alarm(ParkReason.INTEGRITY, "A second checkout deduction for transaction " + transactionId
                    + " was refused", integrity);
        }
        if (!result.replayed()) {
            FaultHooks.hit("inventory.apply.afterCommit");
            signal.signal(List.of(result.outcomeEventId()));
            return new ApplyResult(Outcome.valueOf(result.outcome().name()), result.rejectedSku(), false,
                    result.outcomeEventId());
        }
        return switch (result.outcome()) {
            case PAYLOAD_MISMATCH -> throw alarm(ParkReason.PAYLOAD_MISMATCH, "Checkout attempt " + transactionId + "/"
                    + attemptNumber + " was repeated with a different payload", null);
            case STOCK_RESTORED -> {
                OutboxRow refusal = events.refused(attempt);
                if (operations.recordRefusal(refusal)) {
                    signal.signal(List.of(refusal.eventId()));
                } else {
                    resend(refusal.eventId());
                }
                yield new ApplyResult(Outcome.STOCK_RESTORED, null, true, refusal.eventId());
            }
            default -> {
                resend(result.outcomeEventId());
                yield new ApplyResult(Outcome.valueOf(result.outcome().name()), result.rejectedSku(), true,
                        result.outcomeEventId());
            }
        };
    }

    /** The recorded outcome, or NOT_PROCESSED; never changes state. */
    public RecordedOutcome outcome(String transactionId, int attemptNumber) {
        Optional<CheckoutOperationStore.Recorded> recorded = operations.find(attempt(transactionId, attemptNumber));
        if (recorded.isEmpty()) return new RecordedOutcome(Outcome.NOT_PROCESSED, null);
        CheckoutOperationStore.Recorded row = recorded.get();
        return switch (row.state()) {
            case APPLIED -> new RecordedOutcome(Outcome.APPLIED, null);
            case REJECTED -> new RecordedOutcome(Outcome.valueOf(row.rejectionReason()), row.rejectedSku());
            case RESTORED, BLOCKED -> new RecordedOutcome(Outcome.STOCK_RESTORED, null);
        };
    }

    /**
     * The reserved restoration (FR-039, D8), requested by the event {@code requestEventId}; no normal
     * checkout path calls it. A fresh commit records one {@code StockRestored} or {@code RestorationNotNeeded}
     * and hits {@code inventory.restore.afterCommit}; a redelivered request re-sends the recorded event.
     */
    public RestoreResult restore(String transactionId, int attemptNumber, String requestEventId) {
        CheckoutOperationStore.Attempt attempt = attempt(transactionId, attemptNumber);
        Objects.requireNonNull(requestEventId, "requestEventId");
        CheckoutOperationStore.RestoreResult result = operations.restore(attempt,
                outcome -> events.restoration(attempt, requestEventId, outcome));
        if (result.replayed()) {
            resend(result.eventId());
        } else {
            FaultHooks.hit("inventory.restore.afterCommit");
            signal.signal(List.of(result.eventId()));
        }
        List<Line> lines = result.lines() == null ? List.of() : parse(result.lines());
        return new RestoreResult(RestoreOutcome.valueOf(result.outcome().name()), lines, result.replayed(),
                result.eventId());
    }

    private void resend(String eventId) {
        RESENT.increment();
        signal.resend(eventId);
    }

    /** {@code [{"sku":"…","quantity":n},…]}: sorted by SKU, no whitespace. */
    public static String canonical(List<CheckoutOperationStore.Line> lines) {
        List<String> entries = new ArrayList<>(lines.size());
        for (CheckoutOperationStore.Line line : lines.stream()
                .sorted(Comparator.comparing(CheckoutOperationStore.Line::sku)).toList()) {
            entries.add(Json.object("sku", Json.quote(line.sku()), "quantity", Json.number(line.quantity())));
        }
        return Json.array(entries);
    }

    private static List<Line> parse(String canonical) {
        List<Line> lines = new ArrayList<>();
        for (Object entry : (List<?>) Json.parse(canonical)) {
            Map<String, Object> line = Json.asObject(entry);
            lines.add(new Line(Json.getString(line, "sku"), Json.getInt(line, "quantity")));
        }
        return lines;
    }

    private static CheckoutOperationStore.Attempt attempt(String transactionId, int attemptNumber) {
        if (transactionId == null || transactionId.isEmpty() || transactionId.length() > 40) {
            throw new InvalidCheckoutRequest("transaction_id must be 1-40 characters");
        }
        if (attemptNumber < 1) throw new InvalidCheckoutRequest("attempt_number must be at least 1");
        return new CheckoutOperationStore.Attempt(transactionId, attemptNumber);
    }

    private static List<CheckoutOperationStore.Line> validated(List<Line> lines) {
        if (lines == null || lines.isEmpty()) throw new InvalidCheckoutRequest("lines must not be empty");
        if (lines.size() > MAX_LINES) throw new InvalidCheckoutRequest("lines must have at most " + MAX_LINES + " entries");
        Set<String> skus = new HashSet<>();
        List<CheckoutOperationStore.Line> ordered = new ArrayList<>(lines.size());
        for (Line line : lines) {
            String sku = line.sku();
            if (sku == null || sku.isEmpty() || sku.length() > 20) throw new InvalidCheckoutRequest("sku must be 1-20 characters");
            if (!skus.add(sku)) throw new InvalidCheckoutRequest("sku " + sku + " appears more than once");
            if (line.quantity() < 1) throw new InvalidCheckoutRequest("quantity must be at least 1");
            ordered.add(new CheckoutOperationStore.Line(sku, line.quantity()));
        }
        ordered.sort(Comparator.comparing(CheckoutOperationStore.Line::sku));
        return ordered;
    }

    private static PermanentEventFailure alarm(ParkReason reason, String message, Throwable cause) {
        INTEGRITY_ALARMS.increment();
        System.err.println("[inventory] ERROR integrity alarm: " + message);
        return new PermanentEventFailure(reason, message, cause);
    }

    /** A malformed request: a programming fault, never retried; the consumer parks it as INVALID_PAYLOAD. */
    public static final class InvalidCheckoutRequest extends IllegalArgumentException {
        public InvalidCheckoutRequest(String message) {
            super(message);
        }
    }
}
