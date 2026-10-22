package inventory;

import api.json.Json;
import database.CheckoutOperationStore;
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
 * Inventory's side of the checkout Saga (R5, R8; contracts/grpc-internal-contracts.md §ApplyCheckout):
 * request validation, the canonical payload, and outcome mapping. Each attempt identifier is applied at
 * most once; stock is never cached, and every decision reads the locked stock rows.
 */
public final class CheckoutStockService {
    public static final int MAX_LINES = 2000;
    private static final LongAdder INTEGRITY_ALARMS = PerfMetrics.counter("inventory.apply.integrityAlarms");

    private final CheckoutOperationStore operations;

    public CheckoutStockService(CheckoutOperationStore operations) {
        this.operations = Objects.requireNonNull(operations, "operations");
    }

    public enum Outcome { APPLIED, INSUFFICIENT_STOCK, INVALID_REQUEST, STOCK_RESTORED, NOT_PROCESSED }

    public enum RestoreOutcome { RESTORED, ALREADY_RESTORED, NOTHING_TO_RESTORE }

    /** One requested line; quantities arrive unsigned, so anything below 1 is malformed. */
    public record Line(String sku, int quantity) { }

    /** {@code rejectedSku} is set for INSUFFICIENT_STOCK and INVALID_REQUEST. */
    public record ApplyResult(Outcome outcome, String rejectedSku, boolean replayed) { }

    public record RecordedOutcome(Outcome outcome, String rejectedSku) { }

    /** {@code lines} are the restored units for RESTORED and ALREADY_RESTORED, else empty. */
    public record RestoreResult(RestoreOutcome outcome, List<Line> lines) {
        public RestoreResult {
            lines = List.copyOf(lines);
        }
    }

    /**
     * Applies the attempt's lines atomically, or replays its recorded outcome.
     *
     * @throws InvalidCheckoutRequest a malformed request; nothing is recorded
     * @throws CheckoutConflict       the identifier was reused with another payload, or the transaction
     *                                already has an applied operation
     */
    public ApplyResult apply(String transactionId, int attemptNumber, List<Line> lines) {
        CheckoutOperationStore.Attempt attempt = attempt(transactionId, attemptNumber);
        List<CheckoutOperationStore.Line> ordered = validated(lines);
        CheckoutOperationStore.ApplyResult result;
        try {
            result = operations.apply(attempt, ordered, canonical(ordered));
        } catch (CheckoutOperationStore.IntegrityFailure integrity) {
            throw alarm(new CheckoutConflict("A second checkout deduction for transaction " + transactionId
                    + " was refused", integrity));
        }
        return switch (result.outcome()) {
            case APPLIED -> new ApplyResult(Outcome.APPLIED, null, result.replayed());
            case INSUFFICIENT_STOCK -> new ApplyResult(Outcome.INSUFFICIENT_STOCK, result.rejectedSku(), result.replayed());
            case INVALID_REQUEST -> new ApplyResult(Outcome.INVALID_REQUEST, result.rejectedSku(), result.replayed());
            case STOCK_RESTORED -> new ApplyResult(Outcome.STOCK_RESTORED, null, result.replayed());
            case PAYLOAD_MISMATCH -> throw alarm(new CheckoutConflict("Checkout attempt " + transactionId + "/"
                    + attemptNumber + " was repeated with a different payload", null));
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

    /** The reserved restoration (FR-032); no normal checkout path calls it. */
    public RestoreResult restore(String transactionId, int attemptNumber) {
        CheckoutOperationStore.RestoreResult result = operations.restore(attempt(transactionId, attemptNumber));
        List<Line> lines = result.lines() == null ? List.of() : parse(result.lines());
        return new RestoreResult(RestoreOutcome.valueOf(result.outcome().name()), lines);
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

    private static CheckoutConflict alarm(CheckoutConflict conflict) {
        INTEGRITY_ALARMS.increment();
        System.err.println("[inventory] ERROR integrity alarm: " + conflict.getMessage());
        return conflict;
    }

    /** A malformed request: a programming fault, never retried (INVALID_ARGUMENT). */
    public static final class InvalidCheckoutRequest extends IllegalArgumentException {
        public InvalidCheckoutRequest(String message) {
            super(message);
        }
    }

    /** An integrity fault: never a rejection; the caller keeps its attempt pending (FAILED_PRECONDITION). */
    public static final class CheckoutConflict extends RuntimeException {
        public CheckoutConflict(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
