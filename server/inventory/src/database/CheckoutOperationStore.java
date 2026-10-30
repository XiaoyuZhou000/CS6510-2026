package database;

import events.OutboxRow;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Inventory's checkout operations, one per attempt identifier (R8; 006 D8, contracts/checkout-saga.md
 * "Steps"). An operation record, its stock effect, and its outcome event always share one commit, so a
 * crash before the commit leaves no trace and a redelivered request is processed fresh.
 */
public interface CheckoutOperationStore {

    /**
     * Deducts every line atomically and records APPLIED, or records the rejection and deducts nothing, and
     * inserts the outcome event {@code outcomes} builds for that decision into {@code outbox_event}, storing
     * its {@code event_id} as {@code outcome_event_id}, all in one commit. A request for an attempt that is
     * already recorded rolls back and replays the recorded outcome instead, with its {@code outcome_event_id}.
     *
     * @param lines          unique SKUs in ascending order, each with a positive quantity
     * @param canonicalLines the payload text stored with the outcome; a replay must carry the same text
     * @throws StoreFailure     {@link StoreFailure#contended()} when deadlock or lock-wait retries ran out
     * @throws IntegrityFailure when the transaction already has another APPLIED operation (FR-029)
     */
    ApplyResult apply(Attempt attempt, List<Line> lines, String canonicalLines, OutcomeEvents outcomes)
            throws StoreFailure;

    /**
     * Records {@code refusal}, the {@code StockDeductionRefused} answer to a request for a RESTORED or BLOCKED
     * attempt, unless a row with its {@code event_id} is recorded already; true when this call inserted it.
     */
    boolean recordRefusal(OutboxRow refusal) throws StoreFailure;

    /** The recorded operation, or empty when Inventory never processed the attempt. Reads only. */
    Optional<Recorded> find(Attempt attempt) throws StoreFailure;

    /**
     * The reserved restoration (FR-039, D8): the state table of the contract, plus the outcome event
     * {@code events} builds, in the same commit. Idempotent per attempt; a restoration whose event is
     * recorded already rolls back and returns {@link RestoreResult#replayed()}.
     */
    RestoreResult restore(Attempt attempt, RestorationEvents events) throws StoreFailure;

    /** Builds the outcome event of a freshly decided apply, inside its transaction. */
    @FunctionalInterface
    interface OutcomeEvents {
        OutboxRow outcome(ApplyOutcome outcome, String rejectedSku);
    }

    /** Builds the outcome event of a restoration, inside its transaction; the same request gives the same ID. */
    @FunctionalInterface
    interface RestorationEvents {
        OutboxRow restoration(RestoreOutcome outcome);
    }

    record Attempt(String transactionId, int attemptNumber) {
        public Attempt {
            Objects.requireNonNull(transactionId, "transactionId");
            if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be at least 1");
        }
    }

    record Line(String sku, int quantity) {
        public Line {
            Objects.requireNonNull(sku, "sku");
            if (quantity < 1) throw new IllegalArgumentException("quantity must be at least 1");
        }
    }

    enum ApplyOutcome { APPLIED, INSUFFICIENT_STOCK, INVALID_REQUEST, STOCK_RESTORED, PAYLOAD_MISMATCH }

    /**
     * {@code rejectedSku} is set for INSUFFICIENT_STOCK and INVALID_REQUEST. {@code outcomeEventId} is the
     * recorded outcome event: this call's for a fresh apply, the stored one for a replay.
     */
    record ApplyResult(ApplyOutcome outcome, String rejectedSku, boolean replayed, String outcomeEventId) {
        public ApplyResult {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(outcomeEventId, "outcomeEventId");
        }
    }

    enum State { APPLIED, REJECTED, RESTORED, BLOCKED }

    /** One stored row; {@code lines} is null only for a BLOCKED tombstone. */
    record Recorded(State state, String rejectionReason, String rejectedSku, String lines, String outcomeEventId,
                    String restorationEventId) {
        public Recorded {
            Objects.requireNonNull(state, "state");
        }
    }

    enum RestoreOutcome { RESTORED, ALREADY_RESTORED, NOTHING_TO_RESTORE }

    /**
     * {@code lines} is the restored canonical payload for RESTORED and ALREADY_RESTORED, else null.
     * {@code eventId} is the restoration's outcome event; {@code replayed} means it was recorded before.
     */
    record RestoreResult(RestoreOutcome outcome, String lines, String eventId, boolean replayed) {
        public RestoreResult {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(eventId, "eventId");
        }
    }

    /** A second effective deduction for one transaction was refused by the unique applied index (FR-029). */
    final class IntegrityFailure extends RuntimeException {
        public IntegrityFailure(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
