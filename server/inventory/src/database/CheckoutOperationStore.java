package database;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Inventory's checkout operations, one per attempt identifier (R8; contracts/grpc-internal-contracts.md
 * §ApplyCheckout, §GetCheckoutOutcome, §RestoreCheckout). An outcome record and its stock effect always
 * share one commit, so a crash before the commit leaves no trace and a replay processes the attempt fresh.
 */
public interface CheckoutOperationStore {

    /**
     * Deducts every line atomically and records APPLIED, or records the rejection and deducts nothing; a
     * request for an attempt that is already recorded replays its outcome instead.
     *
     * @param lines          unique SKUs in ascending order, each with a positive quantity
     * @param canonicalLines the payload text stored with the outcome; a replay must carry the same text
     * @throws StoreFailure     {@link StoreFailure#contended()} when deadlock or lock-wait retries ran out
     * @throws IntegrityFailure when the transaction already has another APPLIED operation (FR-029)
     */
    ApplyResult apply(Attempt attempt, List<Line> lines, String canonicalLines) throws StoreFailure;

    /** The recorded operation, or empty when Inventory never processed the attempt. Reads only. */
    Optional<Recorded> find(Attempt attempt) throws StoreFailure;

    /** The reserved restoration (FR-032): see the state table in the contract. Idempotent per attempt. */
    RestoreResult restore(Attempt attempt) throws StoreFailure;

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

    /** {@code rejectedSku} is set for INSUFFICIENT_STOCK and INVALID_REQUEST. */
    record ApplyResult(ApplyOutcome outcome, String rejectedSku, boolean replayed) {
        public ApplyResult {
            Objects.requireNonNull(outcome, "outcome");
        }
    }

    enum State { APPLIED, REJECTED, RESTORED, BLOCKED }

    /** One stored row; {@code lines} is null only for a BLOCKED tombstone. */
    record Recorded(State state, String rejectionReason, String rejectedSku, String lines) {
        public Recorded {
            Objects.requireNonNull(state, "state");
        }
    }

    enum RestoreOutcome { RESTORED, ALREADY_RESTORED, NOTHING_TO_RESTORE }

    /** {@code lines} is the restored canonical payload for RESTORED and ALREADY_RESTORED, else null. */
    record RestoreResult(RestoreOutcome outcome, String lines) {
        public RestoreResult {
            Objects.requireNonNull(outcome, "outcome");
        }
    }

    /** A second effective deduction for one transaction was refused by the unique applied index (FR-029). */
    final class IntegrityFailure extends RuntimeException {
        public IntegrityFailure(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
