package database;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The Transaction side of the checkout Saga (R5, R9; contracts/checkout-saga.md "Steps"). Every write to
 * the {@code transaction} row is a guarded conditional UPDATE: when its {@code WHERE} clause matches no
 * row, the unit rolls back and returns {@link WriteResult#GUARD_MISS}, and the caller re-evaluates from
 * the database. A failing commit raises {@link StoreFailure} with {@code commitOutcomeUnknown} and is
 * never retried here.
 */
public interface CheckoutAttemptStore {
    int DEFAULT_PAGE_SIZE = 100;

    /**
     * T1: moves the transaction from {@code attempt_count = n - 1} with no active attempt and the expected
     * {@code item_count} to an active attempt {@code n}, and inserts attempt {@code n} as PENDING, in one
     * commit. A stale expectation writes nothing.
     */
    WriteResult begin(Begin begin) throws StoreFailure;

    /**
     * T3: completes the transaction whose active attempt is {@code key}, and settles the attempt as
     * COMPLETED, in one commit. {@code answering} records {@code client_outcome = SUCCESS} for the request
     * that is about to answer 200, unless another outcome is recorded already.
     */
    WriteResult complete(AttemptKey key, Instant completedAt, boolean answering) throws StoreFailure;

    /** Compensates T1: clears the active attempt, leaving the transaction OPEN with its basket, and settles it REJECTED. */
    WriteResult reopen(AttemptKey key, RejectionReason reason, String rejectedSku, Instant settledAt, boolean answering)
            throws StoreFailure;

    /** Records the response of the attempt's own request, only while none is recorded; true when it wrote. */
    boolean recordClientOutcome(AttemptKey key, ClientOutcome outcome) throws StoreFailure;

    /**
     * Before a pending 500: {@code client_outcome} NONE becomes PENDING_ERROR on a PENDING attempt, and
     * {@code next_retry_at} becomes {@code now} in the same UPDATE, so recovery takes over at once.
     */
    boolean markPendingError(AttemptKey key, Instant now) throws StoreFailure;

    /**
     * At most {@code limit} PENDING attempts due at {@code dueBefore}, in
     * {@code (next_retry_at, transaction_id, attempt_number)} order, after {@code after} when it is not
     * null (keyset pagination over the {@code (state, next_retry_at)} index).
     */
    List<AttemptRecord> findPending(Instant dueBefore, PageCursor after, int limit) throws StoreFailure;

    Optional<AttemptRecord> findAttempt(AttemptKey key) throws StoreFailure;

    /** Recovery bookkeeping for an uncertain outcome: one more retry, due at {@code nextRetryAt}. */
    void scheduleRetry(AttemptKey key, Instant nextRetryAt, String lastError) throws StoreFailure;

    /** The attempt identifier shared with Inventory (FR-028). */
    record AttemptKey(String transactionId, int attemptNumber) {
        public AttemptKey {
            Objects.requireNonNull(transactionId, "transactionId");
            if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be at least 1");
        }

        @Override
        public String toString() {
            return transactionId + "#" + attemptNumber;
        }
    }

    /**
     * Attempt {@code key} over a basket of {@code itemCount} units, the cached expectation; the cached
     * {@code attempt_count} is {@code key.attemptNumber() - 1}. {@code lines} is the canonical snapshot.
     */
    record Begin(AttemptKey key, int itemCount, BigDecimal totalAmount, String lines, Instant createdAt,
                 Instant nextRetryAt) {
        public Begin {
            Objects.requireNonNull(key, "key");
            if (itemCount < 1) throw new IllegalArgumentException("an attempt needs at least one unit");
            Objects.requireNonNull(totalAmount, "totalAmount");
            Objects.requireNonNull(lines, "lines");
            Objects.requireNonNull(createdAt, "createdAt");
            Objects.requireNonNull(nextRetryAt, "nextRetryAt");
        }
    }

    enum WriteResult { OK, GUARD_MISS }

    enum State { PENDING, COMPLETED, REJECTED }

    enum RejectionReason { INSUFFICIENT_STOCK, INVALID_REQUEST, STOCK_RESTORED }

    enum ClientOutcome { NONE, SUCCESS, REJECTED, PENDING_ERROR }

    record AttemptRecord(AttemptKey key, State state, RejectionReason rejectionReason, String rejectedSku, String lines,
                         int itemCount, BigDecimal totalAmount, ClientOutcome clientOutcome, Instant createdAt,
                         Instant settledAt, int retryCount, Instant nextRetryAt, String lastError) {
        public AttemptRecord {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(clientOutcome, "clientOutcome");
        }

        /** The position after this row, for the next page. */
        public PageCursor cursor() {
            return new PageCursor(nextRetryAt, key);
        }
    }

    record PageCursor(Instant nextRetryAt, AttemptKey key) {
        public PageCursor {
            Objects.requireNonNull(nextRetryAt, "nextRetryAt");
            Objects.requireNonNull(key, "key");
        }
    }
}
