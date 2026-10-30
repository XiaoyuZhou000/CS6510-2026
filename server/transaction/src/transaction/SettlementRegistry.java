package transaction;

import database.CheckoutAttemptStore.AttemptKey;
import database.CheckoutAttemptStore.RejectionReason;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Process-local settlement signals for pending checkout attempts (R7, D7; contracts/checkout-saga.md
 * "Response budget and claiming the response"). Each pending attempt has one entry: a future completed
 * when the attempt settles, and the state of its responder, the live Complete request of this process
 * that waits for it.
 *
 * <ul>
 *   <li>The responder starts WAITING ({@link #register}). Exactly one of the outcome handler
 *       ({@link #claimForHandler}, WAITING → ANSWERING) and the request at its budget end
 *       ({@link #claimTimeout}, WAITING → TIMED_OUT) wins, by compare-and-set, and only the winner writes
 *       {@code client_outcome}. An ANSWERING responder receives the settlement however late it comes.</li>
 *   <li>Futures are created and completed only under the transaction's stripe in {@link TransactionLocks},
 *       and a settler completes one only after its settlement commit, so a waiter that took its future
 *       under the stripe can never miss the wake-up.</li>
 * </ul>
 *
 * <p>The registry is never authoritative: a waiter also checks the database, which sees settlements made
 * by other processes, and an entry without a responder is completed like any other (Implementation
 * Rule 1).</p>
 */
public final class SettlementRegistry {
    public enum Responder { WAITING, ANSWERING, TIMED_OUT }

    public enum Kind { COMPLETED, REOPENED, FAILED }

    /**
     * How an attempt settled, as its settler knows it. {@code completedAt} is set for COMPLETED, the reason
     * and SKU for REOPENED. {@code outcomeRecorded} says whether the settler's commit also wrote the
     * claimed request's {@code client_outcome}; FAILED means a claimed handler could not commit, and the
     * attempt is still pending.
     */
    public record Settlement(Kind kind, Instant completedAt, RejectionReason reason, String rejectedSku,
                             boolean outcomeRecorded) {
        public Settlement {
            Objects.requireNonNull(kind, "kind");
        }

        public static Settlement completed(Instant completedAt, boolean outcomeRecorded) {
            return new Settlement(Kind.COMPLETED, completedAt, null, null, outcomeRecorded);
        }

        public static Settlement reopened(RejectionReason reason, String rejectedSku, boolean outcomeRecorded) {
            return new Settlement(Kind.REOPENED, null, reason, rejectedSku, outcomeRecorded);
        }

        public static Settlement failed() {
            return new Settlement(Kind.FAILED, null, null, null, false);
        }
    }

    private static final class Entry {
        final CompletableFuture<Settlement> future = new CompletableFuture<>();
        final AtomicReference<Responder> responder = new AtomicReference<>();
    }

    private final TransactionLocks locks;
    private final Map<AttemptKey, Entry> entries = new ConcurrentHashMap<>();

    public SettlementRegistry(TransactionLocks locks) {
        this.locks = Objects.requireNonNull(locks, "locks");
    }

    public TransactionLocks locks() {
        return locks;
    }

    /** The attempt's settlement future, created on first use; for requests that wait on another's attempt. */
    public CompletableFuture<Settlement> futureFor(AttemptKey key) {
        requireLock(key);
        return entries.computeIfAbsent(key, ignored -> new Entry()).future;
    }

    /** Registers the attempt's own request as its WAITING responder and returns the settlement future. */
    public CompletableFuture<Settlement> register(AttemptKey key) {
        requireLock(key);
        Entry entry = entries.computeIfAbsent(key, ignored -> new Entry());
        if (!entry.responder.compareAndSet(null, Responder.WAITING)) {
            throw new IllegalStateException("Attempt " + key + " already has a responder");
        }
        return entry.future;
    }

    /** The handler's claim, WAITING → ANSWERING; false when no responder waits here, or it timed out. */
    public boolean claimForHandler(AttemptKey key) {
        Entry entry = entries.get(key);
        return entry != null && entry.responder.compareAndSet(Responder.WAITING, Responder.ANSWERING);
    }

    /** The request's claim at its budget end, WAITING → TIMED_OUT; false when the handler claimed first. */
    public boolean claimTimeout(AttemptKey key) {
        Entry entry = entries.get(key);
        return entry != null && entry.responder.compareAndSet(Responder.WAITING, Responder.TIMED_OUT);
    }

    /** The responder's state, or null when no live request of this process answers the attempt. */
    public Responder responder(AttemptKey key) {
        Entry entry = entries.get(key);
        return entry == null ? null : entry.responder.get();
    }

    /** True while the attempt's own request still waits for or receives its settlement here. */
    public boolean owned(AttemptKey key) {
        Responder state = responder(key);
        return state == Responder.WAITING || state == Responder.ANSWERING;
    }

    /**
     * Wakes every waiter of an attempt whose settlement committed, here or elsewhere, and drops the entry.
     * A FAILED settlement wakes them too; they find the attempt still pending and wait again.
     */
    public void complete(AttemptKey key, Settlement settlement) {
        Objects.requireNonNull(settlement, "settlement");
        requireLock(key);
        Entry entry = entries.remove(key);
        if (entry != null) entry.future.complete(settlement);
    }

    /**
     * The request leaves without an answer from the registry, for example on interruption: a WAITING
     * responder becomes TIMED_OUT, so that recovery no longer treats the attempt as owned.
     */
    public void release(AttemptKey key) {
        Entry entry = entries.get(key);
        if (entry != null) entry.responder.compareAndSet(Responder.WAITING, Responder.TIMED_OUT);
    }

    public int waiting() {
        return entries.size();
    }

    private void requireLock(AttemptKey key) {
        if (!locks.heldByCurrentThread(key.transactionId())) {
            throw new IllegalStateException("The caller must hold the lock of transaction " + key.transactionId());
        }
    }
}
