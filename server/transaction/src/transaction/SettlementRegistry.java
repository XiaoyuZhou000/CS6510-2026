package transaction;

import database.CheckoutAttemptStore.AttemptKey;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-local settlement signals for pending checkout attempts (R7; contracts/checkout-saga.md
 * "Bounded waiting"): one future per pending attempt, completed when the attempt settles, plus the
 * attempts a live request in this process still owns, which the recovery worker's safety scan leaves
 * alone.
 *
 * <p>Futures are created and completed only under the transaction's stripe in {@link TransactionLocks},
 * and a settler completes one only after its settlement commit, so a waiter that took its future under
 * the stripe can never miss the wake-up. Correctness never depends on the registry: a waiter also checks
 * the database, which sees settlements made by other processes (Implementation Rule 3).</p>
 */
public final class SettlementRegistry {
    public enum Settlement { COMPLETED, REOPENED }

    private final TransactionLocks locks;
    private final Map<AttemptKey, CompletableFuture<Settlement>> futures = new ConcurrentHashMap<>();
    private final Set<AttemptKey> owned = ConcurrentHashMap.newKeySet();

    public SettlementRegistry(TransactionLocks locks) {
        this.locks = Objects.requireNonNull(locks, "locks");
    }

    public TransactionLocks locks() {
        return locks;
    }

    /** The attempt's settlement future, created on first use. */
    public CompletableFuture<Settlement> futureFor(AttemptKey key) {
        requireLock(key);
        return futures.computeIfAbsent(key, ignored -> new CompletableFuture<>());
    }

    /** Wakes every waiter of an attempt whose settlement committed, here or elsewhere. */
    public void settled(AttemptKey key, Settlement settlement) {
        Objects.requireNonNull(settlement, "settlement");
        requireLock(key);
        CompletableFuture<Settlement> future = futures.remove(key);
        if (future != null) future.complete(settlement);
    }

    /** A waiter that learned of the settlement from the database drops its local future. */
    public void forget(AttemptKey key, CompletableFuture<Settlement> future) {
        futures.remove(key, future);
    }

    /** Marks the attempt as being answered by a live request in this process. */
    public void own(AttemptKey key) {
        owned.add(Objects.requireNonNull(key, "key"));
    }

    public void release(AttemptKey key) {
        owned.remove(key);
    }

    public boolean owned(AttemptKey key) {
        return owned.contains(key);
    }

    public int waiting() {
        return futures.size();
    }

    private void requireLock(AttemptKey key) {
        if (!locks.heldByCurrentThread(key.transactionId())) {
            throw new IllegalStateException("The caller must hold the lock of transaction " + key.transactionId());
        }
    }
}
