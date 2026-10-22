package transaction;

import metrics.LatencyHistogram;
import metrics.PerfMetrics;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Serializes requests per transaction with a fixed array of lock stripes (R19).
 *
 * <ul>
 *   <li>Stripes are never created, evicted, or replaced, so evicting or invalidating a cache entry can
 *       never produce a second lock for one transaction.</li>
 *   <li>A thread holds at most one stripe, and never across a remote call or a wait. Two transactions
 *       on one stripe (a collision) therefore only wait for one short local transaction; they cannot
 *       deadlock, and the database guards still decide every outcome.</li>
 *   <li>This is a process-local optimization: correctness never depends on it (Implementation Rule 3).</li>
 * </ul>
 *
 * Every acquisition records {@code transaction.lock.wait} and {@code transaction.lock.hold}, and counts
 * {@code transaction.lock.contended} and, when a different transaction held the stripe,
 * {@code transaction.lock.collisions} (R21).
 */
public final class TransactionLocks {
    public static final int DEFAULT_STRIPES = 4096;
    private static final LatencyHistogram WAIT = PerfMetrics.timer("transaction.lock.wait");
    private static final LatencyHistogram HOLD = PerfMetrics.timer("transaction.lock.hold");
    private static final LongAdder ACQUISITIONS = PerfMetrics.counter("transaction.lock.acquisitions");
    private static final LongAdder CONTENDED = PerfMetrics.counter("transaction.lock.contended");
    private static final LongAdder COLLISIONS = PerfMetrics.counter("transaction.lock.collisions");

    private final Stripe[] stripes;
    private final int mask;
    private final ThreadLocal<Stripe> held = new ThreadLocal<>();

    public TransactionLocks() {
        this(DEFAULT_STRIPES);
    }

    /** Test-only: a smaller power-of-two stripe count, for example 1 to make every pair collide. */
    public TransactionLocks(int stripeCount) {
        if (stripeCount <= 0 || Integer.bitCount(stripeCount) != 1) {
            throw new IllegalArgumentException("stripeCount must be a positive power of two");
        }
        stripes = new Stripe[stripeCount];
        for (int i = 0; i < stripeCount; i++) stripes[i] = new Stripe();
        mask = stripeCount - 1;
    }

    public int stripeCount() {
        return stripes.length;
    }

    public int stripeOf(String transactionId) {
        int hash = transactionId.hashCode();
        return (hash ^ (hash >>> 16)) & mask;
    }

    /** The stripe's lock; the same object for a transaction ID for the lifetime of this instance. */
    public ReentrantLock lockFor(String transactionId) {
        return stripes[stripeOf(transactionId)].lock;
    }

    public boolean heldByCurrentThread(String transactionId) {
        return lockFor(transactionId).isHeldByCurrentThread();
    }

    /** Blocks until the transaction's stripe is held; close the handle to release it. */
    public Held acquire(String transactionId) {
        Stripe stripe = stripes[stripeOf(transactionId)];
        Stripe current = held.get();
        if (current != null && current != stripe) {
            throw new IllegalStateException("A thread may hold only one transaction lock at a time");
        }
        long started = System.nanoTime();
        if (!stripe.lock.tryLock()) {
            CONTENDED.increment();
            String holder = stripe.holder;
            if (holder != null && !holder.equals(transactionId)) COLLISIONS.increment();
            stripe.lock.lock();
        }
        long acquired = System.nanoTime();
        WAIT.record(acquired - started);
        ACQUISITIONS.increment();
        boolean outermost = stripe.lock.getHoldCount() == 1;
        if (outermost) {
            stripe.holder = transactionId;
            stripe.acquiredAt = acquired;
            held.set(stripe);
        }
        return new Held(stripe, outermost);
    }

    /** A held stripe. */
    public final class Held implements AutoCloseable {
        private final Stripe stripe;
        private final boolean outermost;
        private boolean released;

        private Held(Stripe stripe, boolean outermost) {
            this.stripe = stripe;
            this.outermost = outermost;
        }

        @Override
        public void close() {
            if (released) return;
            released = true;
            if (outermost) {
                HOLD.record(System.nanoTime() - stripe.acquiredAt);
                stripe.holder = null;
                held.remove();
            }
            stripe.lock.unlock();
        }
    }

    private static final class Stripe {
        final ReentrantLock lock = new ReentrantLock();
        /** The transaction holding the stripe, kept only to attribute contention. */
        volatile String holder;
        long acquiredAt;
    }
}
