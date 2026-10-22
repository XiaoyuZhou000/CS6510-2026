package transaction;

import database.CheckoutAttemptStore.AttemptKey;
import metrics.PerfMetrics;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The recovery worker's in-memory delay queue of checkout attempts to resume (R7; contracts/checkout-saga.md
 * "Recovery worker"). It is fed by request handovers at budget expiry, by reconcile entries after
 * ambiguous T1 commits, and by the worker's own backoff retries. Each key is held once, at its earliest
 * due time.
 *
 * <p>The schedule is a process-local optimization: the durable {@code next_retry_at} of every PENDING
 * attempt, the worker's startup scan, and its bounded safety scan rebuild anything it held, so a crash
 * loses nothing (Implementation Rule 3).</p>
 */
public final class RecoverySchedule {
    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Map<AttemptKey, Entry> byKey = new HashMap<>();
    private final TreeSet<Entry> byDue = new TreeSet<>(Comparator.comparing(Entry::dueAt).thenComparingLong(Entry::order));
    private long order;

    public RecoverySchedule() {
        this(Clock.systemUTC());
    }

    public RecoverySchedule(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
        PerfMetrics.gauge("transaction.recovery.scheduleSize", this::size);
    }

    /** Schedules {@code key} at {@code dueAt}; a key already scheduled keeps the earlier of the two times. */
    public void enqueue(AttemptKey key, Instant dueAt) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(dueAt, "dueAt");
        lock.lock();
        try {
            Entry existing = byKey.get(key);
            if (existing != null) {
                if (!dueAt.isBefore(existing.dueAt())) return;
                byDue.remove(existing);
            }
            Entry entry = new Entry(key, dueAt, order++);
            byKey.put(key, entry);
            byDue.add(entry);
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** A reconcile entry, due now: settle the attempt if it exists, or drop the entry if it does not. */
    public void reconcile(AttemptKey key) {
        enqueue(key, clock.instant());
    }

    /** Removes and returns the next due key, waiting up to {@code timeout}; null when none became due. */
    public AttemptKey take(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        lock.lock();
        try {
            while (true) {
                Instant now = clock.instant();
                Entry first = byDue.isEmpty() ? null : byDue.first();
                if (first != null && !first.dueAt().isAfter(now)) {
                    byDue.remove(first);
                    byKey.remove(first.key());
                    return first.key();
                }
                long left = deadline - System.nanoTime();
                if (left <= 0) return null;
                long untilDue = first == null ? left : Math.max(1, Duration.between(now, first.dueAt()).toNanos());
                changed.awaitNanos(Math.min(left, untilDue));
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean contains(AttemptKey key) {
        lock.lock();
        try {
            return byKey.containsKey(key);
        } finally {
            lock.unlock();
        }
    }

    /** The due time of {@code key}, or null when it is not scheduled. */
    public Instant dueAt(AttemptKey key) {
        lock.lock();
        try {
            Entry entry = byKey.get(key);
            return entry == null ? null : entry.dueAt();
        } finally {
            lock.unlock();
        }
    }

    public boolean remove(AttemptKey key) {
        lock.lock();
        try {
            Entry entry = byKey.remove(key);
            if (entry == null) return false;
            byDue.remove(entry);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return byKey.size();
        } finally {
            lock.unlock();
        }
    }

    private record Entry(AttemptKey key, Instant dueAt, long order) { }
}
