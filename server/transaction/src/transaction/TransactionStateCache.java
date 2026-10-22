package transaction;

import metrics.PerfMetrics;
import transaction.TransactionOperations.TransactionStatus;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * A bounded LRU projection of OPEN transactions (R19; data-model.md "Transaction state cache"). The
 * database stays authoritative:
 *
 * <ul>
 *   <li>Every read and update of an entry requires the caller to hold that transaction's stripe in
 *       {@link TransactionLocks}; entries hold no lock themselves.</li>
 *   <li>An entry changes only after a successful commit ({@link #put}), and is invalidated before the
 *       stripe is released when a commit's outcome is unknown, so the next request reloads it.</li>
 *   <li>COMPLETED transactions are not cached. {@code TRANSACTION_CACHE_MAX_ENTRIES=0} disables caching,
 *       so every access loads fresh.</li>
 * </ul>
 */
public final class TransactionStateCache {
    public static final int DEFAULT_MAX_ENTRIES = 100_000;
    private static final LongAdder HITS = PerfMetrics.counter("transaction.cache.hits");
    private static final LongAdder MISSES = PerfMetrics.counter("transaction.cache.misses");
    private static final LongAdder GUARD_MISSES = PerfMetrics.counter("transaction.cache.guardMisses");
    private static final LongAdder INVALIDATIONS = PerfMetrics.counter("transaction.cache.invalidations");
    private static final LongAdder EVICTIONS = PerfMetrics.counter("transaction.cache.evictions");

    private final TransactionLocks locks;
    private final int maxEntries;
    /** Access-ordered; guarded by its own monitor, since requests for different stripes share it. */
    private final LinkedHashMap<String, State> entries;

    public TransactionStateCache(TransactionLocks locks, int maxEntries) {
        this.locks = Objects.requireNonNull(locks, "locks");
        if (maxEntries < 0) throw new IllegalArgumentException("maxEntries must not be negative");
        this.maxEntries = maxEntries;
        this.entries = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, State> eldest) {
                if (size() <= TransactionStateCache.this.maxEntries) return false;
                EVICTIONS.increment();
                return true;
            }
        };
    }

    /** {@code TRANSACTION_CACHE_MAX_ENTRIES}, default 100,000; 0 disables the cache. */
    public static int maxEntries(Map<String, String> environment) {
        String configured = environment.get("TRANSACTION_CACHE_MAX_ENTRIES");
        return configured == null || configured.isBlank() ? DEFAULT_MAX_ENTRIES : Integer.parseInt(configured.trim());
    }

    public TransactionLocks locks() {
        return locks;
    }

    public boolean enabled() {
        return maxEntries > 0;
    }

    /** The cached entry, or the loader's result, which is cached when it is OPEN. */
    public Optional<State> getOrLoad(String transactionId, Function<String, Optional<State>> loader) {
        requireLock(transactionId);
        if (enabled()) {
            State hit;
            synchronized (entries) {
                hit = entries.get(transactionId);
            }
            if (hit != null) {
                HITS.increment();
                return Optional.of(hit);
            }
        }
        MISSES.increment();
        Optional<State> loaded = loader.apply(transactionId);
        loaded.ifPresent(this::store);
        return loaded;
    }

    /** Records a committed state; a state that is no longer OPEN leaves the cache. */
    public void put(State state) {
        requireLock(state.transactionId());
        store(state);
    }

    /** Drops an entry that may disagree with the database, for example after an ambiguous commit. */
    public void invalidate(String transactionId) {
        requireLock(transactionId);
        if (remove(transactionId)) INVALIDATIONS.increment();
    }

    public void evict(String transactionId) {
        requireLock(transactionId);
        if (remove(transactionId)) EVICTIONS.increment();
    }

    public boolean contains(String transactionId) {
        requireLock(transactionId);
        synchronized (entries) {
            return entries.containsKey(transactionId);
        }
    }

    public int size() {
        synchronized (entries) {
            return entries.size();
        }
    }

    /** Counts a guarded write that matched no row: the cache disagreed with the database. */
    public void recordGuardMiss() {
        GUARD_MISSES.increment();
    }

    private void store(State state) {
        if (!enabled()) return;
        if (state.status() != TransactionStatus.OPEN) {
            if (remove(state.transactionId())) EVICTIONS.increment();
            return;
        }
        synchronized (entries) {
            entries.put(state.transactionId(), state);
        }
    }

    private boolean remove(String transactionId) {
        synchronized (entries) {
            return entries.remove(transactionId) != null;
        }
    }

    private void requireLock(String transactionId) {
        if (!locks.heldByCurrentThread(transactionId)) {
            throw new IllegalStateException("The caller must hold the lock of transaction " + transactionId);
        }
    }

    /**
     * One transaction's committed state. Immutable; {@code lines} are keyed and ordered by SKU, and
     * their quantities and captured prices add up to {@code itemCount} and {@code totalAmount}.
     * {@code stationId} and {@code startedAt} let Complete build its receipt with no read (R5); they
     * are null only in states built without them.
     */
    public record State(String transactionId, TransactionStatus status, int itemCount, BigDecimal totalAmount,
                        int attemptCount, Integer activeAttemptNumber, Map<String, Line> lines, String stationId,
                        Instant startedAt) {
        public State(String transactionId, TransactionStatus status, int itemCount, BigDecimal totalAmount,
                     int attemptCount, Integer activeAttemptNumber, Map<String, Line> lines) {
            this(transactionId, status, itemCount, totalAmount, attemptCount, activeAttemptNumber, lines, null, null);
        }

        public State {
            Objects.requireNonNull(transactionId, "transactionId");
            Objects.requireNonNull(status, "status");
            totalAmount = Objects.requireNonNull(totalAmount, "totalAmount").setScale(2, RoundingMode.UNNECESSARY);
            SortedMap<String, Line> ordered = new TreeMap<>(Objects.requireNonNull(lines, "lines"));
            int units = 0;
            BigDecimal sum = new BigDecimal("0.00");
            for (Map.Entry<String, Line> entry : ordered.entrySet()) {
                Line line = entry.getValue();
                if (!entry.getKey().equals(line.sku())) throw new IllegalArgumentException("lines must be keyed by SKU");
                units += line.quantity();
                sum = sum.add(line.unitPrice().multiply(BigDecimal.valueOf(line.quantity())));
            }
            if (units != itemCount) throw new IllegalArgumentException("itemCount must equal the line quantities");
            if (sum.compareTo(totalAmount) != 0) throw new IllegalArgumentException("totalAmount must equal the lines");
            lines = Collections.unmodifiableSortedMap(ordered);
        }

        public static State open(String transactionId) {
            return open(transactionId, null, null);
        }

        /** A just-started transaction, as Start commits it. */
        public static State open(String transactionId, String stationId, Instant startedAt) {
            return new State(transactionId, TransactionStatus.OPEN, 0, new BigDecimal("0.00"), 0, null, Map.of(),
                    stationId, startedAt);
        }

        /** True while a checkout attempt is pending and the basket is frozen (FR-035). */
        public boolean pending() {
            return activeAttemptNumber != null;
        }

        /** This state after T1 committed attempt {@code attemptNumber} (R5). */
        public State withActiveAttempt(int attemptNumber) {
            return new State(transactionId, status, itemCount, totalAmount, attemptNumber, attemptNumber, lines,
                    stationId, startedAt);
        }

        /** This state after the active attempt was settled as rejected: OPEN again, with the same basket. */
        public State reopened() {
            return new State(transactionId, status, itemCount, totalAmount, attemptCount, null, lines, stationId,
                    startedAt);
        }

        /** This state plus one unit of {@code sku}; a repeated SKU must reuse its line's captured values. */
        public State withScan(String sku, String name, BigDecimal unitPrice) {
            Line existing = lines.get(sku);
            if (existing != null && (existing.unitPrice().compareTo(unitPrice) != 0 || !existing.name().equals(name))) {
                throw new IllegalArgumentException("A repeated SKU must reuse its captured name and price");
            }
            Map<String, Line> next = new TreeMap<>(lines);
            next.put(sku, existing == null ? new Line(sku, name, unitPrice, 1)
                    : new Line(sku, existing.name(), existing.unitPrice(), existing.quantity() + 1));
            return new State(transactionId, status, itemCount + 1, totalAmount.add(unitPrice), attemptCount,
                    activeAttemptNumber, next, stationId, startedAt);
        }
    }

    /** One basket line: the captured name and price of the SKU's first accepted unit. */
    public record Line(String sku, String name, BigDecimal unitPrice, int quantity) {
        public Line {
            Objects.requireNonNull(sku, "sku");
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(unitPrice, "unitPrice");
            if (quantity <= 0) throw new IllegalArgumentException("quantity must be positive");
        }
    }
}
