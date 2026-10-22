package unit.transaction;

import org.junit.jupiter.api.Test;
import transaction.TransactionLocks;
import transaction.TransactionOperations.TransactionStatus;
import transaction.TransactionStateCache;
import transaction.TransactionStateCache.State;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/** The transaction state cache: lock-guarded, post-commit updates, invalidation, and eviction (R19). */
class TransactionStateCacheTest {
    private final TransactionLocks locks = new TransactionLocks();

    @Test
    void getOrLoadCallsTheLoaderOnceAndEntriesChangeOnlyThroughPostCommitUpdates() {
        TransactionStateCache cache = new TransactionStateCache(locks, 10);
        CountingLoader loader = new CountingLoader();
        try (var held = locks.acquire("tx-1")) {
            State first = cache.getOrLoad("tx-1", loader).orElseThrow();
            State again = cache.getOrLoad("tx-1", loader).orElseThrow();
            assertEquals(1, loader.calls.get(), "the second access is a hit");
            assertSame(first, again);
            assertEquals(0, first.itemCount());

            State scanned = first.withScan("A", "Apple", new BigDecimal("1.25"));
            assertEquals(0, cache.getOrLoad("tx-1", loader).orElseThrow().itemCount(),
                    "deriving a new state does not change the cached entry");
            cache.put(scanned);
            State updated = cache.getOrLoad("tx-1", loader).orElseThrow();
            assertEquals(1, updated.itemCount());
            assertEquals(new BigDecimal("1.25"), updated.totalAmount());
            assertEquals(1, updated.lines().get("A").quantity());
            assertEquals(1, loader.calls.get());
            assertThrows(UnsupportedOperationException.class, () -> updated.lines().put("B", null),
                    "entries are immutable values");
        }
    }

    @Test
    void invalidateRemovesTheEntrySoTheNextAccessReloads() {
        TransactionStateCache cache = new TransactionStateCache(locks, 10);
        CountingLoader loader = new CountingLoader();
        try (var held = locks.acquire("tx-1")) {
            cache.getOrLoad("tx-1", loader);
            assertTrue(cache.contains("tx-1"));
            cache.invalidate("tx-1");
            assertFalse(cache.contains("tx-1"));
            cache.getOrLoad("tx-1", loader);
            assertEquals(2, loader.calls.get(), "an ambiguous commit is reconciled by a reload");
        }
        try (var held = locks.acquire("absent")) {
            assertTrue(cache.getOrLoad("absent", id -> Optional.empty()).isEmpty());
            assertFalse(cache.contains("absent"), "a missing transaction is never cached");
        }
    }

    @Test
    void everyReadAndUpdateRequiresTheTransactionsLock() {
        TransactionStateCache cache = new TransactionStateCache(locks, 10);
        CountingLoader loader = new CountingLoader();
        State open = State.open("tx-1");
        assertThrows(IllegalStateException.class, () -> cache.getOrLoad("tx-1", loader));
        assertThrows(IllegalStateException.class, () -> cache.put(open));
        assertThrows(IllegalStateException.class, () -> cache.invalidate("tx-1"));
        assertThrows(IllegalStateException.class, () -> cache.evict("tx-1"));
        assertThrows(IllegalStateException.class, () -> cache.contains("tx-1"));
        assertEquals(0, loader.calls.get());

        String other = idOnAnotherStripe("tx-1");
        try (var held = locks.acquire(other)) {
            assertThrows(IllegalStateException.class, () -> cache.getOrLoad("tx-1", loader),
                    "holding another transaction's stripe is not enough");
        }
        assertThrows(IllegalArgumentException.class, () -> new TransactionStateCache(new TransactionLocks(), -1));
    }

    @Test
    void completedEntriesAreEvictedAndTheLruCapIsEnforced() {
        TransactionStateCache cache = new TransactionStateCache(locks, 2);
        CountingLoader loader = new CountingLoader();
        load(cache, "a", loader);
        load(cache, "b", loader);
        load(cache, "a", loader);
        load(cache, "c", loader);
        assertEquals(3, loader.calls.get());
        assertEquals(2, cache.size());
        assertTrue(contains(cache, "a"), "a was used more recently than b");
        assertFalse(contains(cache, "b"), "the least recently used entry is evicted");
        assertTrue(contains(cache, "c"));

        try (var held = locks.acquire("a")) {
            State a = cache.getOrLoad("a", loader).orElseThrow();
            cache.put(new State("a", TransactionStatus.COMPLETED, a.itemCount(), a.totalAmount(), a.attemptCount(),
                    null, a.lines()));
            assertFalse(cache.contains("a"), "COMPLETED transactions leave the cache");
            loader.status = TransactionStatus.COMPLETED;
            assertEquals(TransactionStatus.COMPLETED, cache.getOrLoad("a", loader).orElseThrow().status());
            assertFalse(cache.contains("a"), "a loaded COMPLETED transaction is returned but not cached");
        }
        try (var held = locks.acquire("c")) {
            cache.evict("c");
        }
        assertFalse(contains(cache, "c"));
    }

    @Test
    void zeroMaxEntriesLoadsFreshOnEveryAccess() {
        assertEquals(100_000, TransactionStateCache.maxEntries(Map.of()));
        assertEquals(0, TransactionStateCache.maxEntries(Map.of("TRANSACTION_CACHE_MAX_ENTRIES", "0")));
        assertEquals(25, TransactionStateCache.maxEntries(Map.of("TRANSACTION_CACHE_MAX_ENTRIES", "25")));

        TransactionStateCache cache = new TransactionStateCache(locks,
                TransactionStateCache.maxEntries(Map.of("TRANSACTION_CACHE_MAX_ENTRIES", "0")));
        assertFalse(cache.enabled());
        CountingLoader loader = new CountingLoader();
        try (var held = locks.acquire("tx-1")) {
            for (int i = 1; i <= 3; i++) {
                cache.getOrLoad("tx-1", loader);
                assertEquals(i, loader.calls.get());
            }
            cache.put(State.open("tx-1"));
            assertFalse(cache.contains("tx-1"));
            assertEquals(0, cache.size());
        }
    }

    @Test
    void stateArithmeticKeepsTheFirstCapturedLine() {
        State state = State.open("tx-1")
                .withScan("B", "Bread", new BigDecimal("2.50"))
                .withScan("A", "Apple", new BigDecimal("1.25"))
                .withScan("B", "Bread", new BigDecimal("2.50"));
        assertEquals(3, state.itemCount());
        assertEquals(new BigDecimal("6.25"), state.totalAmount());
        assertEquals(2, state.lines().get("B").quantity());
        assertEquals(List.of("A", "B"), List.copyOf(state.lines().keySet()), "lines are kept in SKU order");
        assertThrows(IllegalArgumentException.class, () -> state.withScan("A", "Apple", new BigDecimal("9.99")),
                "a repeated SKU must reuse its captured price");
    }

    private void load(TransactionStateCache cache, String id, CountingLoader loader) {
        try (var held = locks.acquire(id)) {
            cache.getOrLoad(id, loader);
        }
    }

    private boolean contains(TransactionStateCache cache, String id) {
        try (var held = locks.acquire(id)) {
            return cache.contains(id);
        }
    }

    private String idOnAnotherStripe(String id) {
        for (int i = 0; ; i++) {
            String candidate = "other-" + i;
            if (locks.stripeOf(candidate) != locks.stripeOf(id)) return candidate;
        }
    }

    private static final class CountingLoader implements Function<String, Optional<State>> {
        final AtomicInteger calls = new AtomicInteger();
        TransactionStatus status = TransactionStatus.OPEN;

        @Override
        public Optional<State> apply(String id) {
            calls.incrementAndGet();
            return Optional.of(new State(id, status, 0, new BigDecimal("0.00"), 0, null, Map.of()));
        }
    }
}
