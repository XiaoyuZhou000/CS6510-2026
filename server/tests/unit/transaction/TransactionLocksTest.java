package unit.transaction;

import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import transaction.TransactionLocks;
import transaction.TransactionStateCache;
import transaction.TransactionStateCache.State;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

/** Striped per-transaction locks that are independent of cache entries (R19, Implementation Rule 2). */
class TransactionLocksTest {

    @Test
    void lockForAnIdNeverChangesAcrossEvictionInvalidationAndReload() {
        TransactionLocks locks = new TransactionLocks();
        TransactionStateCache cache = new TransactionStateCache(locks, 1);
        ReentrantLock original = locks.lockFor("tx-1");
        try (var held = locks.acquire("tx-1")) {
            cache.getOrLoad("tx-1", id -> Optional.of(State.open(id)));
            assertSame(original, locks.lockFor("tx-1"));
            cache.invalidate("tx-1");
            assertSame(original, locks.lockFor("tx-1"), "invalidation creates no second lock");
            cache.getOrLoad("tx-1", id -> Optional.of(State.open(id)));
            assertSame(original, locks.lockFor("tx-1"), "a reload creates no second lock");
        }
        for (int i = 0; i < 10; i++) {
            String other = "other-" + i;
            try (var held = locks.acquire(other)) {
                cache.getOrLoad(other, id -> Optional.of(State.open(id)));
            }
        }
        try (var held = locks.acquire("tx-1")) {
            assertFalse(cache.contains("tx-1"), "the cap of 1 evicted tx-1");
        }
        assertSame(original, locks.lockFor("tx-1"), "evicting the entry and others' entries keeps the stripe");
    }

    @Test
    void manyThreadsWithConstantEvictionNeverShareATransactionsCriticalSection() throws Exception {
        TransactionLocks locks = new TransactionLocks();
        TransactionStateCache cache = new TransactionStateCache(locks, 1);
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 8; i++) ids.add("tx-" + i);
        AtomicIntegerArray inside = new AtomicIntegerArray(ids.size());
        AtomicInteger worstOverlap = new AtomicInteger();
        AtomicInteger loads = new AtomicInteger();
        ExecutorService threads = Executors.newFixedThreadPool(64);
        try {
            List<Future<?>> work = new ArrayList<>();
            for (int t = 0; t < 64; t++) {
                work.add(threads.submit(() -> {
                    for (int n = 0; n < 300; n++) {
                        int index = ThreadLocalRandom.current().nextInt(ids.size());
                        String id = ids.get(index);
                        try (var held = locks.acquire(id)) {
                            int now = inside.incrementAndGet(index);
                            worstOverlap.accumulateAndGet(now, Math::max);
                            State state = cache.getOrLoad(id, key -> {
                                loads.incrementAndGet();
                                return Optional.of(State.open(key));
                            }).orElseThrow();
                            cache.put(state);
                            Thread.onSpinWait();
                            inside.decrementAndGet(index);
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> future : work) future.get(60, TimeUnit.SECONDS);
        } finally {
            threads.shutdownNow();
        }
        assertEquals(1, worstOverlap.get(), "at most one thread per transaction at any time");
        assertTrue(loads.get() > 8, "the cap of 1 forced reloads while other requests were in flight");
    }

    @Test
    void locksForDifferentIdsAreIndependentApartFromStripeCollisions() throws Exception {
        TransactionLocks locks = new TransactionLocks();
        assertEquals(4096, locks.stripeCount());
        assertEquals(1, Integer.bitCount(locks.stripeCount()), "a fixed power of two");
        assertEquals(TransactionLocks.DEFAULT_STRIPES, locks.stripeCount());
        assertThrows(IllegalArgumentException.class, () -> new TransactionLocks(3));
        assertThrows(IllegalArgumentException.class, () -> new TransactionLocks(0));

        String first = "tx-first";
        String second = onAnotherStripe(locks, first);
        CountDownLatch holding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = Thread.ofPlatform().start(() -> {
            try (var held = locks.acquire(first)) {
                holding.countDown();
                release.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(holding.await(5, TimeUnit.SECONDS));
        try {
            ExecutorService other = Executors.newSingleThreadExecutor();
            try {
                Future<Boolean> acquired = other.submit(() -> {
                    try (var held = locks.acquire(second)) {
                        return locks.heldByCurrentThread(second);
                    }
                });
                assertTrue(acquired.get(2, TimeUnit.SECONDS), "another stripe is never blocked by this holder");
            } finally {
                other.shutdownNow();
            }
        } finally {
            release.countDown();
            holder.join(5000);
        }
    }

    @Test
    void aThreadHoldsAtMostOneStripe() {
        TransactionLocks locks = new TransactionLocks();
        String first = "tx-first";
        String second = onAnotherStripe(locks, first);
        try (var held = locks.acquire(first)) {
            assertThrows(IllegalStateException.class, () -> locks.acquire(second),
                    "holding two stripes could deadlock");
        }
        try (var held = locks.acquire(second)) {
            assertTrue(locks.heldByCurrentThread(second));
        }
        assertFalse(locks.heldByCurrentThread(second));
    }

    @Test
    void acquisitionsRecordWaitHoldContentionAndCollisions() throws Exception {
        long acquisitions = PerfMetrics.counterValue("transaction.lock.acquisitions");
        long waits = PerfMetrics.timerSnapshot("transaction.lock.wait").count();
        long holds = PerfMetrics.timerSnapshot("transaction.lock.hold").count();
        TransactionLocks locks = new TransactionLocks();
        try (var held = locks.acquire("tx-1")) {
            assertTrue(locks.heldByCurrentThread("tx-1"));
        }
        assertEquals(acquisitions + 1, PerfMetrics.counterValue("transaction.lock.acquisitions"));
        assertEquals(waits + 1, PerfMetrics.timerSnapshot("transaction.lock.wait").count());
        assertEquals(holds + 1, PerfMetrics.timerSnapshot("transaction.lock.hold").count());

        long contended = PerfMetrics.counterValue("transaction.lock.contended");
        long collisions = PerfMetrics.counterValue("transaction.lock.collisions");
        blockOnce(locks, "tx-1", "tx-1");
        assertEquals(contended + 1, PerfMetrics.counterValue("transaction.lock.contended"));
        assertEquals(collisions, PerfMetrics.counterValue("transaction.lock.collisions"),
                "waiting for the same transaction is contention, not a collision");

        TransactionLocks single = new TransactionLocks(1);
        assertEquals(single.stripeOf("tx-a"), single.stripeOf("tx-b"));
        blockOnce(single, "tx-a", "tx-b");
        assertEquals(contended + 2, PerfMetrics.counterValue("transaction.lock.contended"));
        assertEquals(collisions + 1, PerfMetrics.counterValue("transaction.lock.collisions"),
                "a different holder on the same stripe is a collision");
    }

    private static String onAnotherStripe(TransactionLocks locks, String id) {
        for (int i = 0; ; i++) {
            String candidate = "tx-other-" + i;
            if (locks.stripeOf(candidate) != locks.stripeOf(id)) return candidate;
        }
    }

    /** Holds {@code holderId} until a second thread is queued for {@code waiterId}, then releases it. */
    private static void blockOnce(TransactionLocks locks, String holderId, String waiterId) throws Exception {
        CountDownLatch holding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = Thread.ofPlatform().start(() -> {
            try (var held = locks.acquire(holderId)) {
                holding.countDown();
                release.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(holding.await(5, TimeUnit.SECONDS));
        Thread waiter = Thread.ofPlatform().start(() -> {
            try (var held = locks.acquire(waiterId)) {
                assertTrue(locks.heldByCurrentThread(waiterId));
            }
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!locks.lockFor(waiterId).hasQueuedThread(waiter)) {
            assertTrue(System.nanoTime() < deadline, "the waiter never queued");
            Thread.sleep(1);
        }
        release.countDown();
        holder.join(5000);
        waiter.join(5000);
        assertFalse(waiter.isAlive());
    }
}
