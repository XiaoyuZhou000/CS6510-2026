package performance;

import database.ScanStore;
import database.TransactionStore;
import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.TransactionTestSchema;
import transaction.TransactionOperations;
import transaction.TransactionStateCache;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Entries are evicted and reloaded while other requests for the same transaction are in flight, and
 * stripe collisions cost only time (R19, Implementation Rule 2).
 */
class CacheEvictionRaceTest {
    private static final int THREADS = 32;
    private static final int TRANSACTIONS = 8;
    private static final int PRODUCTS = 20;

    @Test
    void randomScansAndLookupsWithATinyCacheKeepEveryInvariant() throws Exception {
        try (TransactionTestSchema schema = seeded()) {
            Run run = new Run(schema, 0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            run.execute(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                if (System.nanoTime() >= deadline) return false;
                String id = run.ids.get(random.nextInt(TRANSACTIONS));
                if (random.nextInt(4) == 0) run.lookup(id);
                else run.scan(id, InventoryTestSchema.sku(1 + random.nextInt(PRODUCTS)));
                return true;
            });
            assertTrue(run.scans.get() > TRANSACTIONS * 4, "the run made progress: " + run.scans.get());
            run.assertInvariants();
        }
    }

    @Test
    void stripeCollisionsChangeOnlyTimingNeverOutcomes() throws Exception {
        Map<Integer, String> wide;
        Map<Integer, String> narrow;
        long collisions;
        try (TransactionTestSchema schema = seeded()) {
            Run run = new Run(schema, 0);
            run.executePlan(new Random(1));
            run.assertInvariants();
            wide = run.outcomes();
        }
        try (TransactionTestSchema schema = seeded()) {
            Run run = new Run(schema, 1);
            long before = PerfMetrics.counterValue("transaction.lock.collisions");
            long started = System.nanoTime();
            run.executePlan(new Random(2));
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(120), "one stripe finishes in time");
            run.assertInvariants();
            narrow = run.outcomes();
            collisions = PerfMetrics.counterValue("transaction.lock.collisions") - before;
        }
        assertEquals(wide, narrow, "every transaction ends identically with 4,096 stripes and with one");
        assertTrue(collisions > 0, "with one stripe, unrelated transactions collided");
    }

    /** One service with a cache of 2 entries over 8 open transactions, instrumented for overlap. */
    private static final class Run {
        final TransactionTestSchema schema;
        final InProcessTransaction transaction;
        final List<String> ids = new ArrayList<>();
        final Map<String, AtomicInteger> inside = new ConcurrentHashMap<>();
        final AtomicInteger worstOverlap = new AtomicInteger();
        final AtomicInteger scans = new AtomicInteger();
        final ConcurrentLinkedQueue<String> acknowledged = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<String> failures = new ConcurrentLinkedQueue<>();

        Run(TransactionTestSchema schema, int stripes) throws Exception {
            this.schema = schema;
            this.transaction = InProcessTransaction.over(schema).pool(8, 8).cacheEntries(2).stripes(stripes)
                    .wrapScans(this::instrument).build();
            for (int i = 0; i < TRANSACTIONS; i++) ids.add(transaction.start());
        }

        private ScanStore instrument(ScanStore delegate) {
            return (id, expected, total, row) -> {
                int now = inside.computeIfAbsent(id, ignored -> new AtomicInteger()).incrementAndGet();
                worstOverlap.accumulateAndGet(now, Math::max);
                try {
                    return delegate.appendScan(id, expected, total, row);
                } finally {
                    inside.get(id).decrementAndGet();
                }
            };
        }

        void scan(String id, String sku) {
            try {
                TransactionOperations.ScanView view = transaction.scan(id, sku);
                scans.incrementAndGet();
                acknowledged.add(id + "#" + view.itemCount() + "#" + view.sku() + "#" + view.unitPrice());
            } catch (RuntimeException failure) {
                failures.add(id + " " + failure);
            }
        }

        void lookup(String id) {
            try {
                transaction.service.get(id);
            } catch (RuntimeException failure) {
                failures.add(id + " lookup " + failure);
            }
        }

        void execute(Step step) throws Exception {
            ExecutorService threads = Executors.newFixedThreadPool(THREADS);
            try {
                List<Future<?>> work = new ArrayList<>();
                for (int t = 0; t < THREADS; t++) {
                    work.add(threads.submit(() -> {
                        while (step.next()) {
                            // each step is one request
                        }
                        return null;
                    }));
                }
                for (Future<?> future : work) future.get(180, TimeUnit.SECONDS);
            } finally {
                threads.shutdownNow();
            }
        }

        /** The same multiset of scans per transaction in every run, in a run-specific random order. */
        void executePlan(Random order) throws Exception {
            List<String[]> plan = new ArrayList<>();
            for (int t = 0; t < TRANSACTIONS; t++) {
                for (int k = 0; k < 30; k++) plan.add(new String[]{ids.get(t), InventoryTestSchema.sku(1 + (t * 7 + k) % PRODUCTS)});
            }
            Collections.shuffle(plan, order);
            ConcurrentLinkedQueue<String[]> queue = new ConcurrentLinkedQueue<>(plan);
            execute(() -> {
                String[] next = queue.poll();
                if (next == null) return false;
                scan(next[0], next[1]);
                if (ThreadLocalRandom.current().nextInt(3) == 0) lookup(next[0]);
                return true;
            });
        }

        void assertInvariants() throws Exception {
            assertEquals(List.of(), List.copyOf(failures), "every Scan and Lookup of an open transaction succeeds");
            assertEquals(1, worstOverlap.get(), "TransactionLocks never admits two holders for one transaction");
            List<String> rows = new ArrayList<>();
            for (String id : ids) {
                var row = schema.transaction(id).orElseThrow();
                List<TransactionTestSchema.ScanRow> events = schema.scans(id);
                assertEquals(events.size(), row.itemCount(), "item_count equals the number of accepted_scan rows");
                BigDecimal sum = new BigDecimal("0.00");
                for (int i = 0; i < events.size(); i++) {
                    TransactionTestSchema.ScanRow event = events.get(i);
                    assertEquals(i + 1, event.scanSequence(), "sequences are contiguous from 1");
                    sum = sum.add(event.unitPrice());
                    rows.add(id + "#" + event.scanSequence() + "#" + event.sku() + "#" + event.unitPrice());
                }
                assertEquals(sum, row.totalAmount(), "total_amount is the sum of the captured prices");
            }
            List<String> responses = new ArrayList<>(acknowledged);
            Collections.sort(responses);
            Collections.sort(rows);
            assertEquals(rows, responses, "every 200 has exactly one row and every row exactly one 200");

            for (String id : ids) {
                TransactionStore.TransactionState durable = transaction.transactions.loadState(id).orElseThrow();
                try (var held = transaction.locks.acquire(id)) {
                    if (!transaction.cache.contains(id)) continue;
                    TransactionStateCache.State cached = transaction.cache.getOrLoad(id, ignored -> {
                        throw new AssertionError("a contained entry must be a hit");
                    }).orElseThrow();
                    assertEquals(durable.transaction().itemCount(), cached.itemCount());
                    assertEquals(durable.transaction().totalAmount(), cached.totalAmount());
                    Map<String, String> lines = new TreeMap<>();
                    durable.lines().forEach(line -> lines.put(line.sku(), line.quantity() + "@" + line.unitPrice() + " " + line.name()));
                    Map<String, String> cachedLines = new TreeMap<>();
                    cached.lines().values().forEach(line -> cachedLines.put(line.sku(),
                            line.quantity() + "@" + line.unitPrice() + " " + line.name()));
                    assertEquals(lines, cachedLines, "the cached entry equals a fresh reload");
                }
            }
        }

        /** Per transaction, by its creation index: item count, total, and lines. */
        Map<Integer, String> outcomes() throws Exception {
            Map<Integer, String> outcomes = new TreeMap<>();
            for (int t = 0; t < ids.size(); t++) {
                TransactionStore.TransactionState state = transaction.transactions.loadState(ids.get(t)).orElseThrow();
                outcomes.put(t, state.transaction().itemCount() + " " + state.transaction().totalAmount() + " " + state.lines());
            }
            return outcomes;
        }
    }

    @FunctionalInterface
    private interface Step {
        boolean next() throws Exception;
    }

    private static TransactionTestSchema seeded() throws Exception {
        TransactionTestSchema schema = TransactionTestSchema.create(TransactionTestSchema.Options.defaults().pool(8, 8));
        schema.seedReplica(PRODUCTS, java.util.UUID.randomUUID().toString());
        return schema;
    }
}
