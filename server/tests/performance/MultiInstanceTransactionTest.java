package performance;

import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import support.FakeInventoryGateway;
import support.InProcessInventory;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.TransactionTestSchema;
import transaction.CheckoutOrchestrator;
import transaction.CheckoutPendingFailure;
import transaction.InventoryGateway;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two independent Transaction Service instances, each with its own cache and locks, over one schema:
 * the database, not either cache, decides every outcome (R19 rule 8; Implementation Rule 3), for Scan
 * and for Complete against one shared Inventory, and for recovery: a worker against a live request, two
 * workers against each other, and waiters in both instances (T147).
 */
class MultiInstanceTransactionTest {
    private static final String MISSES = "transaction.cache.guardMisses";

    @Test
    void concurrentScansThroughTwoInstancesAreAllowedResponsesBackedByRows() throws Exception {
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction one = InProcessTransaction.over(schema).pool(4, 8).build();
            InProcessTransaction two = InProcessTransaction.over(schema).pool(4, 8).build();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 4; i++) ids.add((i % 2 == 0 ? one : two).start());
            long misses = PerfMetrics.counterValue(MISSES);

            ConcurrentLinkedQueue<String> acknowledged = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<String> unexpected = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<String> refused = new ConcurrentLinkedQueue<>();
            ExecutorService threads = Executors.newFixedThreadPool(16);
            try {
                List<Future<?>> work = new ArrayList<>();
                for (int t = 0; t < 16; t++) {
                    InProcessTransaction instance = t % 2 == 0 ? one : two;
                    work.add(threads.submit(() -> {
                        for (int n = 0; n < 40; n++) {
                            String id = ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
                            String sku = InventoryTestSchema.sku(1 + ThreadLocalRandom.current().nextInt(10));
                            try {
                                TransactionOperations.ScanView view = instance.scan(id, sku);
                                acknowledged.add(id + "#" + view.itemCount() + "#" + view.sku() + "#" + view.unitPrice());
                            } catch (TransactionFailure failure) {
                                unexpected.add(id + " " + failure.code());
                            } catch (IllegalStateException contention) {
                                refused.add(id + " " + contention.getMessage());
                            }
                        }
                        return null;
                    }));
                }
                for (Future<?> future : work) future.get(180, TimeUnit.SECONDS);
            } finally {
                threads.shutdownNow();
            }

            assertEquals(List.of(), List.copyOf(unexpected), "open transactions and known SKUs never get a 404 or 409");
            assertTrue(refused.size() < acknowledged.size(), "repeated contention is rare: " + refused.size());
            List<String> rows = new ArrayList<>();
            for (String id : ids) {
                var row = schema.transaction(id).orElseThrow();
                List<TransactionTestSchema.ScanRow> events = schema.scans(id);
                assertEquals(events.size(), row.itemCount());
                BigDecimal sum = new BigDecimal("0.00");
                for (int i = 0; i < events.size(); i++) {
                    assertEquals(i + 1, events.get(i).scanSequence(), "contiguous sequences across both instances");
                    sum = sum.add(events.get(i).unitPrice());
                    rows.add(id + "#" + events.get(i).scanSequence() + "#" + events.get(i).sku() + "#" + events.get(i).unitPrice());
                }
                assertEquals(sum, row.totalAmount());
                Set<String> prices = new HashSet<>();
                for (var event : events) prices.add(event.sku() + "@" + event.unitPrice());
                assertEquals(events.stream().map(TransactionTestSchema.ScanRow::sku).distinct().count(), prices.size(),
                        "one captured price per SKU, whichever instance captured it");
            }
            List<String> responses = new ArrayList<>(acknowledged);
            Collections.sort(responses);
            Collections.sort(rows);
            assertEquals(rows, responses, "every acknowledged Scan has its row, and a refused one has none");
            assertTrue(PerfMetrics.counterValue(MISSES) > misses, "the instances' stale caches missed their guards");
        }
    }

    @Test
    void theInstanceThatMissesReloadsAndReEvaluates() throws Exception {
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction one = InProcessTransaction.over(schema).build();
            InProcessTransaction two = InProcessTransaction.over(schema).build();
            String id = one.start();
            assertEquals(1, one.scan(id, "SKU-000001").itemCount());
            assertEquals(2, two.scan(id, "SKU-000002").itemCount(), "the second instance loads the durable state");

            long misses = PerfMetrics.counterValue(MISSES);
            TransactionOperations.ScanView view = one.scan(id, "SKU-000001");
            assertEquals(misses + 1, PerfMetrics.counterValue(MISSES), "the first instance's entry was stale");
            assertEquals(3, view.itemCount(), "it reloaded and kept the other instance's unit");
            assertEquals(new BigDecimal("2.90"), view.runningTotal());
            assertEquals(new BigDecimal("0.85"), view.unitPrice());
            assertEquals(List.of(1, 2, 3), schema.scans(id).stream().map(TransactionTestSchema.ScanRow::scanSequence).toList());
        }
    }

    @Test
    void everyNotFoundOrNotOpenFromEitherInstanceIsConfirmedAgainstTheDatabase() throws Exception {
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction one = InProcessTransaction.over(schema).build();
            InProcessTransaction two = InProcessTransaction.over(schema).build();
            String id = one.start();
            one.scan(id, "SKU-000001");
            two.scan(id, "SKU-000001");
            schema.update("UPDATE `transaction` SET status = 'COMPLETED', completed_at = NOW(3) WHERE transaction_id = ?", id);

            for (InProcessTransaction instance : List.of(one, two)) {
                assertEquals(TransactionFailure.Code.NOT_OPEN, code(() -> instance.scan(id, "SKU-000003")));
                assertEquals(TransactionFailure.Code.NOT_OPEN, code(() -> instance.scan(id, "NO-SUCH-SKU")),
                        "a stale OPEN entry never turns a completed transaction into SKU_NOT_FOUND");
                assertEquals(TransactionFailure.Code.NOT_FOUND, code(() -> instance.scan("missing-" + UUID.randomUUID().toString().substring(0, 8), "SKU-000001")));
            }
            assertEquals("COMPLETED", schema.transaction(id).orElseThrow().status());
            assertEquals(2, schema.scans(id).size(), "no rejected Scan wrote a row");

            String reopened = two.start();
            one.scan(reopened, "SKU-000001");
            assertEquals(TransactionFailure.Code.UNKNOWN_SKU, code(() -> two.scan(reopened, "NO-SUCH-SKU")),
                    "an instance that never saw the transaction confirms it exists before answering SKU_NOT_FOUND");
        }
    }

    @Test
    void concurrentCompletesThroughBothInstancesGiveOneAttemptOneDeductionAndOne200() throws Exception {
        try (TransactionTestSchema schema = seeded();
             InventoryTestSchema stock = InventoryTestSchema.create(InventoryTestSchema.Options.defaults().pool(4, 16));
             InProcessInventory inventory = InProcessInventory.over(stock)) {
            stock.seed(20, 100);
            InProcessTransaction one = InProcessTransaction.over(schema).pool(4, 8).inventory(inventory.gateway()).build();
            InProcessTransaction two = InProcessTransaction.over(schema).pool(4, 8).inventory(inventory.gateway()).build();
            int rounds = 5;
            for (int round = 0; round < rounds; round++) {
                String id = one.start();
                one.scan(id, "SKU-000001");
                two.scan(id, "SKU-000002");
                List<Callable<String>> completes = new ArrayList<>();
                CountDownLatch go = new CountDownLatch(1);
                for (int i = 0; i < 16; i++) {
                    InProcessTransaction instance = i % 2 == 0 ? one : two;
                    completes.add(() -> {
                        go.await();
                        try {
                            return "OK " + instance.complete(id).itemCount();
                        } catch (TransactionFailure failure) {
                            return failure.code().name();
                        } catch (CheckoutPendingFailure pending) {
                            return "PENDING";
                        }
                    });
                }
                List<String> outcomes = new ArrayList<>();
                ExecutorService threads = Executors.newFixedThreadPool(16);
                try {
                    List<Future<String>> futures = new ArrayList<>();
                    for (Callable<String> complete : completes) futures.add(threads.submit(complete));
                    go.countDown();
                    for (Future<String> future : futures) outcomes.add(future.get(60, TimeUnit.SECONDS));
                } finally {
                    threads.shutdownNow();
                }
                assertEquals(1, outcomes.stream().filter(outcome -> outcome.equals("OK 2")).count(),
                        "round " + round + ": exactly one 200 " + outcomes);
                assertTrue(outcomes.stream().allMatch(outcome -> outcome.equals("OK 2") || outcome.equals("NOT_OPEN")),
                        "the others wait for the settlement and answer 409 TRANSACTION_NOT_OPEN: " + outcomes);
                List<TransactionTestSchema.AttemptRow> attempts = schema.attempts(id);
                assertEquals(1, attempts.size(), "round " + round + ": exactly one attempt");
                assertEquals("COMPLETED", attempts.get(0).state());
                assertEquals("COMPLETED", schema.transaction(id).orElseThrow().status());
            }
            assertEquals(100 - rounds, stock.stock("SKU-000001"), "one deduction per transaction");
            assertEquals(100 - rounds, stock.stock("SKU-000002"));
        }
    }

    @Test
    void aScanThroughOneInstanceNeverJoinsTheOtherInstancesFrozenBasket() throws Exception {
        try (TransactionTestSchema schema = seeded();
             InventoryTestSchema stock = InventoryTestSchema.create();
             InProcessInventory inventory = InProcessInventory.over(stock)) {
            stock.seed(20, 100);
            CountDownLatch inside = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            FakeInventoryGateway blocking = FakeInventoryGateway.applying().thenAnswer(call -> {
                inside.countDown();
                assertTrue(release.await(30, TimeUnit.SECONDS));
                return inventory.gateway().applyCheckout(call.key(), call.lines(), call.deadline());
            });
            InProcessTransaction one = InProcessTransaction.over(schema).inventory(blocking).build();
            InProcessTransaction two = InProcessTransaction.over(schema).inventory(inventory.gateway()).build();
            String id = one.start();
            one.scan(id, "SKU-000001");
            two.scan(id, "SKU-000001");

            CompletableFuture<TransactionOperations.ReceiptView> completing = CompletableFuture.supplyAsync(() -> one.complete(id));
            CompletableFuture<String> otherScan;
            CompletableFuture<String> ownScan;
            try {
                assertTrue(inside.await(30, TimeUnit.SECONDS));
                // The other instance's stale entry misses its guard, finds the basket frozen, and waits (T085).
                otherScan = CompletableFuture.supplyAsync(() -> scanOutcome(two, id, "SKU-000002"));
                ownScan = CompletableFuture.supplyAsync(() -> scanOutcome(one, id, "SKU-000003"));
                awaitWaiting(one, 1);
                awaitWaiting(two, 1);
                assertFalse(otherScan.isDone() || ownScan.isDone(), "both Scans wait for the settlement");
                assertEquals(2, schema.scans(id).size(), "no unit joined the frozen basket");
            } finally {
                release.countDown();
            }
            assertEquals(2, completing.get(30, TimeUnit.SECONDS).itemCount());
            assertEquals("NOT_OPEN", ownScan.get(10, TimeUnit.SECONDS), "woken by the signal, then 409");
            assertEquals("NOT_OPEN", otherScan.get(10, TimeUnit.SECONDS), "woken by its database backstop, then 409");
            assertEquals(0, one.registry.waiting() + two.registry.waiting(), "no local future is left behind");
            assertEquals(2, schema.scans(id).size());
            assertEquals(98, stock.stock("SKU-000001"));
            assertEquals(100, stock.stock("SKU-000002"));
        }
    }

    @Test
    void afterARejectionThroughOneInstanceTheOthersStaleAttemptCountReloadsIntoTheNextAttempt() throws Exception {
        try (TransactionTestSchema schema = seeded();
             InventoryTestSchema stock = InventoryTestSchema.create();
             InProcessInventory inventory = InProcessInventory.over(stock)) {
            stock.seed(20, 100);
            stock.setStock("SKU-000001", 1);
            InProcessTransaction one = InProcessTransaction.over(schema).inventory(inventory.gateway()).build();
            InProcessTransaction two = InProcessTransaction.over(schema).inventory(inventory.gateway()).build();
            String id = one.start();
            one.scan(id, "SKU-000001");
            two.scan(id, "SKU-000001");
            assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK, code(() -> one.complete(id)));
            assertEquals(1, stock.stock("SKU-000001"));

            stock.setStock("SKU-000001", 5);
            long misses = PerfMetrics.counterValue(MISSES);
            TransactionOperations.ReceiptView receipt = two.complete(id);
            assertEquals(2, receipt.itemCount(), "no spurious 500");
            assertEquals(misses + 1, PerfMetrics.counterValue(MISSES), "the stale attempt_count missed T1's guard once");
            List<TransactionTestSchema.AttemptRow> attempts = schema.attempts(id);
            assertEquals(List.of(1, 2), attempts.stream().map(TransactionTestSchema.AttemptRow::attemptNumber).toList());
            assertEquals("REJECTED", attempts.get(0).state());
            assertEquals("COMPLETED", attempts.get(1).state());
            assertEquals(3, stock.stock("SKU-000001"));
        }
    }

    /**
     * Worker against live request (T147): the second instance's startup scan resumes an attempt that the
     * first instance's live request still owns. Replay plus guarded writes settle it once, and the live
     * request answers from the settled state, recording its own response.
     */
    @Test
    void aWorkerResumingALiveRequestsAttemptSettlesItOnceAndTheRequestRecordsItsOwnOutcome() throws Exception {
        try (TransactionTestSchema schema = seeded();
             InventoryTestSchema stock = InventoryTestSchema.create();
             InProcessInventory inventory = InProcessInventory.over(stock)) {
            stock.seed(20, 100);
            CountDownLatch inside = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            FakeInventoryGateway blocking = FakeInventoryGateway.applying().thenAnswer(call -> {
                inside.countDown();
                assertTrue(release.await(30, TimeUnit.SECONDS));
                return inventory.gateway().applyCheckout(call.key(), call.lines(), call.deadline());
            });
            InProcessTransaction one = InProcessTransaction.over(schema).inventory(blocking).build();
            InProcessTransaction two = InProcessTransaction.over(schema).inventory(inventory.gateway()).build();
            String id = one.start();
            one.scan(id, "SKU-000001");

            CompletableFuture<TransactionOperations.ReceiptView> completing = CompletableFuture.supplyAsync(() -> one.complete(id));
            try {
                assertTrue(inside.await(30, TimeUnit.SECONDS));
                assertEquals(1, two.recovery.startupScan(), "after a restart nothing is owned: the attempt is resumed");
                assertTrue(two.recovery.runOnce(Duration.ofSeconds(5)));
                TransactionTestSchema.AttemptRow settled = schema.attempts(id).get(0);
                assertEquals("COMPLETED", settled.state(), "the second instance's worker settled it");
                assertEquals("NONE", settled.clientOutcome(), "recovery never sets client_outcome");
            } finally {
                release.countDown();
            }
            assertEquals(1, completing.get(30, TimeUnit.SECONDS).itemCount(), "the live request answers 200");
            List<TransactionTestSchema.AttemptRow> attempts = schema.attempts(id);
            assertEquals(1, attempts.size());
            assertEquals("COMPLETED", attempts.get(0).state());
            assertEquals("SUCCESS", attempts.get(0).clientOutcome(), "it recorded its own response (FR-054)");
            assertEquals(99, stock.stock("SKU-000001"), "one deduction");
            assertEquals(1, stock.operationCount("APPLIED"));
        }
    }

    /** Worker against worker (T147): both instances' workers recover one attempt at once; it settles once. */
    @Test
    void twoWorkersRecoveringOneAttemptAtOnceSettleItOnceWithOneDeduction() throws Exception {
        try (TransactionTestSchema schema = seeded();
             InventoryTestSchema stock = InventoryTestSchema.create();
             InProcessInventory inventory = InProcessInventory.over(stock)) {
            stock.seed(20, 100);
            String id = handedOver(schema);
            CyclicBarrier together = new CyclicBarrier(2);
            FakeInventoryGateway gateway = FakeInventoryGateway.applying().otherwise(call -> {
                together.await(10, TimeUnit.SECONDS);
                return inventory.gateway().applyCheckout(call.key(), call.lines(), call.deadline());
            });
            InProcessTransaction one = InProcessTransaction.over(schema).inventory(gateway).build();
            InProcessTransaction two = InProcessTransaction.over(schema).inventory(gateway).build();
            assertEquals(1, one.recovery.startupScan());
            assertEquals(1, two.recovery.startupScan());

            ExecutorService workers = Executors.newFixedThreadPool(2);
            try {
                Future<Boolean> first = workers.submit(() -> one.recovery.runOnce(Duration.ofSeconds(5)));
                Future<Boolean> second = workers.submit(() -> two.recovery.runOnce(Duration.ofSeconds(5)));
                assertTrue(first.get(30, TimeUnit.SECONDS), "neither worker errors");
                assertTrue(second.get(30, TimeUnit.SECONDS));
            } finally {
                workers.shutdownNow();
            }
            assertEquals(2, gateway.applyCalls(), "both workers replayed the attempt");
            List<TransactionTestSchema.AttemptRow> attempts = schema.attempts(id);
            assertEquals(1, attempts.size());
            assertEquals("COMPLETED", attempts.get(0).state(), "settled once");
            assertEquals("PENDING_ERROR", attempts.get(0).clientOutcome(), "a background completion, counted once");
            assertEquals("COMPLETED", schema.transaction(id).orElseThrow().status());
            assertEquals(99, stock.stock("SKU-000001"), "one deduction");
            assertEquals(1, stock.operationCount("APPLIED"));
        }
    }

    /**
     * Waiters in both processes (T147): Scans and Completes waiting on an attempt that one instance's
     * worker settles end through the signal there and through the database backstop in the other, with
     * the same serial outcome and no local future left behind.
     */
    @Test
    void waitersInBothInstancesEndWithTheSameSerialOutcomeWhenOneWorkerSettles() throws Exception {
        try (TransactionTestSchema schema = seeded();
             InventoryTestSchema stock = InventoryTestSchema.create();
             InProcessInventory inventory = InProcessInventory.over(stock)) {
            stock.seed(20, 100);
            String id = handedOver(schema);
            AtomicBoolean up = new AtomicBoolean(false);
            FakeInventoryGateway gateway = FakeInventoryGateway.applying().otherwise(call -> up.get()
                    ? inventory.gateway().applyCheckout(call.key(), call.lines(), call.deadline())
                    : new InventoryGateway.Uncertain("UNAVAILABLE"));
            InProcessTransaction one = InProcessTransaction.over(schema).inventory(gateway).build();
            InProcessTransaction two = InProcessTransaction.over(schema).inventory(inventory.gateway()).build();

            List<CompletableFuture<String>> waiters = List.of(
                    CompletableFuture.supplyAsync(() -> scanOutcome(one, id, "SKU-000002")),
                    CompletableFuture.supplyAsync(() -> completeOutcome(one, id)),
                    CompletableFuture.supplyAsync(() -> scanOutcome(two, id, "SKU-000002")),
                    CompletableFuture.supplyAsync(() -> completeOutcome(two, id)));
            awaitWaiting(one, 1);
            awaitWaiting(two, 1);
            Thread.sleep(300);
            assertTrue(waiters.stream().noneMatch(CompletableFuture::isDone), "every request waits");

            up.set(true);
            assertEquals(1, one.recovery.startupScan());
            assertTrue(one.recovery.runOnce(Duration.ofSeconds(5)));
            for (CompletableFuture<String> waiter : waiters) {
                assertEquals("NOT_OPEN", waiter.get(10, TimeUnit.SECONDS), "settled COMPLETED: 409 everywhere");
            }
            assertEquals(0, one.registry.waiting(), "the signal removed the future");
            assertEquals(0, two.registry.waiting(), "the backstop waiter removed its local future");
            assertEquals(1, schema.scans(id).size(), "no unit joined the frozen basket");
            assertEquals(1, schema.attempts(id).size(), "no waiting Complete began another attempt");
            assertEquals(99, stock.stock("SKU-000001"));
        }
    }

    /**
     * A transaction with one unit of SKU-000001 whose attempt 1 a third instance handed over to recovery
     * at budget expiry, as if Inventory were down: PENDING, with {@code client_outcome = PENDING_ERROR}.
     */
    private static String handedOver(TransactionTestSchema schema) throws Exception {
        InProcessTransaction starter = InProcessTransaction.over(schema)
                .inventory(FakeInventoryGateway.applying().otherwise(call -> new InventoryGateway.Uncertain("UNAVAILABLE")))
                .settings(CheckoutOrchestrator.Settings.defaults().withResponseBudget(Duration.ofMillis(300))).build();
        String id = starter.start();
        starter.scan(id, "SKU-000001");
        assertThrows(CheckoutPendingFailure.class, () -> starter.complete(id));
        TransactionTestSchema.AttemptRow attempt = schema.attempts(id).get(0);
        assertEquals("PENDING", attempt.state());
        assertEquals("PENDING_ERROR", attempt.clientOutcome());
        return id;
    }

    private static String scanOutcome(InProcessTransaction instance, String id, String sku) {
        try {
            return "OK " + instance.scan(id, sku).itemCount();
        } catch (TransactionFailure failure) {
            return failure.code().name();
        } catch (CheckoutPendingFailure pending) {
            return "PENDING";
        }
    }

    private static String completeOutcome(InProcessTransaction instance, String id) {
        try {
            return "OK " + instance.complete(id).itemCount();
        } catch (TransactionFailure failure) {
            return failure.code().name();
        } catch (CheckoutPendingFailure pending) {
            return "PENDING";
        }
    }

    private static void awaitWaiting(InProcessTransaction instance, int futures) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (instance.registry.waiting() < futures) {
            assertTrue(System.nanoTime() < deadline, "no request started waiting");
            Thread.sleep(5);
        }
    }

    private static TransactionFailure.Code code(Runnable action) {
        return assertThrows(TransactionFailure.class, action::run).code();
    }

    private static TransactionTestSchema seeded() throws Exception {
        TransactionTestSchema schema = TransactionTestSchema.create(TransactionTestSchema.Options.defaults().pool(4, 8));
        schema.seedReplica(20, UUID.randomUUID().toString());
        return schema;
    }
}
