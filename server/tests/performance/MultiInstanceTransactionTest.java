package performance;

import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import support.FakeInventoryGateway;
import support.InProcessInventory;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.TransactionTestSchema;
import transaction.CheckoutPendingFailure;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two independent Transaction Service instances, each with its own cache and locks, over one schema:
 * the database, not either cache, decides every outcome (R19 rule 8; Implementation Rule 3), for Scan
 * and for Complete against one shared Inventory.
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
                assertTrue(outcomes.stream().allMatch(outcome -> outcome.equals("OK 2") || outcome.equals("NOT_OPEN")
                        || outcome.equals("PENDING")), "only 409 TRANSACTION_NOT_OPEN or a pending 500 otherwise: " + outcomes);
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
            try {
                assertTrue(inside.await(30, TimeUnit.SECONDS));
                assertThrows(CheckoutPendingFailure.class, () -> two.scan(id, "SKU-000002"),
                        "the other instance's stale entry misses its guard and finds the basket frozen");
                assertThrows(CheckoutPendingFailure.class, () -> one.scan(id, "SKU-000003"));
                assertEquals(2, schema.scans(id).size(), "no unit joined the frozen basket");
            } finally {
                release.countDown();
            }
            assertEquals(2, completing.get(30, TimeUnit.SECONDS).itemCount());
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

    private static TransactionFailure.Code code(Runnable action) {
        return assertThrows(TransactionFailure.class, action::run).code();
    }

    private static TransactionTestSchema seeded() throws Exception {
        TransactionTestSchema schema = TransactionTestSchema.create(TransactionTestSchema.Options.defaults().pool(4, 8));
        schema.seedReplica(20, UUID.randomUUID().toString());
        return schema;
    }
}
