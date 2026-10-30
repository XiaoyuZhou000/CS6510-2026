package integration;

import org.junit.jupiter.api.Test;
import support.InMemoryEventBus;
import support.InProcessCheckout;
import support.InventoryTestSchema;
import support.TransactionTestSchema;
import transaction.CheckoutPendingFailure;
import transaction.CheckoutRejectedFailure;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Message faults on the checkout and outcome topics (US3 scenarios 2, 5, and 10; SC-003; 006 D5, D6), over an
 * {@link InMemoryEventBus} between the real Transaction and Inventory, with Transaction's republish worker
 * running: duplicated, dropped, delayed, reordered, and redelivered events, and a broker paused past the
 * response budget. Every attempt settles exactly once, through republish where an event was lost, and after a
 * randomized mix of all of them over 200 transactions, initial minus final stock equals the units of
 * COMPLETED transactions and no stock is negative.
 */
class CheckoutMessageFaultTest {
    private static final int STOCK = 50;
    private static final Duration SETTLE = Duration.ofSeconds(60);

    @Test
    void eachMessageFaultStillSettlesEveryAttemptExactlyOnce() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(20, STOCK, 81)) {
            saga.transaction.startRecovery();
            InMemoryEventBus bus = saga.bus;
            int[] product = {0};
            record Case(String name, Consumer<InMemoryEventBus> arm, Consumer<InMemoryEventBus> disarm) { }
            List<Case> cases = List.of(
                    new Case("duplicate every checkout and outcome event", b -> b.duplicateNextSends(2), b -> { }),
                    new Case("drop CheckoutRequested before it is stored", b -> b.dropNextSends(1), b -> { }),
                    new Case("lose an outcome commit, so the outcome is redelivered", b -> b.loseNextCommits(4), b -> { }),
                    new Case("reorder records across partitions", b -> b.reorderAcrossPartitions(true),
                            b -> b.reorderAcrossPartitions(false)),
                    new Case("delay delivery past the 8 s budget", b -> b.delayDelivery(Duration.ofSeconds(9)),
                            b -> b.delayDelivery(Duration.ZERO)),
                    new Case("pause the broker past the budget", b -> b.pauseBroker(true), b -> b.pauseBroker(false)));
            for (Case c : cases) {
                String sku = InventoryTestSchema.sku(++product[0]);
                String id = start(saga, sku, 2);
                c.arm().accept(bus);
                String outcome = complete(saga, id);
                c.disarm().accept(bus);
                assertTrue(outcome.equals("OK") || outcome.equals("PENDING"), c.name() + ": " + outcome);
                awaitSettled(saga, id, c.name());
                assertCompletedOnce(saga, id, sku, c.name());
                if (outcome.equals("PENDING")) {
                    assertEquals("PENDING_ERROR", saga.transactionSchema.attempts(id).get(0).clientOutcome(),
                            c.name() + ": a background completion, counted as such");
                }
            }

            // The outcome is lost in transit on Inventory's side: Inventory's sweep or Transaction's republish sends it.
            String sku = InventoryTestSchema.sku(++product[0]);
            String id = start(saga, sku, 2);
            AtomicBoolean dropOutcome = new AtomicBoolean(true);
            Thread dropper = Thread.ofPlatform().start(() -> {
                while (dropOutcome.get() && saga.bus.values(saga.names.checkout()).stream().noneMatch(
                        value -> new String(value, java.nio.charset.StandardCharsets.ISO_8859_1).contains(id))) {
                    Thread.onSpinWait();
                }
                bus.dropNextSends(1);
            });
            String outcome = complete(saga, id);
            dropOutcome.set(false);
            dropper.join();
            assertTrue(outcome.equals("OK") || outcome.equals("PENDING"), "dropped outcome: " + outcome);
            awaitSettled(saga, id, "dropped outcome");
            assertCompletedOnce(saga, id, sku, "dropped outcome");

            // A stale outcome for an earlier attempt never settles a newer one.
            String stale = InventoryTestSchema.sku(++product[0]);
            saga.inventorySchema.setStock(stale, 1);
            String retried = start(saga, stale, 2);
            assertEquals("INSUFFICIENT_STOCK", complete(saga, retried));
            String rejectedOutcome = saga.inventorySchema.queryString(
                    "SELECT outcome_event_id FROM checkout_operation WHERE transaction_id = ? AND attempt_number = 1", retried);
            saga.inventorySchema.setStock(stale, 5);
            bus.delayDelivery(Duration.ofSeconds(2));
            var next = java.util.concurrent.CompletableFuture.supplyAsync(() -> complete(saga, retried));
            awaitAttempt(saga, retried, 2);
            saga.inventory.dispatcher.resend(rejectedOutcome);
            bus.delayDelivery(Duration.ZERO);
            assertEquals("OK", next.get(20, TimeUnit.SECONDS), "attempt 2 completes despite attempt 1's stale rejection");
            assertEquals(List.of("REJECTED", "COMPLETED"), saga.transactionSchema.attempts(retried).stream()
                    .map(TransactionTestSchema.AttemptRow::state).toList());
            assertEquals(3, saga.stock(stale));
            assertEquals(0, saga.transactionSchema.parkedCount(), "a stale outcome is a no-op, not a poison record");
        }
    }

    @Test
    void aRandomizedMixOfMessageFaultsOver200TransactionsConservesStock() throws Exception {
        int products = 10;
        int stock = 40;
        try (InProcessCheckout saga = InProcessCheckout.start(products, stock, 6510)) {
            saga.transaction.startRecovery();
            Random random = new Random(6510);
            List<String> ids = new ArrayList<>();
            for (int t = 0; t < 200; t++) {
                List<String> basket = new ArrayList<>();
                int units = 1 + random.nextInt(3);
                for (int u = 0; u < units; u++) basket.add(InventoryTestSchema.sku(1 + random.nextInt(products)));
                String id = saga.checkout().start(new TransactionOperations.StartCommand("mix")).transactionId();
                for (String sku : basket) saga.checkout().scan(new TransactionOperations.ScanCommand(id, sku));
                ids.add(id);
            }

            AtomicBoolean faulting = new AtomicBoolean(true);
            Thread chaos = Thread.ofPlatform().start(() -> {
                Random faults = new Random(81);
                while (faulting.get()) {
                    switch (faults.nextInt(7)) {
                        case 0 -> saga.bus.duplicateNextSends(1 + faults.nextInt(3));
                        case 1 -> saga.bus.dropNextSends(1 + faults.nextInt(2));
                        case 2 -> saga.bus.loseNextCommits(1 + faults.nextInt(3));
                        case 3 -> saga.bus.reorderAcrossPartitions(faults.nextBoolean());
                        case 4 -> saga.bus.delayDelivery(Duration.ofMillis(faults.nextInt(400)));
                        case 5 -> {
                            saga.bus.pauseBroker(true);
                            sleep(100 + faults.nextInt(300));
                            saga.bus.pauseBroker(false);
                        }
                        default -> saga.bus.delayDelivery(Duration.ZERO);
                    }
                    sleep(50);
                }
            });
            Map<String, String> responses = new ConcurrentHashMap<>();
            ExecutorService stations = Executors.newFixedThreadPool(16);
            try {
                List<Future<?>> work = new ArrayList<>();
                for (String id : ids) work.add(stations.submit(() -> responses.put(id, complete(saga, id))));
                for (Future<?> done : work) done.get(5, TimeUnit.MINUTES);
            } finally {
                faulting.set(false);
                chaos.join();
                stations.shutdownNow();
                saga.bus.pauseBroker(false);
                saga.bus.delayDelivery(Duration.ZERO);
                saga.bus.reorderAcrossPartitions(false);
            }

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (saga.transactionSchema.pendingAttemptCount() > 0) {
                assertTrue(System.nanoTime() < deadline, "every attempt settles through republish");
                Thread.sleep(200);
            }

            Map<String, Integer> sold = new HashMap<>();
            for (String id : ids) {
                var attempts = saga.transactionSchema.attempts(id);
                assertTrue(attempts.size() <= 1, id + ": each Complete began at most one attempt");
                boolean completed = !attempts.isEmpty() && attempts.get(0).state().equals("COMPLETED");
                if (responses.get(id).equals("OK")) assertTrue(completed, id + ": a 200 only for a COMPLETED attempt");
                if (responses.get(id).equals("INSUFFICIENT_STOCK")) {
                    assertEquals("REJECTED", attempts.get(0).state(), id + ": a 409 only for a REJECTED attempt");
                }
                if (completed) {
                    for (var scan : saga.transactionSchema.scans(id)) sold.merge(scan.sku(), 1, Integer::sum);
                }
            }
            for (int n = 1; n <= products; n++) {
                String sku = InventoryTestSchema.sku(n);
                int finalStock = saga.stock(sku);
                assertTrue(finalStock >= 0, sku + " is never negative");
                assertEquals(stock - sold.getOrDefault(sku, 0), finalStock,
                        sku + ": initial minus final stock equals the units of COMPLETED transactions");
            }
            assertEquals(saga.inventorySchema.operationCount("APPLIED"), ids.stream().filter(id -> {
                try {
                    var attempts = saga.transactionSchema.attempts(id);
                    return !attempts.isEmpty() && attempts.get(0).state().equals("COMPLETED");
                } catch (Exception failure) {
                    throw new AssertionError(failure);
                }
            }).count(), "every applied operation is a COMPLETED attempt, and the reverse");
        }
    }

    private static String start(InProcessCheckout saga, String sku, int units) {
        String id = saga.checkout().start(new TransactionOperations.StartCommand("faults")).transactionId();
        for (int u = 0; u < units; u++) saga.checkout().scan(new TransactionOperations.ScanCommand(id, sku));
        return id;
    }

    private static String complete(InProcessCheckout saga, String id) {
        try {
            saga.checkout().complete(id);
            return "OK";
        } catch (TransactionFailure failure) {
            return failure.code().name();
        } catch (CheckoutPendingFailure pending) {
            return "PENDING";
        } catch (CheckoutRejectedFailure rejected) {
            return "REJECTED_" + rejected.reason();
        }
    }

    private static void assertCompletedOnce(InProcessCheckout saga, String id, String sku, String label) throws Exception {
        var attempts = saga.transactionSchema.attempts(id);
        assertEquals(1, attempts.size(), label + ": one attempt");
        assertEquals("COMPLETED", attempts.get(0).state(), label);
        assertEquals(TransactionOperations.TransactionStatus.COMPLETED, saga.checkout().get(id).status(),
                label + ": lookup shows COMPLETED");
        assertEquals("NOT_OPEN", complete(saga, id), label + ": a repeated Complete is 409");
        assertEquals(STOCK - 2, saga.stock(sku), label + ": stock changed exactly once");
        assertEquals(1, saga.inventorySchema.queryLong(
                "SELECT COUNT(*) FROM checkout_operation WHERE transaction_id = ?", id), label + ": one operation");
    }

    private static void awaitSettled(InProcessCheckout saga, String id, String label) throws Exception {
        long deadline = System.nanoTime() + SETTLE.toNanos();
        while (saga.transactionSchema.attempts(id).stream().anyMatch(attempt -> attempt.state().equals("PENDING"))) {
            assertTrue(System.nanoTime() < deadline, label + ": the attempt never settled");
            Thread.sleep(50);
        }
    }

    private static void awaitAttempt(InProcessCheckout saga, String id, int number) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (saga.transactionSchema.attempts(id).size() < number) {
            assertTrue(System.nanoTime() < deadline, "attempt " + number + " never began");
            Thread.sleep(10);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
