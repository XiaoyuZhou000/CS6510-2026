package integration;

import database.CheckoutAttemptStore;
import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import support.FakeInventoryGateway;
import support.FaultInjection;
import support.InProcessInventory;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.TestSchema;
import support.TransactionTestSchema;
import transaction.CheckoutPendingFailure;
import transaction.InventoryGateway;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bounded waiting for requests that arrive during a pending checkout (FR-035, R7; US3 scenario 6). The
 * attempt stays pending because Inventory's reply is delayed by its {@code inventory.apply.afterCommit}
 * hook, armed in this JVM over the real Inventory checkout path, or because a scripted Inventory blocks.
 * Two service instances over one schema stand for two Transaction processes (Implementation Rule 3).
 */
class BoundedWaitTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final String DB_CHECKS = "transaction.wait.dbChecks";

    @Test
    void aSettlementAsCompletedAnswersTheWaitersWith409SoonAfterItsCommitWithNoDatabaseCheck() throws Exception {
        try (Fixture fixture = new Fixture(); var armed = delayedReplies()) {
            InProcessTransaction service = fixture.instance();
            String id = service.start();
            service.scan(id, A);
            long checks = PerfMetrics.counterValue(DB_CHECKS);

            CompletableFuture<Timed> owner = timed(() -> "OK " + service.complete(id).itemCount());
            awaitPending(fixture, id);
            long lookupStarted = System.nanoTime();
            assertEquals(TransactionOperations.TransactionStatus.OPEN, service.service.get(id).status(),
                    "Lookup never waits: OPEN until T3 commits");
            assertTrue(System.nanoTime() - lookupStarted < TimeUnit.MILLISECONDS.toNanos(500));
            CompletableFuture<Timed> scan = timed(() -> scanOutcome(service, id, B));
            CompletableFuture<Timed> complete = timed(() -> completeOutcome(service, id));

            assertEquals("OK 1", owner.get(20, TimeUnit.SECONDS).outcome());
            Timed scanned = scan.get(20, TimeUnit.SECONDS);
            Timed completed = complete.get(20, TimeUnit.SECONDS);
            assertEquals("NOT_OPEN", scanned.outcome(), "the transaction completed: 409 TRANSACTION_NOT_OPEN");
            assertEquals("NOT_OPEN", completed.outcome());
            long settled = fixture.settledAt(id);
            assertWithin(200, settled, scanned, "the Scan woke on the settlement signal");
            assertWithin(200, settled, completed, "the Complete woke on the settlement signal");
            assertEquals(checks, PerfMetrics.counterValue(DB_CHECKS),
                    "no database check for an attempt owned in the same process");
            assertEquals(1, fixture.transactionSchema.scans(id).size(), "no unit joined the frozen basket");
            assertEquals(1, fixture.transactionSchema.attempts(id).size());
            assertEquals(0, service.registry.waiting());
            assertEquals(49, fixture.inventorySchema.stock(A));
        }
    }

    @Test
    void aSettlementAsOpenAcceptsTheWaitingScanOrBeginsTheNextAttemptInSerialOrder() throws Exception {
        try (Fixture fixture = new Fixture(); var armed = delayedReplies()) {
            InProcessTransaction service = fixture.instance();
            fixture.inventorySchema.setStock(A, 1);
            String id = service.start();
            service.scan(id, A);
            service.scan(id, A);

            CompletableFuture<Timed> owner = timed(() -> completeOutcome(service, id));
            awaitPending(fixture, id);
            CompletableFuture<Timed> scan = timed(() -> scanOutcome(service, id, B));
            CompletableFuture<Timed> complete = timed(() -> completeOutcome(service, id));
            awaitWaiting(service);
            fixture.inventorySchema.setStock(A, 10);

            assertEquals("INSUFFICIENT_STOCK", owner.get(20, TimeUnit.SECONDS).outcome(), "attempt 1 was rejected");
            String scanned = scan.get(20, TimeUnit.SECONDS).outcome();
            String completed = complete.get(20, TimeUnit.SECONDS).outcome();
            List<TransactionTestSchema.AttemptRow> attempts = fixture.transactionSchema.attempts(id);
            assertEquals(List.of("REJECTED", "COMPLETED"), attempts.stream().map(TransactionTestSchema.AttemptRow::state)
                    .toList(), "the waiting Complete began attempt 2 after the reopen");
            if (scanned.startsWith("OK")) {
                assertEquals("OK 3", scanned, "the reopened basket accepted the waiting Scan first");
                assertEquals("OK 3", completed, "and attempt 2 checked it out");
                assertEquals(49, fixture.inventorySchema.stock(B), "B was sold once");
            } else {
                assertEquals("NOT_OPEN", scanned, "attempt 2 froze the basket first; the Scan waited again, then 409");
                assertEquals("OK 2", completed);
                assertEquals(50, fixture.inventorySchema.stock(B));
            }
            assertEquals(8, fixture.inventorySchema.stock(A), "two units of A, deducted once");
        }
    }

    @Test
    void stillPendingAtTheBoundGives500WithNoChangeAfterOneDatabaseCheck() throws Exception {
        try (Fixture fixture = new Fixture()) {
            CountDownLatch inside = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            FakeInventoryGateway blocking = FakeInventoryGateway.applying().thenAnswer(call -> {
                inside.countDown();
                assertTrue(release.await(60, TimeUnit.SECONDS));
                return fixture.inventory.gateway().applyCheckout(call.key(), call.lines(), call.deadline());
            });
            InProcessTransaction service = fixture.instance(blocking);
            String id = service.start();
            service.scan(id, A);

            CompletableFuture<Timed> owner = timed(() -> completeOutcome(service, id));
            try {
                assertTrue(inside.await(20, TimeUnit.SECONDS));
                long checks = PerfMetrics.counterValue(DB_CHECKS);
                CompletableFuture<Timed> scan = timed(() -> scanOutcome(service, id, B));
                CompletableFuture<Timed> complete = timed(() -> completeOutcome(service, id));
                Timed scanned = scan.get(20, TimeUnit.SECONDS);
                Timed completed = complete.get(20, TimeUnit.SECONDS);
                assertEquals("PENDING", scanned.outcome(), "500 INTERNAL_ERROR at the bound");
                assertEquals("PENDING", completed.outcome());
                assertTrue(scanned.elapsedNanos() >= TimeUnit.SECONDS.toNanos(8), "it waited out the 8 s budget");
                assertEquals(checks + 2, PerfMetrics.counterValue(DB_CHECKS), "exactly one database check each, at the bound");
                assertEquals(1, fixture.transactionSchema.scans(id).size(), "no basket change");
                TransactionTestSchema.AttemptRow attempt = fixture.transactionSchema.attempts(id).get(0);
                assertEquals("PENDING", attempt.state(), "the pending attempt is untouched");
                assertEquals("NONE", attempt.clientOutcome(), "the waiters recorded nothing on it");
                assertEquals(1, fixture.transactionSchema.attempts(id).size());
            } finally {
                release.countDown();
            }
            assertEquals("OK 1", owner.get(20, TimeUnit.SECONDS).outcome());
        }
    }

    @Test
    void aWaiterInAnotherProcessSeesTheSettlementThroughItsBackstopAndGivesTheSameSerialOutcome() throws Exception {
        try (Fixture fixture = new Fixture(); var armed = delayedReplies()) {
            InProcessTransaction one = fixture.instance();
            InProcessTransaction two = fixture.instance();
            String id = one.start();
            one.scan(id, A);
            long checks = PerfMetrics.counterValue(DB_CHECKS);

            CompletableFuture<Timed> owner = timed(() -> completeOutcome(one, id));
            awaitPending(fixture, id);
            CompletableFuture<Timed> scan = timed(() -> scanOutcome(two, id, B));
            CompletableFuture<Timed> complete = timed(() -> completeOutcome(two, id));

            assertEquals("OK 1", owner.get(20, TimeUnit.SECONDS).outcome());
            Timed scanned = scan.get(20, TimeUnit.SECONDS);
            Timed completed = complete.get(20, TimeUnit.SECONDS);
            assertEquals("NOT_OPEN", scanned.outcome(), "the same serial outcome as in one process");
            assertEquals("NOT_OPEN", completed.outcome());
            long settled = fixture.settledAt(id);
            long bound = two.checkout.settings().waitBackstop().toMillis() + 500;
            assertWithin(bound, settled, scanned, "the backstop saw the other process's settlement");
            assertWithin(bound, settled, completed, "the backstop saw the other process's settlement");
            assertTrue(PerfMetrics.counterValue(DB_CHECKS) > checks, "through database checks");
            assertEquals(0, two.registry.waiting(), "the waiters removed their local future");
            assertEquals(1, fixture.transactionSchema.scans(id).size());
            assertEquals(1, fixture.transactionSchema.attempts(id).size());
        }
    }

    /** Inventory's first reply to each attempt arrives after the 2 s deadline; the retry replays it at once. */
    private static FaultInjection.Armed delayedReplies() {
        return FaultInjection.spec().delay("inventory.apply.afterCommit", 3000).armInThisJvm();
    }

    private static void assertWithin(long millis, long settledNanos, Timed waiter, String message) {
        long late = waiter.endedNanos() - settledNanos;
        assertTrue(late <= TimeUnit.MILLISECONDS.toNanos(millis),
                message + ": answered " + TimeUnit.NANOSECONDS.toMillis(late) + " ms after the settlement commit");
    }

    private static void awaitPending(Fixture fixture, String id) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (fixture.transactionSchema.transaction(id).orElseThrow().activeAttemptNumber() == null) {
            assertTrue(System.nanoTime() < deadline, "the checkout never became pending");
            Thread.sleep(10);
        }
    }

    private static void awaitWaiting(InProcessTransaction service) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (service.registry.waiting() == 0) {
            assertTrue(System.nanoTime() < deadline, "no request started waiting");
            Thread.sleep(5);
        }
        Thread.sleep(100);
    }

    private static String scanOutcome(InProcessTransaction service, String id, String sku) {
        try {
            return "OK " + service.scan(id, sku).itemCount();
        } catch (TransactionFailure failure) {
            return failure.code().name();
        } catch (CheckoutPendingFailure pending) {
            return "PENDING";
        }
    }

    private static String completeOutcome(InProcessTransaction service, String id) {
        try {
            return "OK " + service.complete(id).itemCount();
        } catch (TransactionFailure failure) {
            return failure.code().name();
        } catch (CheckoutPendingFailure pending) {
            return "PENDING";
        }
    }

    /** One request's outcome and when it ended, on the monotonic clock. */
    private record Timed(String outcome, long startedNanos, long endedNanos) {
        long elapsedNanos() {
            return endedNanos - startedNanos;
        }
    }

    private static CompletableFuture<Timed> timed(java.util.function.Supplier<String> request) {
        return CompletableFuture.supplyAsync(() -> {
            long started = System.nanoTime();
            String outcome = request.get();
            return new Timed(outcome, started, System.nanoTime());
        });
    }

    /**
     * Transaction and Inventory schemas, the real Inventory checkout path in this JVM, and service
     * instances whose attempt stores record when each settlement committed.
     */
    private static final class Fixture implements AutoCloseable {
        final TransactionTestSchema transactionSchema;
        final InventoryTestSchema inventorySchema;
        final InProcessInventory inventory;
        private final java.util.Map<String, AtomicLong> settled = new java.util.concurrent.ConcurrentHashMap<>();

        Fixture() throws Exception {
            transactionSchema = TransactionTestSchema.create(TestSchema.Options.defaults().pool(4, 16));
            inventorySchema = InventoryTestSchema.create(TestSchema.Options.defaults().pool(4, 16));
            transactionSchema.seedReplica(10, inventorySchema.seed(10, 50));
            inventory = InProcessInventory.over(inventorySchema);
        }

        InProcessTransaction instance() throws Exception {
            return instance(inventory.gateway());
        }

        InProcessTransaction instance(InventoryGateway gateway) throws Exception {
            return InProcessTransaction.over(transactionSchema).pool(4, 16).inventory(gateway)
                    .wrapAttempts(store -> new Recording(store, settled)).build();
        }

        /** When the transaction's latest settlement (T3 or reopen) committed. */
        long settledAt(String id) {
            AtomicLong at = settled.get(id);
            assertNotNull(at, "no settlement was recorded for " + id);
            return at.get();
        }

        @Override
        public void close() throws Exception {
            try (transactionSchema; inventorySchema) {
                inventory.close();
            }
        }
    }

    /** Delegates to the real store and notes the monotonic time right after each settlement commit. */
    private record Recording(CheckoutAttemptStore store, java.util.Map<String, AtomicLong> settled)
            implements CheckoutAttemptStore {
        @Override
        public WriteResult begin(Begin begin) {
            return store.begin(begin);
        }

        @Override
        public WriteResult complete(AttemptKey key, Instant completedAt, boolean answering) {
            return note(key, store.complete(key, completedAt, answering));
        }

        @Override
        public WriteResult reopen(AttemptKey key, RejectionReason reason, String rejectedSku, Instant settledAt,
                                  boolean answering) {
            return note(key, store.reopen(key, reason, rejectedSku, settledAt, answering));
        }

        private WriteResult note(AttemptKey key, WriteResult result) {
            if (result == WriteResult.OK) {
                settled.computeIfAbsent(key.transactionId(), ignored -> new AtomicLong()).set(System.nanoTime());
            }
            return result;
        }

        @Override
        public boolean recordClientOutcome(AttemptKey key, ClientOutcome outcome) {
            return store.recordClientOutcome(key, outcome);
        }

        @Override
        public boolean markPendingError(AttemptKey key, Instant now) {
            return store.markPendingError(key, now);
        }

        @Override
        public List<AttemptRecord> findPending(Instant dueBefore, PageCursor after, int limit) {
            return store.findPending(dueBefore, after, limit);
        }

        @Override
        public Optional<AttemptRecord> findAttempt(AttemptKey key) {
            return store.findAttempt(key);
        }

        @Override
        public void scheduleRetry(AttemptKey key, Instant nextRetryAt, String lastError) {
            store.scheduleRetry(key, nextRetryAt, lastError);
        }
    }
}
