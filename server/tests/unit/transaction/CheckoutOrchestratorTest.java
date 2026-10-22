package unit.transaction;

import api.ApiErrorMapper;
import database.CheckoutAttemptStore;
import database.StoreFailure;
import database.TransactionStore;
import org.junit.jupiter.api.Test;
import support.FakeInventoryGateway;
import transaction.CheckoutOrchestrator;
import transaction.CheckoutPendingFailure;
import transaction.CheckoutRejectedFailure;
import transaction.InventoryGateway;
import transaction.RecoverySchedule;
import transaction.SettlementRegistry;
import transaction.TransactionFailure;
import transaction.TransactionLocks;
import transaction.TransactionOperations;
import transaction.TransactionStateCache;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The checkout Saga's orchestration with fake stores, a scripted Inventory, and a controllable clock
 * (contracts/checkout-saga.md "Steps", "Response budget and mapping"; R6, R9, R19).
 */
class CheckoutOrchestratorTest {
    private static final Instant BASE = Instant.parse("2026-10-21T12:00:00Z");
    private static final String ID = "tx-1";

    private final FakeTiming timing = new FakeTiming();
    private final Db db = new Db();
    private final FakeInventoryGateway inventory = FakeInventoryGateway.applying();
    private final TransactionLocks locks = new TransactionLocks();
    private final TransactionStateCache cache = new TransactionStateCache(locks, 100);
    private final SettlementRegistry registry = new SettlementRegistry(locks);
    private final RecoverySchedule schedule = new RecoverySchedule(Clock.fixed(BASE, ZoneOffset.UTC));
    private final CheckoutOrchestrator orchestrator = new CheckoutOrchestrator(db, db, inventory, cache, registry, schedule,
            CheckoutOrchestrator.Settings.defaults(), timing);

    @Test
    void appliedRunsT3AndAnswersFromTheCachedSnapshotWithTheSuppliedCompletionTime() {
        db.open(ID, line("A", "Apple", "1.25", 2), line("B", "Bread", "2.50", 1));
        warm(ID);
        int loads = db.loads;
        timing.advance(Duration.ofMillis(7));

        var receipt = orchestrator.complete(ID);
        assertEquals(loads, db.loads, "a warm Complete reads nothing, before or after T3");
        assertEquals(ID, receipt.transactionId());
        assertEquals("station", receipt.stationId());
        assertEquals(3, receipt.itemCount());
        assertEquals(new BigDecimal("5.00"), receipt.totalAmount());
        assertEquals(db.row(ID).startedAt(), receipt.startedAt());
        assertEquals(timing.now(), receipt.completedAt(), "the service-supplied completed_at");
        assertEquals(db.row(ID).completedAt(), receipt.completedAt(), "the same value T3 stored");
        assertEquals(List.of("A", "B"), receipt.lines().stream().map(TransactionOperations.ReceiptLineView::sku).toList());
        assertEquals(new BigDecimal("1.25"), receipt.lines().get(0).unitPrice());

        var attempt = db.attempt(ID, 1);
        assertEquals(CheckoutAttemptStore.State.COMPLETED, attempt.state());
        assertEquals(CheckoutAttemptStore.ClientOutcome.SUCCESS, attempt.clientOutcome());
        assertEquals("[{\"sku\":\"A\",\"quantity\":2},{\"sku\":\"B\",\"quantity\":1}]", attempt.lines());
        assertEquals(BASE.plus(Duration.ofMillis(7)).plus(Duration.ofSeconds(8)), attempt.nextRetryAt(),
                "T1 sets next_retry_at to the end of the response budget");
        assertEquals(1, inventory.applyCalls());
        assertEquals(List.of(new InventoryGateway.CheckoutLine("A", 2), new InventoryGateway.CheckoutLine("B", 1)),
                inventory.calls().get(0).lines());
        try (var held = locks.acquire(ID)) {
            assertFalse(cache.contains(ID), "a COMPLETED transaction leaves the cache");
        }
        assertFalse(registry.owned(new CheckoutAttemptStore.AttemptKey(ID, 1)), "the request released its attempt");
    }

    @Test
    void insufficientStockReopensTheBasketAndAnswers409NamingTheSku() {
        db.open(ID, line("A", "Apple", "1.25", 2));
        inventory.then(new InventoryGateway.Rejected(InventoryGateway.RejectionReason.INSUFFICIENT_STOCK, "A"));
        TransactionFailure failure = assertThrows(TransactionFailure.class, () -> orchestrator.complete(ID));
        assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK, failure.code());
        assertEquals("Insufficient stock for SKU: A", failure.getMessage());
        var mapped = ApiErrorMapper.map(failure);
        assertEquals(409, mapped.httpStatus());
        assertEquals("INSUFFICIENT_STOCK", mapped.error());

        assertEquals(TransactionStore.TransactionStatus.OPEN, db.row(ID).status());
        assertNull(db.row(ID).active(), "the basket is open again");
        assertEquals(2, db.row(ID).itemCount());
        var attempt = db.attempt(ID, 1);
        assertEquals(CheckoutAttemptStore.State.REJECTED, attempt.state());
        assertEquals(CheckoutAttemptStore.RejectionReason.INSUFFICIENT_STOCK, attempt.rejectionReason());
        assertEquals("A", attempt.rejectedSku());
        assertEquals(CheckoutAttemptStore.ClientOutcome.REJECTED, attempt.clientOutcome());
        try (var held = locks.acquire(ID)) {
            var cached = cache.getOrLoad(ID, ignored -> {
                throw new AssertionError("the reopened entry stays cached");
            }).orElseThrow();
            assertFalse(cached.pending());
            assertEquals(1, cached.attemptCount());
        }
        int loads = db.loads;
        orchestrator.complete(ID);
        assertEquals(loads, db.loads, "the retry begins attempt 2 from the cached entry");
        assertEquals(2, inventory.calls().get(1).key().attemptNumber());
    }

    @Test
    void invalidRequestAndStockRestoredReopenAndAnswer500() {
        for (InventoryGateway.ApplyResult refusal : List.of(
                new InventoryGateway.Rejected(InventoryGateway.RejectionReason.INVALID_REQUEST, "GONE"),
                new InventoryGateway.StockRestored())) {
            String id = "tx-" + refusal.getClass().getSimpleName();
            db.open(id, line("GONE", "Gone", "1.00", 1));
            inventory.then(refusal);
            CheckoutRejectedFailure failure = assertThrows(CheckoutRejectedFailure.class, () -> orchestrator.complete(id));
            assertEquals(500, ApiErrorMapper.map(failure).httpStatus(), "an unmapped failure: 500 INTERNAL_ERROR");
            assertEquals("INTERNAL_ERROR", ApiErrorMapper.map(failure).error());
            assertNull(db.row(id).active(), "the basket reopened");
            var attempt = db.attempt(id, 1);
            assertEquals(CheckoutAttemptStore.State.REJECTED, attempt.state());
            assertEquals(refusal instanceof InventoryGateway.StockRestored
                            ? CheckoutAttemptStore.RejectionReason.STOCK_RESTORED : CheckoutAttemptStore.RejectionReason.INVALID_REQUEST,
                    attempt.rejectionReason());
        }
    }

    @Test
    void uncertainStatusesRepeatTheSameKeyAfter50And100And200And400Milliseconds() {
        db.open(ID, line("A", "Apple", "1.25", 1));
        for (int i = 0; i < 4; i++) inventory.then(new InventoryGateway.Uncertain("UNAVAILABLE"));
        orchestrator.complete(ID);
        assertEquals(List.of(Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofMillis(400)),
                timing.sleeps);
        assertEquals(5, inventory.applyCalls());
        var key = new CheckoutAttemptStore.AttemptKey(ID, 1);
        for (FakeInventoryGateway.Call call : inventory.calls()) {
            assertEquals(key, call.key(), "every retry reuses the attempt identifier");
            assertEquals(List.of(new InventoryGateway.CheckoutLine("A", 1)), call.lines(), "and the same payload");
            assertTrue(call.deadline().compareTo(Duration.ofSeconds(2)) <= 0, "each call gets at most 2 s");
        }
        assertEquals(CheckoutAttemptStore.State.COMPLETED, db.attempt(ID, 1).state());
    }

    @Test
    void theTransactionsLockIsNotHeldWhileInventoryIsCalled() throws Exception {
        db.open(ID, line("A", "Apple", "1.25", 1));
        AtomicBoolean heldByCaller = new AtomicBoolean(true);
        AtomicBoolean freeForOthers = new AtomicBoolean(false);
        inventory.thenAnswer(call -> {
            heldByCaller.set(locks.heldByCurrentThread(ID));
            CompletableFuture<Boolean> other = CompletableFuture.supplyAsync(() -> {
                try {
                    boolean locked = locks.lockFor(ID).tryLock(1, TimeUnit.SECONDS);
                    if (locked) locks.lockFor(ID).unlock();
                    return locked;
                } catch (InterruptedException interrupted) {
                    return false;
                }
            });
            freeForOthers.set(other.get(5, TimeUnit.SECONDS));
            return new InventoryGateway.Applied(false);
        });
        orchestrator.complete(ID);
        assertFalse(heldByCaller.get(), "the request released its stripe before the remote call");
        assertTrue(freeForOthers.get(), "another thread can take the stripe during ApplyCheckout");
    }

    @Test
    void onBudgetExpiryTheAttemptIsHandedOverStillPending() {
        db.open(ID, line("A", "Apple", "1.25", 1));
        inventory.otherwise(call -> {
            timing.advance(call.deadline());
            return new InventoryGateway.Uncertain("DEADLINE_EXCEEDED");
        });
        assertThrows(CheckoutPendingFailure.class, () -> orchestrator.complete(ID));
        assertTrue(Duration.ofNanos(timing.nanos).compareTo(Duration.ofSeconds(8)) <= 0, "the 500 comes within the budget");
        var key = new CheckoutAttemptStore.AttemptKey(ID, 1);
        var attempt = db.attempt(ID, 1);
        assertEquals(CheckoutAttemptStore.State.PENDING, attempt.state(), "a 500 is never a cancellation");
        assertEquals(CheckoutAttemptStore.ClientOutcome.PENDING_ERROR, attempt.clientOutcome());
        assertEquals(timing.now(), attempt.nextRetryAt(), "recovery takes over at once");
        assertEquals(List.of("markPendingError " + key), db.calls.subList(db.calls.size() - 1, db.calls.size()),
                "PENDING_ERROR is committed before the 500");
        assertEquals(timing.now(), schedule.dueAt(key), "queued in the recovery schedule, due now");
        assertEquals(1, db.row(ID).active(), "the basket stays frozen");
        assertEquals(500, ApiErrorMapper.map(new CheckoutPendingFailure(ID)).httpStatus());
    }

    @Test
    void aFailedPendingErrorWriteStillQueuesTheAttempt() {
        db.open(ID, line("A", "Apple", "1.25", 1));
        inventory.otherwise(call -> {
            timing.advance(call.deadline());
            return new InventoryGateway.Uncertain("UNAVAILABLE");
        });
        db.pendingErrorFailure = new StoreFailure("database down");
        assertThrows(CheckoutPendingFailure.class, () -> orchestrator.complete(ID));
        var key = new CheckoutAttemptStore.AttemptKey(ID, 1);
        assertTrue(schedule.contains(key), "the handover happens in a finally block");
        assertEquals(CheckoutAttemptStore.ClientOutcome.NONE, db.attempt(ID, 1).clientOutcome());
    }

    @Test
    void anAmbiguousT1CommitInvalidatesQueuesAReconcileEntryAndNeverCallsInventory() {
        for (boolean durable : List.of(false, true)) {
            String id = "tx-ambiguous-" + durable;
            db.open(id, line("A", "Apple", "1.25", 1));
            warm(id);
            db.beginFailure = new Failure(new StoreFailure("commit lost", null, true), durable);
            StoreFailure failure = assertThrows(StoreFailure.class, () -> orchestrator.complete(id));
            assertTrue(failure.commitOutcomeUnknown());
            assertEquals(500, ApiErrorMapper.map(failure).httpStatus());
            assertEquals(0, inventory.applyCalls(), "no Inventory call after an ambiguous T1");
            try (var held = locks.acquire(id)) {
                assertFalse(cache.contains(id), "the entry was invalidated, so the next request reloads first");
            }
            assertTrue(schedule.contains(new CheckoutAttemptStore.AttemptKey(id, 1)), "a reconcile entry for (id, 1)");
            assertEquals(durable ? 1 : 0, db.attempts(id).size());
        }
    }

    @Test
    void anAmbiguousT3CommitIsRetriedAndAnswersFromTheDurableCompletion() {
        db.open(ID, line("A", "Apple", "1.25", 1));
        db.completeFailures.add(new Failure(new StoreFailure("commit lost", null, true), true));
        Instant completedAt = timing.now();
        var receipt = orchestrator.complete(ID);
        assertEquals(completedAt, receipt.completedAt(), "the durable completed_at from the first commit");
        assertEquals(db.row(ID).completedAt(), receipt.completedAt());
        assertEquals(List.of(Duration.ofMillis(50)), timing.sleeps, "one retry within the budget");
        assertEquals(CheckoutAttemptStore.ClientOutcome.SUCCESS, db.attempt(ID, 1).clientOutcome(),
                "the first commit recorded the success; nothing is recorded twice");
        assertEquals(0, db.clientOutcomeWrites);
        assertEquals(1, inventory.applyCalls());

        String flaky = "tx-transient";
        db.open(flaky, line("A", "Apple", "1.25", 1));
        db.completeFailures.add(new Failure(new StoreFailure("database down"), false));
        var retried = orchestrator.complete(flaky);
        assertEquals(CheckoutAttemptStore.State.COMPLETED, db.attempt(flaky, 1).state());
        assertEquals(db.row(flaky).completedAt(), retried.completedAt());
    }

    @Test
    void anAnswerFromAnotherActorsSettlementRecordsTheRequestsOwnOutcomeFirst() {
        db.open(ID, line("A", "Apple", "1.25", 1));
        Instant foreignAt = BASE.plusSeconds(3);
        inventory.thenAnswer(call -> {
            db.complete(call.key(), foreignAt, false);
            return new InventoryGateway.Applied(true);
        });
        var receipt = orchestrator.complete(ID);
        assertEquals(foreignAt, receipt.completedAt(), "the settled attempt's completed_at");
        assertEquals(CheckoutAttemptStore.ClientOutcome.SUCCESS, db.attempt(ID, 1).clientOutcome());
        assertEquals(1, db.clientOutcomeWrites);

        String rejected = "tx-rejected";
        db.open(rejected, line("A", "Apple", "1.25", 1));
        inventory.thenAnswer(call -> {
            db.reopen(call.key(), CheckoutAttemptStore.RejectionReason.INSUFFICIENT_STOCK, "A", foreignAt, false);
            return new InventoryGateway.Rejected(InventoryGateway.RejectionReason.INSUFFICIENT_STOCK, "A");
        });
        assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK,
                assertThrows(TransactionFailure.class, () -> orchestrator.complete(rejected)).code());
        assertEquals(CheckoutAttemptStore.ClientOutcome.REJECTED, db.attempt(rejected, 1).clientOutcome());

        String failing = "tx-unrecorded";
        db.open(failing, line("A", "Apple", "1.25", 1));
        inventory.thenAnswer(call -> {
            db.complete(call.key(), foreignAt, false);
            return new InventoryGateway.Applied(true);
        });
        db.clientOutcomeFailure = new StoreFailure("database down");
        RuntimeException unrecorded = assertThrows(RuntimeException.class, () -> orchestrator.complete(failing));
        assertEquals(500, ApiErrorMapper.map(unrecorded).httpStatus(),
                "an unrecorded outcome answers 500, so the background-completion counts stay exact");
        assertEquals(CheckoutAttemptStore.ClientOutcome.NONE, db.attempt(failing, 1).clientOutcome());
    }

    @Test
    void failedPreconditionAndInvalidArgumentNeverReopenTheBasket() {
        for (InventoryGateway.ApplyResult fault : List.of(new InventoryGateway.PayloadMismatch("mismatch"),
                new InventoryGateway.InvalidRequest("malformed"))) {
            String id = "tx-" + fault.getClass().getSimpleName();
            db.open(id, line("A", "Apple", "1.25", 1));
            inventory.then(fault);
            assertThrows(CheckoutPendingFailure.class, () -> orchestrator.complete(id));
            var attempt = db.attempt(id, 1);
            assertEquals(CheckoutAttemptStore.State.PENDING, attempt.state(), "an integrity fault is never a rejection");
            assertEquals(CheckoutAttemptStore.ClientOutcome.PENDING_ERROR, attempt.clientOutcome());
            assertEquals(1, db.row(id).active(), "the basket stays frozen for recovery");
            assertTrue(schedule.contains(new CheckoutAttemptStore.AttemptKey(id, 1)));
        }
    }

    @Test
    void preChecksAnswer404ThenNotOpenThenEmptyBasketEachConfirmedAgainstTheStore() {
        assertCode(TransactionFailure.Code.NOT_FOUND, () -> orchestrator.complete("missing"));

        db.open(ID, line("A", "Apple", "1.25", 1));
        warm(ID);
        db.completeBehindTheCache(ID);
        int loads = db.loads;
        assertCode(TransactionFailure.Code.NOT_OPEN, () -> orchestrator.complete(ID));
        assertEquals(loads + 1, db.loads, "the stale OPEN entry was confirmed by a reload");

        String empty = "tx-empty";
        db.open(empty);
        warm(empty);
        db.foreignScan(empty, line("B", "Bread", "2.50", 1));
        var receipt = orchestrator.complete(empty);
        assertEquals(1, receipt.itemCount(), "a stale empty entry is confirmed before 409, and the store had a unit");

        String reallyEmpty = "tx-really-empty";
        db.open(reallyEmpty);
        warm(reallyEmpty);
        loads = db.loads;
        assertCode(TransactionFailure.Code.EMPTY_BASKET, () -> orchestrator.complete(reallyEmpty));
        assertEquals(loads + 1, db.loads);

        String pending = "tx-pending";
        db.open(pending, line("A", "Apple", "1.25", 1));
        db.beginBehindTheCache(pending);
        long started = timing.nanos;
        assertThrows(CheckoutPendingFailure.class, () -> orchestrator.complete(pending),
                "another instance's attempt that never settles: a bounded wait, then 500 at the bound");
        assertEquals(Duration.ofSeconds(8), Duration.ofNanos(timing.nanos - started), "the whole 8 s budget");
        assertEquals(0, inventory.calls().stream().filter(call -> call.key().transactionId().equals(pending)).count());
        assertEquals(CheckoutAttemptStore.State.PENDING, db.attempt(pending, 1).state(), "the pending attempt is untouched");
        assertEquals(CheckoutAttemptStore.ClientOutcome.NONE, db.attempt(pending, 1).clientOutcome());
    }

    @Test
    void aCompleteWaitingForThisProcesssAttemptWakesOnItsSettlementWithNoDatabaseCheckAndAnswers409() throws Exception {
        db.open(ID, line("A", "Apple", "1.25", 1));
        java.util.concurrent.CountDownLatch inside = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        inventory.thenAnswer(call -> {
            inside.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));
            return new InventoryGateway.Applied(false);
        });
        CheckoutOrchestrator realTime = new CheckoutOrchestrator(db, db, inventory, cache, registry, schedule,
                CheckoutOrchestrator.Settings.defaults());
        CompletableFuture<TransactionOperations.ReceiptView> owner = CompletableFuture.supplyAsync(() -> realTime.complete(ID));
        assertTrue(inside.await(10, TimeUnit.SECONDS));
        int finds = db.finds;
        CompletableFuture<TransactionFailure.Code> duplicate = CompletableFuture.supplyAsync(() ->
                assertThrows(TransactionFailure.class, () -> realTime.complete(ID)).code());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (registry.waiting() == 0) {
            assertTrue(System.nanoTime() < deadline, "the duplicate never started waiting");
            Thread.sleep(5);
        }
        Thread.sleep(1_500);
        assertEquals(finds, db.finds, "no database check while this process's live request owns the attempt");
        release.countDown();
        assertEquals(1, owner.get(10, TimeUnit.SECONDS).itemCount());
        assertEquals(TransactionFailure.Code.NOT_OPEN, duplicate.get(10, TimeUnit.SECONDS),
                "the restarted duplicate finds the transaction COMPLETED");
        assertEquals(finds, db.finds, "the settlement signal woke the waiter");
        assertEquals(0, registry.waiting(), "no local future is left behind");
        assertEquals(1, inventory.applyCalls(), "FR-036: the duplicate never reached Inventory");
    }

    @Test
    void aWaiterSeesAnotherProcesssSettlementThroughItsBackstopAndBeginsTheNextAttempt() {
        db.open(ID, line("A", "Apple", "1.25", 1));
        db.beginBehindTheCache(ID);
        var foreign = new CheckoutAttemptStore.AttemptKey(ID, 1);
        timing.onAwait = () -> {
            if (timing.nanos >= Duration.ofSeconds(2).toNanos() && db.row(ID).active() != null) {
                db.reopen(foreign, CheckoutAttemptStore.RejectionReason.INSUFFICIENT_STOCK, "A", timing.now(), false);
            }
        };
        var receipt = orchestrator.complete(ID);
        assertEquals(1, receipt.itemCount());
        assertEquals(2, inventory.calls().get(0).key().attemptNumber(), "the reopened basket begins attempt 2");
        assertEquals(3, db.finds, "one backstop check per second until the other process settled");
        assertEquals(0, registry.waiting(), "the waiter dropped its local future");
        assertEquals(CheckoutAttemptStore.ClientOutcome.NONE, db.attempt(ID, 1).clientOutcome(),
                "the waiter records nothing on another request's attempt");
    }

    @Test
    void aStaleItemCountMissesTheT1GuardAndTheReloadedBasketIsCheckedOut() {
        db.open(ID, line("A", "Apple", "1.25", 1));
        warm(ID);
        db.foreignScan(ID, line("B", "Bread", "2.50", 1));
        var receipt = orchestrator.complete(ID);
        assertEquals(2, receipt.itemCount(), "the other writer's unit is part of the checkout");
        assertEquals(new BigDecimal("3.75"), receipt.totalAmount());
        assertEquals(1, db.guardMisses);
    }

    private void warm(String id) {
        try (var held = locks.acquire(id)) {
            cache.getOrLoad(id, key -> db.loadStateAsCache(key));
        }
    }

    private static void assertCode(TransactionFailure.Code code, Runnable action) {
        assertEquals(code, assertThrows(TransactionFailure.class, action::run).code());
    }

    private static TransactionStore.TransactionLine line(String sku, String name, String price, int quantity) {
        return new TransactionStore.TransactionLine(sku, name, quantity, new BigDecimal(price));
    }

    /** Monotonic time that moves only on sleeps and explicit advances. */
    private static final class FakeTiming implements CheckoutOrchestrator.Timing {
        long nanos;
        final List<Duration> sleeps = new ArrayList<>();

        @Override
        public long nanoTime() {
            return nanos;
        }

        @Override
        public Instant now() {
            return BASE.plusNanos(nanos).truncatedTo(ChronoUnit.MILLIS);
        }

        @Override
        public void sleep(Duration duration) {
            sleeps.add(duration);
            nanos += duration.toNanos();
        }

        /** Runs before every fake wait slice, for example to settle an attempt "elsewhere". */
        Runnable onAwait = () -> { };

        /** A wait that is not already settled lets its whole slice pass at once. */
        @Override
        public <T> T await(CompletableFuture<T> future, Duration timeout) {
            onAwait.run();
            if (future.isDone()) return future.getNow(null);
            nanos += timeout.toNanos();
            return null;
        }

        void advance(Duration duration) {
            nanos += duration.toNanos();
        }
    }

    record Failure(StoreFailure failure, boolean afterCommit) { }

    /** Transaction rows, basket lines, and attempts, behind the same guards as the real stores. */
    private final class Db implements TransactionStore, CheckoutAttemptStore {
        record Row(String stationId, TransactionStatus status, int itemCount, BigDecimal total, int attemptCount,
                   Integer active, Instant startedAt, Instant completedAt) { }

        final Map<String, Row> rows = new HashMap<>();
        final Map<String, List<TransactionLine>> lines = new HashMap<>();
        final Map<AttemptKey, AttemptRecord> attempts = new HashMap<>();
        final List<String> calls = new ArrayList<>();
        final Deque<Failure> completeFailures = new ArrayDeque<>();
        Failure beginFailure;
        StoreFailure pendingErrorFailure;
        StoreFailure clientOutcomeFailure;
        int loads;
        int finds;
        int guardMisses;
        int clientOutcomeWrites;

        synchronized void open(String id, TransactionLine... basket) {
            int units = 0;
            BigDecimal total = new BigDecimal("0.00");
            for (TransactionLine line : basket) {
                units += line.quantity();
                total = total.add(line.unitPrice().multiply(BigDecimal.valueOf(line.quantity())));
            }
            rows.put(id, new Row("station", TransactionStatus.OPEN, units, total, 0, null, BASE.minusSeconds(60), null));
            lines.put(id, new ArrayList<>(List.of(basket)));
        }

        synchronized Row row(String id) {
            return rows.get(id);
        }

        synchronized AttemptRecord attempt(String id, int number) {
            return attempts.get(new AttemptKey(id, number));
        }

        synchronized List<AttemptRecord> attempts(String id) {
            return attempts.values().stream().filter(a -> a.key().transactionId().equals(id)).toList();
        }

        synchronized void completeBehindTheCache(String id) {
            Row r = rows.get(id);
            rows.put(id, new Row(r.stationId(), TransactionStatus.COMPLETED, r.itemCount(), r.total(), r.attemptCount(),
                    null, r.startedAt(), BASE));
        }

        /** Another instance's T1: the row's active attempt and its PENDING attempt row. */
        synchronized void beginBehindTheCache(String id) {
            Row r = rows.get(id);
            int number = r.attemptCount() + 1;
            rows.put(id, new Row(r.stationId(), r.status(), r.itemCount(), r.total(), number, number, r.startedAt(), null));
            AttemptKey key = new AttemptKey(id, number);
            attempts.put(key, new AttemptRecord(key, State.PENDING, null, null, "[]", r.itemCount(), r.total(),
                    ClientOutcome.NONE, BASE, null, 0, BASE.plusSeconds(8), null));
        }

        synchronized void foreignScan(String id, TransactionLine line) {
            Row r = rows.get(id);
            lines.get(id).add(line);
            rows.put(id, new Row(r.stationId(), r.status(), r.itemCount() + line.quantity(),
                    r.total().add(line.unitPrice().multiply(BigDecimal.valueOf(line.quantity()))), r.attemptCount(),
                    r.active(), r.startedAt(), r.completedAt()));
        }

        /** The service's own cold-path mapping, used to warm the cache as a Scan would leave it. */
        Optional<TransactionStateCache.State> loadStateAsCache(String id) {
            return loadState(id).map(state -> {
                Map<String, TransactionStateCache.Line> basket = new java.util.TreeMap<>();
                for (TransactionLine line : state.lines()) {
                    basket.put(line.sku(), new TransactionStateCache.Line(line.sku(), line.name(), line.unitPrice(),
                            line.quantity()));
                }
                var row = state.transaction();
                return new TransactionStateCache.State(id, TransactionOperations.TransactionStatus.valueOf(row.status().name()),
                        row.itemCount(), row.totalAmount(), state.attemptCount(), state.activeAttemptNumber(), basket,
                        row.stationId(), row.startedAt());
            });
        }

        @Override
        public void insertOpen(String transactionId, String stationId, Instant startedAt) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized Optional<TransactionRecord> findById(String id) {
            Row r = rows.get(id);
            return r == null ? Optional.empty() : Optional.of(record(id, r));
        }

        @Override
        public synchronized Optional<TransactionState> loadState(String id) {
            loads++;
            Row r = rows.get(id);
            if (r == null) return Optional.empty();
            return Optional.of(new TransactionState(record(id, r), r.attemptCount(), r.active(), lines.get(id)));
        }

        private TransactionRecord record(String id, Row r) {
            return new TransactionRecord(id, r.stationId(), r.status(), r.total(), r.startedAt(), r.completedAt(),
                    r.itemCount());
        }

        @Override
        public synchronized WriteResult begin(Begin begin) {
            AttemptKey key = begin.key();
            Failure failure = beginFailure;
            beginFailure = null;
            if (failure != null && !failure.afterCommit()) throw failure.failure();
            Row r = rows.get(key.transactionId());
            if (r == null || r.status() != TransactionStatus.OPEN || r.active() != null || r.itemCount() != begin.itemCount()
                    || r.attemptCount() != key.attemptNumber() - 1) {
                guardMisses++;
                return WriteResult.GUARD_MISS;
            }
            rows.put(key.transactionId(), new Row(r.stationId(), r.status(), r.itemCount(), r.total(), key.attemptNumber(),
                    key.attemptNumber(), r.startedAt(), null));
            attempts.put(key, new AttemptRecord(key, State.PENDING, null, null, begin.lines(), begin.itemCount(),
                    begin.totalAmount(), ClientOutcome.NONE, begin.createdAt(), null, 0, begin.nextRetryAt(), null));
            calls.add("begin " + key);
            if (failure != null) throw failure.failure();
            return WriteResult.OK;
        }

        @Override
        public synchronized WriteResult complete(AttemptKey key, Instant completedAt, boolean answering) {
            Failure failure = completeFailures.poll();
            if (failure != null && !failure.afterCommit()) throw failure.failure();
            Row r = rows.get(key.transactionId());
            if (r == null || !Integer.valueOf(key.attemptNumber()).equals(r.active())) {
                guardMisses++;
                return WriteResult.GUARD_MISS;
            }
            rows.put(key.transactionId(), new Row(r.stationId(), TransactionStatus.COMPLETED, r.itemCount(), r.total(),
                    r.attemptCount(), null, r.startedAt(), completedAt));
            settle(key, State.COMPLETED, null, null, completedAt, answering ? ClientOutcome.SUCCESS : null);
            calls.add("complete " + key);
            if (failure != null) throw failure.failure();
            return WriteResult.OK;
        }

        @Override
        public synchronized WriteResult reopen(AttemptKey key, RejectionReason reason, String rejectedSku,
                                               Instant settledAt, boolean answering) {
            Row r = rows.get(key.transactionId());
            if (r == null || !Integer.valueOf(key.attemptNumber()).equals(r.active())) {
                guardMisses++;
                return WriteResult.GUARD_MISS;
            }
            rows.put(key.transactionId(), new Row(r.stationId(), r.status(), r.itemCount(), r.total(), r.attemptCount(),
                    null, r.startedAt(), null));
            settle(key, State.REJECTED, reason, rejectedSku, settledAt, answering ? ClientOutcome.REJECTED : null);
            calls.add("reopen " + key);
            return WriteResult.OK;
        }

        private void settle(AttemptKey key, State state, RejectionReason reason, String sku, Instant at,
                            ClientOutcome answer) {
            AttemptRecord a = attempts.get(key);
            ClientOutcome outcome = answer != null && a.clientOutcome() == ClientOutcome.NONE ? answer : a.clientOutcome();
            attempts.put(key, new AttemptRecord(key, state, reason, sku, a.lines(), a.itemCount(), a.totalAmount(),
                    outcome, a.createdAt(), at, a.retryCount(), a.nextRetryAt(), a.lastError()));
        }

        @Override
        public synchronized boolean recordClientOutcome(AttemptKey key, ClientOutcome outcome) {
            if (clientOutcomeFailure != null) throw clientOutcomeFailure;
            AttemptRecord a = attempts.get(key);
            if (a == null || a.clientOutcome() != ClientOutcome.NONE) return false;
            clientOutcomeWrites++;
            attempts.put(key, new AttemptRecord(key, a.state(), a.rejectionReason(), a.rejectedSku(), a.lines(),
                    a.itemCount(), a.totalAmount(), outcome, a.createdAt(), a.settledAt(), a.retryCount(),
                    a.nextRetryAt(), a.lastError()));
            calls.add("recordClientOutcome " + key);
            return true;
        }

        @Override
        public synchronized boolean markPendingError(AttemptKey key, Instant now) {
            if (pendingErrorFailure != null) throw pendingErrorFailure;
            AttemptRecord a = attempts.get(key);
            if (a == null || a.state() != State.PENDING || a.clientOutcome() != ClientOutcome.NONE) return false;
            attempts.put(key, new AttemptRecord(key, a.state(), null, null, a.lines(), a.itemCount(), a.totalAmount(),
                    ClientOutcome.PENDING_ERROR, a.createdAt(), null, a.retryCount(), now, a.lastError()));
            calls.add("markPendingError " + key);
            return true;
        }

        @Override
        public List<AttemptRecord> findPending(Instant dueBefore, PageCursor after, int limit) {
            throw new UnsupportedOperationException("recovery is not part of this test");
        }

        @Override
        public synchronized Optional<AttemptRecord> findAttempt(AttemptKey key) {
            finds++;
            return Optional.ofNullable(attempts.get(key));
        }

        @Override
        public void scheduleRetry(AttemptKey key, Instant nextRetryAt, String lastError) {
            throw new UnsupportedOperationException("recovery is not part of this test");
        }
    }
}
