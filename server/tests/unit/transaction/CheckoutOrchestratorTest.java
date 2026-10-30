package unit.transaction;

import api.ApiErrorMapper;
import database.CheckoutAttemptStore;
import database.CheckoutAttemptStore.AttemptKey;
import database.StoreFailure;
import database.TransactionStore;
import events.ConsumerLoop;
import events.EnvelopeCodec;
import events.OutboxDispatcher;
import events.OutboxRow;
import events.OutboxSignal;
import events.TopicNames;
import messaging.CheckoutOutcomeConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.CheckoutRequested;
import support.FakeInventoryReactor;
import support.FakeInventoryReactor.Reply;
import support.InMemoryDeadLetterStore;
import support.InMemoryEventBus;
import transaction.CheckoutEvents;
import transaction.CheckoutOrchestrator;
import transaction.CheckoutOutcomeHandler;
import transaction.CheckoutPendingFailure;
import transaction.CheckoutRejectedFailure;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static unit.transaction.CheckoutDb.line;

/**
 * The request side of the choreographed checkout Saga with fake stores, the real dispatcher and outcome
 * consumer on an {@link InMemoryEventBus}, a scripted {@link FakeInventoryReactor}, and a controllable clock
 * (D7; contracts/checkout-saga.md "Steps", "Response budget and claiming the response"; R9, R19). The
 * in-request retry loop and the RPC deadline of feature 005 are gone (FR-031, FR-037): the request records
 * {@code CheckoutRequested}, signals, and waits holding nothing.
 */
class CheckoutOrchestratorTest {
    private static final Instant BASE = Instant.parse("2026-10-21T12:00:00Z");
    private static final String ID = "tx-1";

    private final FakeTiming timing = new FakeTiming();
    private final CheckoutDb db = new CheckoutDb(BASE);
    private final TransactionLocks locks = new TransactionLocks();
    private final TransactionStateCache cache = new TransactionStateCache(locks, 100);
    private final SettlementRegistry registry = new SettlementRegistry(locks);
    private final RecoverySchedule schedule = new RecoverySchedule(Clock.fixed(BASE, ZoneOffset.UTC));
    private final TopicNames names = TopicNames.unprefixed();
    private final InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 21);
    private final FakeInventoryReactor inventory = FakeInventoryReactor.deducting(bus, names).start();
    private final RecordingSignal signal = new RecordingSignal(new OutboxDispatcher("transaction", db.outbox,
            java.util.Map.of(names.checkout(), bus), List.of(names.checkout()), OutboxDispatcher.Settings.defaults(),
            Clock.systemUTC()));
    private final ConsumerLoop outcomes = CheckoutOutcomeConsumer.create(names, bus::stream,
            new CheckoutOutcomeHandler(db, cache, registry, timing), new InMemoryDeadLetterStore(),
            ConsumerLoop.Settings.defaults().withThreads(2));
    private final CheckoutOrchestrator orchestrator = new CheckoutOrchestrator(db, db, new CheckoutEvents(names), signal,
            cache, registry, schedule, CheckoutOrchestrator.Settings.defaults(), timing);

    {
        signal.dispatcher.start();
        outcomes.start();
    }

    @AfterEach
    void stop() throws InterruptedException {
        outcomes.stop(Duration.ofSeconds(5));
        signal.dispatcher.stop(Duration.ofSeconds(5));
        inventory.close();
    }

    @Test
    void appliedRunsT3AndAnswersFromTheCachedSnapshotWithTheSuppliedCompletionTime() {
        db.open(ID, line("B", "Bread", "2.50", 1), line("A", "Apple", "1.25", 2));
        warm(ID);
        int loads = db.loads;
        timing.advance(Duration.ofMillis(7));
        Instant createdAt = timing.now();

        var receipt = orchestrator.complete(ID);
        assertEquals(loads, db.loads, "a warm Complete reads nothing, before or after T3");
        assertEquals(ID, receipt.transactionId());
        assertEquals("station", receipt.stationId());
        assertEquals(3, receipt.itemCount());
        assertEquals(new BigDecimal("5.00"), receipt.totalAmount());
        assertEquals(db.row(ID).startedAt(), receipt.startedAt());
        assertEquals(db.row(ID).completedAt(), receipt.completedAt(), "the completed_at T3 stored, from the settlement");
        assertFalse(receipt.completedAt().isBefore(createdAt));
        assertEquals(List.of("A", "B"), receipt.lines().stream().map(TransactionOperations.ReceiptLineView::sku).toList());
        assertEquals(new BigDecimal("1.25"), receipt.lines().get(0).unitPrice());

        var attempt = db.attempt(ID, 1);
        assertEquals(CheckoutAttemptStore.State.COMPLETED, attempt.state());
        assertEquals(CheckoutAttemptStore.ClientOutcome.SUCCESS, attempt.clientOutcome(),
                "the handler claimed the response and wrote it inside T3");
        assertEquals(0, db.clientOutcomeWrites, "no extra commit records the outcome");
        assertEquals("[{\"sku\":\"A\",\"quantity\":2},{\"sku\":\"B\",\"quantity\":1}]", attempt.lines());
        assertEquals(createdAt.plus(Duration.ofSeconds(2)), attempt.nextRetryAt(),
                "T1 sets next_retry_at to created_at + CHECKOUT_REPUBLISH_AFTER_MS");
        assertEquals(1, inventory.requests());
        assertEquals(List.of(new FakeInventoryReactor.Line("A", 2), new FakeInventoryReactor.Line("B", 1)),
                inventory.calls().get(0).lines(), "the request's lines are sorted by SKU, one per SKU");
        try (var held = locks.acquire(ID)) {
            assertFalse(cache.contains(ID), "a COMPLETED transaction leaves the cache");
        }
        assertFalse(registry.owned(new AttemptKey(ID, 1)), "the request released its attempt");
    }

    @Test
    void t1WritesTheAttemptAndOneCheckoutRequestedRowAndTheRequestThreadNeverCallsTheSender() throws Exception {
        db.open(ID, line("C", "Cheese", "4.00", 1), line("A", "Apple", "1.25", 3), line("B", "Bread", "2.50", 2));
        Thread requestThread = Thread.currentThread();
        orchestrator.complete(ID);

        List<OutboxRow> rows = db.outbox.findByAggregateKey(ID + ":1");
        assertEquals(1, rows.size(), "one CheckoutRequested row per attempt");
        OutboxRow row = rows.get(0);
        assertEquals(db.attempt(ID, 1).requestEventId(), row.eventId(), "linked from request_event_id");
        assertEquals(names.checkout(), row.topic());
        assertEquals(ID, row.routingKey(), "keyed by the transaction ID");
        var event = EnvelopeCodec.decode(row.envelope(), ID);
        assertEquals("transaction", event.producer());
        assertEquals(ID, event.correlationId());
        CheckoutRequested request = event.payload(CheckoutRequested.class);
        assertEquals(List.of("A", "B", "C"), request.getLinesList().stream().map(l -> l.getSku()).toList());
        assertEquals(List.of(3, 2, 1), request.getLinesList().stream().map(l -> l.getQuantity()).toList());
        assertEquals(List.of(List.of(row.eventId())), signal.signals, "the request only signalled its row");
        assertEquals(List.of(requestThread), signal.threads, "after its commit, on its own thread");
        assertTrue(inventory.calls().stream().noneMatch(call -> call.thread() == requestThread),
                "the request thread never sent anything");
    }

    @Test
    void insufficientStockReopensTheBasketAndAnswers409NamingTheSku() {
        db.open(ID, line("A", "Apple", "1.25", 2));
        inventory.then(Reply.insufficient("A"));
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
        for (Reply refusal : List.of(Reply.invalid("GONE"), Reply.refuse())) {
            String id = "tx-" + refusal.kind();
            db.open(id, line("GONE", "Gone", "1.00", 1));
            inventory.then(refusal);
            CheckoutRejectedFailure failure = assertThrows(CheckoutRejectedFailure.class, () -> orchestrator.complete(id));
            assertEquals(500, ApiErrorMapper.map(failure).httpStatus(), "an unmapped failure: 500 INTERNAL_ERROR");
            assertEquals("INTERNAL_ERROR", ApiErrorMapper.map(failure).error());
            assertNull(db.row(id).active(), "the basket reopened");
            var attempt = db.attempt(id, 1);
            assertEquals(CheckoutAttemptStore.State.REJECTED, attempt.state());
            assertEquals(refusal.kind() == FakeInventoryReactor.Kind.REFUSE
                            ? CheckoutAttemptStore.RejectionReason.STOCK_RESTORED : CheckoutAttemptStore.RejectionReason.INVALID_REQUEST,
                    attempt.rejectionReason());
        }
    }

    @Test
    void theTransactionsLockIsNotHeldWhileInventoryIsCalled() throws Exception {
        db.open(ID, line("A", "Apple", "1.25", 1));
        CountDownLatch requested = new CountDownLatch(1);
        CountDownLatch checked = new CountDownLatch(1);
        AtomicBoolean freeForOthers = new AtomicBoolean(false);
        inventory.thenAnswer(call -> {
            requested.countDown();
            try {
                assertTrue(checked.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                throw new AssertionError(interrupted);
            }
            return Reply.deduct();
        });
        CheckoutOrchestrator realTime = new CheckoutOrchestrator(db, db, new CheckoutEvents(names), signal, cache,
                registry, schedule, CheckoutOrchestrator.Settings.defaults());
        CompletableFuture<TransactionOperations.ReceiptView> request = CompletableFuture.supplyAsync(() -> realTime.complete(ID));
        assertTrue(requested.await(10, TimeUnit.SECONDS));
        boolean locked = locks.lockFor(ID).tryLock(1, TimeUnit.SECONDS);
        if (locked) locks.lockFor(ID).unlock();
        freeForOthers.set(locked);
        checked.countDown();
        assertEquals(1, request.get(10, TimeUnit.SECONDS).itemCount());
        assertTrue(freeForOthers.get(), "another thread can take the stripe while the request waits for its outcome");
    }

    @Test
    void onBudgetExpiryTheAttemptIsHandedOverStillPending() {
        db.open(ID, line("A", "Apple", "1.25", 1));
        inventory.then(Reply.ignore());
        timing.realWait = Duration.ofMillis(20);
        assertThrows(CheckoutPendingFailure.class, () -> orchestrator.complete(ID));
        assertTrue(Duration.ofNanos(timing.nanos).compareTo(Duration.ofSeconds(8)) <= 0, "the 500 comes within the budget");
        var key = new AttemptKey(ID, 1);
        var attempt = db.attempt(ID, 1);
        assertEquals(CheckoutAttemptStore.State.PENDING, attempt.state(), "a 500 is never a cancellation");
        assertEquals(CheckoutAttemptStore.ClientOutcome.PENDING_ERROR, attempt.clientOutcome());
        assertEquals(timing.now(), attempt.nextRetryAt(), "recovery takes over at once");
        assertEquals("markPendingError " + key, db.calls.get(db.calls.size() - 1), "PENDING_ERROR is committed before the 500");
        assertEquals(timing.now(), schedule.dueAt(key), "queued in the recovery schedule, due now");
        assertEquals(1, db.row(ID).active(), "the basket stays frozen");
        assertEquals(SettlementRegistry.Responder.TIMED_OUT, registry.responder(key), "the request claimed the timeout");
        assertEquals(500, ApiErrorMapper.map(new CheckoutPendingFailure(ID)).httpStatus());
    }

    @Test
    void aFailedPendingErrorWriteStillQueuesTheAttempt() {
        db.open(ID, line("A", "Apple", "1.25", 1));
        inventory.then(Reply.ignore());
        timing.realWait = Duration.ofMillis(20);
        db.pendingErrorFailure = new StoreFailure("database down");
        assertThrows(CheckoutPendingFailure.class, () -> orchestrator.complete(ID));
        var key = new AttemptKey(ID, 1);
        assertTrue(schedule.contains(key), "the handover happens in a finally block");
        assertEquals(CheckoutAttemptStore.ClientOutcome.NONE, db.attempt(ID, 1).clientOutcome());
    }

    @Test
    void anAmbiguousT1CommitInvalidatesQueuesAReconcileEntryAndNeverCallsInventory() throws Exception {
        for (boolean durable : List.of(false, true)) {
            String id = "tx-ambiguous-" + durable;
            db.open(id, line("A", "Apple", "1.25", 1));
            warm(id);
            db.beginFailure = new CheckoutDb.Failure(new StoreFailure("commit lost", null, true), durable);
            StoreFailure failure = assertThrows(StoreFailure.class, () -> orchestrator.complete(id));
            assertTrue(failure.commitOutcomeUnknown());
            assertEquals(500, ApiErrorMapper.map(failure).httpStatus());
            assertTrue(signal.signals.isEmpty(), "no signal after an ambiguous T1");
            try (var held = locks.acquire(id)) {
                assertFalse(cache.contains(id), "the entry was invalidated, so the next request reloads first");
            }
            assertTrue(schedule.contains(new AttemptKey(id, 1)), "a reconcile entry for (id, 1)");
            assertEquals(durable ? 1 : 0, db.attempts(id).size());
            assertEquals(durable ? 1 : 0, db.outbox.findByAggregateKey(id + ":1").size(),
                    "the request row exists exactly when the attempt does; the sweep publishes it");
        }
    }

    @Test
    void anAmbiguousT3CommitIsRetriedAndAnswersFromTheDurableCompletion() {
        db.open(ID, line("A", "Apple", "1.25", 1));
        db.completeFailures.add(new CheckoutDb.Failure(new StoreFailure("commit lost", null, true), true));
        var receipt = orchestrator.complete(ID);
        assertEquals(db.row(ID).completedAt(), receipt.completedAt(), "the durable completed_at from the first commit");
        assertEquals(List.of(Duration.ofMillis(50)), timing.sleeps, "the handler retried once under the lock");
        assertEquals(CheckoutAttemptStore.ClientOutcome.SUCCESS, db.attempt(ID, 1).clientOutcome(),
                "the first commit recorded the success; nothing is recorded twice");
        assertEquals(0, db.clientOutcomeWrites);
        assertEquals(1, inventory.requests());

        String flaky = "tx-transient";
        db.open(flaky, line("A", "Apple", "1.25", 1));
        db.completeFailures.add(new CheckoutDb.Failure(new StoreFailure("database down"), false));
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
            return Reply.deduct();
        });
        var receipt = orchestrator.complete(ID);
        assertEquals(foreignAt, receipt.completedAt(), "the settled attempt's completed_at");
        assertEquals(CheckoutAttemptStore.ClientOutcome.SUCCESS, db.attempt(ID, 1).clientOutcome());
        assertEquals(1, db.clientOutcomeWrites);

        String rejected = "tx-rejected";
        db.open(rejected, line("A", "Apple", "1.25", 1));
        inventory.thenAnswer(call -> {
            db.reopen(call.key(), CheckoutAttemptStore.RejectionReason.INSUFFICIENT_STOCK, "A", foreignAt, false);
            return Reply.insufficient("A");
        });
        assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK,
                assertThrows(TransactionFailure.class, () -> orchestrator.complete(rejected)).code());
        assertEquals(CheckoutAttemptStore.ClientOutcome.REJECTED, db.attempt(rejected, 1).clientOutcome());

        String failing = "tx-unrecorded";
        db.open(failing, line("A", "Apple", "1.25", 1));
        inventory.thenAnswer(call -> {
            db.complete(call.key(), foreignAt, false);
            return Reply.deduct();
        });
        db.clientOutcomeFailure = new StoreFailure("database down");
        RuntimeException unrecorded = assertThrows(RuntimeException.class, () -> orchestrator.complete(failing));
        assertEquals(500, ApiErrorMapper.map(unrecorded).httpStatus(),
                "an unrecorded outcome answers 500, so the background-completion counts stay exact");
        assertEquals(CheckoutAttemptStore.ClientOutcome.NONE, db.attempt(failing, 1).clientOutcome());
    }

    @Test
    void failedPreconditionAndInvalidArgumentNeverReopenTheBasket() {
        // Inventory parks a mismatched or malformed request and answers nothing (FR-038): the request's
        // attempt is never settled on an assumed outcome.
        String id = "tx-parked";
        db.open(id, line("A", "Apple", "1.25", 1));
        inventory.then(Reply.ignore());
        timing.realWait = Duration.ofMillis(20);
        assertThrows(CheckoutPendingFailure.class, () -> orchestrator.complete(id));
        var attempt = db.attempt(id, 1);
        assertEquals(CheckoutAttemptStore.State.PENDING, attempt.state(), "an integrity fault is never a rejection");
        assertEquals(CheckoutAttemptStore.ClientOutcome.PENDING_ERROR, attempt.clientOutcome());
        assertEquals(1, db.row(id).active(), "the basket stays frozen for recovery");
        assertTrue(schedule.contains(new AttemptKey(id, 1)));
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
        assertEquals(1, db.outbox.ordered().size(), "404, 409 NOT_OPEN, and 409 EMPTY_BASKET record no event (FR-032)");

        String pending = "tx-pending";
        db.open(pending, line("A", "Apple", "1.25", 1));
        db.beginBehindTheCache(pending);
        timing.realWait = Duration.ofMillis(20);
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
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        inventory.thenAnswer(call -> {
            inside.countDown();
            try {
                assertTrue(release.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                throw new AssertionError(interrupted);
            }
            return Reply.deduct();
        });
        CheckoutOrchestrator realTime = new CheckoutOrchestrator(db, db, new CheckoutEvents(names), signal, cache,
                registry, schedule, CheckoutOrchestrator.Settings.defaults());
        CompletableFuture<TransactionOperations.ReceiptView> owner = CompletableFuture.supplyAsync(() -> realTime.complete(ID));
        assertTrue(inside.await(10, TimeUnit.SECONDS));
        Thread[] duplicateThread = new Thread[1];
        CompletableFuture<TransactionFailure.Code> duplicate = CompletableFuture.supplyAsync(() -> {
            duplicateThread[0] = Thread.currentThread();
            return assertThrows(TransactionFailure.class, () -> realTime.complete(ID)).code();
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (duplicateThread[0] == null || registry.waiting() == 0) {
            assertTrue(System.nanoTime() < deadline, "the duplicate never started waiting");
            Thread.sleep(5);
        }
        Thread.sleep(1_500);
        assertEquals(0, db.findsBy(duplicateThread[0]), "no database check while this process's live request owns the attempt");
        release.countDown();
        assertEquals(1, owner.get(10, TimeUnit.SECONDS).itemCount());
        assertEquals(TransactionFailure.Code.NOT_OPEN, duplicate.get(10, TimeUnit.SECONDS),
                "the restarted duplicate finds the transaction COMPLETED");
        assertEquals(0, db.findsBy(duplicateThread[0]), "the settlement signal woke the waiter");
        assertEquals(0, registry.waiting(), "no local future is left behind");
        assertEquals(1, inventory.requests(), "FR-042: the duplicate never reached Inventory");
    }

    @Test
    void aWaiterSeesAnotherProcesssSettlementThroughItsBackstopAndBeginsTheNextAttempt() {
        db.open(ID, line("A", "Apple", "1.25", 1));
        AttemptKey foreign = db.beginBehindTheCache(ID);
        timing.realWait = Duration.ofMillis(20);
        AtomicBoolean reopened = new AtomicBoolean();
        timing.onAwait = () -> {
            if (reopened.get()) {
                // The own attempt that follows is settled by the real consumer: give it real time.
                timing.realWait = Duration.ofSeconds(5);
            } else if (timing.nanos >= Duration.ofSeconds(2).toNanos()) {
                db.reopen(foreign, CheckoutAttemptStore.RejectionReason.INSUFFICIENT_STOCK, "A", timing.now(), false);
                reopened.set(true);
            }
        };
        int findsBefore = db.finds;
        var receipt = orchestrator.complete(ID);
        assertEquals(1, receipt.itemCount());
        assertEquals(2, inventory.calls().get(0).key().attemptNumber(), "the reopened basket begins attempt 2");
        assertEquals(3, db.finds - findsBefore, "one backstop check per second until the other process settled");
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

    /** Records each signal and its thread before handing it to the real dispatcher. */
    private static final class RecordingSignal implements OutboxSignal {
        final OutboxDispatcher dispatcher;
        final List<List<String>> signals = new CopyOnWriteArrayList<>();
        final List<Thread> threads = new CopyOnWriteArrayList<>();

        RecordingSignal(OutboxDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        public void signal(List<String> eventIds) {
            signals.add(List.copyOf(eventIds));
            threads.add(Thread.currentThread());
            dispatcher.signal(eventIds);
        }

        @Override
        public void resend(String eventId) {
            dispatcher.resend(eventId);
        }
    }

    /**
     * Service time that moves only on sleeps, explicit advances, and unsettled waits. A wait gives the real
     * consumer up to {@link #realWait} of wall time to settle the future; when it does not, the whole fake
     * slice passes at once.
     */
    private static final class FakeTiming implements CheckoutOrchestrator.Timing {
        volatile long nanos;
        final List<Duration> sleeps = new CopyOnWriteArrayList<>();
        volatile Duration realWait = Duration.ofSeconds(5);
        /** Runs before every wait slice, for example to settle an attempt "elsewhere". */
        volatile Runnable onAwait = () -> { };

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

        @Override
        public <T> T await(CompletableFuture<T> future, Duration timeout) throws InterruptedException {
            onAwait.run();
            try {
                return future.get(realWait.toNanos(), TimeUnit.NANOSECONDS);
            } catch (TimeoutException | ExecutionException notSettled) {
                nanos += timeout.toNanos();
                return null;
            }
        }

        void advance(Duration duration) {
            nanos += duration.toNanos();
        }
    }
}
