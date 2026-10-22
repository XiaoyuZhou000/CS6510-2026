package unit.transaction;

import database.CheckoutAttemptStore;
import database.CheckoutAttemptStore.AttemptKey;
import database.CheckoutAttemptStore.AttemptRecord;
import database.CheckoutAttemptStore.ClientOutcome;
import database.CheckoutAttemptStore.RejectionReason;
import database.CheckoutAttemptStore.State;
import database.CheckoutAttemptStore.WriteResult;
import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import support.FakeInventoryGateway;
import transaction.CheckoutRecoveryWorker;
import transaction.InventoryGateway;
import transaction.RecoverySchedule;
import transaction.SettlementRegistry;
import transaction.TransactionLocks;
import transaction.TransactionOperations;
import transaction.TransactionStateCache;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The recovery worker with fake stores, a scripted Inventory, and a controllable clock
 * (contracts/checkout-saga.md "Recovery worker"; FR-030, FR-031, FR-039, R7).
 */
class CheckoutRecoveryWorkerTest {
    private static final Instant BASE = Instant.parse("2026-10-21T12:00:00Z");
    private static final String LINES = "[{\"sku\":\"A\",\"quantity\":2},{\"sku\":\"B\",\"quantity\":1}]";

    private final MutableClock clock = new MutableClock(BASE);
    private final Db db = new Db();
    private final FakeInventoryGateway inventory = FakeInventoryGateway.applying();
    private final TransactionLocks locks = new TransactionLocks();
    private final TransactionStateCache cache = new TransactionStateCache(locks, 100);
    private final SettlementRegistry registry = new SettlementRegistry(locks);
    private final RecoverySchedule schedule = new RecoverySchedule(clock);
    private final CheckoutRecoveryWorker worker = worker(CheckoutRecoveryWorker.Settings.defaults());

    @Test
    void handoverAndReconcileEntriesAreSettledAsSoonAsTheyAreDue() throws Exception {
        AttemptKey handover = db.pending("tx-handover", 1, BASE, ClientOutcome.PENDING_ERROR, 0);
        schedule.enqueue(handover, BASE);
        assertTrue(worker.runOnce(Duration.ZERO), "a handover is due at once");
        assertEquals(List.of(handover), inventory.calls().stream().map(FakeInventoryGateway.Call::key).toList(),
                "the stored attempt is replayed under its own key");
        assertEquals(List.of(new InventoryGateway.CheckoutLine("A", 2), new InventoryGateway.CheckoutLine("B", 1)),
                inventory.calls().get(0).lines(), "with its stored lines");
        assertEquals(State.COMPLETED, db.attempt(handover).state());
        assertEquals(TransactionOperations.TransactionStatus.COMPLETED, db.status("tx-handover"));
        assertEquals(ClientOutcome.PENDING_ERROR, db.attempt(handover).clientOutcome(), "a background completion");

        AttemptKey later = db.pending("tx-later", 1, BASE, ClientOutcome.NONE, 0);
        schedule.enqueue(later, BASE.plusSeconds(1));
        assertFalse(worker.runOnce(Duration.ZERO), "not due yet");
        clock.advance(Duration.ofSeconds(1));
        assertTrue(worker.runOnce(Duration.ZERO), "due once the clock reaches it");
        assertEquals(State.COMPLETED, db.attempt(later).state());

        int writes = db.writes();
        int calls = inventory.applyCalls();
        schedule.reconcile(new AttemptKey("tx-never-began", 1));
        assertTrue(worker.runOnce(Duration.ZERO));
        assertEquals(writes, db.writes(), "a reconcile entry without an attempt is dropped with no write");
        assertEquals(calls, inventory.applyCalls(), "and Inventory is not asked");
    }

    @Test
    void anAttemptSettledElsewhereIsDroppedAndItsLocalWaitersAreWoken() throws Exception {
        AttemptKey key = db.pending("tx-elsewhere", 1, BASE, ClientOutcome.NONE, 0);
        warmPending(key);
        CompletableFuture<SettlementRegistry.Settlement> waiter;
        try (var held = locks.acquire(key.transactionId())) {
            waiter = registry.futureFor(key);
        }
        db.completeElsewhere(key);
        schedule.reconcile(key);
        assertTrue(worker.runOnce(Duration.ZERO));
        assertEquals(0, inventory.applyCalls(), "a settled attempt is never replayed");
        assertEquals(SettlementRegistry.Settlement.COMPLETED, waiter.getNow(null), "its waiter wakes");
        assertEquals(0, registry.waiting());
        try (var held = locks.acquire(key.transactionId())) {
            assertFalse(cache.contains(key.transactionId()), "the stale pending entry is dropped");
        }
    }

    @Test
    void aSettlementByAnotherProcessDuringTheReplayEndsWithoutError() throws Exception {
        AttemptKey applied = db.pending("tx-raced-applied", 1, BASE, ClientOutcome.NONE, 0);
        inventory.thenAnswer(call -> {
            db.completeElsewhere(call.key());
            return new InventoryGateway.Applied(true);
        });
        schedule.enqueue(applied, BASE);
        assertTrue(worker.runOnce(Duration.ZERO), "the T3 guard misses, the worker reads the settled state");
        assertEquals(State.COMPLETED, db.attempt(applied).state());
        assertEquals(1, db.guardMisses);

        AttemptKey rejected = db.pending("tx-raced-rejected", 1, BASE, ClientOutcome.NONE, 0);
        inventory.thenAnswer(call -> {
            db.reopenElsewhere(call.key());
            return new InventoryGateway.Rejected(InventoryGateway.RejectionReason.INSUFFICIENT_STOCK, "A");
        });
        schedule.enqueue(rejected, BASE);
        assertTrue(worker.runOnce(Duration.ZERO));
        assertEquals(State.REJECTED, db.attempt(rejected).state());
        assertEquals(2, db.guardMisses, "the reopen guard missed too");
        assertEquals(0, schedule.size(), "nothing is re-enqueued after a settlement");
    }

    @Test
    void theStartupScanResumesEveryPendingAttemptInPagesRegardlessOfItsRetryTime() {
        CheckoutRecoveryWorker paged = worker(new CheckoutRecoveryWorker.Settings(Duration.ofSeconds(30),
                Duration.ofSeconds(2), 2));
        List<AttemptKey> keys = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            keys.add(db.pending("tx-live-" + i, 1, BASE.plusSeconds(8 + i), ClientOutcome.NONE, 0));
        }
        AttemptKey owned = db.pending("tx-owned", 1, BASE.plusSeconds(8), ClientOutcome.NONE, 0);
        registry.own(owned);
        db.calls.clear();

        assertEquals(5, paged.startupScan(), "nothing but this process's own live requests is owned after a restart");
        assertEquals(List.of("findPending", "findPending", "findPending", "findPending"), db.calls,
                "six pending rows in keyset-paginated pages of at most 2, until a page is not full");
        for (AttemptKey key : keys) assertEquals(BASE, schedule.dueAt(key), "each due now");
        assertFalse(schedule.contains(owned));
    }

    @Test
    void theSafetyScanEnqueuesOnlyDueUnownedUnscheduledAttemptsAndAnIdleScheduleCostsNoReads() throws Exception {
        AttemptKey due = db.pending("tx-due", 1, BASE.minusSeconds(1), ClientOutcome.PENDING_ERROR, 0);
        AttemptKey live = db.pending("tx-live", 1, BASE.plusSeconds(7), ClientOutcome.NONE, 0);
        AttemptKey owned = db.pending("tx-owned", 1, BASE.minusSeconds(1), ClientOutcome.NONE, 0);
        AttemptKey scheduled = db.pending("tx-scheduled", 1, BASE.minusSeconds(1), ClientOutcome.NONE, 0);
        registry.own(owned);
        schedule.enqueue(scheduled, BASE.plusSeconds(3));
        db.calls.clear();
        long queries = PerfMetrics.counterValue("transaction.recovery.scanQueries");

        assertEquals(1, worker.safetyScan());
        assertEquals(List.of("findPending"), db.calls, "one bounded, indexed page");
        assertEquals(queries + 1, PerfMetrics.counterValue("transaction.recovery.scanQueries"));
        assertEquals(BASE, schedule.dueAt(due));
        assertFalse(schedule.contains(live), "a live request's attempt is not due until its budget ends");
        assertFalse(schedule.contains(owned), "a live request here owns it");
        assertEquals(BASE.plusSeconds(3), schedule.dueAt(scheduled), "already scheduled: left as it was");

        Db idle = new Db();
        RecoverySchedule empty = new RecoverySchedule(clock);
        CheckoutRecoveryWorker running = new CheckoutRecoveryWorker(idle, inventory, cache, registry, empty,
                new CheckoutRecoveryWorker.Settings(Duration.ofHours(1), Duration.ofSeconds(2), 100), clock);
        long before = PerfMetrics.counterValue("transaction.recovery.scanQueries");
        running.start();
        try {
            Thread.sleep(400);
        } finally {
            running.close();
        }
        assertEquals(List.of("findPending"), idle.calls, "only the startup scan read anything");
        assertEquals(before + 1, PerfMetrics.counterValue("transaction.recovery.scanQueries"));
    }

    @Test
    void definitiveOutcomesSettleUnderTheStripeUpdateTheCacheAndWakeWaiters() throws Exception {
        AttemptKey applied = db.pending("tx-applied", 1, BASE, ClientOutcome.NONE, 0);
        warmPending(applied);
        CompletableFuture<SettlementRegistry.Settlement> completed = futureFor(applied);
        schedule.enqueue(applied, BASE);
        assertTrue(worker.runOnce(Duration.ZERO));
        assertEquals(SettlementRegistry.Settlement.COMPLETED, completed.getNow(null));
        try (var held = locks.acquire(applied.transactionId())) {
            assertFalse(cache.contains(applied.transactionId()), "a COMPLETED transaction leaves the cache");
        }

        for (InventoryGateway.ApplyResult refusal : List.of(
                new InventoryGateway.Rejected(InventoryGateway.RejectionReason.INSUFFICIENT_STOCK, "A"),
                new InventoryGateway.Rejected(InventoryGateway.RejectionReason.INVALID_REQUEST, "B"),
                new InventoryGateway.StockRestored())) {
            String id = "tx-" + refusal.getClass().getSimpleName() + "-" + db.attempts.size();
            AttemptKey key = db.pending(id, 1, BASE, ClientOutcome.NONE, 0);
            warmPending(key);
            CompletableFuture<SettlementRegistry.Settlement> reopened = futureFor(key);
            inventory.then(refusal);
            schedule.enqueue(key, BASE);
            assertTrue(worker.runOnce(Duration.ZERO));
            assertEquals(SettlementRegistry.Settlement.REOPENED, reopened.getNow(null));
            AttemptRecord attempt = db.attempt(key);
            assertEquals(State.REJECTED, attempt.state());
            assertEquals(refusal instanceof InventoryGateway.StockRestored ? RejectionReason.STOCK_RESTORED
                    : refusal instanceof InventoryGateway.Rejected r && r.sku().equals("A")
                    ? RejectionReason.INSUFFICIENT_STOCK : RejectionReason.INVALID_REQUEST, attempt.rejectionReason());
            assertNull(db.active(id), "the basket is OPEN again");
            try (var held = locks.acquire(id)) {
                assertFalse(cache.contains(id), "the next request reloads the OPEN state");
            }
        }
        assertTrue(db.settledUnderTheStripe, "every T3 and reopen ran under the transaction's stripe");
        assertEquals(0, registry.waiting());
    }

    @Test
    void uncertainOutcomesBackOffDurablyAndInMemoryUpToTheFiveSecondCap() throws Exception {
        AttemptKey key = db.pending("tx-uncertain", 1, BASE, ClientOutcome.PENDING_ERROR, 3);
        inventory.then(new InventoryGateway.Uncertain("UNAVAILABLE"));
        schedule.enqueue(key, BASE);
        assertTrue(worker.runOnce(Duration.ZERO));
        AttemptRecord attempt = db.attempt(key);
        assertEquals(State.PENDING, attempt.state());
        assertEquals(4, attempt.retryCount(), "retry_count is incremented");
        assertEquals(BASE.plusMillis(800), attempt.nextRetryAt(), "100 ms x 2^3");
        assertEquals("UNAVAILABLE", attempt.lastError());
        assertEquals(BASE.plusMillis(800), schedule.dueAt(key), "re-enqueued in memory for the same time");

        AttemptKey capped = db.pending("tx-capped", 1, BASE, ClientOutcome.NONE, 6);
        inventory.then(new InventoryGateway.Uncertain("DEADLINE_EXCEEDED"));
        schedule.enqueue(capped, BASE);
        assertTrue(worker.runOnce(Duration.ZERO));
        assertEquals(BASE.plusSeconds(5), db.attempt(capped).nextRetryAt(), "6.4 s is capped at 5 s");
        assertEquals(List.of(Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofMillis(400),
                        Duration.ofSeconds(5), Duration.ofSeconds(5)),
                List.of(CheckoutRecoveryWorker.backoff(0), CheckoutRecoveryWorker.backoff(1), CheckoutRecoveryWorker.backoff(2),
                        CheckoutRecoveryWorker.backoff(6), CheckoutRecoveryWorker.backoff(100)), "no retry limit");
        assertEquals(ClientOutcome.PENDING_ERROR, db.attempt(key).clientOutcome());
    }

    @Test
    void failedPreconditionAndInvalidArgumentKeepTheAttemptPendingAndRaiseAnAlarm() throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            for (InventoryGateway.ApplyResult fault : List.of(new InventoryGateway.PayloadMismatch("mismatch"),
                    new InventoryGateway.InvalidRequest("malformed"))) {
                AttemptKey key = db.pending("tx-" + fault.getClass().getSimpleName(), 1, BASE, ClientOutcome.NONE, 0);
                inventory.then(fault);
                schedule.enqueue(key, BASE);
                assertTrue(worker.runOnce(Duration.ZERO));
                AttemptRecord attempt = db.attempt(key);
                assertEquals(State.PENDING, attempt.state(), "an integrity fault is never a rejection");
                assertNotNull(db.active(key.transactionId()), "the basket stays frozen");
                assertEquals(BASE.plusSeconds(30), attempt.nextRetryAt(), "retried at the safety scan's cadence");
                assertFalse(schedule.contains(key), "the safety scan resumes it, so the log is not flooded");
            }
        } finally {
            System.setErr(original);
        }
        String log = captured.toString(StandardCharsets.UTF_8);
        assertTrue(log.contains("[checkout-alarm]") && log.contains("FAILED_PRECONDITION"), log);
        assertTrue(log.contains("INVALID_ARGUMENT"), log);
    }

    @Test
    void recoveryNeverSetsTheClientOutcome() throws Exception {
        AttemptKey none = db.pending("tx-none", 1, BASE, ClientOutcome.NONE, 0);
        AttemptKey rejected = db.pending("tx-rejected", 1, BASE, ClientOutcome.NONE, 0);
        inventory.then(new InventoryGateway.Applied(false))
                .then(new InventoryGateway.Rejected(InventoryGateway.RejectionReason.INSUFFICIENT_STOCK, "A"));
        schedule.enqueue(none, BASE);
        assertTrue(worker.runOnce(Duration.ZERO));
        schedule.enqueue(rejected, BASE);
        assertTrue(worker.runOnce(Duration.ZERO));
        assertEquals(ClientOutcome.NONE, db.attempt(none).clientOutcome(), "a completion without a recorded response");
        assertEquals(ClientOutcome.NONE, db.attempt(rejected).clientOutcome());
        assertFalse(db.answered, "T3 and reopen were never told that a request is answering");
        assertEquals(0, db.clientOutcomeWrites);
    }

    private CheckoutRecoveryWorker worker(CheckoutRecoveryWorker.Settings settings) {
        return new CheckoutRecoveryWorker(db, inventory, cache, registry, schedule, settings, clock);
    }

    private CompletableFuture<SettlementRegistry.Settlement> futureFor(AttemptKey key) {
        try (var held = locks.acquire(key.transactionId())) {
            return registry.futureFor(key);
        }
    }

    /** A cached entry showing the attempt pending, as the live request that began it left it. */
    private void warmPending(AttemptKey key) {
        try (var held = locks.acquire(key.transactionId())) {
            cache.put(new TransactionStateCache.State(key.transactionId(), TransactionOperations.TransactionStatus.OPEN,
                    3, new BigDecimal("5.00"), key.attemptNumber(), key.attemptNumber(),
                    Map.of("A", new TransactionStateCache.Line("A", "Apple", new BigDecimal("1.25"), 2),
                            "B", new TransactionStateCache.Line("B", "Bread", new BigDecimal("2.50"), 1))));
        }
    }

    private static final class MutableClock extends Clock {
        private volatile Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    /** Transaction rows and attempts behind the same guards as the real store; records each call. */
    private final class Db implements CheckoutAttemptStore {
        final Map<String, Integer> active = new HashMap<>();
        final Map<String, TransactionOperations.TransactionStatus> statuses = new HashMap<>();
        final Map<AttemptKey, AttemptRecord> attempts = new HashMap<>();
        final List<String> calls = new ArrayList<>();
        boolean settledUnderTheStripe = true;
        boolean answered;
        int guardMisses;
        int clientOutcomeWrites;

        synchronized AttemptKey pending(String id, int number, Instant nextRetryAt, ClientOutcome outcome, int retries) {
            AttemptKey key = new AttemptKey(id, number);
            active.put(id, number);
            statuses.put(id, TransactionOperations.TransactionStatus.OPEN);
            attempts.put(key, new AttemptRecord(key, State.PENDING, null, null, LINES, 3, new BigDecimal("5.00"), outcome,
                    BASE.minusSeconds(10), null, retries, nextRetryAt, null));
            return key;
        }

        synchronized AttemptRecord attempt(AttemptKey key) {
            return attempts.get(key);
        }

        synchronized Integer active(String id) {
            return active.get(id);
        }

        synchronized TransactionOperations.TransactionStatus status(String id) {
            return statuses.get(id);
        }

        synchronized int writes() {
            return (int) calls.stream().filter(call -> !call.startsWith("find")).count();
        }

        /** Another process's T3 for the attempt. */
        synchronized void completeElsewhere(AttemptKey key) {
            active.remove(key.transactionId());
            statuses.put(key.transactionId(), TransactionOperations.TransactionStatus.COMPLETED);
            settle(key, State.COMPLETED, null, null);
        }

        /** Another process's reopen of the attempt. */
        synchronized void reopenElsewhere(AttemptKey key) {
            active.remove(key.transactionId());
            settle(key, State.REJECTED, RejectionReason.INSUFFICIENT_STOCK, "A");
        }

        private void settle(AttemptKey key, State state, RejectionReason reason, String sku) {
            AttemptRecord a = attempts.get(key);
            attempts.put(key, new AttemptRecord(key, state, reason, sku, a.lines(), a.itemCount(), a.totalAmount(),
                    a.clientOutcome(), a.createdAt(), clock.instant(), a.retryCount(), a.nextRetryAt(), a.lastError()));
        }

        @Override
        public WriteResult begin(Begin begin) {
            throw new UnsupportedOperationException("recovery never begins an attempt");
        }

        @Override
        public synchronized WriteResult complete(AttemptKey key, Instant completedAt, boolean answering) {
            calls.add("complete");
            settledUnderTheStripe &= locks.heldByCurrentThread(key.transactionId());
            answered |= answering;
            if (!Integer.valueOf(key.attemptNumber()).equals(active.get(key.transactionId()))) {
                guardMisses++;
                return WriteResult.GUARD_MISS;
            }
            completeElsewhere(key);
            return WriteResult.OK;
        }

        @Override
        public synchronized WriteResult reopen(AttemptKey key, RejectionReason reason, String rejectedSku,
                                               Instant settledAt, boolean answering) {
            calls.add("reopen");
            settledUnderTheStripe &= locks.heldByCurrentThread(key.transactionId());
            answered |= answering;
            if (!Integer.valueOf(key.attemptNumber()).equals(active.get(key.transactionId()))) {
                guardMisses++;
                return WriteResult.GUARD_MISS;
            }
            active.remove(key.transactionId());
            settle(key, State.REJECTED, reason, rejectedSku);
            return WriteResult.OK;
        }

        @Override
        public synchronized boolean recordClientOutcome(AttemptKey key, ClientOutcome outcome) {
            calls.add("recordClientOutcome");
            clientOutcomeWrites++;
            return false;
        }

        @Override
        public boolean markPendingError(AttemptKey key, Instant now) {
            throw new UnsupportedOperationException("recovery never answers a client");
        }

        @Override
        public synchronized List<AttemptRecord> findPending(Instant dueBefore, PageCursor after, int limit) {
            calls.add("findPending");
            Comparator<AttemptRecord> order = Comparator.comparing(AttemptRecord::nextRetryAt)
                    .thenComparing(a -> a.key().transactionId()).thenComparingInt(a -> a.key().attemptNumber());
            return attempts.values().stream()
                    .filter(a -> a.state() == State.PENDING && !a.nextRetryAt().isAfter(dueBefore))
                    .filter(a -> after == null || order.compare(a, new AttemptRecord(after.key(), State.PENDING, null,
                            null, "[]", 1, BigDecimal.ONE, ClientOutcome.NONE, BASE, null, 0, after.nextRetryAt(), null)) > 0)
                    .sorted(order).limit(limit).toList();
        }

        @Override
        public synchronized Optional<AttemptRecord> findAttempt(AttemptKey key) {
            calls.add("findAttempt");
            return Optional.ofNullable(attempts.get(key));
        }

        @Override
        public synchronized void scheduleRetry(AttemptKey key, Instant nextRetryAt, String lastError) {
            calls.add("scheduleRetry");
            AttemptRecord a = attempts.get(key);
            if (a == null || a.state() != State.PENDING) return;
            attempts.put(key, new AttemptRecord(key, a.state(), null, null, a.lines(), a.itemCount(), a.totalAmount(),
                    a.clientOutcome(), a.createdAt(), null, a.retryCount() + 1, nextRetryAt, lastError));
        }
    }
}
