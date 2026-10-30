package unit.transaction;

import database.CheckoutAttemptStore;
import database.CheckoutAttemptStore.AttemptKey;
import database.CheckoutAttemptStore.AttemptRecord;
import database.CheckoutAttemptStore.ClientOutcome;
import database.CheckoutAttemptStore.RejectionReason;
import database.CheckoutAttemptStore.State;
import database.CheckoutAttemptStore.WriteResult;
import events.OutboxSignal;
import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import transaction.CheckoutRecoveryWorker;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The republish worker with fake stores, a recording outbox signal, and a controllable clock (D7;
 * contracts/checkout-saga.md "Republish and recovery"; FR-037, FR-038, R7). The worker re-sends a pending
 * attempt's recorded {@code CheckoutRequested} and schedules the next republish; it never settles an
 * attempt, which only an outcome event does (see CheckoutOutcomeHandlerTest).
 */
class CheckoutRecoveryWorkerTest {
    private static final Instant BASE = Instant.parse("2026-10-21T12:00:00Z");
    private static final String LINES = "[{\"sku\":\"A\",\"quantity\":2},{\"sku\":\"B\",\"quantity\":1}]";

    private final MutableClock clock = new MutableClock(BASE);
    private final Db db = new Db();
    private final Signal signal = new Signal();
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
        assertEquals(List.of(db.attempt(handover).requestEventId()), signal.resends,
                "the attempt's recorded CheckoutRequested is re-sent, with its own event_id");
        AttemptRecord republished = db.attempt(handover);
        assertEquals(State.PENDING, republished.state(), "only an outcome event settles it");
        assertEquals(1, republished.retryCount());
        assertEquals(BASE.plusSeconds(2), republished.nextRetryAt(), "the next republish after 2 s");
        assertEquals(CheckoutRecoveryWorker.REPUBLISHED, republished.lastError());
        assertEquals(BASE.plusSeconds(2), schedule.dueAt(handover), "and in memory for the same time");
        assertEquals(ClientOutcome.PENDING_ERROR, republished.clientOutcome());

        AttemptKey later = db.pending("tx-later", 1, BASE, ClientOutcome.NONE, 0);
        schedule.remove(handover);
        schedule.enqueue(later, BASE.plusSeconds(1));
        assertFalse(worker.runOnce(Duration.ZERO), "not due yet");
        clock.advance(Duration.ofSeconds(1));
        assertTrue(worker.runOnce(Duration.ZERO), "due once the clock reaches it");
        assertEquals(2, signal.resends.size());

        int writes = db.writes();
        int resends = signal.resends.size();
        schedule.reconcile(new AttemptKey("tx-never-began", 1));
        assertTrue(worker.runOnce(Duration.ZERO));
        assertEquals(writes, db.writes(), "a reconcile entry without an attempt is dropped with no write");
        assertEquals(resends, signal.resends.size(), "and nothing is re-sent");
    }

    @Test
    void anAttemptSettledElsewhereIsDroppedAndItsLocalWaitersAreWoken() throws Exception {
        AttemptKey key = db.pending("tx-elsewhere", 1, BASE, ClientOutcome.NONE, 0);
        warmPending(key);
        CompletableFuture<SettlementRegistry.Settlement> waiter = futureFor(key);
        db.completeElsewhere(key);
        schedule.reconcile(key);
        assertTrue(worker.runOnce(Duration.ZERO));
        assertTrue(signal.resends.isEmpty(), "a settled attempt is never republished");
        assertEquals(SettlementRegistry.Kind.COMPLETED, waiter.getNow(null).kind(), "its waiter wakes");
        assertEquals(0, registry.waiting());
        try (var held = locks.acquire(key.transactionId())) {
            assertFalse(cache.contains(key.transactionId()), "the stale pending entry is dropped");
        }
    }

    @Test
    void aSettlementByAnotherProcessDuringTheReplayEndsWithoutError() throws Exception {
        AttemptKey applied = db.pending("tx-raced-applied", 1, BASE, ClientOutcome.NONE, 0);
        signal.onResend = eventId -> db.completeElsewhere(applied);
        schedule.enqueue(applied, BASE);
        assertTrue(worker.runOnce(Duration.ZERO), "the republish races an outcome settled elsewhere");
        assertEquals(State.COMPLETED, db.attempt(applied).state());
        assertEquals(0, db.attempt(applied).retryCount(), "the retry bookkeeping applies only to a PENDING attempt");

        AttemptKey rejected = db.pending("tx-raced-rejected", 1, BASE, ClientOutcome.NONE, 0);
        signal.onResend = eventId -> db.reopenElsewhere(rejected);
        schedule.enqueue(rejected, BASE);
        assertTrue(worker.runOnce(Duration.ZERO));
        assertEquals(State.REJECTED, db.attempt(rejected).state());
        clock.advance(Duration.ofSeconds(5));
        signal.onResend = eventId -> { };
        int resends = signal.resends.size();
        while (worker.runOnce(Duration.ZERO)) {
            // drain the due entries
        }
        assertEquals(resends, signal.resends.size(), "the next turn finds both settled and drops them");
        assertEquals(0, schedule.size(), "nothing is re-enqueued after a settlement");
    }

    @Test
    void theStartupScanResumesEveryPendingAttemptInPagesRegardlessOfItsRetryTime() {
        CheckoutRecoveryWorker paged = worker(new CheckoutRecoveryWorker.Settings(Duration.ofSeconds(30),
                Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(60), Duration.ofSeconds(30), 2));
        List<AttemptKey> keys = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            keys.add(db.pending("tx-live-" + i, 1, BASE.plusSeconds(2 + i), ClientOutcome.NONE, 0));
        }
        AttemptKey owned = db.pending("tx-owned", 1, BASE.plusSeconds(2), ClientOutcome.NONE, 0);
        register(owned);
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
        AttemptKey live = db.pending("tx-live", 1, BASE.plusSeconds(1), ClientOutcome.NONE, 0);
        AttemptKey owned = db.pending("tx-owned", 1, BASE.minusSeconds(1), ClientOutcome.NONE, 0);
        AttemptKey scheduled = db.pending("tx-scheduled", 1, BASE.minusSeconds(1), ClientOutcome.NONE, 0);
        register(owned);
        schedule.enqueue(scheduled, BASE.plusSeconds(3));
        db.calls.clear();
        long queries = PerfMetrics.counterValue("transaction.recovery.scanQueries");

        assertEquals(1, worker.safetyScan());
        assertEquals(List.of("findPending"), db.calls, "one bounded, indexed page");
        assertEquals(queries + 1, PerfMetrics.counterValue("transaction.recovery.scanQueries"));
        assertEquals(BASE, schedule.dueAt(due));
        assertFalse(schedule.contains(live), "an attempt is not due before its first republish time");
        assertFalse(schedule.contains(owned), "a live request here owns it");
        assertEquals(BASE.plusSeconds(3), schedule.dueAt(scheduled), "already scheduled: left as it was");

        Db idle = new Db();
        RecoverySchedule empty = new RecoverySchedule(clock);
        CheckoutRecoveryWorker running = new CheckoutRecoveryWorker(idle, signal, cache, registry, empty,
                new CheckoutRecoveryWorker.Settings(Duration.ofHours(1), Duration.ofSeconds(2), Duration.ofSeconds(5),
                        Duration.ofSeconds(60), Duration.ofSeconds(30), 100), clock);
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
    void theWorkerNeverSettlesAnAttemptItOnlyRepublishesIt() throws Exception {
        for (int i = 0; i < 3; i++) {
            AttemptKey key = db.pending("tx-pending-" + i, 1, BASE, ClientOutcome.NONE, i);
            warmPending(key);
            CompletableFuture<SettlementRegistry.Settlement> waiter = futureFor(key);
            schedule.enqueue(key, BASE);
            assertTrue(worker.runOnce(Duration.ZERO));
            assertEquals(State.PENDING, db.attempt(key).state());
            assertEquals(1, db.active(key.transactionId()), "the basket stays frozen");
            assertFalse(waiter.isDone(), "no settlement was signalled");
        }
        assertFalse(db.calls.contains("complete") || db.calls.contains("reopen"), "neither T3 nor reopen ran");
        assertEquals(3, signal.resends.size());
        assertEquals(3, registry.waiting(), "every waiter still waits for an outcome event");
    }

    @Test
    void uncertainOutcomesBackOffDurablyAndInMemoryUpToTheFiveSecondCap() throws Exception {
        AttemptKey key = db.pending("tx-republish", 1, BASE, ClientOutcome.PENDING_ERROR, 1);
        schedule.enqueue(key, BASE);
        assertTrue(worker.runOnce(Duration.ZERO));
        AttemptRecord attempt = db.attempt(key);
        assertEquals(State.PENDING, attempt.state());
        assertEquals(2, attempt.retryCount(), "retry_count is incremented");
        assertEquals(BASE.plusSeconds(4), attempt.nextRetryAt(), "2 s x 2^1");
        assertEquals(CheckoutRecoveryWorker.REPUBLISHED, attempt.lastError());
        assertEquals(BASE.plusSeconds(4), schedule.dueAt(key), "re-enqueued in memory for the same time");

        AttemptKey capped = db.pending("tx-capped", 1, BASE, ClientOutcome.NONE, 6);
        signal.failure = new IllegalStateException("the dispatcher is stopping");
        schedule.enqueue(capped, BASE);
        assertTrue(worker.runOnce(Duration.ZERO));
        assertEquals(BASE.plusSeconds(5), db.attempt(capped).nextRetryAt(), "128 s is capped at 5 s");
        assertEquals(CheckoutRecoveryWorker.PUBLISH_FAILED, db.attempt(capped).lastError(),
                "a re-send that could not be queued is recorded, and retried at the next turn");
        assertEquals(List.of(Duration.ofSeconds(2), Duration.ofSeconds(4), Duration.ofSeconds(5),
                        Duration.ofSeconds(5), Duration.ofSeconds(5)),
                List.of(worker.backoff(0), worker.backoff(1), worker.backoff(2), worker.backoff(6), worker.backoff(100)),
                "from 2 s to the 5 s cap, with no retry limit");
        assertEquals(ClientOutcome.PENDING_ERROR, db.attempt(key).clientOutcome());
    }

    @Test
    void failedPreconditionAndInvalidArgumentKeepTheAttemptPendingAndRaiseAnAlarm() throws Exception {
        // An outcome that never comes, for example for a request parked in Inventory (FR-038): after
        // CHECKOUT_STUCK_ALARM_SECONDS the attempt is alarmed as STUCK, logged once per status interval, and
        // still republished; its state never changes.
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        AttemptKey young = db.pending("tx-young", 1, BASE, ClientOutcome.NONE, 0);
        AttemptKey stuck = db.pending("tx-stuck", 1, BASE, ClientOutcome.NONE, 0, BASE.minusSeconds(61));
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            schedule.enqueue(young, BASE);
            schedule.enqueue(stuck, BASE);
            assertTrue(worker.runOnce(Duration.ZERO));
            assertTrue(worker.runOnce(Duration.ZERO));
            schedule.remove(young);
            clock.advance(Duration.ofSeconds(5));
            assertTrue(worker.runOnce(Duration.ZERO), "the stuck attempt is republished again");
        } finally {
            System.setErr(original);
        }
        assertEquals(CheckoutRecoveryWorker.REPUBLISHED, db.attempt(young).lastError(), "younger than 60 s");
        AttemptRecord alarmed = db.attempt(stuck);
        assertEquals(CheckoutRecoveryWorker.STUCK, alarmed.lastError());
        assertEquals(State.PENDING, alarmed.state(), "an alarm never settles the attempt");
        assertNotNull(db.active(stuck.transactionId()), "the basket stays frozen");
        assertEquals(2, signal.resends.stream().filter(id -> id.equals(alarmed.requestEventId())).count(),
                "republished indefinitely");
        String log = captured.toString(StandardCharsets.UTF_8);
        assertTrue(log.contains("[checkout-alarm]") && log.contains(stuck.toString()) && log.contains("61 s"), log);
        assertEquals(1, log.lines().filter(line -> line.contains(stuck.toString())).count(),
                "logged once per status interval, not on every republish");
        assertFalse(log.contains(young.toString()), log);
    }

    @Test
    void recoveryNeverSetsTheClientOutcome() throws Exception {
        AttemptKey none = db.pending("tx-none", 1, BASE, ClientOutcome.NONE, 0);
        AttemptKey handedOver = db.pending("tx-handed-over", 1, BASE, ClientOutcome.PENDING_ERROR, 0);
        schedule.enqueue(none, BASE);
        assertTrue(worker.runOnce(Duration.ZERO));
        schedule.enqueue(handedOver, BASE);
        assertTrue(worker.runOnce(Duration.ZERO));
        assertEquals(ClientOutcome.NONE, db.attempt(none).clientOutcome(), "a completion without a recorded response");
        assertEquals(ClientOutcome.PENDING_ERROR, db.attempt(handedOver).clientOutcome());
        assertFalse(db.answered, "T3 and reopen were never told that a request is answering");
        assertEquals(0, db.clientOutcomeWrites);
    }

    private CheckoutRecoveryWorker worker(CheckoutRecoveryWorker.Settings settings) {
        return new CheckoutRecoveryWorker(db, signal, cache, registry, schedule, settings, clock);
    }

    private CompletableFuture<SettlementRegistry.Settlement> futureFor(AttemptKey key) {
        try (var held = locks.acquire(key.transactionId())) {
            return registry.futureFor(key);
        }
    }

    /** The attempt's own live request, waiting here. */
    private void register(AttemptKey key) {
        try (var held = locks.acquire(key.transactionId())) {
            registry.register(key);
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

    /** Records every re-send; {@link #failure} makes the next one throw, as a stopping dispatcher does. */
    private static final class Signal implements OutboxSignal {
        final List<String> resends = new CopyOnWriteArrayList<>();
        volatile Consumer<String> onResend = eventId -> { };
        volatile RuntimeException failure;

        @Override
        public void signal(List<String> eventIds) {
            throw new AssertionError("the worker never signals new rows");
        }

        @Override
        public void resend(String eventId) {
            RuntimeException next = failure;
            failure = null;
            if (next != null) throw next;
            resends.add(eventId);
            onResend.accept(eventId);
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
        boolean answered;
        int clientOutcomeWrites;

        synchronized AttemptKey pending(String id, int number, Instant nextRetryAt, ClientOutcome outcome, int retries) {
            return pending(id, number, nextRetryAt, outcome, retries, BASE.minusSeconds(10));
        }

        synchronized AttemptKey pending(String id, int number, Instant nextRetryAt, ClientOutcome outcome, int retries,
                                        Instant createdAt) {
            AttemptKey key = new AttemptKey(id, number);
            active.put(id, number);
            statuses.put(id, TransactionOperations.TransactionStatus.OPEN);
            attempts.put(key, new AttemptRecord(key, State.PENDING, null, null, LINES, 3, new BigDecimal("5.00"), outcome,
                    createdAt, null, retries, nextRetryAt, null, "request-" + id + "-" + number));
            return key;
        }

        synchronized AttemptRecord attempt(AttemptKey key) {
            return attempts.get(key);
        }

        synchronized Integer active(String id) {
            return active.get(id);
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
                    a.clientOutcome(), a.createdAt(), clock.instant(), a.retryCount(), a.nextRetryAt(), a.lastError(),
                    a.requestEventId()));
        }

        @Override
        public WriteResult begin(Begin begin) {
            throw new UnsupportedOperationException("recovery never begins an attempt");
        }

        @Override
        public synchronized WriteResult complete(AttemptKey key, Instant completedAt, boolean answering) {
            calls.add("complete");
            answered |= answering;
            throw new AssertionError("recovery never settles an attempt");
        }

        @Override
        public synchronized WriteResult reopen(AttemptKey key, RejectionReason reason, String rejectedSku,
                                               Instant settledAt, boolean answering) {
            calls.add("reopen");
            answered |= answering;
            throw new AssertionError("recovery never settles an attempt");
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
                            null, "[]", 1, BigDecimal.ONE, ClientOutcome.NONE, BASE, null, 0, after.nextRetryAt(), null,
                            null)) > 0)
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
                    a.clientOutcome(), a.createdAt(), null, a.retryCount() + 1, nextRetryAt, lastError, a.requestEventId()));
        }

        @Override
        public synchronized boolean recordError(AttemptKey key, String lastError) {
            calls.add("recordError");
            return attempts.containsKey(key);
        }
    }
}
