package transaction;

import api.json.Json;
import database.CheckoutAttemptStore;
import database.CheckoutAttemptStore.AttemptKey;
import database.CheckoutAttemptStore.AttemptRecord;
import database.CheckoutAttemptStore.ClientOutcome;
import database.CheckoutAttemptStore.RejectionReason;
import database.CheckoutAttemptStore.WriteResult;
import database.StoreFailure;
import database.TransactionStore;
import events.OutboxRow;
import events.OutboxSignal;
import faults.FaultHooks;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;
import time.ResponseBudget;
import transaction.SettlementRegistry.Settlement;
import transaction.TransactionOperations.ReceiptLineView;
import transaction.TransactionOperations.ReceiptView;
import transaction.TransactionOperations.TransactionStatus;
import transaction.TransactionStateCache.State;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

/**
 * Runs {@code POST /transactions/{id}/complete} as the request side of the choreographed checkout Saga
 * (D7; contracts/checkout-saga.md):
 *
 * <ol>
 *   <li><b>Pre-checks and T1</b>, under the transaction's stripe: 404, then 409 TRANSACTION_NOT_OPEN, then a
 *       pending attempt, then 409 EMPTY_BASKET, each confirmed against the database; then the guarded begin
 *       of attempt {@code n}, which also records its {@code CheckoutRequested} outbox row (FR-010). The
 *       request then registers as the attempt's responder and releases the stripe.</li>
 *   <li><b>Wait</b>: after the commit it hits {@code checkout.afterBegin}, signals the dispatcher, and waits
 *       on the attempt's settlement future for the rest of the {@code CHECKOUT_RESPONSE_BUDGET_MS} (8,000)
 *       budget, holding no lock, connection, or transaction (FR-040). The request never calls the broker or
 *       Inventory; {@link CheckoutOutcomeHandler} settles the attempt when Inventory's outcome arrives.</li>
 *   <li><b>Answer</b>: a settlement the handler claimed gives the 200 receipt from the cached snapshot and the
 *       supplied completion time, 409 INSUFFICIENT_STOCK naming the SKU, or 500 for INVALID_REQUEST and
 *       STOCK_RESTORED. Once per {@code CHECKOUT_WAIT_BACKSTOP_MS} (1,000) the request reads its attempt, so a
 *       settlement made by another Transaction process is answered too (D11).</li>
 * </ol>
 *
 * When the budget ends first and the request wins the claim, it commits {@code client_outcome =
 * PENDING_ERROR} with {@code next_retry_at = now}, queues the attempt in {@link RecoverySchedule}, and
 * answers 500 through {@link CheckoutPendingFailure}. A 500 is never a cancellation (FR-024).
 *
 * <p>A Scan or Complete that finds another attempt pending waits for its settlement within its own budget
 * ({@link #waitFor}, {@link #awaitSettlement}; FR-042), then restarts from the beginning. Every process-local
 * structure here is an optimization: the database guards and Inventory's idempotency decide every outcome
 * (Implementation Rule 1).</p>
 */
public final class CheckoutOrchestrator {
    public static final Duration DEFAULT_RESPONSE_BUDGET = Duration.ofMillis(8_000);
    public static final Duration DEFAULT_WAIT_BACKSTOP = Duration.ofMillis(1_000);
    public static final Duration DEFAULT_REPUBLISH_AFTER = Duration.ofMillis(2_000);
    /** Reloads after T1 guard misses before a Complete gives up with a 500. */
    private static final int MAX_GUARD_RELOADS = 3;
    private static final LatencyHistogram T1 = PerfMetrics.timer("transaction.complete.t1");
    private static final LatencyHistogram OUTCOME_WAIT = PerfMetrics.timer("transaction.complete.outcomeWait");
    private static final LongAdder BUDGET_EXPIRED = PerfMetrics.counter("transaction.complete.budgetExpired");
    private static final LongAdder AMBIGUOUS = PerfMetrics.counter("transaction.complete.ambiguousCommit");
    private static final LongAdder FOREIGN = PerfMetrics.counter("transaction.complete.foreignSettlements");
    private static final LongAdder LATE_ANSWERS = PerfMetrics.counter("transaction.complete.answeredAfterBudget");
    private static final LatencyHistogram WAIT = PerfMetrics.timer("transaction.wait");
    private static final LongAdder WAIT_SIGNALLED = PerfMetrics.counter("transaction.wait.signalled");
    private static final LongAdder WAIT_DB_CHECKS = PerfMetrics.counter("transaction.wait.dbChecks");
    private static final LongAdder WAIT_TIMED_OUT = PerfMetrics.counter("transaction.wait.timedOut");

    private final TransactionStore transactions;
    private final CheckoutAttemptStore attempts;
    private final CheckoutEvents events;
    private final OutboxSignal signal;
    private final TransactionLocks locks;
    private final TransactionStateCache cache;
    private final SettlementRegistry registry;
    private final RecoverySchedule schedule;
    private final Settings settings;
    private final Timing timing;

    public CheckoutOrchestrator(TransactionStore transactions, CheckoutAttemptStore attempts, CheckoutEvents events,
                                OutboxSignal signal, TransactionStateCache cache, SettlementRegistry registry,
                                RecoverySchedule schedule, Settings settings) {
        this(transactions, attempts, events, signal, cache, registry, schedule, settings, Timing.SYSTEM);
    }

    public CheckoutOrchestrator(TransactionStore transactions, CheckoutAttemptStore attempts, CheckoutEvents events,
                                OutboxSignal signal, TransactionStateCache cache, SettlementRegistry registry,
                                RecoverySchedule schedule, Settings settings, Timing timing) {
        this.transactions = Objects.requireNonNull(transactions, "transactions");
        this.attempts = Objects.requireNonNull(attempts, "attempts");
        this.events = Objects.requireNonNull(events, "events");
        this.signal = Objects.requireNonNull(signal, "signal");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.schedule = Objects.requireNonNull(schedule, "schedule");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.timing = Objects.requireNonNull(timing, "timing");
        this.locks = cache.locks();
        if (registry.locks() != locks) throw new IllegalArgumentException("The registry must check the cache's locks");
    }

    /**
     * The response budget, the database backstop of every wait (R7, D11), and the first republish delay that
     * T1 stores as {@code next_retry_at} (D7).
     */
    public record Settings(Duration responseBudget, Duration waitBackstop, Duration republishAfter) {
        public Settings {
            requirePositive(responseBudget, "responseBudget");
            requirePositive(waitBackstop, "waitBackstop");
            requirePositive(republishAfter, "republishAfter");
        }

        public static Settings defaults() {
            return new Settings(DEFAULT_RESPONSE_BUDGET, DEFAULT_WAIT_BACKSTOP, DEFAULT_REPUBLISH_AFTER);
        }

        /**
         * {@code CHECKOUT_RESPONSE_BUDGET_MS} (8,000), {@code CHECKOUT_WAIT_BACKSTOP_MS} (1,000), and
         * {@code CHECKOUT_REPUBLISH_AFTER_MS} (2,000).
         */
        public static Settings fromEnvironment(Map<String, String> environment) {
            return new Settings(millis(environment, "CHECKOUT_RESPONSE_BUDGET_MS", DEFAULT_RESPONSE_BUDGET),
                    millis(environment, "CHECKOUT_WAIT_BACKSTOP_MS", DEFAULT_WAIT_BACKSTOP),
                    millis(environment, "CHECKOUT_REPUBLISH_AFTER_MS", DEFAULT_REPUBLISH_AFTER));
        }

        public Settings withResponseBudget(Duration budget) {
            return new Settings(budget, waitBackstop, republishAfter);
        }

        public Settings withWaitBackstop(Duration backstop) {
            return new Settings(responseBudget, backstop, republishAfter);
        }

        static Duration millis(Map<String, String> environment, String key, Duration fallback) {
            String configured = environment.get(key);
            return configured == null || configured.isBlank() ? fallback
                    : Duration.ofMillis(Long.parseLong(configured.trim()));
        }

        private static void requirePositive(Duration duration, String field) {
            if (duration == null || duration.isZero() || duration.isNegative()) {
                throw new IllegalArgumentException(field + " must be positive");
            }
        }
    }

    /** The orchestrator's clocks and sleeps; tests substitute a controllable one. */
    public interface Timing {
        Timing SYSTEM = new Timing() {
            @Override
            public long nanoTime() {
                return System.nanoTime();
            }

            @Override
            public Instant now() {
                return Instant.now().truncatedTo(ChronoUnit.MILLIS);
            }

            @Override
            public void sleep(Duration duration) throws InterruptedException {
                Thread.sleep(duration);
            }
        };

        /** Monotonic time for the response budget. */
        long nanoTime();

        /** Service time for stored timestamps, at the database's millisecond precision. */
        Instant now();

        void sleep(Duration duration) throws InterruptedException;

        /** Waits up to {@code timeout} for {@code future}; null when it did not complete in time. */
        default <T> T await(CompletableFuture<T> future, Duration timeout) throws InterruptedException {
            try {
                return future.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            } catch (TimeoutException | ExecutionException notSettled) {
                return null;
            }
        }
    }

    public TransactionStateCache cache() {
        return cache;
    }

    public Settings settings() {
        return settings;
    }

    /** A request's response budget, started now on this orchestrator's clock (R6). */
    public ResponseBudget startBudget() {
        return ResponseBudget.start(settings.responseBudget(), timing::nanoTime);
    }

    public ReceiptView complete(String id) {
        Objects.requireNonNull(id, "id");
        ResponseBudget budget = startBudget();
        boolean restarted = false;
        while (true) {
            Start start = begin(id, restarted ? budget : null);
            if (start.pending() != null) {
                awaitSettlement(start.pending(), budget);
                restarted = true;
                continue;
            }
            Begun begun = start.begun();
            try {
                FaultHooks.hit("checkout.afterBegin");
                signal.signal(List.of(begun.requestEventId()));
                return awaitOutcome(begun, budget);
            } finally {
                registry.release(begun.key());
            }
        }
    }

    /** A committed T1: the attempt, the basket snapshot it froze, its request event, and its future. */
    private record Begun(AttemptKey key, State state, String requestEventId, CompletableFuture<Settlement> future) { }

    /** Either a committed T1, or another attempt this request must wait for first. */
    private record Start(Begun begun, PendingWait pending) { }

    /**
     * A pending attempt a request waits for: its key and its settlement future, taken under the stripe
     * so that no settlement in this process can be missed.
     */
    public record PendingWait(AttemptKey key, CompletableFuture<Settlement> future) {
        public PendingWait {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(future, "future");
        }
    }

    /**
     * Registers a wait for the pending attempt of {@code state}, which the caller confirmed against the
     * database under the transaction's stripe; the caller releases the stripe before waiting.
     */
    public PendingWait waitFor(State state) {
        if (!state.pending()) throw new IllegalArgumentException("Transaction " + state.transactionId() + " is not pending");
        AttemptKey key = new AttemptKey(state.transactionId(), state.activeAttemptNumber());
        return new PendingWait(key, registry.futureFor(key));
    }

    /**
     * Bounded waiting (FR-042, R7): waits within the request's remaining budget for the attempt to
     * settle, holding no lock or connection. A settlement in this process completes the future at once.
     * Without a signal, the database is read once per {@code waitBackstop} to catch settlements made
     * elsewhere, except while a live request of this process owns the attempt and will signal, and once
     * more at the bound; each check borrows a connection for one read. Returns once the attempt is
     * settled, so the caller restarts; at the bound, raises {@link CheckoutPendingFailure} with no write.
     */
    public void awaitSettlement(PendingWait wait, ResponseBudget budget) {
        Objects.requireNonNull(wait, "wait");
        Objects.requireNonNull(budget, "budget");
        AttemptKey key = wait.key();
        if (budget.expired()) {
            // A restart after the check at the bound found another attempt pending: the bound has passed.
            WAIT_TIMED_OUT.increment();
            throw new CheckoutPendingFailure(key.transactionId());
        }
        long started = System.nanoTime();
        try {
            while (true) {
                Duration slice = budget.nextDeadline(settings.waitBackstop());
                if (!slice.isZero()) {
                    Settlement settlement;
                    try {
                        settlement = timing.await(wait.future(), slice);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new CheckoutPendingFailure(key.transactionId());
                    }
                    if (settlement != null && settlement.kind() != SettlementRegistry.Kind.FAILED) {
                        WAIT_SIGNALLED.increment();
                        return;
                    }
                    if (settlement != null) wait = refreshed(wait);
                    if (!budget.expired() && registry.owned(key)) continue;
                }
                WAIT_DB_CHECKS.increment();
                Optional<AttemptRecord> attempt = attempts.findAttempt(key);
                if (attempt.isEmpty() || attempt.get().state() != CheckoutAttemptStore.State.PENDING) {
                    settledElsewhere(key, attempt);
                    return;
                }
                if (budget.expired()) {
                    WAIT_TIMED_OUT.increment();
                    throw new CheckoutPendingFailure(key.transactionId());
                }
            }
        } finally {
            WAIT.record(System.nanoTime() - started);
        }
    }

    /** A FAILED settlement left the attempt pending: wait on a fresh future, taken under the stripe. */
    private PendingWait refreshed(PendingWait wait) {
        try (var held = locks.acquire(wait.key().transactionId())) {
            return new PendingWait(wait.key(), registry.futureFor(wait.key()));
        }
    }

    /** A database check found the attempt settled: wake this process's other waiters and drop the future. */
    private Settlement settledElsewhere(AttemptKey key, Optional<AttemptRecord> attempt) {
        Settlement settlement = settlementOf(attempt);
        try (var held = locks.acquire(key.transactionId())) {
            cache.invalidate(key.transactionId());
            registry.complete(key, settlement);
        }
        return settlement;
    }

    private static Settlement settlementOf(Optional<AttemptRecord> attempt) {
        if (attempt.isEmpty()) return Settlement.reopened(null, null, true);
        AttemptRecord settled = attempt.get();
        boolean recorded = settled.clientOutcome() != ClientOutcome.NONE;
        return settled.state() == CheckoutAttemptStore.State.COMPLETED
                ? Settlement.completed(settled.settledAt(), recorded)
                : Settlement.reopened(settled.rejectionReason(), settled.rejectedSku(), recorded);
    }

    /** {@code restartBudget} is set after a wait: with nothing left of it, no new attempt is begun. */
    private Start begin(String id, ResponseBudget restartBudget) {
        long started = System.nanoTime();
        try (var held = locks.acquire(id)) {
            int guardMisses = 0;
            while (true) {
                boolean[] fresh = {false};
                Optional<State> found = cache.getOrLoad(id, key -> {
                    fresh[0] = true;
                    return TransactionStates.load(transactions, key);
                });
                RuntimeException rejection = rejection(id, found);
                if (rejection != null) {
                    if (!fresh[0]) {
                        cache.invalidate(id);
                        continue;
                    }
                    if (rejection instanceof CheckoutPendingFailure) return new Start(null, waitFor(found.orElseThrow()));
                    throw rejection;
                }
                if (restartBudget != null && restartBudget.expired()) throw new CheckoutPendingFailure(id);
                State state = found.orElseThrow();
                AttemptKey key = new AttemptKey(id, state.attemptCount() + 1);
                List<CheckoutEvents.Line> lines = lines(state);
                Instant createdAt = timing.now();
                OutboxRow request = events.request(key, lines, createdAt);
                WriteResult result;
                try {
                    result = attempts.begin(new CheckoutAttemptStore.Begin(key, state.itemCount(), state.totalAmount(),
                            canonical(lines), createdAt, createdAt.plus(settings.republishAfter()), request));
                } catch (RuntimeException failure) {
                    // The attempt may exist: reload before any further mutation, and let recovery republish or drop it.
                    cache.invalidate(id);
                    if (failure instanceof StoreFailure store && store.commitOutcomeUnknown()) {
                        AMBIGUOUS.increment();
                        schedule.reconcile(key);
                    }
                    throw failure;
                }
                if (result == WriteResult.OK) {
                    cache.put(state.withActiveAttempt(key.attemptNumber()));
                    CompletableFuture<Settlement> future = registry.register(key);
                    return new Start(new Begun(key, state, request.eventId(), future), null);
                }
                cache.recordGuardMiss();
                cache.invalidate(id);
                if (++guardMisses > MAX_GUARD_RELOADS) {
                    throw new IllegalStateException("Transaction " + id + " changed concurrently during "
                            + guardMisses + " checkout attempts");
                }
            }
        } finally {
            T1.record(System.nanoTime() - started);
        }
    }

    /** The established precedence (FR-032, FR-042); null when T1 may proceed. */
    private static RuntimeException rejection(String id, Optional<State> found) {
        if (found.isEmpty()) return new TransactionFailure(TransactionFailure.Code.NOT_FOUND, "Transaction not found: " + id);
        State state = found.get();
        if (state.status() != TransactionStatus.OPEN) {
            return new TransactionFailure(TransactionFailure.Code.NOT_OPEN, "Transaction is not open: " + id);
        }
        if (state.pending()) return new CheckoutPendingFailure(id);
        if (state.itemCount() == 0) return new TransactionFailure(TransactionFailure.Code.EMPTY_BASKET, "Basket is empty: " + id);
        return null;
    }

    /**
     * Waits for the attempt's own settlement, holding nothing. The database is read once per backstop for a
     * settlement made by another process. At the budget's end the request claims TIMED_OUT and hands the
     * attempt over; if the handler claimed first, the request answers from its settlement however late.
     */
    private ReceiptView awaitOutcome(Begun begun, ResponseBudget budget) {
        AttemptKey key = begun.key();
        long started = System.nanoTime();
        try {
            while (true) {
                Duration slice = budget.nextDeadline(settings.waitBackstop());
                if (!slice.isZero()) {
                    Settlement settlement = await(begun.future(), slice);
                    if (settlement != null) return answer(begun, settlement);
                    if (!budget.expired()) {
                        Settlement elsewhere = settledByAnotherProcess(begun);
                        if (elsewhere != null) return answer(begun, elsewhere);
                        continue;
                    }
                }
                if (registry.claimTimeout(key)) throw handOver(key);
                // The handler claimed the response first; it answers even after the budget (D7).
                Settlement late = await(begun.future(), settings.responseBudget());
                if (late == null) throw new CheckoutPendingFailure(key.transactionId());
                LATE_ANSWERS.increment();
                return answer(begun, late);
            }
        } finally {
            OUTCOME_WAIT.record(System.nanoTime() - started);
        }
    }

    /**
     * The backstop read (D11): an attempt settled by another Transaction process. The request claims the
     * response itself, so this process's handler cannot also claim it, and wakes its other waiters.
     */
    private Settlement settledByAnotherProcess(Begun begun) {
        AttemptKey key = begun.key();
        Optional<AttemptRecord> attempt;
        try {
            attempt = attempts.findAttempt(key);
        } catch (StoreFailure unavailable) {
            return null;
        }
        if (attempt.isEmpty() || attempt.get().state() == CheckoutAttemptStore.State.PENDING) return null;
        if (!registry.claimForHandler(key)) return null;
        FOREIGN.increment();
        return settledElsewhere(key, attempt);
    }

    private ReceiptView answer(Begun begun, Settlement settlement) {
        AttemptKey key = begun.key();
        switch (settlement.kind()) {
            case COMPLETED -> {
                if (!settlement.outcomeRecorded()) attempts.recordClientOutcome(key, ClientOutcome.SUCCESS);
                Instant completedAt = settlement.completedAt() != null ? settlement.completedAt() : timing.now();
                return receipt(begun.state(), completedAt);
            }
            case REOPENED -> {
                if (!settlement.outcomeRecorded()) attempts.recordClientOutcome(key, ClientOutcome.REJECTED);
                throw refusal(key.transactionId(), settlement.reason(), settlement.rejectedSku());
            }
            default -> throw handOver(key);
        }
    }

    private Settlement await(CompletableFuture<Settlement> future, Duration timeout) {
        try {
            return timing.await(future, timeout);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * Gives the attempt to recovery: PENDING_ERROR and {@code next_retry_at = now} are committed before the
     * 500 when the database allows it, and the attempt is queued whether or not that write succeeded.
     */
    private CheckoutPendingFailure handOver(AttemptKey key) {
        BUDGET_EXPIRED.increment();
        Instant now = timing.now();
        try {
            attempts.markPendingError(key, now);
        } catch (RuntimeException failure) {
            System.err.println("[transaction] Attempt " + key + " could not record PENDING_ERROR: " + failure);
        } finally {
            schedule.enqueue(key, now);
        }
        return new CheckoutPendingFailure(key.transactionId());
    }

    private static RuntimeException refusal(String id, RejectionReason reason, String rejectedSku) {
        if (reason == RejectionReason.INSUFFICIENT_STOCK) {
            return new TransactionFailure(TransactionFailure.Code.INSUFFICIENT_STOCK,
                    "Insufficient stock for SKU: " + rejectedSku);
        }
        return new CheckoutRejectedFailure(id, String.valueOf(reason), rejectedSku);
    }

    private static ReceiptView receipt(State state, Instant completedAt) {
        List<ReceiptLineView> lines = new ArrayList<>(state.lines().size());
        for (TransactionStateCache.Line line : state.lines().values()) {
            lines.add(new ReceiptLineView(line.sku(), line.name(), line.unitPrice(), line.quantity()));
        }
        return new ReceiptView(state.transactionId(), state.stationId(), state.itemCount(), state.totalAmount(),
                state.startedAt(), completedAt, lines);
    }

    private static List<CheckoutEvents.Line> lines(State state) {
        return state.lines().values().stream()
                .map(line -> new CheckoutEvents.Line(line.sku(), line.quantity())).toList();
    }

    /** The canonical snapshot: {@code [{"sku":"…","quantity":n},…]}, sorted by SKU, no whitespace. */
    static String canonical(List<CheckoutEvents.Line> lines) {
        List<String> entries = new ArrayList<>(lines.size());
        for (CheckoutEvents.Line line : lines.stream().sorted(Comparator.comparing(CheckoutEvents.Line::sku)).toList()) {
            entries.add(Json.object("sku", Json.quote(line.sku()), "quantity", Json.number(line.quantity())));
        }
        return Json.array(entries);
    }

    /** Reads a stored canonical snapshot back. */
    static List<CheckoutEvents.Line> parseCanonical(String canonical) {
        List<CheckoutEvents.Line> lines = new ArrayList<>();
        for (Object entry : (List<?>) Json.parse(canonical)) {
            Map<String, Object> line = Json.asObject(entry);
            lines.add(new CheckoutEvents.Line(Json.getString(line, "sku"), Json.getInt(line, "quantity")));
        }
        return lines;
    }
}
