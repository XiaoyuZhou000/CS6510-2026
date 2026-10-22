package transaction;

import api.json.Json;
import database.CheckoutAttemptStore;
import database.CheckoutAttemptStore.AttemptKey;
import database.CheckoutAttemptStore.ClientOutcome;
import database.CheckoutAttemptStore.RejectionReason;
import database.CheckoutAttemptStore.WriteResult;
import database.StoreFailure;
import database.TransactionStore;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;
import rpc.ResponseBudget;
import transaction.SettlementRegistry.Settlement;
import transaction.TransactionOperations.ReceiptLineView;
import transaction.TransactionOperations.ReceiptView;
import transaction.TransactionOperations.TransactionStatus;
import transaction.TransactionStateCache.State;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

/**
 * Orchestrates {@code POST /transactions/{id}/complete} as a Saga with forward recovery
 * (R5, R6, R9, R19; contracts/checkout-saga.md):
 *
 * <ol>
 *   <li><b>Pre-checks and T1</b>, under the transaction's stripe: 404, then 409 TRANSACTION_NOT_OPEN, then a
 *       pending attempt, then 409 EMPTY_BASKET, each confirmed against the database; then the guarded
 *       begin of attempt {@code n}. The stripe is released before Inventory is called.</li>
 *   <li><b>T2</b>, {@code ApplyCheckout} under {@code (transaction_id, n)}, holding no lock, connection,
 *       or transaction. Uncertain statuses repeat the call with the same key after 50, 100, 200, and
 *       then every 400 ms while the response budget lasts.</li>
 *   <li><b>T3</b> on APPLIED, or <b>reopen</b> on a definitive rejection, under the stripe again; the
 *       receipt comes from the cached snapshot with no read.</li>
 * </ol>
 *
 * When the budget runs out with the attempt still pending, the request commits
 * {@code client_outcome = PENDING_ERROR}, queues the attempt in {@link RecoverySchedule}, and answers 500
 * through {@link CheckoutPendingFailure}. A 500 is never a cancellation (FR-017). Every process-local
 * structure here is an optimization: the database guards and Inventory's idempotency decide every
 * outcome, also when several instances or the recovery worker race (Implementation Rule 3).
 */
public final class CheckoutOrchestrator {
    public static final Duration DEFAULT_RESPONSE_BUDGET = Duration.ofMillis(8_000);
    public static final Duration DEFAULT_RPC_DEADLINE = Duration.ofMillis(2_000);
    public static final List<Duration> DEFAULT_RETRY_DELAYS = List.of(Duration.ofMillis(50), Duration.ofMillis(100),
            Duration.ofMillis(200), Duration.ofMillis(400));
    /** Reloads after T1 guard misses before a Complete gives up with a 500. */
    private static final int MAX_GUARD_RELOADS = 3;
    private static final LatencyHistogram T1 = PerfMetrics.timer("transaction.complete.t1");
    private static final LatencyHistogram T3 = PerfMetrics.timer("transaction.complete.t3");
    private static final LatencyHistogram REOPEN = PerfMetrics.timer("transaction.complete.reopen");
    private static final LongAdder BUDGET_EXPIRED = PerfMetrics.counter("transaction.complete.budgetExpired");
    private static final LongAdder AMBIGUOUS = PerfMetrics.counter("transaction.complete.ambiguousCommit");
    private static final LongAdder APPLY_RETRIES = PerfMetrics.counter("transaction.complete.applyRetries");
    private static final LongAdder ALARMS = PerfMetrics.counter("transaction.complete.integrityAlarms");
    private static final LongAdder FOREIGN = PerfMetrics.counter("transaction.complete.foreignSettlements");

    private final TransactionStore transactions;
    private final CheckoutAttemptStore attempts;
    private final InventoryGateway inventory;
    private final TransactionLocks locks;
    private final TransactionStateCache cache;
    private final SettlementRegistry registry;
    private final RecoverySchedule schedule;
    private final Settings settings;
    private final Timing timing;

    public CheckoutOrchestrator(TransactionStore transactions, CheckoutAttemptStore attempts, InventoryGateway inventory,
                                TransactionStateCache cache, SettlementRegistry registry, RecoverySchedule schedule,
                                Settings settings) {
        this(transactions, attempts, inventory, cache, registry, schedule, settings, Timing.SYSTEM);
    }

    public CheckoutOrchestrator(TransactionStore transactions, CheckoutAttemptStore attempts, InventoryGateway inventory,
                                TransactionStateCache cache, SettlementRegistry registry, RecoverySchedule schedule,
                                Settings settings, Timing timing) {
        this.transactions = Objects.requireNonNull(transactions, "transactions");
        this.attempts = Objects.requireNonNull(attempts, "attempts");
        this.inventory = Objects.requireNonNull(inventory, "inventory");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.schedule = Objects.requireNonNull(schedule, "schedule");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.timing = Objects.requireNonNull(timing, "timing");
        this.locks = cache.locks();
        if (registry.locks() != locks) throw new IllegalArgumentException("The registry must check the cache's locks");
    }

    /** The response budget, the per-call Inventory deadline, and the in-request retry delays (R6). */
    public record Settings(Duration responseBudget, Duration rpcDeadline, List<Duration> retryDelays) {
        public Settings {
            requirePositive(responseBudget, "responseBudget");
            requirePositive(rpcDeadline, "rpcDeadline");
            retryDelays = List.copyOf(retryDelays);
            if (retryDelays.isEmpty()) throw new IllegalArgumentException("retryDelays must not be empty");
            retryDelays.forEach(delay -> requirePositive(delay, "retryDelays"));
        }

        public static Settings defaults() {
            return new Settings(DEFAULT_RESPONSE_BUDGET, DEFAULT_RPC_DEADLINE, DEFAULT_RETRY_DELAYS);
        }

        /** {@code CHECKOUT_RESPONSE_BUDGET_MS} (8,000) and {@code INVENTORY_RPC_DEADLINE_MS} (2,000). */
        public static Settings fromEnvironment(Map<String, String> environment) {
            return new Settings(millis(environment, "CHECKOUT_RESPONSE_BUDGET_MS", DEFAULT_RESPONSE_BUDGET),
                    millis(environment, "INVENTORY_RPC_DEADLINE_MS", DEFAULT_RPC_DEADLINE), DEFAULT_RETRY_DELAYS);
        }

        private static Duration millis(Map<String, String> environment, String key, Duration fallback) {
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
    }

    public TransactionStateCache cache() {
        return cache;
    }

    public ReceiptView complete(String id) {
        Objects.requireNonNull(id, "id");
        ResponseBudget budget = ResponseBudget.start(settings.responseBudget(), timing::nanoTime);
        Begun begun = begin(id);
        try {
            return settle(begun, budget);
        } finally {
            registry.release(begun.key());
        }
    }

    /** A committed T1: the attempt, the basket snapshot it froze, and the lines sent to Inventory. */
    private record Begun(AttemptKey key, State state, List<InventoryGateway.CheckoutLine> lines) { }

    private Begun begin(String id) {
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
                    if (fresh[0]) throw rejection;
                    cache.invalidate(id);
                    continue;
                }
                State state = found.orElseThrow();
                AttemptKey key = new AttemptKey(id, state.attemptCount() + 1);
                List<InventoryGateway.CheckoutLine> lines = lines(state);
                Instant createdAt = timing.now();
                WriteResult result;
                try {
                    result = attempts.begin(new CheckoutAttemptStore.Begin(key, state.itemCount(), state.totalAmount(),
                            canonical(lines), createdAt, createdAt.plus(settings.responseBudget())));
                } catch (RuntimeException failure) {
                    // The attempt may exist: reload before any further mutation, and let recovery settle or drop it.
                    cache.invalidate(id);
                    if (failure instanceof StoreFailure store && store.commitOutcomeUnknown()) {
                        AMBIGUOUS.increment();
                        schedule.reconcile(key);
                    }
                    throw failure;
                }
                if (result == WriteResult.OK) {
                    cache.put(state.withActiveAttempt(key.attemptNumber()));
                    registry.own(key);
                    return new Begun(key, state, lines);
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

    /** The established precedence (FR-025, FR-035); null when T1 may proceed. */
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

    private ReceiptView settle(Begun begun, ResponseBudget budget) {
        AttemptKey key = begun.key();
        for (int retry = 0; ; retry++) {
            Duration deadline = budget.nextDeadline(settings.rpcDeadline());
            if (deadline.isZero()) throw handOver(key);
            InventoryGateway.ApplyResult result;
            try {
                result = inventory.applyCheckout(key, begun.lines(), deadline);
            } catch (RuntimeException unexpected) {
                result = new InventoryGateway.Uncertain(unexpected.getClass().getSimpleName());
            }
            switch (result) {
                case InventoryGateway.Applied applied -> {
                    return recordCompletion(begun, budget);
                }
                case InventoryGateway.Rejected rejected -> {
                    return reopen(begun, rejected.reason() == InventoryGateway.RejectionReason.INSUFFICIENT_STOCK
                            ? RejectionReason.INSUFFICIENT_STOCK : RejectionReason.INVALID_REQUEST, rejected.sku(), budget);
                }
                case InventoryGateway.StockRestored restored -> {
                    return reopen(begun, RejectionReason.STOCK_RESTORED, null, budget);
                }
                case InventoryGateway.PayloadMismatch mismatch -> throw alarm(key, "FAILED_PRECONDITION", mismatch.detail());
                case InventoryGateway.InvalidRequest invalid -> throw alarm(key, "INVALID_ARGUMENT", invalid.detail());
                case InventoryGateway.Uncertain uncertain -> {
                    if (!pause(budget, retry)) throw handOver(key);
                    APPLY_RETRIES.increment();
                }
            }
        }
    }

    /** T3: retried within the budget until it commits; it has no compensation (constitution v5.1.0, Principle V). */
    private ReceiptView recordCompletion(Begun begun, ResponseBudget budget) {
        AttemptKey key = begun.key();
        String id = key.transactionId();
        Instant completedAt = completionTime(begun.state());
        for (int retry = 0; ; retry++) {
            long started = System.nanoTime();
            try (var held = locks.acquire(id)) {
                WriteResult result = null;
                try {
                    result = attempts.complete(key, completedAt, true);
                } catch (RuntimeException failure) {
                    cache.invalidate(id);
                    if (failure instanceof StoreFailure store && store.commitOutcomeUnknown()) AMBIGUOUS.increment();
                }
                if (result == WriteResult.OK) {
                    cache.evict(id);
                    registry.settled(key, Settlement.COMPLETED);
                    return receipt(begun.state(), completedAt);
                }
                if (result == WriteResult.GUARD_MISS) {
                    cache.recordGuardMiss();
                    cache.invalidate(id);
                    return answerFromSettlement(begun);
                }
            } finally {
                T3.record(System.nanoTime() - started);
            }
            if (!pause(budget, retry)) throw handOver(key);
        }
    }

    /** Compensates T1 after a definitive rejection; the basket is OPEN again, unchanged. */
    private ReceiptView reopen(Begun begun, RejectionReason reason, String rejectedSku, ResponseBudget budget) {
        AttemptKey key = begun.key();
        String id = key.transactionId();
        Instant settledAt = timing.now();
        for (int retry = 0; ; retry++) {
            long started = System.nanoTime();
            try (var held = locks.acquire(id)) {
                WriteResult result = null;
                try {
                    result = attempts.reopen(key, reason, rejectedSku, settledAt, true);
                } catch (RuntimeException failure) {
                    cache.invalidate(id);
                    if (failure instanceof StoreFailure store && store.commitOutcomeUnknown()) AMBIGUOUS.increment();
                }
                if (result == WriteResult.OK) {
                    cache.put(begun.state().withActiveAttempt(key.attemptNumber()).reopened());
                    registry.settled(key, Settlement.REOPENED);
                    throw refusal(id, reason, rejectedSku);
                }
                if (result == WriteResult.GUARD_MISS) {
                    cache.recordGuardMiss();
                    cache.invalidate(id);
                    return answerFromSettlement(begun);
                }
            } finally {
                REOPEN.record(System.nanoTime() - started);
            }
            if (!pause(budget, retry)) throw handOver(key);
        }
    }

    /**
     * A T3 or reopen guard missed: the attempt was settled already, by this request's own commit whose
     * outcome was unknown, or by another actor such as a recovery worker. The request answers from the
     * settled attempt, first recording its own response when no outcome is recorded yet (R9); if that
     * write fails, the failure answers 500. The caller holds the stripe.
     */
    private ReceiptView answerFromSettlement(Begun begun) {
        AttemptKey key = begun.key();
        CheckoutAttemptStore.AttemptRecord settled = attempts.findAttempt(key).orElseThrow(
                () -> new IllegalStateException("Attempt " + key + " vanished while it was being settled"));
        switch (settled.state()) {
            case COMPLETED -> {
                claim(key, settled.clientOutcome(), ClientOutcome.SUCCESS);
                registry.settled(key, Settlement.COMPLETED);
                return receipt(begun.state(), settled.settledAt());
            }
            case REJECTED -> {
                claim(key, settled.clientOutcome(), ClientOutcome.REJECTED);
                registry.settled(key, Settlement.REOPENED);
                throw refusal(key.transactionId(), settled.rejectionReason(), settled.rejectedSku());
            }
            default -> throw new IllegalStateException("Attempt " + key + " is pending but no longer active");
        }
    }

    /** Records this request's response when the settler recorded none; an earlier write of our own is kept. */
    private void claim(AttemptKey key, ClientOutcome recorded, ClientOutcome answer) {
        if (recorded != ClientOutcome.NONE) return;
        FOREIGN.increment();
        attempts.recordClientOutcome(key, answer);
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

    /** FAILED_PRECONDITION and INVALID_ARGUMENT are faults, never rejections: the attempt stays pending. */
    private CheckoutPendingFailure alarm(AttemptKey key, String status, String detail) {
        ALARMS.increment();
        System.err.println("[transaction] ERROR Inventory answered " + status + " for attempt " + key + ": " + detail);
        return handOver(key);
    }

    /** Sleeps the next retry delay; false when the budget cannot cover the delay plus another try. */
    private boolean pause(ResponseBudget budget, int retry) {
        List<Duration> delays = settings.retryDelays();
        Duration delay = delays.get(Math.min(retry, delays.size() - 1));
        if (budget.remaining().compareTo(delay) <= 0) return false;
        try {
            timing.sleep(delay);
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static RuntimeException refusal(String id, RejectionReason reason, String rejectedSku) {
        if (reason == RejectionReason.INSUFFICIENT_STOCK) {
            return new TransactionFailure(TransactionFailure.Code.INSUFFICIENT_STOCK,
                    "Insufficient stock for SKU: " + rejectedSku);
        }
        return new CheckoutRejectedFailure(id, String.valueOf(reason), rejectedSku);
    }

    private Instant completionTime(State state) {
        Instant now = timing.now();
        return state.startedAt() != null && now.isBefore(state.startedAt()) ? state.startedAt() : now;
    }

    private static ReceiptView receipt(State state, Instant completedAt) {
        List<ReceiptLineView> lines = new ArrayList<>(state.lines().size());
        for (TransactionStateCache.Line line : state.lines().values()) {
            lines.add(new ReceiptLineView(line.sku(), line.name(), line.unitPrice(), line.quantity()));
        }
        return new ReceiptView(state.transactionId(), state.stationId(), state.itemCount(), state.totalAmount(),
                state.startedAt(), completedAt, lines);
    }

    private static List<InventoryGateway.CheckoutLine> lines(State state) {
        return state.lines().values().stream()
                .map(line -> new InventoryGateway.CheckoutLine(line.sku(), line.quantity())).toList();
    }

    /** The canonical snapshot: {@code [{"sku":"…","quantity":n},…]}, sorted by SKU, no whitespace. */
    static String canonical(List<InventoryGateway.CheckoutLine> lines) {
        List<String> entries = new ArrayList<>(lines.size());
        for (InventoryGateway.CheckoutLine line : lines.stream()
                .sorted(java.util.Comparator.comparing(InventoryGateway.CheckoutLine::sku)).toList()) {
            entries.add(Json.object("sku", Json.quote(line.sku()), "quantity", Json.number(line.quantity())));
        }
        return Json.array(entries);
    }

    /** Reads a stored canonical snapshot back, for the replays that recovery sends. */
    static List<InventoryGateway.CheckoutLine> parseCanonical(String canonical) {
        List<InventoryGateway.CheckoutLine> lines = new ArrayList<>();
        for (Object entry : (List<?>) Json.parse(canonical)) {
            Map<String, Object> line = Json.asObject(entry);
            lines.add(new InventoryGateway.CheckoutLine(Json.getString(line, "sku"), Json.getInt(line, "quantity")));
        }
        return lines;
    }
}
