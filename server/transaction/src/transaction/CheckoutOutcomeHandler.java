package transaction;

import database.CheckoutAttemptStore;
import database.CheckoutAttemptStore.AttemptKey;
import database.CheckoutAttemptStore.AttemptRecord;
import database.CheckoutAttemptStore.ClientOutcome;
import database.CheckoutAttemptStore.RejectionReason;
import database.CheckoutAttemptStore.SaleSource;
import database.CheckoutAttemptStore.WriteResult;
import database.StoreFailure;
import events.DecodedEvent;
import events.EventHandler;
import events.OutboxRow;
import events.OutboxSignal;
import events.ParkReason;
import events.PermanentEventFailure;
import events.TransientEventFailure;
import faults.FaultHooks;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;
import transaction.SettlementRegistry.Kind;
import transaction.SettlementRegistry.Settlement;
import transaction.TransactionStateCache.State;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

/**
 * Settles checkout attempts from Inventory's outcome events (D7; contracts/checkout-saga.md "Outcome
 * handler rules"). Only an outcome event settles an attempt.
 *
 * <ul>
 *   <li>{@code StockDeducted} runs T3, which also records the sale's {@code SaleCompleted} outbox row (007
 *       FR-002): its lines come from the cached state of the active attempt, or else from one read of the
 *       attempt and its transaction ({@link CheckoutAttemptStore#findSaleSource}); lines whose quantities do not
 *       add up to the attempt's {@code item_count} are an integrity fault. After the commit the row's event ID
 *       is signalled to the dispatcher. {@code StockRejected} and {@code StockDeductionRefused} reopen with
 *       the reason and SKU; the reserved {@code StockRestored} reopens with {@code STOCK_RESTORED};
 *       {@code RestorationNotNeeded} changes nothing.</li>
 *   <li>Each action runs under the transaction's stripe lock: it claims the waiting responder, if this
 *       process has one, and the guarded write records {@code client_outcome} exactly when the claim
 *       succeeded. The warm path is that guarded write alone: three statements and one commit for T3 (two
 *       for a reopen); the state cache is then updated and the settlement future completed.</li>
 *   <li>A guard miss reads the attempt: the same result already recorded is a no-op, and so is an outcome
 *       for an older attempt. A contradiction is an integrity fault (FR-038): {@code last_error} records
 *       {@code INTEGRITY:<reason>} in its own commit and the record is parked as {@link ParkReason#INTEGRITY};
 *       an unknown attempt is parked as {@link ParkReason#UNKNOWN_ATTEMPT}.</li>
 *   <li>A transient failure of the write is retried under the lock after 50, 100, and 200 ms (005 rule).
 *       When those retries run out, a claimed request answers 500 from a FAILED settlement and the handler
 *       throws {@link TransientEventFailure}, so the offset stays uncommitted and the event returns.</li>
 * </ul>
 *
 * <p>Test-only fault hooks: {@code checkout.outcome.beforeCommit} before the first write, and
 * {@code checkout.afterRecordCompletion} and then {@code transaction.sale.afterT3Commit} after the T3 commit,
 * before the dispatcher is signalled and the future completes.</p>
 */
public final class CheckoutOutcomeHandler implements EventHandler {
    public static final List<Duration> DEFAULT_RETRY_DELAYS = List.of(Duration.ofMillis(50), Duration.ofMillis(100),
            Duration.ofMillis(200));
    private static final LatencyHistogram T3 = PerfMetrics.timer("transaction.complete.t3");
    private static final LatencyHistogram REOPEN = PerfMetrics.timer("transaction.complete.reopen");
    private static final LongAdder NO_OPS = PerfMetrics.counter("transaction.outcome.noOps");
    private static final LongAdder CLAIMS = PerfMetrics.counter("transaction.outcome.claims");
    private static final LongAdder INTEGRITY = PerfMetrics.counter("transaction.outcome.integrityAlarms");

    private final CheckoutAttemptStore attempts;
    private final TransactionStateCache cache;
    private final TransactionLocks locks;
    private final SettlementRegistry registry;
    private final CheckoutOrchestrator.Timing timing;
    private final SaleEvents sales;
    private final OutboxSignal signal;
    private final List<Duration> retryDelays;

    public CheckoutOutcomeHandler(CheckoutAttemptStore attempts, TransactionStateCache cache, SettlementRegistry registry,
                                  CheckoutOrchestrator.Timing timing, SaleEvents sales, OutboxSignal signal) {
        this(attempts, cache, registry, timing, sales, signal, DEFAULT_RETRY_DELAYS);
    }

    public CheckoutOutcomeHandler(CheckoutAttemptStore attempts, TransactionStateCache cache, SettlementRegistry registry,
                                  CheckoutOrchestrator.Timing timing, SaleEvents sales, OutboxSignal signal,
                                  List<Duration> retryDelays) {
        this.attempts = Objects.requireNonNull(attempts, "attempts");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.timing = Objects.requireNonNull(timing, "timing");
        this.sales = Objects.requireNonNull(sales, "sales");
        this.signal = Objects.requireNonNull(signal, "signal");
        this.retryDelays = List.copyOf(retryDelays);
        this.locks = cache.locks();
        if (registry.locks() != locks) throw new IllegalArgumentException("The registry must check the cache's locks");
    }

    @Override
    public void handle(DecodedEvent event) {
        switch (CheckoutEvents.outcome(event)) {
            case CheckoutEvents.Deducted deducted -> complete(deducted.key());
            case CheckoutEvents.Rejected rejected -> reopen(rejected.key(), rejected.reason(), rejected.sku(), false);
            case CheckoutEvents.Refused refused -> reopen(refused.key(), RejectionReason.STOCK_RESTORED, null, false);
            case CheckoutEvents.Restored restored -> reopen(restored.key(), RejectionReason.STOCK_RESTORED, null, true);
            case CheckoutEvents.NotNeeded notNeeded -> NO_OPS.increment();
        }
    }

    /** T3 for {@code StockDeducted}: the pivot happened, so the attempt ends COMPLETED. */
    private void complete(AttemptKey key) {
        String id = key.transactionId();
        long started = System.nanoTime();
        try (var held = locks.acquire(id)) {
            State cached = cache.peek(id);
            Instant now = timing.now();
            Instant completedAt = cached != null && cached.startedAt() != null && now.isBefore(cached.startedAt())
                    ? cached.startedAt() : now;
            boolean claimed = claim(key);
            FaultHooks.hit("checkout.outcome.beforeCommit");
            OutboxRow sale = null;
            for (int retry = 0; ; retry++) {
                WriteResult result;
                try {
                    if (sale == null) {
                        Optional<OutboxRow> built = saleCompleted(key, cached, completedAt, claimed);
                        if (built.isEmpty()) {
                            settledBefore(key, claimed, Kind.COMPLETED, false);
                            return;
                        }
                        sale = built.get();
                    }
                    result = attempts.complete(key, completedAt, claimed, sale);
                } catch (StoreFailure failure) {
                    cache.invalidate(id);
                    if (pause(retry)) continue;
                    throw unsettled(key, claimed, "T3", failure);
                }
                if (result == WriteResult.OK) {
                    cache.evict(id);
                    FaultHooks.hit("checkout.afterRecordCompletion");
                    FaultHooks.hit("transaction.sale.afterT3Commit");
                    signal.signal(List.of(sale.eventId()));
                    registry.complete(key, Settlement.completed(completedAt, claimed));
                    return;
                }
                cache.recordGuardMiss();
                cache.invalidate(id);
                settledBefore(key, claimed, Kind.COMPLETED, false);
                return;
            }
        } finally {
            T3.record(System.nanoTime() - started);
        }
    }

    /**
     * Reopen for a rejection, a refusal, or (with {@code restoration}) the reserved {@code StockRestored}:
     * the active attempt is cleared and the basket is OPEN again, unchanged.
     */
    private void reopen(AttemptKey key, RejectionReason reason, String rejectedSku, boolean restoration) {
        String id = key.transactionId();
        long started = System.nanoTime();
        try (var held = locks.acquire(id)) {
            boolean claimed = claim(key);
            FaultHooks.hit("checkout.outcome.beforeCommit");
            for (int retry = 0; ; retry++) {
                WriteResult result;
                try {
                    result = attempts.reopen(key, reason, rejectedSku, timing.now(), claimed);
                } catch (StoreFailure failure) {
                    cache.invalidate(id);
                    if (pause(retry)) continue;
                    throw unsettled(key, claimed, "reopen", failure);
                }
                if (result == WriteResult.OK) {
                    State cached = cache.peek(id);
                    if (cached != null && Objects.equals(cached.activeAttemptNumber(), key.attemptNumber())) {
                        cache.put(cached.reopened());
                    } else {
                        cache.invalidate(id);
                    }
                    registry.complete(key, Settlement.reopened(reason, rejectedSku, claimed));
                    return;
                }
                cache.recordGuardMiss();
                cache.invalidate(id);
                settledBefore(key, claimed, Kind.REOPENED, restoration);
                return;
            }
        } finally {
            REOPEN.record(System.nanoTime() - started);
        }
    }

    /**
     * The sale's {@code SaleCompleted} row for attempt {@code key}: from the cached state when it holds this
     * attempt as active (the basket is frozen while it is pending, FR-035), otherwise from one read. Empty when
     * Transaction never recorded the attempt. Lines that do not add up to the attempt's unit count are an
     * integrity fault.
     */
    private Optional<OutboxRow> saleCompleted(AttemptKey key, State cached, Instant completedAt, boolean claimed) {
        List<CheckoutEvents.Line> lines;
        int itemCount;
        String stationId;
        if (cached != null && cached.stationId() != null && Objects.equals(cached.activeAttemptNumber(), key.attemptNumber())) {
            lines = cached.lines().values().stream().map(line -> new CheckoutEvents.Line(line.sku(), line.quantity())).toList();
            itemCount = cached.itemCount();
            stationId = cached.stationId();
        } else {
            Optional<SaleSource> source = attempts.findSaleSource(key);
            if (source.isEmpty()) return Optional.empty();
            lines = CheckoutOrchestrator.parseCanonical(source.get().lines());
            itemCount = source.get().itemCount();
            stationId = source.get().stationId();
        }
        int units = lines.stream().mapToInt(CheckoutEvents.Line::quantity).sum();
        if (lines.isEmpty() || units != itemCount) {
            if (claimed) registry.complete(key, Settlement.failed());
            throw integrity(ParkReason.INTEGRITY, key, "sale-lines-mismatch", "the attempt lines hold " + units
                    + " units but its item_count is " + itemCount);
        }
        try {
            return Optional.of(sales.saleCompleted(key.transactionId(), key.attemptNumber(), completedAt, stationId, lines));
        } catch (IllegalArgumentException invalid) {
            if (claimed) registry.complete(key, Settlement.failed());
            throw integrity(ParkReason.INTEGRITY, key, "sale-lines-invalid", invalid.getMessage());
        }
    }

    private boolean claim(AttemptKey key) {
        boolean claimed = registry.claimForHandler(key);
        if (claimed) CLAIMS.increment();
        return claimed;
    }

    /**
     * The guarded write missed: the attempt is not the transaction's active one. The caller holds the
     * stripe. The recorded state decides: the expected result means a no-op that still wakes this
     * process's waiters; a contradiction is an integrity fault; no attempt at all is unknown.
     */
    private void settledBefore(AttemptKey key, boolean claimed, Kind expected, boolean restoration) {
        Optional<AttemptRecord> found;
        try {
            found = attempts.findAttempt(key);
        } catch (StoreFailure failure) {
            throw unsettled(key, claimed, "settled-state read", failure);
        }
        if (found.isEmpty()) {
            if (claimed) registry.complete(key, Settlement.failed());
            throw integrity(ParkReason.UNKNOWN_ATTEMPT, key, null, "the outcome names an attempt Transaction never recorded");
        }
        AttemptRecord attempt = found.get();
        boolean recorded = attempt.clientOutcome() != ClientOutcome.NONE;
        switch (attempt.state()) {
            case COMPLETED -> {
                registry.complete(key, Settlement.completed(attempt.settledAt(), recorded));
                if (expected == Kind.COMPLETED) {
                    NO_OPS.increment();
                    return;
                }
                throw integrity(ParkReason.INTEGRITY, key, restoration ? "restored-after-completion"
                        : "rejection-after-completion", "a completed sale is never reversed");
            }
            case REJECTED -> {
                registry.complete(key, Settlement.reopened(attempt.rejectionReason(), attempt.rejectedSku(), recorded));
                if (expected == Kind.REOPENED) {
                    NO_OPS.increment();
                    return;
                }
                throw integrity(ParkReason.INTEGRITY, key, "deduction-after-reject",
                        "Inventory deducted stock for a rejected attempt");
            }
            case PENDING -> {
                if (claimed) registry.complete(key, Settlement.failed());
                throw integrity(ParkReason.INTEGRITY, key, "pending-not-active",
                        "the attempt is pending but not the transaction's active attempt");
            }
        }
    }

    /** Records {@code INTEGRITY:<reason>} in its own commit, raises the alarm, and returns the parking failure. */
    private PermanentEventFailure integrity(ParkReason park, AttemptKey key, String reason, String detail) {
        INTEGRITY.increment();
        String lastError = reason == null ? null : "INTEGRITY:" + reason;
        System.err.println("[checkout-alarm] ERROR " + park + " for attempt " + key
                + (lastError == null ? "" : " (" + lastError + ")") + ": " + detail);
        if (lastError != null) {
            try {
                attempts.recordError(key, lastError);
            } catch (StoreFailure failure) {
                throw new TransientEventFailure("Could not record " + lastError + " for attempt " + key, failure);
            }
        }
        return new PermanentEventFailure(park, "Attempt " + key + ": " + detail);
    }

    /** The write could not commit: a claimed request answers 500, and the event is retried (D6). */
    private TransientEventFailure unsettled(AttemptKey key, boolean claimed, String step, StoreFailure failure) {
        if (claimed) registry.complete(key, Settlement.failed());
        return new TransientEventFailure(step + " for attempt " + key + " did not commit", failure);
    }

    private boolean pause(int retry) {
        if (retry >= retryDelays.size()) return false;
        try {
            timing.sleep(retryDelays.get(retry));
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
