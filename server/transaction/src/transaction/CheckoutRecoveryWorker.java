package transaction;

import database.CheckoutAttemptStore;
import database.CheckoutAttemptStore.AttemptKey;
import database.CheckoutAttemptStore.AttemptRecord;
import database.CheckoutAttemptStore.RejectionReason;
import database.CheckoutAttemptStore.WriteResult;
import metrics.PerfMetrics;
import transaction.SettlementRegistry.Settlement;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

/**
 * Forward recovery of pending checkout attempts (FR-030, FR-031, R7; contracts/checkout-saga.md "Recovery
 * worker"). One thread takes due keys from the in-memory {@link RecoverySchedule}; it never polls the
 * database on a short interval.
 *
 * <ul>
 *   <li><b>Startup scan</b>: nothing is owned after a restart, so every PENDING attempt is enqueued, read
 *       in pages of at most 100 rows.</li>
 *   <li><b>Safety scan</b>: every {@code CHECKOUT_RECOVERY_SCAN_SECONDS} (30), one bounded, indexed page of
 *       due attempts, repeated only while pages are full, enqueuing attempts that no live request owns
 *       and that are not scheduled. It catches anything the schedule missed, such as another process's
 *       handover.</li>
 *   <li><b>Settlement</b>: the attempt is read by key first; an absent or settled attempt is dropped and
 *       any local waiter is woken. Otherwise {@code ApplyCheckout} is replayed with the stored lines under
 *       the same key, and T3 or reopen runs under the transaction's stripe, which then updates the cache
 *       and wakes waiters. Uncertain outcomes back off from 100 ms to a 5 s cap, durably and in memory,
 *       with no retry limit.</li>
 * </ul>
 *
 * <p>The schedule and the registry cover only this process: another instance may replay the same
 * attempt, and Inventory's idempotency and the guarded writes keep it to one effect (Implementation
 * Rule 3). The worker never sets {@code client_outcome}: an attempt with no recorded response keeps
 * {@code NONE} (FR-054).</p>
 */
public final class CheckoutRecoveryWorker implements AutoCloseable {
    public static final Duration DEFAULT_SCAN_INTERVAL = Duration.ofSeconds(30);
    public static final Duration BACKOFF_BASE = Duration.ofMillis(100);
    public static final Duration BACKOFF_CAP = Duration.ofSeconds(5);
    /** The delay before reading an attempt again after its own read failed. */
    private static final Duration READ_RETRY = Duration.ofSeconds(1);
    /** A due bound later than any stored {@code next_retry_at}, within the TIMESTAMP range. */
    private static final Instant STARTUP_HORIZON = Instant.parse("2038-01-01T00:00:00Z");
    private static final LongAdder PENDING = PerfMetrics.counter("transaction.recovery.pending");
    private static final LongAdder SETTLED = PerfMetrics.counter("transaction.recovery.settled");
    private static final LongAdder ALARMS = PerfMetrics.counter("transaction.recovery.alarms");
    private static final LongAdder SCAN_QUERIES = PerfMetrics.counter("transaction.recovery.scanQueries");
    private static final LongAdder SCAN_ROWS = PerfMetrics.counter("transaction.recovery.scanRows");
    private static final LongAdder SCHEDULED = PerfMetrics.counter("transaction.recovery.scheduled");
    private static final LongAdder RETRIES = PerfMetrics.counter("transaction.recovery.retries");
    private static final LongAdder DROPPED = PerfMetrics.counter("transaction.recovery.dropped");

    private final CheckoutAttemptStore attempts;
    private final InventoryGateway inventory;
    private final TransactionStateCache cache;
    private final TransactionLocks locks;
    private final SettlementRegistry registry;
    private final RecoverySchedule schedule;
    private final Settings settings;
    private final Clock clock;
    private volatile Thread thread;
    private volatile boolean running;

    public CheckoutRecoveryWorker(CheckoutAttemptStore attempts, InventoryGateway inventory, TransactionStateCache cache,
                                  SettlementRegistry registry, RecoverySchedule schedule, Settings settings, Clock clock) {
        this.attempts = Objects.requireNonNull(attempts, "attempts");
        this.inventory = Objects.requireNonNull(inventory, "inventory");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.schedule = Objects.requireNonNull(schedule, "schedule");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.locks = cache.locks();
        if (registry.locks() != locks) throw new IllegalArgumentException("The registry must check the cache's locks");
    }

    /** The safety-scan interval, the per-call Inventory deadline, and the scan page size. */
    public record Settings(Duration scanInterval, Duration rpcDeadline, int pageSize) {
        public Settings {
            if (scanInterval == null || scanInterval.isZero() || scanInterval.isNegative()) {
                throw new IllegalArgumentException("scanInterval must be positive");
            }
            if (rpcDeadline == null || rpcDeadline.isZero() || rpcDeadline.isNegative()) {
                throw new IllegalArgumentException("rpcDeadline must be positive");
            }
            if (pageSize < 1) throw new IllegalArgumentException("pageSize must be positive");
        }

        public static Settings defaults() {
            return new Settings(DEFAULT_SCAN_INTERVAL, CheckoutOrchestrator.DEFAULT_RPC_DEADLINE,
                    CheckoutAttemptStore.DEFAULT_PAGE_SIZE);
        }

        /** {@code CHECKOUT_RECOVERY_SCAN_SECONDS} (30) and {@code INVENTORY_RPC_DEADLINE_MS} (2,000). */
        public static Settings fromEnvironment(Map<String, String> environment) {
            String seconds = environment.get("CHECKOUT_RECOVERY_SCAN_SECONDS");
            Duration interval = seconds == null || seconds.isBlank() ? DEFAULT_SCAN_INTERVAL
                    : Duration.ofSeconds(Long.parseLong(seconds.trim()));
            return new Settings(interval, CheckoutOrchestrator.Settings.millis(environment, "INVENTORY_RPC_DEADLINE_MS",
                    CheckoutOrchestrator.DEFAULT_RPC_DEADLINE), CheckoutAttemptStore.DEFAULT_PAGE_SIZE);
        }
    }

    /** Starts the worker thread: the startup scan, then due keys and periodic safety scans. */
    public synchronized void start() {
        if (thread != null) throw new IllegalStateException("The recovery worker was started already");
        running = true;
        thread = Thread.ofPlatform().name("checkout-recovery").daemon(true).start(this::run);
    }

    /** Stops the worker thread, waiting briefly for its current attempt. */
    @Override
    public void close() {
        running = false;
        Thread current = thread;
        if (current == null) return;
        current.interrupt();
        try {
            current.join(Duration.ofSeconds(5));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        while (running && !startupScanSafely()) {
            if (!pause(READ_RETRY)) return;
        }
        long nextScan = System.nanoTime() + settings.scanInterval().toNanos();
        while (running) {
            long untilScan = nextScan - System.nanoTime();
            if (untilScan <= 0) {
                safetyScanSafely();
                nextScan = System.nanoTime() + settings.scanInterval().toNanos();
                continue;
            }
            AttemptKey key;
            try {
                key = schedule.take(Duration.ofNanos(untilScan));
            } catch (InterruptedException interrupted) {
                return;
            }
            if (key != null) recoverSafely(key);
        }
    }

    /** Takes the next key due within {@code timeout} and recovers it; false when none became due. */
    public boolean runOnce(Duration timeout) throws InterruptedException {
        AttemptKey key = schedule.take(timeout);
        if (key == null) return false;
        recover(key);
        return true;
    }

    /** Enqueues every PENDING attempt that no live request here owns, due now; returns how many. */
    public int startupScan() {
        Instant now = now();
        int enqueued = 0;
        CheckoutAttemptStore.PageCursor after = null;
        while (true) {
            List<AttemptRecord> page = page(STARTUP_HORIZON, after);
            for (AttemptRecord attempt : page) {
                if (registry.owned(attempt.key())) continue;
                schedule.enqueue(attempt.key(), now);
                enqueued++;
            }
            if (page.size() < settings.pageSize()) break;
            after = page.get(page.size() - 1).cursor();
        }
        SCHEDULED.add(enqueued);
        return enqueued;
    }

    /** One bounded, indexed scan of due attempts; enqueues those neither owned nor scheduled. */
    public int safetyScan() {
        Instant now = now();
        int enqueued = 0;
        CheckoutAttemptStore.PageCursor after = null;
        while (true) {
            List<AttemptRecord> page = page(now, after);
            for (AttemptRecord attempt : page) {
                AttemptKey key = attempt.key();
                if (registry.owned(key) || schedule.contains(key)) continue;
                schedule.enqueue(key, now);
                enqueued++;
            }
            if (page.size() < settings.pageSize()) break;
            after = page.get(page.size() - 1).cursor();
        }
        SCHEDULED.add(enqueued);
        return enqueued;
    }

    private List<AttemptRecord> page(Instant dueBefore, CheckoutAttemptStore.PageCursor after) {
        SCAN_QUERIES.increment();
        List<AttemptRecord> page = attempts.findPending(dueBefore, after, settings.pageSize());
        SCAN_ROWS.add(page.size());
        return page;
    }

    /** Settles one attempt, or schedules its next try; never throws for a store or Inventory failure. */
    public void recover(AttemptKey key) {
        Objects.requireNonNull(key, "key");
        Optional<AttemptRecord> found;
        try {
            found = attempts.findAttempt(key);
        } catch (RuntimeException failure) {
            System.err.println("[transaction] Recovery could not read attempt " + key + ": " + failure);
            schedule.enqueue(key, now().plus(READ_RETRY));
            return;
        }
        if (found.isEmpty()) {
            // A reconcile entry for a T1 whose commit never happened: nothing to settle and nothing to write.
            DROPPED.increment();
            return;
        }
        AttemptRecord attempt = found.get();
        if (attempt.state() != CheckoutAttemptStore.State.PENDING) {
            DROPPED.increment();
            wake(key, settlementOf(attempt));
            return;
        }
        PENDING.increment();
        InventoryGateway.ApplyResult result;
        try {
            result = inventory.applyCheckout(key, CheckoutOrchestrator.parseCanonical(attempt.lines()),
                    settings.rpcDeadline());
        } catch (RuntimeException unexpected) {
            result = new InventoryGateway.Uncertain(unexpected.getClass().getSimpleName());
        }
        switch (result) {
            case InventoryGateway.Applied applied -> complete(attempt);
            case InventoryGateway.Rejected rejected -> reopen(attempt,
                    rejected.reason() == InventoryGateway.RejectionReason.INSUFFICIENT_STOCK
                            ? RejectionReason.INSUFFICIENT_STOCK : RejectionReason.INVALID_REQUEST, rejected.sku());
            case InventoryGateway.StockRestored restored -> reopen(attempt, RejectionReason.STOCK_RESTORED, null);
            case InventoryGateway.PayloadMismatch mismatch -> alarm(attempt, "FAILED_PRECONDITION", mismatch.detail());
            case InventoryGateway.InvalidRequest invalid -> alarm(attempt, "INVALID_ARGUMENT", invalid.detail());
            case InventoryGateway.Uncertain uncertain -> retryLater(attempt, uncertain.status());
        }
    }

    /** T3 for an attempt Inventory applied; a guard miss means another actor settled it first. */
    private void complete(AttemptRecord attempt) {
        AttemptKey key = attempt.key();
        String id = key.transactionId();
        try (var held = locks.acquire(id)) {
            WriteResult result;
            try {
                result = attempts.complete(key, now(), false);
            } catch (RuntimeException failure) {
                cache.invalidate(id);
                retryLater(attempt, "T3 failed: " + failure.getMessage());
                return;
            }
            if (result == WriteResult.OK) {
                cache.evict(id);
                registry.settled(key, Settlement.COMPLETED);
                SETTLED.increment();
                return;
            }
            cache.invalidate(id);
            settledByAnother(attempt);
        }
    }

    /** Reopens the basket after a definitive rejection; the next request reloads the OPEN state. */
    private void reopen(AttemptRecord attempt, RejectionReason reason, String rejectedSku) {
        AttemptKey key = attempt.key();
        String id = key.transactionId();
        try (var held = locks.acquire(id)) {
            WriteResult result;
            try {
                result = attempts.reopen(key, reason, rejectedSku, now(), false);
            } catch (RuntimeException failure) {
                cache.invalidate(id);
                retryLater(attempt, "reopen failed: " + failure.getMessage());
                return;
            }
            cache.invalidate(id);
            if (result == WriteResult.OK) {
                registry.settled(key, Settlement.REOPENED);
                SETTLED.increment();
                return;
            }
            settledByAnother(attempt);
        }
    }

    /** The caller holds the stripe; reads the settled state and wakes this process's waiters. */
    private void settledByAnother(AttemptRecord attempt) {
        AttemptKey key = attempt.key();
        Optional<AttemptRecord> settled;
        try {
            settled = attempts.findAttempt(key);
        } catch (RuntimeException failure) {
            retryLater(attempt, "settled-state read failed: " + failure.getMessage());
            return;
        }
        if (settled.isPresent() && settled.get().state() == CheckoutAttemptStore.State.PENDING) {
            alarm(attempt, "INTEGRITY", "the attempt is pending but no longer active");
            return;
        }
        DROPPED.increment();
        registry.settled(key, settled.map(CheckoutRecoveryWorker::settlementOf).orElse(Settlement.REOPENED));
    }

    private void wake(AttemptKey key, Settlement settlement) {
        try (var held = locks.acquire(key.transactionId())) {
            cache.invalidate(key.transactionId());
            registry.settled(key, settlement);
        }
    }

    /** Uncertain: one more durable retry at {@code min(5 s, 100 ms x 2^retry_count)}, and in memory. */
    private void retryLater(AttemptRecord attempt, String error) {
        RETRIES.increment();
        Instant due = now().plus(backoff(attempt.retryCount()));
        try {
            attempts.scheduleRetry(attempt.key(), due, error);
        } catch (RuntimeException failure) {
            System.err.println("[transaction] Recovery could not record a retry for " + attempt.key() + ": " + failure);
        } finally {
            schedule.enqueue(attempt.key(), due);
        }
    }

    /**
     * FAILED_PRECONDITION and INVALID_ARGUMENT are faults, never rejections: the attempt stays pending and
     * is retried at the safety scan's cadence, so the alarm repeats without flooding the log (FR-039).
     */
    private void alarm(AttemptRecord attempt, String status, String detail) {
        ALARMS.increment();
        System.err.println("[checkout-alarm] ERROR Inventory answered " + status + " for attempt " + attempt.key()
                + ": " + detail);
        try {
            attempts.scheduleRetry(attempt.key(), now().plus(settings.scanInterval()), status + ": " + detail);
        } catch (RuntimeException failure) {
            System.err.println("[transaction] Recovery could not record the alarm for " + attempt.key() + ": " + failure);
        }
    }

    /** {@code min(5 s, 100 ms x 2^retryCount)}. */
    public static Duration backoff(int retryCount) {
        int shift = Math.min(Math.max(retryCount, 0), 16);
        Duration delay = BACKOFF_BASE.multipliedBy(1L << shift);
        return delay.compareTo(BACKOFF_CAP) < 0 ? delay : BACKOFF_CAP;
    }

    private static Settlement settlementOf(AttemptRecord attempt) {
        return attempt.state() == CheckoutAttemptStore.State.COMPLETED ? Settlement.COMPLETED : Settlement.REOPENED;
    }

    private void recoverSafely(AttemptKey key) {
        try {
            recover(key);
        } catch (RuntimeException unexpected) {
            System.err.println("[transaction] Recovery of " + key + " failed: " + unexpected);
            schedule.enqueue(key, now().plus(READ_RETRY));
        }
    }

    private boolean startupScanSafely() {
        try {
            startupScan();
            return true;
        } catch (RuntimeException failure) {
            System.err.println("[transaction] Recovery startup scan failed, retrying: " + failure);
            return false;
        }
    }

    private void safetyScanSafely() {
        try {
            safetyScan();
        } catch (RuntimeException failure) {
            System.err.println("[transaction] Recovery safety scan failed: " + failure);
        }
    }

    private boolean pause(Duration duration) {
        try {
            Thread.sleep(duration);
            return true;
        } catch (InterruptedException interrupted) {
            return false;
        }
    }

    private Instant now() {
        return clock.instant().truncatedTo(ChronoUnit.MILLIS);
    }
}
