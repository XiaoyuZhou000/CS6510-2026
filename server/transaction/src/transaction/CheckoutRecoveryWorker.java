package transaction;

import database.CheckoutAttemptStore;
import database.CheckoutAttemptStore.AttemptKey;
import database.CheckoutAttemptStore.AttemptRecord;
import events.OutboxSignal;
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
 * Republishes pending checkout attempts (FR-037, FR-038, D7; contracts/checkout-saga.md "Republish and
 * recovery"). One thread takes due keys from the in-memory {@link RecoverySchedule}; it never polls the
 * database on a short interval.
 *
 * <ul>
 *   <li><b>Startup scan</b>: nothing is owned after a restart, so every PENDING attempt is enqueued, read
 *       in pages of at most 100 rows.</li>
 *   <li><b>Safety scan</b>: every {@code CHECKOUT_RECOVERY_SCAN_SECONDS} (30), one bounded, indexed page of
 *       due attempts, repeated only while pages are full, enqueuing attempts that no live request owns
 *       and that are not scheduled. It catches anything the schedule missed, such as another process's
 *       handover.</li>
 *   <li><b>Republish</b>: the attempt is read by key first; an absent or settled attempt is dropped and any
 *       local waiter is woken. Otherwise its recorded {@code CheckoutRequested} is re-sent through
 *       {@link OutboxSignal#resend} with the same {@code event_id} and bytes, and the next republish is due
 *       after {@code CHECKOUT_REPUBLISH_AFTER_MS} (2,000) doubling to {@code CHECKOUT_REPUBLISH_MAX_MS}
 *       (5,000), with no limit; {@code retry_count} grows and {@code last_error} becomes
 *       {@code REPUBLISHED}, or {@code PUBLISH_FAILED} when the re-send could not be queued.</li>
 *   <li><b>Stuck alarm</b>: an attempt still PENDING {@code CHECKOUT_STUCK_ALARM_SECONDS} (60) after it began
 *       records {@code last_error = STUCK} instead, without any change of state, and is logged with its age
 *       once per status interval.</li>
 * </ul>
 *
 * <p>The worker never settles an attempt: only an outcome event does ({@link CheckoutOutcomeHandler}). It
 * never sets {@code client_outcome}. The schedule and the registry cover only this process; another
 * instance may republish the same attempt, and Inventory's idempotency keeps it to one effect.</p>
 */
public final class CheckoutRecoveryWorker implements AutoCloseable {
    public static final Duration DEFAULT_SCAN_INTERVAL = Duration.ofSeconds(30);
    public static final Duration DEFAULT_REPUBLISH_MAX = Duration.ofMillis(5_000);
    public static final Duration DEFAULT_STUCK_ALARM = Duration.ofSeconds(60);
    public static final Duration DEFAULT_ALARM_INTERVAL = Duration.ofSeconds(30);
    public static final String REPUBLISHED = "REPUBLISHED";
    public static final String PUBLISH_FAILED = "PUBLISH_FAILED";
    public static final String STUCK = "STUCK";
    /** The delay before reading an attempt again after its own read failed. */
    private static final Duration READ_RETRY = Duration.ofSeconds(1);
    /** A due bound later than any stored {@code next_retry_at}, within the TIMESTAMP range. */
    private static final Instant STARTUP_HORIZON = Instant.parse("2038-01-01T00:00:00Z");
    private static final LongAdder PENDING = PerfMetrics.counter("transaction.recovery.pending");
    private static final LongAdder REPUBLISHES = PerfMetrics.counter("transaction.recovery.republished");
    private static final LongAdder PUBLISH_FAILURES = PerfMetrics.counter("transaction.recovery.publishFailed");
    private static final LongAdder STUCK_ALARMS = PerfMetrics.counter("transaction.recovery.stuckAlarms");
    private static final LongAdder SCAN_QUERIES = PerfMetrics.counter("transaction.recovery.scanQueries");
    private static final LongAdder SCAN_ROWS = PerfMetrics.counter("transaction.recovery.scanRows");
    private static final LongAdder SCHEDULED = PerfMetrics.counter("transaction.recovery.scheduled");
    private static final LongAdder DROPPED = PerfMetrics.counter("transaction.recovery.dropped");

    private final CheckoutAttemptStore attempts;
    private final OutboxSignal signal;
    private final TransactionStateCache cache;
    private final TransactionLocks locks;
    private final SettlementRegistry registry;
    private final RecoverySchedule schedule;
    private final Settings settings;
    private final Clock clock;
    private volatile Thread thread;
    private volatile boolean running;

    public CheckoutRecoveryWorker(CheckoutAttemptStore attempts, OutboxSignal signal, TransactionStateCache cache,
                                  SettlementRegistry registry, RecoverySchedule schedule, Settings settings, Clock clock) {
        this.attempts = Objects.requireNonNull(attempts, "attempts");
        this.signal = Objects.requireNonNull(signal, "signal");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.schedule = Objects.requireNonNull(schedule, "schedule");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.locks = cache.locks();
        if (registry.locks() != locks) throw new IllegalArgumentException("The registry must check the cache's locks");
    }

    /**
     * The safety-scan interval, the republish backoff from {@code republishAfter} to {@code republishMax},
     * the age at which a pending attempt is stuck, how often a stuck attempt is logged again, and the scan
     * page size.
     */
    public record Settings(Duration scanInterval, Duration republishAfter, Duration republishMax, Duration stuckAfter,
                           Duration alarmInterval, int pageSize) {
        public Settings {
            for (Duration value : new Duration[] {scanInterval, republishAfter, republishMax, stuckAfter, alarmInterval}) {
                if (value == null || value.isZero() || value.isNegative()) {
                    throw new IllegalArgumentException("recovery durations must be positive");
                }
            }
            if (republishMax.compareTo(republishAfter) < 0) throw new IllegalArgumentException("republishMax < republishAfter");
            if (pageSize < 1) throw new IllegalArgumentException("pageSize must be positive");
        }

        public static Settings defaults() {
            return new Settings(DEFAULT_SCAN_INTERVAL, CheckoutOrchestrator.DEFAULT_REPUBLISH_AFTER, DEFAULT_REPUBLISH_MAX,
                    DEFAULT_STUCK_ALARM, DEFAULT_ALARM_INTERVAL, CheckoutAttemptStore.DEFAULT_PAGE_SIZE);
        }

        /**
         * {@code CHECKOUT_RECOVERY_SCAN_SECONDS} (30), {@code CHECKOUT_REPUBLISH_AFTER_MS} (2,000),
         * {@code CHECKOUT_REPUBLISH_MAX_MS} (5,000), and {@code CHECKOUT_STUCK_ALARM_SECONDS} (60).
         */
        public static Settings fromEnvironment(Map<String, String> environment) {
            return new Settings(seconds(environment, "CHECKOUT_RECOVERY_SCAN_SECONDS", DEFAULT_SCAN_INTERVAL),
                    CheckoutOrchestrator.Settings.millis(environment, "CHECKOUT_REPUBLISH_AFTER_MS",
                            CheckoutOrchestrator.DEFAULT_REPUBLISH_AFTER),
                    CheckoutOrchestrator.Settings.millis(environment, "CHECKOUT_REPUBLISH_MAX_MS", DEFAULT_REPUBLISH_MAX),
                    seconds(environment, "CHECKOUT_STUCK_ALARM_SECONDS", DEFAULT_STUCK_ALARM),
                    DEFAULT_ALARM_INTERVAL, CheckoutAttemptStore.DEFAULT_PAGE_SIZE);
        }

        public Settings withStuckAfter(Duration stuck) {
            return new Settings(scanInterval, republishAfter, republishMax, stuck, alarmInterval, pageSize);
        }

        private static Duration seconds(Map<String, String> environment, String key, Duration fallback) {
            String value = environment.get(key);
            return value == null || value.isBlank() ? fallback : Duration.ofSeconds(Long.parseLong(value.trim()));
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

    /** Republishes one pending attempt and schedules the next republish; never throws for a store failure. */
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
            // A reconcile entry for a T1 whose commit never happened: nothing to republish and nothing to write.
            DROPPED.increment();
            return;
        }
        AttemptRecord attempt = found.get();
        if (attempt.state() != CheckoutAttemptStore.State.PENDING) {
            DROPPED.increment();
            wake(key, attempt);
            return;
        }
        PENDING.increment();
        String lastError;
        try {
            signal.resend(attempt.requestEventId());
            REPUBLISHES.increment();
            lastError = REPUBLISHED;
        } catch (RuntimeException failure) {
            PUBLISH_FAILURES.increment();
            System.err.println("[transaction] Could not republish attempt " + key + ": " + failure);
            lastError = PUBLISH_FAILED;
        }
        Instant now = now();
        if (stuck(attempt, now)) lastError = STUCK;
        Instant due = now.plus(backoff(attempt.retryCount()));
        try {
            attempts.scheduleRetry(key, due, lastError);
        } catch (RuntimeException failure) {
            System.err.println("[transaction] Recovery could not record a republish for " + key + ": " + failure);
        } finally {
            schedule.enqueue(key, due);
        }
    }

    /**
     * True from {@code CHECKOUT_STUCK_ALARM_SECONDS} after the attempt began. It is logged when it first becomes
     * stuck, and then once per alarm interval: every {@code alarmInterval / republishMax} republishes, the
     * capped cadence of a stuck attempt, so the worker keeps no per-attempt state.
     */
    private boolean stuck(AttemptRecord attempt, Instant now) {
        if (attempt.createdAt() == null) return false;
        Duration age = Duration.between(attempt.createdAt(), now);
        if (age.compareTo(settings.stuckAfter()) < 0) return false;
        long every = Math.max(1, settings.alarmInterval().toMillis() / settings.republishMax().toMillis());
        if (!STUCK.equals(attempt.lastError()) || attempt.retryCount() % every == 0) {
            STUCK_ALARMS.increment();
            System.err.println("[checkout-alarm] ERROR attempt " + attempt.key() + " has been pending for "
                    + age.toSeconds() + " s" + (attempt.lastError() == null ? "" : " (" + attempt.lastError() + ")"));
        }
        return true;
    }

    /** The republish delay after {@code retryCount} republishes: {@code republishAfter x 2^retryCount}, capped. */
    public Duration backoff(int retryCount) {
        int shift = Math.min(Math.max(retryCount, 0), 16);
        Duration delay = settings.republishAfter().multipliedBy(1L << shift);
        return delay.compareTo(settings.republishMax()) < 0 ? delay : settings.republishMax();
    }

    private void wake(AttemptKey key, AttemptRecord attempt) {
        boolean recorded = attempt.clientOutcome() != CheckoutAttemptStore.ClientOutcome.NONE;
        Settlement settlement = attempt.state() == CheckoutAttemptStore.State.COMPLETED
                ? Settlement.completed(attempt.settledAt(), recorded)
                : Settlement.reopened(attempt.rejectionReason(), attempt.rejectedSku(), recorded);
        try (var held = locks.acquire(key.transactionId())) {
            cache.invalidate(key.transactionId());
            registry.complete(key, settlement);
        }
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
