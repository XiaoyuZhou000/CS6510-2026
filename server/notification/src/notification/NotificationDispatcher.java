package notification;

import database.NotificationStore;
import database.NotificationStore.Pending;
import events.RecordSender;
import faults.FaultHooks;
import metrics.PerfMetrics;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes stored {@code notification} rows to their subscribers' channels (007 research N8, contracts/
 * notification-delivery.md "Dispatch"). It is Notification's only caller of {@link RecordSender} (Implementation
 * Rule 2) and sends only rows that exist in the table, with their stored identity and contents. It is one thread
 * plus the sender's callbacks; the table, not this object, decides every state.
 *
 * <ul>
 *   <li><b>Intake</b>: rows handed over after a selection commit (the immediate path), and a sweep every
 *       {@code NOTIFICATION_SWEEP_MS} of up to 1,000 {@code PENDING} rows that are due, oldest first. A row is
 *       tracked from intake until its outcome is written, so the sweep never sends it twice meanwhile. The first
 *       round after a start sweeps at once, and while sweeps come back full (a backlog, for example after a
 *       restart) the next one runs as soon as fewer than half that many rows are tracked, not a second later.
 *       {@link #requestSweep()} (an operator retry) sweeps in the next round.</li>
 *   <li><b>Lanes</b>: one FIFO per subscriber, with at most {@code NOTIFICATION_LANE_IN_FLIGHT} sends in flight,
 *       served round-robin. A lane holds at most four times that many queued rows; further rows of a full lane wait
 *       in the table and the sweep leaves that subscriber out, so a stalled channel never crowds out the others.</li>
 *   <li><b>Provisioning</b>: before the first send to a channel not known to be ready, its topic is created; a
 *       failure is an uncharged failure for the lane's queued rows.</li>
 *   <li><b>Success</b>: the channel's confirmation time is recorded, and the row joins the marking batch, written
 *       every 20 ms or 200 IDs after {@code FaultHooks.hit("notification.delivery.afterSend")}.</li>
 *   <li><b>Failure</b>: charged when another channel confirmed a send after this attempt started
 *       ({@link ChargingTracker}), else uncharged; batched like the marks, with the backoff
 *       {@code min(200 ms × 2^n, 5 s)}. The {@code NOTIFICATION_PARK_AFTER_CHARGED}-th charged failure parks the
 *       row and logs an alarm; {@link NotificationStatus} keeps the alarm from the database count while rows stay
 *       parked.</li>
 *   <li><b>Stall alarm</b>: no confirmed send on any channel for 60 s while work is outstanding (a broker-wide
 *       outage); nothing is parked for it.</li>
 * </ul>
 *
 * <p>{@link #runOnce()} is one round of the loop; tests drive it directly with a manual clock instead of
 * {@link #start()}. A crash between a confirmed send and its mark leaves the row {@code PENDING}, and the sweep
 * after a restart sends it again with the same {@code notification_id} (a redelivery, FR-015).</p>
 */
public final class NotificationDispatcher implements AutoCloseable {
    public record Settings(int laneInFlight, int parkAfterCharged, Duration sweepInterval, Duration markInterval,
                           int markBatch, int sweepLimit, Duration stallAlarmAfter, Duration provisionRetry) {
        public Settings {
            if (laneInFlight < 1 || parkAfterCharged < 1 || markBatch < 1 || sweepLimit < 1) {
                throw new IllegalArgumentException("dispatcher limits must be positive");
            }
            for (Duration value : new Duration[] {sweepInterval, markInterval, stallAlarmAfter, provisionRetry}) {
                if (value == null || value.isNegative() || value.isZero()) {
                    throw new IllegalArgumentException("dispatcher intervals must be positive");
                }
            }
        }

        public static Settings defaults() {
            return new Settings(NotificationConfig.DEFAULT_LANE_IN_FLIGHT, NotificationConfig.DEFAULT_PARK_AFTER_CHARGED,
                    Duration.ofMillis(NotificationConfig.DEFAULT_SWEEP_MS), Duration.ofMillis(20), 200, 1_000,
                    Duration.ofSeconds(60), Duration.ofSeconds(1));
        }

        public static Settings of(NotificationConfig config) {
            Settings defaults = defaults();
            return new Settings(config.laneInFlight(), config.parkAfterCharged(), Duration.ofMillis(config.sweepMs()),
                    defaults.markInterval(), defaults.markBatch(), defaults.sweepLimit(), defaults.stallAlarmAfter(),
                    defaults.provisionRetry());
        }

        public Settings withSweepInterval(Duration interval) {
            return new Settings(laneInFlight, parkAfterCharged, interval, markInterval, markBatch, sweepLimit,
                    stallAlarmAfter, provisionRetry);
        }

        public Settings withLaneInFlight(int inFlight) {
            return new Settings(inFlight, parkAfterCharged, sweepInterval, markInterval, markBatch, sweepLimit,
                    stallAlarmAfter, provisionRetry);
        }

        int laneQueueCap() { return laneInFlight * 4; }
    }

    private static final class Lane {
        final String subscriberId;
        final ArrayDeque<Pending> queue = new ArrayDeque<>();
        int inFlight;
        Channels.Channel channel;
        Instant provisionRetryAt = Instant.MIN;

        Lane(String subscriberId) {
            this.subscriberId = subscriberId;
        }
    }

    private record Completion(Pending row, Instant startedAt, Instant at, Throwable failure) { }

    private final NotificationStore store;
    private final Channels channels;
    private final RecordSender sender;
    private final ChargingTracker tracker;
    private final Settings settings;
    private final Clock clock;
    private final Queue<Pending> intake = new ConcurrentLinkedQueue<>();
    private final Queue<Completion> completions = new ConcurrentLinkedQueue<>();
    private final Set<String> tracked = ConcurrentHashMap.newKeySet();
    private final Map<String, Lane> lanes = new LinkedHashMap<>();
    private final List<String> marks = new ArrayList<>();
    private final List<NotificationStore.Failure> failures = new ArrayList<>();
    private final Semaphore signal = new Semaphore(0);
    private final AtomicLong parked = new AtomicLong();
    private final AtomicLong confirmed = new AtomicLong();
    private Instant marksSince;
    private Instant failuresSince;
    private final AtomicBoolean sweepRequested = new AtomicBoolean();
    private Instant nextSweepAt = Instant.MIN;
    private boolean lastSweepFull;
    private boolean started;
    private Instant outstandingSince;
    private boolean failedSinceConfirm;
    private volatile boolean stalled;
    private volatile Duration stalledFor = Duration.ZERO;
    private Instant lastStallLog = Instant.MIN;
    private Instant lastStoreErrorLog = Instant.MIN;
    private volatile boolean running;
    private volatile boolean draining;
    private Thread thread;

    public NotificationDispatcher(NotificationStore store, Channels channels, RecordSender sender, ChargingTracker tracker,
                                  Settings settings, Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.channels = Objects.requireNonNull(channels, "channels");
        this.sender = Objects.requireNonNull(sender, "sender");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** The immediate path: rows a selection just committed. Never waits for the broker. */
    public void accept(List<Pending> rows) {
        if (rows.isEmpty()) return;
        intake.addAll(rows);
        wake();
    }

    public synchronized void start() {
        if (thread != null) throw new IllegalStateException("dispatcher already started");
        running = true;
        thread = new Thread(this::loop, "notification-dispatcher");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Stops the loop, waits up to {@code timeout} for the sends in flight, and writes the final marking and failure
     * batches; a send confirmed later stays {@code PENDING} and is sent again after the restart.
     */
    public void stop(Duration timeout) {
        if (!halt(timeout)) return;
        long deadline = System.nanoTime() + timeout.toNanos();
        draining = true;
        while (inFlight() > 0 && System.nanoTime() < deadline) {
            drainCompletions();
            try {
                signal.tryAcquire(10, TimeUnit.MILLISECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        drainCompletions();
        flush(clock.instant(), true);
    }

    /** Stops the loop as a killed process would: nothing in memory is written. */
    public void crash() {
        halt(Duration.ofSeconds(5));
        draining = true;
    }

    @Override
    public void close() {
        stop(Duration.ofSeconds(5));
    }

    /** One round: completions, the sweep when due, intake, the lanes, the due batches, and the stall alarm. */
    public void runOnce() {
        Instant now = clock.instant();
        drainCompletions();
        boolean requested = sweepRequested.getAndSet(false);
        boolean backlog = lastSweepFull && tracked.size() < settings.sweepLimit() / 2;
        if (requested || backlog || !now.isBefore(nextSweepAt)) {
            int fetched = sweep(now);
            if (!started) {
                started = true;
                if (fetched > 0) {
                    System.out.println("[notification] Dispatcher took " + fetched + (fetched >= settings.sweepLimit()
                            ? "+" : "") + " due notifications at start");
                }
            }
            lastSweepFull = fetched >= settings.sweepLimit();
            nextSweepAt = now.plus(settings.sweepInterval());
        }
        drainIntake();
        dispatchLanes();
        flush(clock.instant(), false);
        observe(clock.instant());
    }

    /** Writes the marking and failure batches now, whatever their age. */
    public void flushNow() {
        drainCompletions();
        flush(clock.instant(), true);
    }

    public boolean stalled() { return stalled; }

    /**
     * Asks for a sweep in the next round instead of at the next interval, for example after an operator retry moved
     * parked rows back to {@code PENDING}.
     */
    public void requestSweep() {
        sweepRequested.set(true);
        wake();
    }

    /**
     * The alarms only this process knows, for the status line and {@code ops/status}: the stall alarm. Parked rows
     * and parked records are alarmed from the database counts ({@link NotificationStatus}), so those alarms survive
     * a restart and clear once an operator retry delivered the rows.
     */
    public List<String> alarms() {
        List<String> alarms = new ArrayList<>();
        if (stalled) {
            alarms.add("DELIVERY_STALLED: no confirmed send on any channel for " + stalledFor.toSeconds()
                    + " s while notifications are pending");
        }
        return alarms;
    }

    public Optional<Instant> lastConfirmedSend() { return tracker.lastConfirmedSend(); }

    /** Rows taken in and not yet written back. */
    public int tracked() { return tracked.size(); }

    public long parkedSinceStart() { return parked.get(); }

    public long confirmedSinceStart() { return confirmed.get(); }

    public synchronized int inFlight() {
        int total = 0;
        for (Lane lane : lanes.values()) total += lane.inFlight;
        return total;
    }

    /** Sends in flight on one subscriber's lane. */
    public synchronized int inFlight(String subscriberId) {
        Lane lane = lanes.get(subscriberId);
        return lane == null ? 0 : lane.inFlight;
    }

    /** Rows queued, not yet sent, on one subscriber's lane. */
    public synchronized int queued(String subscriberId) {
        Lane lane = lanes.get(subscriberId);
        return lane == null ? 0 : lane.queue.size();
    }

    private void loop() {
        while (running) {
            try {
                runOnce();
            } catch (RuntimeException failure) {
                System.err.println("[notification] Dispatcher round failed: " + failure);
            }
            try {
                signal.tryAcquire(waitMillis(), TimeUnit.MILLISECONDS);
                signal.drainPermits();
            } catch (InterruptedException interrupted) {
                if (!running) return;
            }
        }
    }

    private long waitMillis() {
        Instant now = clock.instant();
        long wait = Math.max(1, Duration.between(now, nextSweepAt).toMillis());
        if (marksSince != null || failuresSince != null) wait = Math.min(wait, settings.markInterval().toMillis());
        return Math.min(wait, settings.sweepInterval().toMillis());
    }

    private boolean halt(Duration timeout) {
        Thread current;
        synchronized (this) {
            if (!running) return false;
            running = false;
            current = thread;
        }
        wake();
        if (current != null) {
            try {
                current.join(Math.max(1, timeout.toMillis()));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return true;
    }

    private void wake() {
        signal.release();
    }

    /** One sweep; returns the rows it read (0 when the read failed). */
    private int sweep(Instant now) {
        List<String> full = new ArrayList<>();
        synchronized (this) {
            for (Lane lane : lanes.values()) {
                if (lane.queue.size() + lane.inFlight >= settings.laneQueueCap()) full.add(lane.subscriberId);
            }
        }
        List<Pending> due;
        try {
            due = store.selectDue(now, settings.sweepLimit(), full);
        } catch (RuntimeException failure) {
            logStoreError(now, "sweep", failure);
            return 0;
        }
        for (Pending row : due) take(row);
        return due.size();
    }

    private void drainIntake() {
        for (Pending row; (row = intake.poll()) != null; ) take(row);
    }

    private synchronized void take(Pending row) {
        if (!tracked.add(row.notificationId())) return;
        Lane lane = lanes.computeIfAbsent(row.subscriberId(), Lane::new);
        if (lane.queue.size() + lane.inFlight >= settings.laneQueueCap()) {
            // The row stays PENDING in the table; the sweep takes it once this lane has room.
            tracked.remove(row.notificationId());
            return;
        }
        lane.queue.add(row);
    }

    private void dispatchLanes() {
        if (draining) return;
        boolean progress = true;
        while (progress) {
            progress = false;
            List<Lane> round;
            synchronized (this) {
                round = new ArrayList<>(lanes.values());
            }
            for (Lane lane : round) {
                if (startOne(lane)) progress = true;
            }
        }
    }

    /** Starts one send on {@code lane}; false when the lane has nothing it may send now. */
    private boolean startOne(Lane lane) {
        Pending row;
        Channels.Channel channel;
        synchronized (this) {
            if (lane.queue.isEmpty() || lane.inFlight >= settings.laneInFlight()) return false;
            channel = lane.channel;
        }
        if (channel == null || !channel.ready()) {
            Instant now = clock.instant();
            if (now.isBefore(lane.provisionRetryAt)) return false;
            try {
                channel = channels.ensureReady(lane.subscriberId);
            } catch (RuntimeException failure) {
                lane.provisionRetryAt = now.plus(settings.provisionRetry());
                PerfMetrics.increment("notification.channel.provisionFailures");
                List<Pending> waiting;
                synchronized (this) {
                    waiting = new ArrayList<>(lane.queue);
                    lane.queue.clear();
                }
                String error = "channel not provisioned: " + describe(failure);
                for (Pending pending : waiting) failed(pending, false, error, now);
                return true;
            }
            lane.channel = channel;
        }
        synchronized (this) {
            row = lane.queue.poll();
            if (row == null) return false;
            lane.inFlight++;
        }
        Instant started = clock.instant();
        CompletableFuture<Void> confirmation;
        try {
            confirmation = sender.send(channel.topic(), row.subscriberId(), NotificationEnvelopes.envelope(row));
        } catch (RuntimeException failure) {
            confirmation = CompletableFuture.failedFuture(failure);
        }
        PerfMetrics.increment("notification.delivery.sends");
        Pending sent = row;
        confirmation.whenComplete((ignored, failure) -> {
            completions.add(new Completion(sent, started, clock.instant(), failure));
            wake();
        });
        return true;
    }

    private void drainCompletions() {
        List<Completion> drained = new ArrayList<>();
        for (Completion completion; (completion = completions.poll()) != null; ) drained.add(completion);
        if (drained.isEmpty()) return;
        // Successes first, so a failure in the same round is charged when another channel just confirmed.
        for (Completion completion : drained) {
            if (completion.failure() != null) continue;
            release(completion.row().subscriberId());
            tracker.confirmed(completion.row().subscriberId(), completion.at());
            confirmed.incrementAndGet();
            failedSinceConfirm = false;
            PerfMetrics.record("notification.delivery.latency",
                    Duration.between(completion.row().createdAt(), completion.at()).toNanos());
            if (marks.isEmpty()) marksSince = completion.at();
            marks.add(completion.row().notificationId());
        }
        for (Completion completion : drained) {
            if (completion.failure() == null) continue;
            release(completion.row().subscriberId());
            boolean charged = tracker.isCharged(completion.startedAt(), completion.row().subscriberId());
            failed(completion.row(), charged, describe(completion.failure()), completion.at());
        }
    }

    private synchronized void release(String subscriberId) {
        Lane lane = lanes.get(subscriberId);
        if (lane != null && lane.inFlight > 0) lane.inFlight--;
    }

    private void failed(Pending row, boolean charged, String error, Instant at) {
        PerfMetrics.increment(charged ? "notification.delivery.chargedFailures" : "notification.delivery.unchargedFailures");
        failedSinceConfirm = true;
        if (failures.isEmpty()) failuresSince = at;
        failures.add(new NotificationStore.Failure(row.notificationId(), charged, error));
    }

    private void flush(Instant now, boolean force) {
        if (!marks.isEmpty() && (force || marks.size() >= settings.markBatch()
                || !now.isBefore(marksSince.plus(settings.markInterval())))) {
            FaultHooks.hit("notification.delivery.afterSend");
            while (!marks.isEmpty()) {
                List<String> batch = new ArrayList<>(marks.subList(0, Math.min(marks.size(), settings.markBatch())));
                try {
                    store.markDelivered(batch, now);
                } catch (RuntimeException failure) {
                    logStoreError(now, "marking", failure);
                    break;
                }
                marks.subList(0, batch.size()).clear();
                batch.forEach(tracked::remove);
            }
            marksSince = marks.isEmpty() ? null : now;
        }
        if (!failures.isEmpty() && (force || failures.size() >= settings.markBatch()
                || !now.isBefore(failuresSince.plus(settings.markInterval())))) {
            List<NotificationStore.Failure> batch = new ArrayList<>(failures);
            try {
                int parkedNow = store.recordFailures(batch, now, settings.parkAfterCharged());
                failures.clear();
                failuresSince = null;
                for (NotificationStore.Failure failure : batch) tracked.remove(failure.notificationId());
                if (parkedNow > 0) {
                    parked.addAndGet(parkedNow);
                    PerfMetrics.add("notification.delivery.parked", parkedNow);
                    System.err.println("[notification] ALARM parked " + parkedNow + " notifications after "
                            + settings.parkAfterCharged() + " charged failures (" + parked.get() + " since start); "
                            + "retry them with POST /notifications/ops/parked/retry");
                }
            } catch (RuntimeException failure) {
                logStoreError(now, "failure bookkeeping", failure);
            }
        }
    }

    private void observe(Instant now) {
        boolean outstanding = !tracked.isEmpty() || failedSinceConfirm;
        if (!outstanding) {
            outstandingSince = null;
            stalled = false;
            stalledFor = Duration.ZERO;
            return;
        }
        if (outstandingSince == null) outstandingSince = now;
        Instant reference = outstandingSince;
        Optional<Instant> last = tracker.lastConfirmedSend();
        if (last.isPresent() && last.get().isAfter(reference)) reference = last.get();
        Duration quiet = Duration.between(reference, now);
        boolean nowStalled = quiet.compareTo(settings.stallAlarmAfter()) >= 0;
        stalledFor = nowStalled ? quiet : Duration.ZERO;
        if (nowStalled && (!stalled || !now.isBefore(lastStallLog.plusSeconds(30)))) {
            lastStallLog = now;
            System.err.println("[notification] ALARM delivery stalled: no confirmed send on any channel for "
                    + quiet.toSeconds() + " s while " + tracked.size() + " notifications are outstanding");
        }
        stalled = nowStalled;
    }

    private void logStoreError(Instant now, String what, RuntimeException failure) {
        PerfMetrics.increment("notification.delivery.storeFailures");
        if (now.isBefore(lastStoreErrorLog.plusSeconds(10))) return;
        lastStoreErrorLog = now;
        System.err.println("[notification] Dispatcher " + what + " failed; retrying: " + failure);
    }

    private static String describe(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return cause.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }
}
