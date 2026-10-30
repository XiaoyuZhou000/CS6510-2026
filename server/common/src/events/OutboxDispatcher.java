package events;

import database.StoreFailure;
import faults.FaultHooks;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Publishes a service's outbox rows (D5). Business code writes a row inside its own local transaction and
 * calls {@link #signal} after the commit; nothing reaches the broker without a recorded row (FR-010).
 *
 * <ul>
 *   <li><b>Immediate path</b>: {@link #signal} only queues the event IDs. One sender thread loads the rows,
 *       in one query per drained batch, and sends each row's stored envelope with its {@code routing_key}
 *       through the {@link RecordSender} registered for the row's topic, in signal order.</li>
 *   <li><b>Confirmation</b>: an ID joins the marking batch only after its send future succeeded. The marker
 *       flushes a batch every {@code OUTBOX_MARK_BATCH_MS} (20) or at 200 IDs with one
 *       {@link OutboxStore#markPublished}.</li>
 *   <li><b>Sweep</b>: every {@code OUTBOX_SWEEP_MS} (1,000) it selects up to 500 unpublished rows older than
 *       1 s in {@code outbox_seq} order, skips rows in flight, awaiting their mark, or still backing off
 *       after a failed send (100 ms doubling to 5 s), counts them in {@code send_count}, and queues them for
 *       the sender thread. The sweep alone is enough for correctness; the immediate path removes latency.</li>
 *   <li><b>Re-send</b>: {@link #resend} queues one recorded row by {@code event_id}, with its stored
 *       envelope, and increments its {@code send_count} (D5 point 4).</li>
 * </ul>
 *
 * <p>Because every send goes through the one sender thread, a key's events leave in commit order; Kafka's
 * idempotent producer keeps that order on the partition. Test-only fault hooks: {@code outbox.beforeSend},
 * {@code outbox.afterConfirm} (on the marker thread, before the ID joins a batch), and
 * {@code outbox.beforeMark}. Timers {@code outbox.commitToSend} (signal to send) and
 * {@code outbox.sendToConfirm}; counters {@code outbox.markBatches} and {@code outbox.markedEvents}, whose
 * ratio is the mean marking batch size, plus {@code outbox.resends} and {@code outbox.sendFailures}.</p>
 */
public final class OutboxDispatcher implements OutboxSignal, AutoCloseable {
    public static final Duration DEFAULT_SWEEP_INTERVAL = Duration.ofMillis(1_000);
    public static final Duration DEFAULT_SWEEP_MIN_AGE = Duration.ofSeconds(1);
    public static final int DEFAULT_SWEEP_LIMIT = 500;
    public static final Duration DEFAULT_MARK_INTERVAL = Duration.ofMillis(20);
    public static final int DEFAULT_MARK_BATCH = 200;
    public static final Duration MIN_BACKOFF = Duration.ofMillis(100);
    public static final Duration MAX_BACKOFF = Duration.ofSeconds(5);
    private static final LatencyHistogram COMMIT_TO_SEND = PerfMetrics.timer("outbox.commitToSend");
    private static final LatencyHistogram SEND_TO_CONFIRM = PerfMetrics.timer("outbox.sendToConfirm");
    private static final LongAdder MARK_BATCHES = PerfMetrics.counter("outbox.markBatches");
    private static final LongAdder MARKED = PerfMetrics.counter("outbox.markedEvents");
    private static final LongAdder RESENDS = PerfMetrics.counter("outbox.resends");
    private static final LongAdder SEND_FAILURES = PerfMetrics.counter("outbox.sendFailures");

    public record Settings(Duration sweepInterval, Duration sweepMinAge, int sweepLimit, Duration markInterval,
                           int markBatchSize, Duration minBackoff, Duration maxBackoff, boolean immediate) {
        public Settings {
            positive(sweepInterval, "sweepInterval");
            positive(markInterval, "markInterval");
            positive(minBackoff, "minBackoff");
            positive(maxBackoff, "maxBackoff");
            if (sweepMinAge == null || sweepMinAge.isNegative()) throw new IllegalArgumentException("sweepMinAge < 0");
            if (sweepLimit < 1) throw new IllegalArgumentException("sweepLimit must be positive");
            if (markBatchSize < 1) throw new IllegalArgumentException("markBatchSize must be positive");
            if (maxBackoff.compareTo(minBackoff) < 0) throw new IllegalArgumentException("maxBackoff < minBackoff");
        }

        public static Settings defaults() {
            return new Settings(DEFAULT_SWEEP_INTERVAL, DEFAULT_SWEEP_MIN_AGE, DEFAULT_SWEEP_LIMIT,
                    DEFAULT_MARK_INTERVAL, DEFAULT_MARK_BATCH, MIN_BACKOFF, MAX_BACKOFF, true);
        }

        /** {@code OUTBOX_SWEEP_MS} (1,000) and {@code OUTBOX_MARK_BATCH_MS} (20); the rest is fixed by D5. */
        public static Settings fromEnvironment(Map<String, String> environment) {
            Settings defaults = defaults();
            return new Settings(millis(environment, "OUTBOX_SWEEP_MS", defaults.sweepInterval()), defaults.sweepMinAge(),
                    defaults.sweepLimit(), millis(environment, "OUTBOX_MARK_BATCH_MS", defaults.markInterval()),
                    defaults.markBatchSize(), defaults.minBackoff(), defaults.maxBackoff(), true);
        }

        /** Test switch: with the immediate path off, only the sweep publishes (plan Complexity Tracking). */
        public Settings withImmediate(boolean enabled) {
            return new Settings(sweepInterval, sweepMinAge, sweepLimit, markInterval, markBatchSize, minBackoff,
                    maxBackoff, enabled);
        }

        public Settings withSweep(Duration interval, Duration minAge) {
            return new Settings(interval, minAge, sweepLimit, markInterval, markBatchSize, minBackoff, maxBackoff,
                    immediate);
        }

        private static Duration millis(Map<String, String> environment, String name, Duration fallback) {
            String value = environment.get(name);
            if (value == null || value.isBlank()) return fallback;
            long millis = Long.parseLong(value.trim());
            if (millis < 1) throw new IllegalArgumentException(name + " must be positive");
            return Duration.ofMillis(millis);
        }

        private static void positive(Duration value, String name) {
            if (value == null || value.isZero() || value.isNegative()) {
                throw new IllegalArgumentException(name + " must be positive");
            }
        }
    }

    /** Work for the sender thread: either signalled IDs to load, or rows already loaded for a re-send. */
    private record Work(List<String> eventIds, List<OutboxRow> rows, long queuedNanos) { }

    private record Confirmed(String eventId) { }

    private record Backoff(int failures, long eligibleNanos) { }

    private final String service;
    private final OutboxStore<?> store;
    private final Map<String, RecordSender> senders;
    private final Settings settings;
    private final Clock clock;
    private final BlockingQueue<Work> work = new LinkedBlockingQueue<>();
    private final BlockingQueue<Confirmed> confirmed = new LinkedBlockingQueue<>();
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
    private final Set<String> awaitingMark = ConcurrentHashMap.newKeySet();
    private final Map<String, Backoff> backoffs = new ConcurrentHashMap<>();
    private final Thread sender;
    private final Thread marker;
    private final Thread sweeper;
    private volatile boolean running;
    private volatile boolean stopping;

    /**
     * @param rowTopics every topic this service's rows may name; each must have a registered sender, so a
     *                  row can never be dropped for want of one
     */
    public OutboxDispatcher(String service, OutboxStore<?> store, Map<String, RecordSender> sendersByTopic,
                            Collection<String> rowTopics, Settings settings, Clock clock) {
        this.service = Objects.requireNonNull(service, "service");
        this.store = Objects.requireNonNull(store, "store");
        this.senders = Map.copyOf(sendersByTopic);
        this.settings = Objects.requireNonNull(settings, "settings");
        this.clock = Objects.requireNonNull(clock, "clock");
        for (String topic : rowTopics) {
            if (!senders.containsKey(topic)) {
                throw new IllegalArgumentException("No RecordSender is registered for outbox topic " + topic);
            }
        }
        this.sender = new Thread(this::sendLoop, service + "-outbox-sender");
        this.marker = new Thread(this::markLoop, service + "-outbox-marker");
        this.sweeper = new Thread(this::sweepLoop, service + "-outbox-sweep");
        for (Thread thread : List.of(sender, marker, sweeper)) thread.setDaemon(true);
    }

    public synchronized void start() {
        if (running || stopping) throw new IllegalStateException("dispatcher already started");
        running = true;
        sender.start();
        marker.start();
        sweeper.start();
    }

    @Override
    public void signal(List<String> eventIds) {
        if (eventIds.isEmpty() || !settings.immediate() || stopping) return;
        work.add(new Work(List.copyOf(eventIds), null, System.nanoTime()));
    }

    @Override
    public void resend(String eventId) {
        if (stopping) return;
        OutboxRow row = store.findByEventId(eventId)
                .orElseThrow(() -> new IllegalArgumentException("No outbox row " + eventId));
        store.incrementSendCount(List.of(eventId));
        RESENDS.increment();
        work.add(new Work(null, List.of(row), System.nanoTime()));
    }

    /** Signalled or re-sent work the sender thread has not started yet. */
    public int queued() {
        return work.size();
    }

    /** Sends whose broker confirmation has not arrived yet. */
    public int inFlight() {
        return inFlight.size();
    }

    /**
     * Stops in order: no new work is accepted, the sweep ends, queued work is sent, outstanding confirmations
     * are awaited up to {@code timeout}, and a final batch is marked.
     */
    public void stop(Duration timeout) {
        synchronized (this) {
            if (stopping) return;
            stopping = true;
        }
        if (!running) return;
        long deadline = System.nanoTime() + timeout.toNanos();
        sweeper.interrupt();
        join(sweeper, deadline);
        running = false;
        join(sender, deadline);
        while (!inFlight.isEmpty() && System.nanoTime() < deadline) sleep(5);
        marker.interrupt();
        join(marker, deadline);
        flushMarks(drainConfirmed(new ArrayList<>()));
    }

    @Override
    public void close() {
        stop(Duration.ofSeconds(5));
    }

    private void sendLoop() {
        while (running || !work.isEmpty()) {
            Work next;
            try {
                next = work.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException interrupted) {
                if (!running) break;
                continue;
            }
            if (next == null) continue;
            try {
                if (next.rows() != null) {
                    for (OutboxRow row : next.rows()) send(row, next.queuedNanos());
                } else {
                    sendSignalled(next);
                }
            } catch (StoreFailure failure) {
                // The rows stay unpublished; the sweep picks them up once the database answers again.
                System.err.println("[" + service + "] Outbox rows could not be loaded; the sweep will publish them: "
                        + failure.getMessage());
            } catch (RuntimeException failure) {
                System.err.println("[" + service + "] ERROR Outbox dispatch failed: " + failure);
            }
        }
    }

    /** Loads every signalled ID queued so far with one query, then sends in signal order. */
    private void sendSignalled(Work first) {
        List<Work> batch = new ArrayList<>();
        batch.add(first);
        int ids = first.eventIds().size();
        while (ids < settings.sweepLimit() && work.peek() != null && work.peek().eventIds() != null) {
            Work more = work.poll();
            if (more == null) break;
            batch.add(more);
            ids += more.eventIds().size();
        }
        Set<String> wanted = new LinkedHashSet<>();
        for (Work item : batch) wanted.addAll(item.eventIds());
        Map<String, OutboxRow> rows = new HashMap<>();
        for (OutboxRow row : store.findByEventIds(wanted)) rows.put(row.eventId(), row);
        for (Work item : batch) {
            for (String eventId : item.eventIds()) {
                OutboxRow row = rows.get(eventId);
                if (row == null) {
                    System.err.println("[" + service + "] Signalled outbox row " + eventId
                            + " is not visible; the sweep will publish it once committed");
                    continue;
                }
                send(row, item.queuedNanos());
            }
        }
        // Re-sends queued between signals keep their place behind these rows.
    }

    private void send(OutboxRow row, long queuedNanos) {
        RecordSender target = senders.get(row.topic());
        if (target == null) {
            System.err.println("[" + service + "] ERROR No RecordSender for topic " + row.topic() + "; outbox row "
                    + row.eventId() + " stays unpublished");
            return;
        }
        if (!inFlight.add(row.eventId())) return;
        FaultHooks.hit("outbox.beforeSend");
        long sending = System.nanoTime();
        COMMIT_TO_SEND.record(sending - queuedNanos);
        CompletableFuture<Void> future;
        try {
            future = target.send(row.topic(), row.routingKey(), row.envelope());
        } catch (RuntimeException failure) {
            future = CompletableFuture.failedFuture(failure);
        }
        future.whenComplete((ignored, failure) -> {
            if (failure == null) {
                SEND_TO_CONFIRM.record(System.nanoTime() - sending);
                backoffs.remove(row.eventId());
                awaitingMark.add(row.eventId());
                inFlight.remove(row.eventId());
                confirmed.add(new Confirmed(row.eventId()));
            } else {
                SEND_FAILURES.increment();
                backoffs.merge(row.eventId(), new Backoff(1, System.nanoTime() + settings.minBackoff().toNanos()),
                        (previous, ignoredNew) -> next(previous));
                inFlight.remove(row.eventId());
            }
        });
    }

    private Backoff next(Backoff previous) {
        int failures = previous.failures() + 1;
        long delay = settings.minBackoff().toNanos() << Math.min(failures - 1, 20);
        delay = Math.min(delay, settings.maxBackoff().toNanos());
        return new Backoff(failures, System.nanoTime() + delay);
    }

    private void markLoop() {
        List<String> batch = new ArrayList<>();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Confirmed first = confirmed.poll(settings.markInterval().toMillis(), TimeUnit.MILLISECONDS);
                if (first == null) {
                    flushMarks(batch);
                    continue;
                }
                accept(first, batch);
                long deadline = System.nanoTime() + settings.markInterval().toNanos();
                while (batch.size() < settings.markBatchSize()) {
                    long left = deadline - System.nanoTime();
                    if (left <= 0) break;
                    Confirmed more = confirmed.poll(left, TimeUnit.NANOSECONDS);
                    if (more == null) break;
                    accept(more, batch);
                }
                flushMarks(batch);
            } catch (InterruptedException interrupted) {
                break;
            }
        }
        drainConfirmed(batch);
        flushMarks(batch);
    }

    private void accept(Confirmed item, List<String> batch) {
        FaultHooks.hit("outbox.afterConfirm");
        batch.add(item.eventId());
    }

    private List<String> drainConfirmed(List<String> batch) {
        Confirmed item;
        while ((item = confirmed.poll()) != null) batch.add(item.eventId());
        return batch;
    }

    /** One {@code markPublished} for the whole batch; a failed mark keeps the IDs for the next batch. */
    private void flushMarks(List<String> batch) {
        while (!batch.isEmpty()) {
            List<String> chunk = new ArrayList<>(batch.subList(0, Math.min(batch.size(), settings.markBatchSize())));
            FaultHooks.hit("outbox.beforeMark");
            try {
                store.markPublished(chunk, clock.instant());
            } catch (StoreFailure failure) {
                System.err.println("[" + service + "] Outbox marking failed; retrying with the next batch: "
                        + failure.getMessage());
                return;
            }
            MARK_BATCHES.increment();
            MARKED.add(chunk.size());
            awaitingMark.removeAll(chunk);
            batch.subList(0, chunk.size()).clear();
        }
    }

    private void sweepLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(settings.sweepInterval().toMillis());
            } catch (InterruptedException interrupted) {
                break;
            }
            try {
                sweep();
            } catch (StoreFailure failure) {
                System.err.println("[" + service + "] Outbox sweep failed; retrying: " + failure.getMessage());
            } catch (RuntimeException failure) {
                System.err.println("[" + service + "] ERROR Outbox sweep failed: " + failure);
            }
        }
    }

    private void sweep() {
        Instant olderThan = clock.instant().minus(settings.sweepMinAge());
        long now = System.nanoTime();
        List<OutboxRow> due = new ArrayList<>();
        for (OutboxRow row : store.selectUnpublished(olderThan, settings.sweepLimit())) {
            if (inFlight.contains(row.eventId()) || awaitingMark.contains(row.eventId())) continue;
            Backoff backoff = backoffs.get(row.eventId());
            if (backoff != null && backoff.eligibleNanos() > now) continue;
            due.add(row);
        }
        if (due.isEmpty()) return;
        store.incrementSendCount(due.stream().map(OutboxRow::eventId).toList());
        RESENDS.add(due.size());
        work.add(new Work(null, List.copyOf(due), now));
    }

    private static void join(Thread thread, long deadlineNanos) {
        long left = deadlineNanos - System.nanoTime();
        if (left <= 0) return;
        try {
            thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(left)));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
