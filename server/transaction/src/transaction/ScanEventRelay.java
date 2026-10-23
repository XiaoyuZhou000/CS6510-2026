package transaction;

import database.RelayStore;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;
import rpc.FaultHooks;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * The outbox relay (contracts/scan-event-delivery.md section 2, R10): one daemon thread per Transaction
 * process delivers accepted scans to Analytics at least once. Scan never waits for it.
 *
 * <ul>
 *   <li>A round selects up to {@code OUTBOX_BATCH_SIZE} (500) undelivered rows in {@code scan_id} order and
 *       sends them in one {@code DeliverScanEvents} call.</li>
 *   <li>Only after OK does it set {@code delivered_at} on exactly those rows, then it starts the next round
 *       at once. Any other outcome leaves the rows undelivered; they are reselected and resent after a
 *       backoff from 100 ms to 5 s. INVALID_ARGUMENT also raises an alarm, and nothing is ever skipped.</li>
 *   <li>With nothing to send it polls every 50 ms, counting {@code transaction.relay.idlePolls}.</li>
 *   <li>{@code OUTBOX_COALESCE_MS} (default 0, off) is the CP3 coalescing switch (R23): after a partial batch
 *       the next round waits that long, so it carries more events and costs fewer delivery commits. A full
 *       batch still starts the next round at once.</li>
 * </ul>
 *
 * <p>No cursor is kept, so a restart resumes from the undelivered rows and a row that commits late with a
 * lower {@code scan_id} is never skipped. A crash between OK and the {@code delivered_at} commit (the test-only
 * hook {@code relay.afterAck}) only causes a redelivery, which Analytics counts as duplicates. Several
 * Transaction processes may deliver the same rows; Analytics' deduplication keeps every scan counted once.</p>
 */
public final class ScanEventRelay implements AutoCloseable {
    public static final int DEFAULT_BATCH_SIZE = 500;
    public static final Duration DEFAULT_IDLE_POLL = Duration.ofMillis(50);
    public static final Duration MIN_BACKOFF = Duration.ofMillis(100);
    public static final Duration MAX_BACKOFF = Duration.ofSeconds(5);
    public static final Duration MAX_COALESCE = Duration.ofSeconds(1);
    private static final LatencyHistogram BATCH = PerfMetrics.timer("transaction.relay.batch");
    private static final LatencyHistogram LATENCY = PerfMetrics.timer("transaction.relay.latency");
    private static final LongAdder EVENTS = PerfMetrics.counter("transaction.relay.events");
    private static final LongAdder DUPLICATES = PerfMetrics.counter("transaction.relay.duplicates");
    private static final LongAdder IDLE_POLLS = PerfMetrics.counter("transaction.relay.idlePolls");
    private static final LongAdder FAILURES = PerfMetrics.counter("transaction.relay.failures");
    private static final LongAdder INVALID = PerfMetrics.counter("transaction.relay.invalidBatches");

    /** What one round did. */
    public enum Round { IDLE, DELIVERED, FAILED }

    public record Settings(int batchSize, Duration idlePoll, Duration minBackoff, Duration maxBackoff,
                           Duration coalesce) {
        public Settings {
            if (batchSize < 1 || batchSize > 1_000) throw new IllegalArgumentException("batchSize must be 1 to 1,000");
            positive(idlePoll, "idlePoll");
            positive(minBackoff, "minBackoff");
            positive(maxBackoff, "maxBackoff");
            if (maxBackoff.compareTo(minBackoff) < 0) throw new IllegalArgumentException("maxBackoff < minBackoff");
            if (coalesce == null || coalesce.isNegative() || coalesce.compareTo(MAX_COALESCE) > 0) {
                throw new IllegalArgumentException("coalesce must be 0 to " + MAX_COALESCE.toMillis() + " ms");
            }
        }

        public static Settings defaults() {
            return new Settings(DEFAULT_BATCH_SIZE, DEFAULT_IDLE_POLL, MIN_BACKOFF, MAX_BACKOFF, Duration.ZERO);
        }

        /**
         * {@code OUTBOX_BATCH_SIZE} (500) and {@code OUTBOX_COALESCE_MS} (0); the poll and backoff are fixed by
         * the contract.
         */
        public static Settings fromEnvironment(Map<String, String> environment) {
            String size = environment.get("OUTBOX_BATCH_SIZE");
            int batchSize = size == null || size.isBlank() ? DEFAULT_BATCH_SIZE : Integer.parseInt(size.trim());
            String coalesce = environment.get("OUTBOX_COALESCE_MS");
            long coalesceMs = coalesce == null || coalesce.isBlank() ? 0 : Long.parseLong(coalesce.trim());
            if (coalesceMs < 0) throw new IllegalArgumentException("OUTBOX_COALESCE_MS must not be negative");
            return new Settings(batchSize, DEFAULT_IDLE_POLL, MIN_BACKOFF, MAX_BACKOFF, Duration.ofMillis(coalesceMs));
        }

        private static void positive(Duration value, String name) {
            if (value == null || value.isZero() || value.isNegative()) throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private final RelayStore store;
    private final ScanEventSink sink;
    private final Settings settings;
    private Duration backoff;
    private boolean lastBatchFull;
    private boolean failing;
    private boolean alarmed;
    private volatile boolean running;
    private Thread thread;

    public ScanEventRelay(RelayStore store, ScanEventSink sink, Settings settings) {
        this.store = Objects.requireNonNull(store, "store");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.backoff = settings.minBackoff();
    }

    /**
     * One round: select, deliver, and, only after OK, mark delivered. Exposed for tests; the relay thread
     * calls it in a loop. It never throws.
     */
    public Round runOnce() {
        long started = System.nanoTime();
        List<RelayStore.UndeliveredScan> batch;
        try {
            batch = store.selectUndelivered(settings.batchSize());
        } catch (RuntimeException failure) {
            return failed("selecting undelivered scans failed: " + failure.getMessage());
        }
        if (batch.isEmpty()) {
            IDLE_POLLS.increment();
            recovered();
            return Round.IDLE;
        }
        ScanEventSink.DeliveryResult result = sink.deliver(batch);
        if (result instanceof ScanEventSink.InvalidBatch invalid) {
            INVALID.increment();
            if (!alarmed) {
                System.err.println("[transaction] ALARM: Analytics refused an outbox batch as invalid; it stays "
                        + "undelivered and is retried: " + invalid.detail());
            }
            alarmed = true;
            return failed(null);
        }
        if (!(result instanceof ScanEventSink.Delivered delivered)) {
            return failed("Analytics delivery was not acknowledged: " + ((ScanEventSink.Uncertain) result).status());
        }
        alarmed = false;
        Instant acknowledged = Instant.now();
        try {
            FaultHooks.hit("relay.afterAck");
            List<Long> ids = new ArrayList<>(batch.size());
            for (RelayStore.UndeliveredScan scan : batch) ids.add(scan.scanId());
            store.markDelivered(ids, acknowledged);
        } catch (RuntimeException failure) {
            // Analytics holds the batch; it is resent and counted as duplicates.
            return failed("recording delivery failed: " + failure.getMessage());
        }
        for (RelayStore.UndeliveredScan scan : batch) {
            LATENCY.record(Math.max(0, Duration.between(scan.acceptedAt(), acknowledged).toNanos()));
        }
        lastBatchFull = batch.size() >= settings.batchSize();
        EVENTS.add(batch.size());
        DUPLICATES.add(delivered.duplicates());
        BATCH.recordSince(started);
        recovered();
        return Round.DELIVERED;
    }

    /**
     * The delay after a round with this outcome; a failure doubles the next backoff up to its ceiling. Exposed
     * for tests; only the relay thread calls it.
     */
    public Duration delayAfter(Round round) {
        return switch (round) {
            case DELIVERED -> lastBatchFull ? Duration.ZERO : settings.coalesce();
            case IDLE -> settings.idlePoll();
            case FAILED -> {
                Duration delay = backoff;
                Duration doubled = backoff.multipliedBy(2);
                backoff = doubled.compareTo(settings.maxBackoff()) > 0 ? settings.maxBackoff() : doubled;
                yield delay;
            }
        };
    }

    /** Starts the relay thread and registers the backlog gauge. */
    public synchronized void start() {
        if (thread != null) throw new IllegalStateException("The relay was started already");
        PerfMetrics.gauge("transaction.relay.backlog", store::countUndelivered);
        running = true;
        thread = Thread.ofPlatform().name("scan-event-relay").daemon(true).start(this::run);
    }

    /** Stops the relay thread, waiting briefly for its current round. */
    @Override
    public void close() {
        running = false;
        Thread current = thread;
        if (current == null) return;
        current.interrupt();
        try {
            current.join(Duration.ofSeconds(6));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        while (running) {
            Duration delay = delayAfter(runOnce());
            if (delay.isZero()) continue;
            try {
                Thread.sleep(delay);
            } catch (InterruptedException interrupted) {
                return;
            }
        }
    }

    private Round failed(String message) {
        FAILURES.increment();
        if (!failing && message != null && running) {
            System.err.println("[transaction] Outbox relay " + message + "; backing off and retrying");
        }
        failing = true;
        return Round.FAILED;
    }

    private void recovered() {
        if (failing && running) System.out.println("[transaction] Outbox relay delivering again");
        failing = false;
        backoff = settings.minBackoff();
    }
}
