package events;

import faults.FaultHooks;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * Runs one consumer group, or one manually assigned reader, over {@link RecordStream}s (D6). Kafka-free:
 * the messaging adapter supplies the streams.
 *
 * <ul>
 *   <li><b>Acknowledge after commit</b> (FR-011): each record is decoded with {@link EnvelopeCodec}, handed to
 *       the {@link EventHandler}, and its offset is committed only after the handler returned, which means
 *       its local transaction committed, or after its {@code dead_letter_event} row committed. Offsets are
 *       committed with {@code commitAsync} after each poll, and with {@code commitSync} on revocation and
 *       shutdown.</li>
 *   <li><b>Failures</b> follow {@link ParkingPolicy}: the failed record's partition alone is paused and
 *       rewound, and the loop retries that record while every other partition keeps flowing. A transient
 *       failure is retried indefinitely with backoff; a permanent one is attempted five times, 200 ms apart,
 *       then parked, and its offset committed. A record whose parking fails stays in retry.</li>
 *   <li><b>Stall alarm</b>: a record in transient retry for longer than 60 s is logged with its topic,
 *       partition, offset, and age once per status interval and reported by {@link #stalledPartitions}.</li>
 *   <li><b>Modes</b>: group mode runs {@code N} threads with one stream each; manual-assignment mode runs one
 *       thread that seeks each partition to a supplied offset (0 for the catalog).</li>
 *   <li><b>Batch group mode</b> ({@link #batchGroup}, Analytics' scan ingestion, D10): the consumer's own
 *       database is the authority for its progress (FR-049). On every assignment each partition seeks to the
 *       stored offset, whatever the group's committed offset; a stored offset beyond the log end (a recreated
 *       topic) raises an alarm and reads from the earliest offset. Each poll's records of one partition go to
 *       the {@link BatchEventHandler} as one batch; a record that does not decode splits the batch, and when a
 *       batch fails its records are tried one at a time under the rules above, so only the failing record
 *       waits or is parked.</li>
 * </ul>
 *
 * <p>A stream failure ends that stream; the thread opens a new one after a backoff, so a consumer survives
 * broker restarts. Test-only fault hook {@code consumer.afterHandlerCommit}, between the handler's return
 * and the offset commit, unless {@link Settings#faultHooks} is off. Timers {@code consumer.<name>.lag} (record timestamp to handler start) and
 * {@code consumer.<name>.handler} (per record, or per batch in batch mode), counters {@code consumer.<name>.parked}
 * and {@code consumer.<name>.offsetResets}, gauge {@code consumer.<name>.lagRecords} ({@link #lag}).</p>
 */
public final class ConsumerLoop implements AutoCloseable {
    /**
     * @param faultHooks whether this loop hits the Saga's test-only hook {@code consumer.afterHandlerCommit}; the
     *                   catalog reader turns it off, so a halt armed for a checkout consumer never fires at startup
     */
    public record Settings(int threads, ParkingPolicy policy, Duration pollTimeout, Duration statusInterval,
                           boolean faultHooks) {
        public Settings {
            if (threads < 1) throw new IllegalArgumentException("threads must be positive");
            Objects.requireNonNull(policy, "policy");
            if (pollTimeout == null || pollTimeout.isNegative() || pollTimeout.isZero()) {
                throw new IllegalArgumentException("pollTimeout must be positive");
            }
            if (statusInterval == null || statusInterval.isNegative()) {
                throw new IllegalArgumentException("statusInterval must not be negative");
            }
        }

        public static Settings defaults() {
            return new Settings(1, ParkingPolicy.defaults(), Duration.ofMillis(100), Duration.ofSeconds(30), true);
        }

        public Settings withThreads(int count) {
            return new Settings(count, policy, pollTimeout, statusInterval, faultHooks);
        }

        public Settings withPolicy(ParkingPolicy replacement) {
            return new Settings(threads, replacement, pollTimeout, statusInterval, faultHooks);
        }

        public Settings withStatusInterval(Duration interval) {
            return new Settings(threads, policy, pollTimeout, interval, faultHooks);
        }

        public Settings withoutFaultHooks() {
            return new Settings(threads, policy, pollTimeout, statusInterval, false);
        }
    }

    /** A partition whose record has waited in transient retry longer than the stall threshold. */
    public record StalledPartition(String topic, int partition, long offset, Duration age) { }

    private final String service;
    private final String name;
    private final String group;
    private final List<String> topics;
    private final List<RecordStream.Partition> manualPartitions;
    private final ToLongFunction<RecordStream.Partition> startOffset;
    private final Supplier<RecordStream> streams;
    private final EventHandler handler;
    /** Null except in batch group mode, where {@link #handler} hands it batches of one record. */
    private final BatchEventHandler batchHandler;
    private final DeadLetterStore deadLetters;
    private final Settings settings;
    private final List<Worker> workers = new ArrayList<>();
    private final LatencyHistogram lagTimer;
    private final LatencyHistogram handlerTimer;
    private final LongAdder parkedCounter;
    private final LongAdder offsetResetCounter;
    private final AtomicLong offsetResets = new AtomicLong();
    private volatile boolean running;

    private ConsumerLoop(String service, String name, String group, List<String> topics,
                         List<RecordStream.Partition> manualPartitions, ToLongFunction<RecordStream.Partition> startOffset,
                         Supplier<RecordStream> streams, EventHandler handler, BatchEventHandler batchHandler,
                         DeadLetterStore deadLetters, Settings settings) {
        this.service = Objects.requireNonNull(service, "service");
        this.name = Objects.requireNonNull(name, "name");
        this.group = group;
        this.topics = topics;
        this.manualPartitions = manualPartitions;
        this.startOffset = startOffset;
        this.streams = Objects.requireNonNull(streams, "streams");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.batchHandler = batchHandler;
        this.deadLetters = Objects.requireNonNull(deadLetters, "deadLetters");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.lagTimer = PerfMetrics.timer("consumer." + name + ".lag");
        this.handlerTimer = PerfMetrics.timer("consumer." + name + ".handler");
        this.parkedCounter = PerfMetrics.counter("consumer." + name + ".parked");
        this.offsetResetCounter = PerfMetrics.counter("consumer." + name + ".offsetResets");
        PerfMetrics.gauge("consumer." + name + ".lagRecords", this::lag);
    }

    /** Group mode: {@code settings.threads()} members of {@code group} reading {@code topic}. */
    public static ConsumerLoop group(String service, String group, String topic, Supplier<RecordStream> streams,
                                     EventHandler handler, DeadLetterStore deadLetters, Settings settings) {
        return new ConsumerLoop(service, group, group, List.of(topic), null, null, streams, handler, null, deadLetters,
                settings);
    }

    /**
     * Batch group mode: one member of {@code group} reading {@code topic}, which seeks every assigned partition
     * to {@code storedOffset}'s offset and hands each poll's records of a partition to {@code handler} together.
     */
    public static ConsumerLoop batchGroup(String service, String group, String topic,
                                          ToLongFunction<RecordStream.Partition> storedOffset,
                                          Supplier<RecordStream> streams, BatchEventHandler handler,
                                          DeadLetterStore deadLetters, Settings settings) {
        Objects.requireNonNull(handler, "handler");
        return new ConsumerLoop(service, group, group, List.of(topic), null, Objects.requireNonNull(storedOffset),
                streams, event -> handler.handleBatch(List.of(event)), handler, deadLetters, settings.withThreads(1));
    }

    /** Manual-assignment mode: one thread reads {@code partitions}, each from {@code startOffset}'s offset. */
    public static ConsumerLoop manual(String service, String name, Collection<RecordStream.Partition> partitions,
                                      ToLongFunction<RecordStream.Partition> startOffset, Supplier<RecordStream> streams,
                                      EventHandler handler, DeadLetterStore deadLetters, Settings settings) {
        return new ConsumerLoop(service, name, null, null, List.copyOf(partitions), Objects.requireNonNull(startOffset),
                streams, handler, null, deadLetters, settings.withThreads(1));
    }

    public synchronized void start() {
        if (running || !workers.isEmpty()) throw new IllegalStateException("consumer already started");
        running = true;
        for (int i = 0; i < settings.threads(); i++) {
            Worker worker = new Worker();
            Thread thread = new Thread(worker, service + "-" + name + "-" + i);
            thread.setDaemon(true);
            worker.thread = thread;
            workers.add(worker);
            thread.start();
        }
    }

    /** Stops every thread; each commits what it processed with {@code commitSync} and closes its stream. */
    public void stop(Duration timeout) {
        List<Worker> current;
        synchronized (this) {
            if (!running) return;
            running = false;
            current = List.copyOf(workers);
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        for (Worker worker : current) worker.wake();
        for (Worker worker : current) {
            long left = deadline - System.nanoTime();
            if (left <= 0) break;
            try {
                worker.thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(left)));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void close() {
        stop(Duration.ofSeconds(5));
    }

    /** Records between each assigned partition's committed offset and its end, summed over all threads. */
    public long lag() {
        long total = 0;
        synchronized (this) {
            for (Worker worker : workers) total += worker.lag.get();
        }
        return total;
    }

    public List<StalledPartition> stalledPartitions() {
        List<StalledPartition> stalled = new ArrayList<>();
        synchronized (this) {
            for (Worker worker : workers) stalled.addAll(worker.stalled);
        }
        return stalled;
    }

    /** Records this loop parked since it started. */
    public long parked() {
        long total = 0;
        synchronized (this) {
            for (Worker worker : workers) total += worker.parked.get();
        }
        return total;
    }

    /** Assignments, in batch group mode, whose stored offset lay beyond the log end and were read from the earliest. */
    public long offsetResets() {
        return offsetResets.get();
    }

    /** The result of one attempt at a record. */
    private record Outcome(ParkingPolicy.Verdict verdict, String eventId, String eventType, Integer schemaVersion) {
        boolean succeeded() { return verdict == null; }
    }

    /** A record its partition waits on. */
    private static final class Retry {
        final RecordStream.Record record;
        final long firstFailureNanos;
        Outcome last;
        int permanentAttempts;
        int transientFailures;
        int parkFailures;
        boolean parkPending;
        long nextNanos;
        long lastStallLogNanos;

        Retry(RecordStream.Record record, long now) {
            this.record = record;
            this.firstFailureNanos = now;
        }
    }

    private final class Worker implements Runnable, RecordStream.Listener {
        private final Map<RecordStream.Partition, Long> processed = new HashMap<>();
        private final Map<RecordStream.Partition, Long> committedNext = new HashMap<>();
        private final Map<RecordStream.Partition, Retry> retries = new HashMap<>();
        private final AtomicLong lag = new AtomicLong();
        private final AtomicLong parked = new AtomicLong();
        private volatile List<StalledPartition> stalled = List.of();
        private volatile RecordStream stream;
        private Thread thread;

        void wake() {
            RecordStream current = stream;
            if (current != null) current.wakeup();
        }

        @Override
        public void run() {
            int failures = 0;
            while (running) {
                RecordStream current = null;
                try {
                    current = streams.get();
                    stream = current;
                    open(current);
                    failures = 0;
                    while (running) {
                        retryDue(current);
                        List<RecordStream.Record> records = current.poll(pollTimeout());
                        Set<RecordStream.Partition> waiting = new HashSet<>(retries.keySet());
                        if (batchHandler != null) {
                            for (List<RecordStream.Record> run : byPartition(records).values()) {
                                if (!waiting.contains(run.getFirst().position())) processBatch(current, run);
                            }
                        } else {
                            for (RecordStream.Record record : records) {
                                RecordStream.Partition partition = record.position();
                                if (waiting.contains(partition)) continue;
                                if (!process(current, record)) waiting.add(partition);
                            }
                        }
                        commit(current, false);
                        observe(current);
                    }
                } catch (RuntimeException failure) {
                    if (running) {
                        failures++;
                        System.err.println("[" + service + "] Consumer " + name + " stream failed; reopening: " + failure);
                    }
                } finally {
                    if (current != null) {
                        try {
                            commit(current, true);
                        } catch (RuntimeException failure) {
                            System.err.println("[" + service + "] Consumer " + name + " final commit failed: " + failure);
                        }
                        try {
                            current.close();
                        } catch (RuntimeException ignored) {
                            // The stream is being discarded either way.
                        }
                    }
                    stream = null;
                    retries.clear();
                    processed.clear();
                    committedNext.clear();
                    lag.set(0);
                    stalled = List.of();
                }
                if (running) sleep(settings.policy().transientBackoff(failures));
            }
        }

        private void open(RecordStream current) {
            if (group != null) {
                current.subscribe(group, topics, this);
                return;
            }
            current.assign(manualPartitions);
            for (RecordStream.Partition partition : manualPartitions) {
                current.seek(partition, startOffset.applyAsLong(partition));
            }
        }

        @Override
        public void onAssigned(Collection<RecordStream.Partition> partitions) {
            // Plain group mode reads from the group's committed offsets; batch mode from the stored ones.
            if (startOffset == null) return;
            for (RecordStream.Partition partition : partitions) seekStored(stream, partition);
        }

        private void seekStored(RecordStream current, RecordStream.Partition partition) {
            long stored = startOffset.applyAsLong(partition);
            long end = current.endOffset(partition);
            if (stored <= end) {
                current.seek(partition, stored);
                return;
            }
            long earliest = current.beginningOffset(partition);
            offsetResets.incrementAndGet();
            offsetResetCounter.increment();
            System.err.println("[" + service + "] ALARM Consumer " + name + " stored offset " + stored + " of "
                    + partition.topic() + "-" + partition.partition() + " is beyond the log end " + end
                    + " (was the topic recreated?); reading from the earliest offset " + earliest);
            current.seek(partition, earliest);
        }

        /**
         * Batch mode: hands {@code run}, one poll's records of one partition, to the batch handler in as few
         * batches as possible. A record that does not decode ends the batch before it and is tried alone; when a
         * batch fails, its records are tried one at a time, so only the failing record waits or is parked.
         */
        private void processBatch(RecordStream current, List<RecordStream.Record> run) {
            int index = 0;
            while (index < run.size()) {
                List<DecodedEvent> events = new ArrayList<>();
                int end = index;
                for (; end < run.size(); end++) {
                    RecordStream.Record record = run.get(end);
                    try {
                        events.add(EnvelopeCodec.decode(record.value(), record.key())
                                .at(record.topic(), record.partition(), record.offset()));
                    } catch (EventDecodeException undecodable) {
                        break;
                    }
                }
                if (!events.isEmpty()) {
                    if (attemptBatch(run.subList(index, end), events)) {
                        processed.put(run.getFirst().position(), run.get(end - 1).offset() + 1);
                    } else {
                        for (int single = index; single < end; single++) {
                            if (!process(current, run.get(single))) return;
                        }
                    }
                    index = end;
                }
                if (index < run.size()) {
                    if (!process(current, run.get(index))) return;
                    index++;
                }
            }
        }

        /** True when the batch handler committed {@code events}, the decoded {@code records}. */
        private boolean attemptBatch(List<RecordStream.Record> records, List<DecodedEvent> events) {
            long now = System.currentTimeMillis();
            for (RecordStream.Record record : records) {
                lagTimer.record(TimeUnit.MILLISECONDS.toNanos(Math.max(0, now - record.timestampMs())));
            }
            try {
                long started = System.nanoTime();
                batchHandler.handleBatch(events);
                handlerTimer.record(System.nanoTime() - started);
                if (settings.faultHooks()) FaultHooks.hit("consumer.afterHandlerCommit");
                return true;
            } catch (RuntimeException failure) {
                if (running) {
                    System.err.println("[" + service + "] Consumer " + name + " batch of " + events.size()
                            + " records failed; trying them one at a time: " + failure);
                }
                return false;
            }
        }

        @Override
        public void onRevoked(Collection<RecordStream.Partition> partitions) {
            Map<RecordStream.Partition, Long> owned = new HashMap<>();
            for (RecordStream.Partition partition : partitions) {
                Long next = processed.remove(partition);
                if (next != null) owned.put(partition, next);
                retries.remove(partition);
                committedNext.remove(partition);
            }
            if (!owned.isEmpty()) stream.commitSync(owned);
        }

        private Duration pollTimeout() {
            long timeout = settings.pollTimeout().toNanos();
            long now = System.nanoTime();
            for (Retry retry : retries.values()) timeout = Math.min(timeout, Math.max(1_000_000L, retry.nextNanos - now));
            return Duration.ofNanos(timeout);
        }

        /** Handles a freshly polled record; false when its partition now waits in retry. */
        private boolean process(RecordStream current, RecordStream.Record record) {
            Outcome outcome = attempt(record);
            RecordStream.Partition partition = record.position();
            if (outcome.succeeded()) {
                processed.put(partition, record.offset() + 1);
                return true;
            }
            long now = System.nanoTime();
            Retry retry = new Retry(record, now);
            if (!schedule(retry, outcome, now)) {
                if (park(retry)) {
                    processed.put(partition, record.offset() + 1);
                    return true;
                }
                retry.parkPending = true;
                retry.parkFailures = 1;
                retry.nextNanos = now + settings.policy().transientBackoff(1).toNanos();
            }
            current.pause(List.of(partition));
            current.seek(partition, record.offset());
            retries.put(partition, retry);
            return false;
        }

        /**
         * Records a failed attempt and sets the next try; false when the permanent attempts are used up and the
         * record must be parked.
         */
        private boolean schedule(Retry retry, Outcome outcome, long now) {
            retry.last = outcome;
            ParkingPolicy policy = settings.policy();
            if (outcome.verdict().transientFailure()) {
                retry.transientFailures++;
                retry.nextNanos = now + policy.transientBackoff(retry.transientFailures).toNanos();
                return true;
            }
            retry.permanentAttempts++;
            if (retry.permanentAttempts >= policy.maxAttempts()) return false;
            retry.nextNanos = now + policy.permanentRetryDelay().toNanos();
            return true;
        }

        private void retryDue(RecordStream current) {
            if (retries.isEmpty()) return;
            long now = System.nanoTime();
            for (Map.Entry<RecordStream.Partition, Retry> entry : new ArrayList<>(retries.entrySet())) {
                Retry retry = entry.getValue();
                if (retry.nextNanos > now) continue;
                if (!retry.parkPending) {
                    Outcome outcome = attempt(retry.record);
                    if (outcome.succeeded()) {
                        finish(current, entry.getKey(), retry);
                        continue;
                    }
                    if (schedule(retry, outcome, System.nanoTime())) continue;
                }
                if (park(retry)) {
                    finish(current, entry.getKey(), retry);
                } else {
                    retry.parkPending = true;
                    retry.parkFailures++;
                    retry.nextNanos = System.nanoTime() + settings.policy().transientBackoff(retry.parkFailures).toNanos();
                }
            }
        }

        private void finish(RecordStream current, RecordStream.Partition partition, Retry retry) {
            long next = retry.record.offset() + 1;
            retries.remove(partition);
            processed.put(partition, next);
            current.seek(partition, next);
            current.resume(List.of(partition));
        }

        private Outcome attempt(RecordStream.Record record) {
            DecodedEvent event = null;
            try {
                event = EnvelopeCodec.decode(record.value(), record.key())
                        .at(record.topic(), record.partition(), record.offset());
                lagTimer.record(TimeUnit.MILLISECONDS.toNanos(Math.max(0, System.currentTimeMillis() - record.timestampMs())));
                long started = System.nanoTime();
                handler.handle(event);
                handlerTimer.record(System.nanoTime() - started);
                if (settings.faultHooks()) FaultHooks.hit("consumer.afterHandlerCommit");
                return new Outcome(null, event.eventId(), event.type().typeName(), event.schemaVersion());
            } catch (EventDecodeException failure) {
                return new Outcome(settings.policy().classify(failure), failure.eventId(), failure.eventType(),
                        failure.schemaVersion());
            } catch (RuntimeException failure) {
                return new Outcome(settings.policy().classify(failure), event == null ? null : event.eventId(),
                        event == null ? null : event.type().typeName(), event == null ? null : event.schemaVersion());
            }
        }

        /** Writes the record's {@code dead_letter_event} row; false when that write failed. */
        private boolean park(Retry retry) {
            Outcome outcome = retry.last;
            RecordStream.Record record = retry.record;
            ParkedRecord parkedRecord = new ParkedRecord(outcome.eventId(), record.topic(), record.partition(),
                    record.offset(), outcome.eventType(), outcome.schemaVersion(), outcome.verdict().reason(),
                    outcome.verdict().detail(), record.value(), Instant.now());
            try {
                deadLetters.park(parkedRecord);
            } catch (RuntimeException failure) {
                System.err.println("[" + service + "] Consumer " + name + " could not park " + record.topic() + "-"
                        + record.partition() + "@" + record.offset() + "; retrying: " + failure);
                return false;
            }
            parked.incrementAndGet();
            parkedCounter.increment();
            System.err.println("[" + service + "] WARN Consumer " + name + " parked " + record.topic() + "-"
                    + record.partition() + "@" + record.offset() + " event " + outcome.eventId() + " as "
                    + outcome.verdict().reason() + ": " + outcome.verdict().detail());
            return true;
        }

        private void commit(RecordStream current, boolean sync) {
            if (processed.isEmpty()) return;
            Map<RecordStream.Partition, Long> offsets = Map.copyOf(processed);
            if (sync) current.commitSync(offsets);
            else current.commitAsync(offsets);
            committedNext.putAll(offsets);
            processed.keySet().removeAll(offsets.keySet());
        }

        /** Updates the lag gauge and the stall alarm on the polling thread. */
        private void observe(RecordStream current) {
            long total = 0;
            for (RecordStream.Partition partition : current.assignment()) {
                try {
                    OptionalLong behind = current.currentLag(partition);
                    if (behind.isEmpty()) continue;
                    long position = current.position(partition);
                    long end = position + behind.getAsLong();
                    long from = processed.getOrDefault(partition, committedNext.getOrDefault(partition, position));
                    Retry retry = retries.get(partition);
                    if (retry != null) from = Math.min(from, retry.record.offset());
                    total += Math.max(0, end - from);
                } catch (RuntimeException ignored) {
                    // A partition being revoked has no position; it is counted again once assigned.
                }
            }
            lag.set(total);

            long now = System.nanoTime();
            long threshold = settings.policy().stallAfter().toNanos();
            List<StalledPartition> current0 = new ArrayList<>();
            for (Retry retry : retries.values()) {
                if (retry.last == null || !retry.last.verdict().transientFailure()) continue;
                long age = now - retry.firstFailureNanos;
                if (age < threshold) continue;
                RecordStream.Record record = retry.record;
                current0.add(new StalledPartition(record.topic(), record.partition(), record.offset(), Duration.ofNanos(age)));
                if (retry.lastStallLogNanos == 0 || now - retry.lastStallLogNanos >= settings.statusInterval().toNanos()) {
                    retry.lastStallLogNanos = now;
                    System.err.println("[" + service + "] ALARM Consumer " + name + " stalled on " + record.topic() + "-"
                            + record.partition() + "@" + record.offset() + " for " + Duration.ofNanos(age).toSeconds()
                            + " s: " + retry.last.verdict().detail());
                }
            }
            stalled = List.copyOf(current0);
        }
    }

    /** {@code records} grouped by partition, each partition's records in poll order. */
    private static Map<RecordStream.Partition, List<RecordStream.Record>> byPartition(List<RecordStream.Record> records) {
        Map<RecordStream.Partition, List<RecordStream.Record>> runs = new LinkedHashMap<>();
        for (RecordStream.Record record : records) {
            runs.computeIfAbsent(record.position(), ignored -> new ArrayList<>()).add(record);
        }
        return runs;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(Math.max(1, duration.toMillis()));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
