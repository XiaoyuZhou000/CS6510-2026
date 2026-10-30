package support;

import events.RecordSender;
import events.RecordStream;
import events.TopicNames;
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A deterministic stand-in for the Kafka broker (D14): a {@link RecordSender} and a factory of
 * {@link RecordStream}s with Kafka's semantics where the Saga, catalog, and analytics suites depend on them.
 *
 * <ul>
 *   <li><b>Partitions</b>: a keyed record goes to {@code murmur2(key) % partitions}, Kafka's default
 *       partitioner, with the production partition counts ({@link #withCatalogTopics}).</li>
 *   <li><b>Groups</b>: members of one group share a topic's partitions round-robin; a join or leave
 *       rebalances, calling {@code onRevoked} on the old owner and {@code onAssigned} on the new one from
 *       inside their next {@code poll}. A newly assigned partition starts at the group's committed offset,
 *       or at 0.</li>
 *   <li><b>Fault controls</b>: duplicate the next sends, drop the next sends in transit (they fail
 *       unconfirmed), delay delivery, reorder across partitions within a poll, lose the next offset commits
 *       (so their records are redelivered after a rebalance), pause the broker (sends fail, polls return
 *       nothing, commits throw), refuse sends as a broker whose storage is full, kill the next consumer
 *       that commits, before the commit lands, forget a group's committed offsets, and recreate a topic.</li>
 * </ul>
 *
 * <p>All randomness comes from the seed given to the constructor, so a run is reproducible.</p>
 */
public final class InMemoryEventBus implements RecordSender {
    private record Stored(String key, byte[] value, long timestampMs, long visibleAtNanos) { }

    private static final class Log {
        final List<List<Stored>> partitions = new ArrayList<>();

        Log(int count) {
            for (int i = 0; i < count; i++) partitions.add(new ArrayList<>());
        }
    }

    private final Object lock = new Object();
    private final Map<String, Log> topics = new LinkedHashMap<>();
    private final Map<String, Map<RecordStream.Partition, Long>> committed = new HashMap<>();
    private final Map<String, List<Stream>> members = new HashMap<>();
    private final Random random;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "in-memory-bus-timer");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicInteger duplicateNext = new AtomicInteger();
    private final AtomicInteger dropNext = new AtomicInteger();
    private final AtomicInteger loseNextCommits = new AtomicInteger();
    private final AtomicInteger killNextCommitter = new AtomicInteger();
    private volatile long deliveryDelayNanos;
    private volatile long confirmDelayNanos;
    private volatile boolean reorder;
    private volatile boolean brokerDown;
    private volatile boolean storageFull;
    private final List<String> sendLog = new CopyOnWriteArrayList<>();

    public InMemoryEventBus(long seed) {
        this.random = new Random(seed);
    }

    /** A bus with the four catalog topics of {@code names} at their production partition counts. */
    public static InMemoryEventBus withCatalogTopics(TopicNames names, long seed) {
        InMemoryEventBus bus = new InMemoryEventBus(seed);
        for (TopicNames.Topic topic : TopicNames.Topic.values()) bus.createTopic(names.name(topic), topic.partitions());
        return bus;
    }

    public void createTopic(String topic, int partitions) {
        synchronized (lock) {
            if (topics.containsKey(topic)) throw new IllegalStateException("topic exists: " + topic);
            topics.put(topic, new Log(partitions));
        }
    }

    /** Kafka's default partition for {@code key}. */
    public int partitionFor(String topic, String key) {
        synchronized (lock) {
            return partitionOf(log(topic), key);
        }
    }

    @Override
    public CompletableFuture<Void> send(String topic, String key, byte[] value) {
        if (brokerDown) return CompletableFuture.failedFuture(new IllegalStateException("broker unavailable"));
        if (storageFull) return CompletableFuture.failedFuture(new IllegalStateException("broker storage is full"));
        if (dropNext.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            return CompletableFuture.failedFuture(new IllegalStateException("record lost in transit"));
        }
        int copies = duplicateNext.getAndUpdate(n -> Math.max(0, n - 1)) > 0 ? 2 : 1;
        synchronized (lock) {
            Log log = log(topic);
            List<Stored> partition = log.partitions.get(partitionOf(log, key));
            long visibleAt = System.nanoTime() + deliveryDelayNanos;
            for (int i = 0; i < copies; i++) {
                partition.add(new Stored(key, value.clone(), System.currentTimeMillis(), visibleAt));
            }
            sendLog.add(topic + "/" + key);
        }
        long confirmDelay = confirmDelayNanos;
        if (confirmDelay <= 0) return CompletableFuture.completedFuture(null);
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        timer.schedule(() -> confirmation.complete(null), confirmDelay, TimeUnit.NANOSECONDS);
        return confirmation;
    }

    /** A new consumer, unsubscribed and unassigned. */
    public RecordStream stream() {
        return new Stream();
    }

    // ---- fault controls ----

    /** The next {@code count} sends are stored twice (a producer retry that the broker also kept). */
    public void duplicateNextSends(int count) { duplicateNext.set(count); }

    /** The next {@code count} sends are lost before the broker stores them; their futures fail. */
    public void dropNextSends(int count) { dropNext.set(count); }

    /** Records sent from now on become visible to consumers only after {@code delay}. */
    public void delayDelivery(Duration delay) { deliveryDelayNanos = delay.toNanos(); }

    /** Confirmations of sends from now on arrive after {@code delay}. */
    public void delayConfirmations(Duration delay) { confirmDelayNanos = delay.toNanos(); }

    /** Each poll returns records of different partitions in a seeded random interleaving. */
    public void reorderAcrossPartitions(boolean enabled) { reorder = enabled; }

    /** The next {@code count} offset commits report success but are not stored. */
    public void loseNextCommits(int count) { loseNextCommits.set(count); }

    /** The next consumer that commits dies before the commit lands: its stream is closed and its group rebalances. */
    public void killNextCommittingConsumer() { killNextCommitter.set(1); }

    /** While paused, sends fail, polls return nothing, and commits throw, like an unreachable broker. */
    public void pauseBroker(boolean paused) { brokerDown = paused; }

    /** While full, sends fail; consumers keep reading what is stored. */
    public void refuseSends(boolean refused) { storageFull = refused; }

    /** Forgets every committed offset of {@code group}, as deleting the group's offsets on a broker would. */
    public void resetCommittedOffsets(String group) {
        synchronized (lock) {
            committed.remove(group);
        }
    }

    /**
     * Empties {@code topic} and forgets every group's committed offsets of it, as deleting and recreating the
     * topic would. A consumer positioned beyond the new end reads from offset 0 again
     * ({@code auto.offset.reset=earliest}).
     */
    public void recreateTopic(String topic) {
        synchronized (lock) {
            for (List<Stored> partition : log(topic).partitions) partition.clear();
            for (Map<RecordStream.Partition, Long> offsets : committed.values()) {
                offsets.keySet().removeIf(partition -> partition.topic().equals(topic));
            }
        }
    }

    // ---- inspection ----

    public int partitions(String topic) {
        synchronized (lock) {
            return log(topic).partitions.size();
        }
    }

    public long endOffset(String topic, int partition) {
        synchronized (lock) {
            return log(topic).partitions.get(partition).size();
        }
    }

    /** Every stored value of {@code topic}, partition by partition. */
    public List<byte[]> values(String topic) {
        synchronized (lock) {
            List<byte[]> values = new ArrayList<>();
            for (List<Stored> partition : log(topic).partitions) {
                for (Stored stored : partition) values.add(stored.value().clone());
            }
            return values;
        }
    }

    public Long committedOffset(String group, String topic, int partition) {
        synchronized (lock) {
            Map<RecordStream.Partition, Long> offsets = committed.get(group);
            return offsets == null ? null : offsets.get(new RecordStream.Partition(topic, partition));
        }
    }

    /** {@code topic/key} of every stored send, in order. */
    public List<String> sendLog() {
        return List.copyOf(sendLog);
    }

    private Log log(String topic) {
        Log log = topics.get(topic);
        if (log == null) throw new IllegalArgumentException("unknown topic " + topic);
        return log;
    }

    private static int partitionOf(Log log, String key) {
        return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % log.partitions.size();
    }

    /** Recomputes {@code group}'s assignment; callbacks run on each member's next poll. */
    private void rebalance(String group) {
        List<Stream> active = members.getOrDefault(group, List.of());
        Map<Stream, Set<RecordStream.Partition>> next = new HashMap<>();
        for (Stream member : active) next.put(member, new LinkedHashSet<>());
        if (!active.isEmpty()) {
            Set<String> subscribed = new LinkedHashSet<>();
            for (Stream member : active) subscribed.addAll(member.topics);
            int index = 0;
            for (String topic : subscribed) {
                for (int p = 0; p < log(topic).partitions.size(); p++) {
                    next.get(active.get(index++ % active.size())).add(new RecordStream.Partition(topic, p));
                }
            }
        }
        for (Stream member : active) member.pendingAssignment = next.get(member);
    }

    private final class Stream implements RecordStream {
        private String group;
        private List<String> topics = List.of();
        private Listener listener;
        private final Set<Partition> assigned = new LinkedHashSet<>();
        private final Set<Partition> paused = new LinkedHashSet<>();
        private final Map<Partition, Long> positions = new HashMap<>();
        private volatile Set<Partition> pendingAssignment;
        private volatile boolean woken;
        private volatile boolean closed;

        @Override
        public void subscribe(String groupName, Collection<String> topicNames, Listener assignmentListener) {
            synchronized (lock) {
                for (String topic : topicNames) log(topic);
                group = groupName;
                topics = List.copyOf(topicNames);
                listener = assignmentListener;
                members.computeIfAbsent(groupName, ignored -> new ArrayList<>()).add(this);
                rebalance(groupName);
            }
        }

        @Override
        public void assign(Collection<Partition> partitions) {
            synchronized (lock) {
                for (Partition partition : partitions) {
                    log(partition.topic());
                    assigned.add(partition);
                    positions.putIfAbsent(partition, 0L);
                }
            }
        }

        @Override
        public List<Record> poll(Duration timeout) {
            ensureOpen();
            long deadline = System.nanoTime() + timeout.toNanos();
            while (true) {
                applyRebalance();
                List<Record> records = brokerDown ? List.of() : fetch();
                if (!records.isEmpty() || woken || System.nanoTime() >= deadline) {
                    woken = false;
                    return records;
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return List.of();
                }
            }
        }

        /**
         * Moves towards the group's target assignment. A partition is revoked first, with the listener committing
         * what it processed; another member takes it only once no member still holds it, and then starts at the
         * group's committed offset, as Kafka's rebalance protocol guarantees.
         */
        private void applyRebalance() {
            Set<Partition> target = pendingAssignment;
            if (target == null) return;
            Set<Partition> revoked;
            synchronized (lock) {
                revoked = new LinkedHashSet<>(assigned);
                revoked.removeAll(target);
            }
            if (!revoked.isEmpty() && listener != null) listener.onRevoked(revoked);
            Set<Partition> added = new LinkedHashSet<>();
            synchronized (lock) {
                assigned.removeAll(revoked);
                paused.removeAll(revoked);
                for (Partition partition : revoked) positions.remove(partition);
                Map<Partition, Long> offsets = committed.getOrDefault(group, Map.of());
                for (Partition partition : target) {
                    if (assigned.contains(partition) || heldByAnotherMember(partition)) continue;
                    assigned.add(partition);
                    positions.put(partition, offsets.getOrDefault(partition, 0L));
                    added.add(partition);
                }
            }
            if (!added.isEmpty() && listener != null) listener.onAssigned(added);
        }

        private boolean heldByAnotherMember(Partition partition) {
            for (Stream member : members.getOrDefault(group, List.of())) {
                if (member != this && member.assigned.contains(partition)) return true;
            }
            return false;
        }

        private List<Record> fetch() {
            List<List<Record>> byPartition = new ArrayList<>();
            long now = System.nanoTime();
            synchronized (lock) {
                for (Partition partition : assigned) {
                    if (paused.contains(partition)) continue;
                    List<Stored> stored = log(partition.topic()).partitions.get(partition.partition());
                    long position = positions.get(partition);
                    if (position > stored.size()) {
                        // Out of range, as after a recreated topic: auto.offset.reset=earliest.
                        position = 0;
                        positions.put(partition, 0L);
                    }
                    List<Record> records = new ArrayList<>();
                    for (long offset = position; offset < stored.size() && records.size() < 500; offset++) {
                        Stored item = stored.get((int) offset);
                        if (item.visibleAtNanos() > now) break;
                        records.add(new Record(partition.topic(), partition.partition(), offset, item.key(),
                                item.value().clone(), item.timestampMs()));
                    }
                    if (!records.isEmpty()) {
                        positions.put(partition, records.get(records.size() - 1).offset() + 1);
                        byPartition.add(records);
                    }
                }
                if (!reorder) {
                    List<Record> all = new ArrayList<>();
                    byPartition.forEach(all::addAll);
                    return all;
                }
                // Interleave partitions randomly while keeping each partition's own order.
                List<Record> all = new ArrayList<>();
                List<Integer> cursor = new ArrayList<>(Collections.nCopies(byPartition.size(), 0));
                while (true) {
                    List<Integer> open = new ArrayList<>();
                    for (int i = 0; i < byPartition.size(); i++) if (cursor.get(i) < byPartition.get(i).size()) open.add(i);
                    if (open.isEmpty()) return all;
                    int pick = open.get(random.nextInt(open.size()));
                    all.add(byPartition.get(pick).get(cursor.get(pick)));
                    cursor.set(pick, cursor.get(pick) + 1);
                }
            }
        }

        @Override
        public void pause(Collection<Partition> partitions) {
            synchronized (lock) {
                paused.addAll(partitions);
            }
        }

        @Override
        public void resume(Collection<Partition> partitions) {
            synchronized (lock) {
                paused.removeAll(partitions);
            }
        }

        @Override
        public void seek(Partition partition, long offset) {
            synchronized (lock) {
                if (!assigned.contains(partition)) throw new IllegalStateException("not assigned: " + partition);
                positions.put(partition, offset);
            }
        }

        @Override
        public void commitAsync(Map<Partition, Long> nextOffsets) {
            commit(nextOffsets);
        }

        @Override
        public void commitSync(Map<Partition, Long> nextOffsets) {
            commit(nextOffsets);
        }

        private void commit(Map<Partition, Long> nextOffsets) {
            ensureOpen();
            if (nextOffsets.isEmpty()) return;
            if (brokerDown) throw new IllegalStateException("broker unavailable: commit failed");
            if (killNextCommitter.getAndSet(0) > 0) {
                close();
                throw new IllegalStateException("consumer killed before its offset commit");
            }
            if (loseNextCommits.getAndUpdate(n -> Math.max(0, n - 1)) > 0) return;
            if (group == null) return;
            synchronized (lock) {
                Map<Partition, Long> offsets = committed.computeIfAbsent(group, ignored -> new HashMap<>());
                offsets.putAll(nextOffsets);
            }
        }

        @Override
        public long position(Partition partition) {
            synchronized (lock) {
                Long position = positions.get(partition);
                if (position == null) throw new IllegalStateException("not assigned: " + partition);
                return position;
            }
        }

        @Override
        public OptionalLong currentLag(Partition partition) {
            synchronized (lock) {
                Long position = positions.get(partition);
                if (position == null) return OptionalLong.empty();
                return OptionalLong.of(log(partition.topic()).partitions.get(partition.partition()).size() - position);
            }
        }

        @Override
        public long beginningOffset(Partition partition) {
            synchronized (lock) {
                log(partition.topic());
                return 0;
            }
        }

        @Override
        public long endOffset(Partition partition) {
            synchronized (lock) {
                return log(partition.topic()).partitions.get(partition.partition()).size();
            }
        }

        @Override
        public Collection<Partition> assignment() {
            synchronized (lock) {
                return List.copyOf(assigned);
            }
        }

        @Override
        public void wakeup() {
            woken = true;
        }

        @Override
        public void close() {
            synchronized (lock) {
                if (closed) return;
                closed = true;
                if (group != null) {
                    List<Stream> group0 = members.get(group);
                    if (group0 != null) group0.remove(this);
                    rebalance(group);
                }
                assigned.clear();
            }
        }

        private void ensureOpen() {
            if (closed) throw new IllegalStateException("consumer is closed");
        }
    }
}
