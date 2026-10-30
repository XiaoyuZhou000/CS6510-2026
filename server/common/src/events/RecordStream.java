package events;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * One consumer's view of the broker (D6), driven by a single {@link ConsumerLoop} thread. Like a Kafka
 * consumer it is not thread-safe, except {@link #wakeup}. The messaging adapter implements it with a
 * {@code KafkaConsumer}; tests use {@code support.InMemoryEventBus}.
 */
public interface RecordStream extends AutoCloseable {
    /** A topic partition. */
    record Partition(String topic, int partition) { }

    /** One consumed record; {@code timestampMs} is the broker's record timestamp. */
    record Record(String topic, int partition, long offset, String key, byte[] value, long timestampMs) {
        public Partition position() { return new Partition(topic, partition); }
    }

    /** Assignment callbacks of group mode, called on the polling thread from inside {@link #poll}. */
    interface Listener {
        void onAssigned(Collection<Partition> partitions);

        /** Called before the partitions move away; the loop commits what it processed here. */
        void onRevoked(Collection<Partition> partitions);
    }

    /** Group mode: joins {@code group} for {@code topics}. */
    void subscribe(String group, Collection<String> topics, Listener listener);

    /** Manual-assignment mode: reads exactly these partitions, outside any group rebalance. */
    void assign(Collection<Partition> partitions);

    List<Record> poll(Duration timeout);

    void pause(Collection<Partition> partitions);

    void resume(Collection<Partition> partitions);

    /** The next {@link #poll} of {@code partition} starts at {@code offset}. */
    void seek(Partition partition, long offset);

    /** Commits, per partition, the offset of the next record to read. */
    void commitAsync(Map<Partition, Long> nextOffsets);

    void commitSync(Map<Partition, Long> nextOffsets);

    /** The offset of the next record {@link #poll} returns for {@code partition}. */
    long position(Partition partition);

    /** Records between {@link #position} and the partition's end, if the stream knows it without a broker call. */
    OptionalLong currentLag(Partition partition);

    Collection<Partition> assignment();

    /** Makes a blocked or the next {@link #poll} return at once; the only method safe from another thread. */
    void wakeup();

    @Override
    void close();
}
