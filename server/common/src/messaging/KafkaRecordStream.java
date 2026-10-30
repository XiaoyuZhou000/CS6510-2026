package messaging;

import events.RecordStream;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * {@link RecordStream} over one Kafka consumer (D6), used by exactly one {@code ConsumerLoop} thread.
 *
 * <ul>
 *   <li>A {@link #wakeup} makes the current or next {@link #poll} return no records.</li>
 *   <li>A commit that loses to a rebalance is dropped, not raised: the partition's new owner re-reads those
 *       records from the last committed offset, which idempotent handlers tolerate.</li>
 *   <li>A consumer built without a {@code group.id} (the catalog reader) commits nothing.</li>
 * </ul>
 */
public final class KafkaRecordStream implements RecordStream {
    private static final Duration OFFSET_LOOKUP_TIMEOUT = Duration.ofSeconds(5);
    private final Consumer<String, byte[]> consumer;
    private final String group;
    private final String name;

    /** {@code group} is the consumer's {@code group.id}, or null when it has none. */
    public KafkaRecordStream(Consumer<String, byte[]> consumer, String group, String name) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.group = group;
        this.name = name;
    }

    @Override
    public void subscribe(String groupName, Collection<String> topics, Listener listener) {
        if (group == null || !group.equals(groupName)) {
            throw new IllegalArgumentException("consumer was built for group " + group + ", not " + groupName);
        }
        consumer.subscribe(List.copyOf(topics), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) listener.onRevoked(toPartitions(partitions));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) listener.onAssigned(toPartitions(partitions));
            }
        });
    }

    @Override
    public void assign(Collection<Partition> partitions) {
        consumer.assign(toTopicPartitions(partitions));
    }

    @Override
    public List<Record> poll(Duration timeout) {
        ConsumerRecords<String, byte[]> records;
        try {
            records = consumer.poll(timeout);
        } catch (WakeupException woken) {
            return List.of();
        }
        List<Record> converted = new ArrayList<>(records.count());
        for (ConsumerRecord<String, byte[]> record : records) {
            converted.add(new Record(record.topic(), record.partition(), record.offset(), record.key(), record.value(),
                    record.timestamp()));
        }
        return converted;
    }

    @Override
    public void pause(Collection<Partition> partitions) {
        consumer.pause(toTopicPartitions(partitions));
    }

    @Override
    public void resume(Collection<Partition> partitions) {
        consumer.resume(toTopicPartitions(partitions));
    }

    @Override
    public void seek(Partition partition, long offset) {
        consumer.seek(new TopicPartition(partition.topic(), partition.partition()), offset);
    }

    @Override
    public void commitAsync(Map<Partition, Long> nextOffsets) {
        if (group == null || nextOffsets.isEmpty()) return;
        consumer.commitAsync(toOffsets(nextOffsets), (offsets, failure) -> {
            if (failure != null) {
                System.err.println("[" + name + "] Offset commit failed; the records are read again if needed: " + failure);
            }
        });
    }

    @Override
    public void commitSync(Map<Partition, Long> nextOffsets) {
        if (group == null || nextOffsets.isEmpty()) return;
        try {
            consumer.commitSync(toOffsets(nextOffsets));
        } catch (CommitFailedException | RebalanceInProgressException lost) {
            System.err.println("[" + name + "] Offset commit lost to a rebalance; the new owner reads again: " + lost);
        } catch (WakeupException woken) {
            // Shutdown woke the consumer during its final commit; retry once without the wakeup.
            consumer.commitSync(toOffsets(nextOffsets));
        }
    }

    @Override
    public long position(Partition partition) {
        return consumer.position(new TopicPartition(partition.topic(), partition.partition()), Duration.ofMillis(500));
    }

    @Override
    public OptionalLong currentLag(Partition partition) {
        return consumer.currentLag(new TopicPartition(partition.topic(), partition.partition()));
    }

    @Override
    public long beginningOffset(Partition partition) {
        TopicPartition topicPartition = new TopicPartition(partition.topic(), partition.partition());
        return consumer.beginningOffsets(List.of(topicPartition), OFFSET_LOOKUP_TIMEOUT).get(topicPartition);
    }

    @Override
    public long endOffset(Partition partition) {
        TopicPartition topicPartition = new TopicPartition(partition.topic(), partition.partition());
        return consumer.endOffsets(List.of(topicPartition), OFFSET_LOOKUP_TIMEOUT).get(topicPartition);
    }

    @Override
    public Collection<Partition> assignment() {
        return toPartitions(consumer.assignment());
    }

    @Override
    public void wakeup() {
        consumer.wakeup();
    }

    @Override
    public void close() {
        try {
            consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
        } catch (WakeupException woken) {
            consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
        }
    }

    private static List<Partition> toPartitions(Collection<TopicPartition> partitions) {
        List<Partition> converted = new ArrayList<>(partitions.size());
        for (TopicPartition partition : partitions) converted.add(new Partition(partition.topic(), partition.partition()));
        return converted;
    }

    private static List<TopicPartition> toTopicPartitions(Collection<Partition> partitions) {
        List<TopicPartition> converted = new ArrayList<>(partitions.size());
        for (Partition partition : partitions) converted.add(new TopicPartition(partition.topic(), partition.partition()));
        return converted;
    }

    private static Map<TopicPartition, OffsetAndMetadata> toOffsets(Map<Partition, Long> nextOffsets) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        nextOffsets.forEach((partition, offset) ->
                offsets.put(new TopicPartition(partition.topic(), partition.partition()), new OffsetAndMetadata(offset)));
        return offsets;
    }
}
