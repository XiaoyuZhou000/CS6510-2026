package unit.common;

import events.RecordStream;
import events.TopicNames;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import support.InMemoryEventBus;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/** The deterministic broker stand-in behaves like Kafka where the suites rely on it, under a fixed seed (D14). */
class InMemoryEventBusTest {
    private static final String TOPIC = "bus.checkout";

    @Test
    void keyedRecordsUseKafkasDefaultPartitionerAndTheProductionPartitionCounts() throws Exception {
        TopicNames names = new TopicNames("run1.");
        InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 1);
        assertEquals(1, bus.partitions(names.catalog()));
        assertEquals(16, bus.partitions(names.checkout()));
        assertEquals(16, bus.partitions(names.checkoutOutcome()));
        assertEquals(1, bus.partitions(names.scanAccepted()));
        for (String key : List.of("tx-1", "tx-2", "4b1f7c2e-0000-4000-8000-000000000001")) {
            int expected = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % 16;
            assertEquals(expected, bus.partitionFor(names.checkout(), key));
            long before = bus.endOffset(names.checkout(), expected);
            bus.send(names.checkout(), key, bytes(key)).get();
            assertEquals(before + 1, bus.endOffset(names.checkout(), expected), "the record lands on that partition");
        }
    }

    @Test
    void groupMembersSharePartitionsAndANewMemberStartsAtTheCommittedOffset() throws Exception {
        InMemoryEventBus bus = bus(1);
        for (int i = 0; i < 64; i++) bus.send(TOPIC, "k" + i, bytes("v" + i)).get();
        Tracker first = new Tracker();
        RecordStream a = bus.stream();
        a.subscribe("g", List.of(TOPIC), first);
        List<RecordStream.Record> read = drain(a);
        assertEquals(64, read.size(), "a sole member owns all 16 partitions");
        assertEquals(16, first.assigned.size());
        a.commitSync(nextOffsets(read));

        Tracker second = new Tracker();
        RecordStream b = bus.stream();
        b.subscribe("g", List.of(TOPIC), second);
        a.poll(Duration.ofMillis(10));
        b.poll(Duration.ofMillis(10));
        assertEquals(8, a.assignment().size(), "the old owner revoked half");
        assertEquals(8, b.assignment().size());
        assertEquals(8, first.revoked.size());
        for (RecordStream.Partition partition : b.assignment()) {
            assertEquals(bus.endOffset(TOPIC, partition.partition()), b.position(partition),
                    "the new owner starts at the committed offset");
        }
        assertTrue(drain(b).isEmpty(), "nothing committed is read twice");
    }

    @Test
    void duplicatedSendsAreStoredTwiceAndDroppedSendsFailUnstored() throws Exception {
        InMemoryEventBus bus = bus(2);
        int partition = bus.partitionFor(TOPIC, "k");
        bus.duplicateNextSends(1);
        bus.send(TOPIC, "k", bytes("dup")).get();
        assertEquals(2, bus.endOffset(TOPIC, partition));
        bus.send(TOPIC, "k", bytes("once")).get();
        assertEquals(3, bus.endOffset(TOPIC, partition), "only the next send was duplicated");

        bus.dropNextSends(1);
        assertThrows(ExecutionException.class, () -> bus.send(TOPIC, "k", bytes("lost")).get(),
                "a record lost before the broker stored it is never confirmed");
        assertEquals(3, bus.endOffset(TOPIC, partition));
    }

    @Test
    void delayedRecordsBecomeVisibleOnlyAfterTheDelay() throws Exception {
        InMemoryEventBus bus = bus(3);
        bus.delayDelivery(Duration.ofMillis(200));
        bus.send(TOPIC, "k", bytes("late")).get();
        RecordStream stream = subscribed(bus, "g");
        long started = System.nanoTime();
        assertTrue(stream.poll(Duration.ofMillis(50)).isEmpty());
        List<RecordStream.Record> records = drain(stream, Duration.ofSeconds(2));
        assertEquals(1, records.size());
        assertTrue(System.nanoTime() - started >= Duration.ofMillis(190).toNanos());
    }

    @Test
    void reorderingInterleavesPartitionsDeterministicallyWhileKeepingEachPartitionsOrder() throws Exception {
        List<String> firstRun = reorderedRun(99);
        assertEquals(firstRun, reorderedRun(99), "the same seed gives the same interleaving");
        assertNotEquals(firstRun, reorderedRun(100), "another seed gives another interleaving");
    }

    private List<String> reorderedRun(long seed) throws Exception {
        InMemoryEventBus bus = bus(seed);
        for (int i = 0; i < 80; i++) bus.send(TOPIC, "k" + (i % 20), bytes("k" + (i % 20) + "#" + i)).get();
        bus.reorderAcrossPartitions(true);
        RecordStream stream = subscribed(bus, "g");
        List<String> order = new ArrayList<>();
        Map<Integer, Long> lastOffset = new java.util.HashMap<>();
        for (RecordStream.Record record : drain(stream)) {
            Long previous = lastOffset.put(record.partition(), record.offset());
            assertTrue(previous == null || previous < record.offset(), "per-partition order is kept");
            order.add(new String(record.value(), StandardCharsets.UTF_8));
        }
        assertEquals(80, order.size());
        return order;
    }

    @Test
    void aLostCommitLeavesItsRecordsForRedeliveryAfterARebalance() throws Exception {
        InMemoryEventBus bus = bus(4);
        bus.send(TOPIC, "k", bytes("v")).get();
        RecordStream first = subscribed(bus, "g");
        List<RecordStream.Record> read = drain(first);
        bus.loseNextCommits(1);
        first.commitSync(nextOffsets(read));
        first.close();
        RecordStream second = subscribed(bus, "g");
        assertEquals(1, drain(second).size(), "the acknowledgement was lost, so the record comes again");
    }

    @Test
    void aPausedBrokerFailsSendsReturnsNoRecordsAndRejectsCommits() throws Exception {
        InMemoryEventBus bus = bus(5);
        bus.send(TOPIC, "k", bytes("before")).get();
        RecordStream stream = subscribed(bus, "g");
        bus.pauseBroker(true);
        assertThrows(ExecutionException.class, () -> bus.send(TOPIC, "k", bytes("during")).get());
        assertTrue(stream.poll(Duration.ofMillis(30)).isEmpty());
        assertThrows(IllegalStateException.class, () -> stream.commitSync(Map.of(new RecordStream.Partition(TOPIC, 0), 1L)));
        bus.pauseBroker(false);
        assertEquals(1, drain(stream).size(), "the stored record is delivered after the broker returns");
        bus.send(TOPIC, "k", bytes("after")).get();
    }

    @Test
    void aFullBrokerRefusesSendsButStillServesStoredRecords() throws Exception {
        InMemoryEventBus bus = bus(6);
        bus.send(TOPIC, "k", bytes("stored")).get();
        bus.refuseSends(true);
        assertThrows(ExecutionException.class, () -> bus.send(TOPIC, "k", bytes("refused")).get());
        assertEquals(1, drain(subscribed(bus, "g")).size());
        bus.refuseSends(false);
        bus.send(TOPIC, "k", bytes("accepted")).get();
    }

    @Test
    void aConsumerKilledBeforeItsCommitHandsItsPartitionsBackAtTheLastCommittedOffset() throws Exception {
        InMemoryEventBus bus = bus(7);
        for (int i = 0; i < 16; i++) bus.send(TOPIC, "k" + i, bytes("v" + i)).get();
        RecordStream victim = subscribed(bus, "g");
        List<RecordStream.Record> read = drain(victim);
        bus.killNextCommittingConsumer();
        assertThrows(IllegalStateException.class, () -> victim.commitSync(nextOffsets(read)));
        assertThrows(IllegalStateException.class, () -> victim.poll(Duration.ofMillis(1)), "the killed consumer is closed");
        RecordStream survivor = subscribed(bus, "g");
        assertEquals(16, drain(survivor).size(), "every record is redelivered to the surviving member");
        assertNull(bus.committedOffset("g", TOPIC, 0));
    }

    private static InMemoryEventBus bus(long seed) {
        InMemoryEventBus bus = new InMemoryEventBus(seed);
        bus.createTopic(TOPIC, 16);
        return bus;
    }

    private static RecordStream subscribed(InMemoryEventBus bus, String group) {
        RecordStream stream = bus.stream();
        stream.subscribe(group, List.of(TOPIC), new Tracker());
        return stream;
    }

    private static List<RecordStream.Record> drain(RecordStream stream) {
        return drain(stream, Duration.ofMillis(200));
    }

    /** Polls until a poll returns nothing after something arrived, or until {@code within} passes. */
    private static List<RecordStream.Record> drain(RecordStream stream, Duration within) {
        List<RecordStream.Record> all = new ArrayList<>();
        long deadline = System.nanoTime() + within.toNanos();
        while (System.nanoTime() < deadline) {
            List<RecordStream.Record> batch = stream.poll(Duration.ofMillis(20));
            if (batch.isEmpty() && !all.isEmpty()) break;
            all.addAll(batch);
        }
        return all;
    }

    private static Map<RecordStream.Partition, Long> nextOffsets(List<RecordStream.Record> records) {
        Map<RecordStream.Partition, Long> offsets = new java.util.HashMap<>();
        for (RecordStream.Record record : records) offsets.merge(record.position(), record.offset() + 1, Math::max);
        return offsets;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class Tracker implements RecordStream.Listener {
        final List<RecordStream.Partition> assigned = new ArrayList<>();
        final List<RecordStream.Partition> revoked = new ArrayList<>();

        @Override
        public void onAssigned(Collection<RecordStream.Partition> partitions) {
            assigned.addAll(partitions);
        }

        @Override
        public void onRevoked(Collection<RecordStream.Partition> partitions) {
            revoked.addAll(partitions);
        }
    }
}
