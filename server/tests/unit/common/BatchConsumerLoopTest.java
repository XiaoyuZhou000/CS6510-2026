package unit.common;

import database.StoreFailure;
import events.BatchEventHandler;
import events.ConsumerLoop;
import events.DecodedEvent;
import events.EnvelopeCodec;
import events.EventTypes;
import events.ParkReason;
import events.ParkingPolicy;
import events.PermanentEventFailure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.ScanAccepted;
import support.InMemoryDeadLetterStore;
import support.InMemoryEventBus;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link ConsumerLoop}'s batch group mode (D10, FR-049), the scan consumer's: each poll's records of a partition
 * form one batch, every assignment seeks to the consumer's stored offset, a stored offset beyond the log end
 * alarms and reads from the earliest, and an undecodable record or a failed batch never makes valid records wait
 * behind it or be parked with it.
 */
class BatchConsumerLoopTest {
    private static final String TOPIC = "test.scan-accepted";
    private static final String GROUP = "test-scan-ingest";
    private static final ParkingPolicy FAST = new ParkingPolicy(5, Duration.ofMillis(10), Duration.ofMillis(10),
            Duration.ofMillis(50), Duration.ofSeconds(60));
    private final List<ConsumerLoop> loops = new ArrayList<>();
    private final InMemoryEventBus bus = new InMemoryEventBus(510);
    private final InMemoryDeadLetterStore parked = new InMemoryDeadLetterStore();
    private final List<List<Long>> batches = new CopyOnWriteArrayList<>();

    BatchConsumerLoopTest() {
        bus.createTopic(TOPIC, 1);
    }

    @AfterEach
    void stopLoops() {
        for (ConsumerLoop loop : loops) loop.stop(Duration.ofSeconds(2));
    }

    @Test
    void eachPollOfAPartitionIsOneBatchAndItsOffsetIsCommittedAfterIt() throws Exception {
        publish(0, 1_200);
        ConsumerLoop loop = start(partition -> 0, recording());
        await(() -> handled().size() == 1_200, "every record handled");
        for (List<Long> batch : batches) {
            assertTrue(batch.size() <= 500, "a poll carries at most 500 records: " + batch.size());
            for (int i = 1; i < batch.size(); i++) assertEquals(batch.get(i - 1) + 1, batch.get(i), "consecutive offsets");
        }
        assertTrue(batches.size() <= 4, "1,200 records arrive in few batches, not one by one: " + batches.size());
        assertEquals(range(0, 1_200), handled());
        await(() -> Long.valueOf(1_200).equals(bus.committedOffset(GROUP, TOPIC, 0)), "the offset committed");
        assertEquals(0, loop.parked());
    }

    @Test
    void everyAssignmentSeeksToTheStoredOffsetWhateverTheGroupCommitted() throws Exception {
        publish(0, 1_000);
        AtomicLong stored = new AtomicLong(700);
        ConsumerLoop first = start(partition -> stored.get(), recording());
        await(() -> handled().size() == 300, "records from the stored offset handled");
        assertEquals(range(700, 1_000), handled(), "nothing before the stored offset is read");
        first.stop(Duration.ofSeconds(2));

        // The group's committed offset is lost; the stored offset says 1,000 were consumed.
        bus.resetCommittedOffsets(GROUP);
        stored.set(1_000);
        batches.clear();
        publish(1_000, 50);
        ConsumerLoop restarted = start(partition -> stored.get(), recording());
        await(() -> handled().size() == 50, "only the new records handled");
        assertEquals(range(1_000, 1_050), handled());
        assertEquals(0, restarted.offsetResets());
    }

    @Test
    void aStoredOffsetBeyondTheLogEndAlarmsAndReadsFromTheEarliestOffset() throws Exception {
        publish(0, 100);
        ConsumerLoop loop = start(partition -> 5_000, recording());
        await(() -> handled().size() == 100, "the log read from its earliest offset");
        assertEquals(range(0, 100), handled());
        assertEquals(1, loop.offsetResets());
    }

    @Test
    void anUndecodableRecordSplitsTheBatchAndIsParkedWithoutBlockingThePartition() throws Exception {
        publish(0, 10);
        bus.send(TOPIC, "tx-bad", "not an envelope".getBytes(StandardCharsets.UTF_8));
        publish(11, 9);
        ConsumerLoop loop = start(partition -> 0, recording());
        await(() -> handled().size() == 19 && parked.countParked() == 1, "valid records handled, the bad one parked");
        List<Long> expected = range(0, 10);
        expected.addAll(range(11, 20));
        assertEquals(expected, handled(), "every valid record is handled once, around the undecodable one");
        assertEquals(10L, parked.rows().getFirst().first.offset());
        assertEquals(ParkReason.UNDECODABLE, parked.rows().getFirst().first.reason());
        assertEquals(1, loop.parked());
        await(() -> Long.valueOf(20).equals(bus.committedOffset(GROUP, TOPIC, 0)), "the partition moved past it");
    }

    @Test
    void aFailedBatchIsRetriedRecordByRecordSoOnlyTheFailingRecordIsParked() throws Exception {
        publish(0, 12);
        List<Long> committedEffects = new CopyOnWriteArrayList<>();
        ConsumerLoop loop = start(partition -> 0, events -> {
            // One local transaction per batch: an invalid event rolls back the whole batch.
            for (DecodedEvent event : events) {
                if (event.offset() == 5) throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, "invalid scan");
            }
            for (DecodedEvent event : events) committedEffects.add(event.offset());
        });
        await(() -> committedEffects.size() == 11 && parked.countParked() == 1, "all but the invalid record committed");
        List<Long> expected = range(0, 5);
        expected.addAll(range(6, 12));
        assertEquals(expected, committedEffects.stream().sorted().toList(), "each valid record committed exactly once");
        assertEquals(5L, parked.rows().getFirst().first.offset());
        assertEquals(ParkReason.INVALID_PAYLOAD, parked.rows().getFirst().first.reason());
        assertEquals(1, loop.parked());
    }

    @Test
    void aTransientBatchFailureHoldsThePartitionUntilItClearsAndParksNothing() throws Exception {
        publish(0, 300);
        AtomicBoolean databaseDown = new AtomicBoolean(true);
        List<Long> committedEffects = new CopyOnWriteArrayList<>();
        AtomicLong failures = new AtomicLong();
        ConsumerLoop loop = start(partition -> 0, events -> {
            if (databaseDown.get()) {
                failures.incrementAndGet();
                throw new StoreFailure("database unavailable");
            }
            for (DecodedEvent event : events) committedEffects.add(event.offset());
        });
        await(() -> failures.get() >= 3, "the batch retried while the database is down");
        assertTrue(committedEffects.isEmpty());
        assertNull(bus.committedOffset(GROUP, TOPIC, 0), "no offset is committed while nothing is stored");
        databaseDown.set(false);
        await(() -> committedEffects.size() == 300, "every record stored once the database returns");
        assertEquals(range(0, 300), committedEffects.stream().sorted().toList());
        assertEquals(300, committedEffects.stream().distinct().count(), "nothing is stored twice");
        assertEquals(0, loop.parked(), "a transient failure never parks");
    }

    private ConsumerLoop start(java.util.function.ToLongFunction<events.RecordStream.Partition> stored,
                               BatchEventHandler handler) {
        ConsumerLoop loop = ConsumerLoop.batchGroup("test", GROUP, TOPIC, stored, bus::stream, handler, parked,
                ConsumerLoop.Settings.defaults().withPolicy(FAST));
        loops.add(loop);
        loop.start();
        return loop;
    }

    private BatchEventHandler recording() {
        return events -> batches.add(events.stream().map(DecodedEvent::offset).toList());
    }

    private List<Long> handled() {
        List<Long> all = new ArrayList<>();
        for (List<Long> batch : batches) all.addAll(batch);
        return all;
    }

    /** {@code count} valid ScanAccepted records whose offsets start at {@code firstOffset}. */
    private void publish(long firstOffset, int count) {
        for (long offset = firstOffset; offset < firstOffset + count; offset++) {
            String transactionId = "tx-" + offset / 100;
            int sequence = (int) (offset % 100) + 1;
            ScanAccepted scan = ScanAccepted.newBuilder().setTransactionId(transactionId).setScanSequence(sequence)
                    .setSku("sku1").setProductName("Item 1").setUnitPrice("1.00").setAcceptedAtMs(1).build();
            bus.send(TOPIC, transactionId, EnvelopeCodec.encode(EventTypes.scanEventId(transactionId, sequence),
                    EventTypes.SCAN_ACCEPTED, transactionId, transactionId, EventTypes.TRANSACTION, 1, scan));
        }
    }

    private static List<Long> range(long from, long to) {
        List<Long> offsets = new ArrayList<>();
        for (long offset = from; offset < to; offset++) offsets.add(offset);
        return offsets;
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("Timed out waiting for: " + what);
            Thread.sleep(5);
        }
    }
}
