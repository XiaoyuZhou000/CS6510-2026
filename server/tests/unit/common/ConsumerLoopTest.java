package unit.common;

import database.StoreFailure;
import events.ConsumerLoop;
import events.EnvelopeCodec;
import events.EventHandler;
import events.EventTypes;
import events.ParkReason;
import events.ParkingPolicy;
import events.PermanentEventFailure;
import events.RecordStream;
import events.TransientEventFailure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.AttemptId;
import selfcheckout.events.v1.CatalogItemPublished;
import selfcheckout.events.v1.StockDeducted;
import support.InMemoryDeadLetterStore;
import support.InMemoryEventBus;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** Acknowledge-after-commit, retry, parking, stall alarm, revocation, and manual assignment (D6). */
class ConsumerLoopTest {
    private static final String TOPIC = "test.checkout-outcome";
    private static final String GROUP = "test-group";
    private final List<ConsumerLoop> loops = new ArrayList<>();

    @AfterEach
    void stopLoops() {
        for (ConsumerLoop loop : loops) loop.stop(Duration.ofSeconds(2));
    }

    @Test
    void policyDefaultsAndClassificationFollowD6() {
        ParkingPolicy policy = ParkingPolicy.defaults();
        assertEquals(5, policy.maxAttempts());
        assertEquals(Duration.ofMillis(200), policy.permanentRetryDelay());
        assertEquals(Duration.ofMillis(100), policy.transientBackoff(1));
        assertEquals(Duration.ofMillis(200), policy.transientBackoff(2));
        assertEquals(Duration.ofMillis(1_600), policy.transientBackoff(5));
        assertEquals(Duration.ofSeconds(5), policy.transientBackoff(30), "capped at 5 s");
        assertEquals(Duration.ofSeconds(60), policy.stallAfter());
        assertEquals(3, ParkingPolicy.fromEnvironment(Map.of("CONSUMER_MAX_ATTEMPTS", "3")).maxAttempts());

        assertTrue(policy.classify(new StoreFailure("database unavailable")).transientFailure());
        assertTrue(policy.classify(StoreFailure.contended("deadlocks", null)).transientFailure());
        assertTrue(policy.classify(new StoreFailure("commit unknown", null, true)).transientFailure());
        assertTrue(policy.classify(new TransientEventFailure("not ready")).transientFailure());
        ParkingPolicy.Verdict integrity = policy.classify(new PermanentEventFailure(ParkReason.INTEGRITY, "conflict"));
        assertFalse(integrity.transientFailure());
        assertEquals(ParkReason.INTEGRITY, integrity.reason());
        ParkingPolicy.Verdict bug = policy.classify(new IllegalStateException("boom"));
        assertFalse(bug.transientFailure(), "an unexpected exception never blocks a partition indefinitely");
        assertEquals(ParkReason.HANDLER_ERROR, bug.reason());
        assertEquals("java.lang.IllegalStateException: boom", bug.detail());
    }

    @Test
    void theOffsetIsCommittedOnlyAfterTheHandlerReturned() throws Exception {
        InMemoryEventBus bus = bus();
        long offset = send(bus, "e1", "tx-1");
        int partition = bus.partitionFor(TOPIC, "tx-1");
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        start(bus, event -> {
            entered.countDown();
            await(release);
        }, settings());
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        assertNull(bus.committedOffset(GROUP, TOPIC, partition), "nothing is committed while the handler runs");
        release.countDown();
        awaitTrue(() -> Long.valueOf(offset + 1).equals(bus.committedOffset(GROUP, TOPIC, partition)));
    }

    @Test
    void aTransientFailureHoldsOnlyItsPartitionAndRetriesTheSameRecordWithBackoff() throws Exception {
        InMemoryEventBus bus = bus();
        String stuckKey = "tx-stuck";
        String otherKey = keyOnAnotherPartition(bus, stuckKey);
        long stuckOffset = send(bus, "stuck", stuckKey);
        send(bus, "after-stuck", stuckKey);
        List<Long> attempts = new CopyOnWriteArrayList<>();
        List<String> handled = new CopyOnWriteArrayList<>();
        AtomicInteger failuresLeft = new AtomicInteger(4);
        start(bus, event -> {
            if (event.eventId().equals("stuck")) {
                attempts.add(System.nanoTime());
                if (failuresLeft.getAndDecrement() > 0) throw new TransientEventFailure("inventory not ready");
            }
            handled.add(event.eventId());
        }, settings());

        awaitTrue(() -> attempts.size() >= 2);
        send(bus, "other", otherKey);
        awaitTrue(() -> handled.contains("other"));
        assertFalse(handled.contains("after-stuck"), "later records of the waiting partition wait");
        int partition = bus.partitionFor(TOPIC, stuckKey);
        Long committed = bus.committedOffset(GROUP, TOPIC, partition);
        assertTrue(committed == null || committed <= stuckOffset, "no offset passes the failing record");

        awaitTrue(() -> handled.contains("after-stuck"));
        assertEquals(List.of("stuck", "after-stuck"), handled.stream().filter(id -> !id.equals("other")).toList(),
                "the same record is retried, then its partition resumes in order");
        assertEquals(5, attempts.size());
        for (int i = 1; i < attempts.size(); i++) {
            long gap = TimeUnit.NANOSECONDS.toMillis(attempts.get(i) - attempts.get(i - 1));
            long expected = 100L << (i - 1);
            assertTrue(gap >= expected - 5, "retry " + i + " waited " + gap + " ms, expected about " + expected);
        }
    }

    @Test
    void aPermanentFailureIsAttemptedFiveTimesThenParkedAndRepeatsIncrementTheParkCount() throws Exception {
        InMemoryEventBus bus = bus();
        InMemoryDeadLetterStore parked = new InMemoryDeadLetterStore();
        long poisonOffset = send(bus, "poison", "tx-p");
        send(bus, "next", "tx-p");
        List<Long> attempts = new CopyOnWriteArrayList<>();
        List<String> handled = new CopyOnWriteArrayList<>();
        start(bus, event -> {
            if (event.eventId().equals("poison")) {
                attempts.add(System.nanoTime());
                throw new PermanentEventFailure(ParkReason.INTEGRITY, "outcome conflicts with a settled attempt");
            }
            handled.add(event.eventId());
        }, parked, settings());

        awaitTrue(() -> handled.contains("next"));
        assertEquals(5, attempts.size(), "CONSUMER_MAX_ATTEMPTS attempts");
        for (int i = 1; i < attempts.size(); i++) {
            assertTrue(TimeUnit.NANOSECONDS.toMillis(attempts.get(i) - attempts.get(i - 1)) >= 195, "200 ms apart");
        }
        InMemoryDeadLetterStore.Row row = parked.byEventId("poison");
        assertNotNull(row);
        assertEquals(ParkReason.INTEGRITY, row.first.reason());
        assertEquals(TOPIC, row.first.topic());
        assertEquals(bus.partitionFor(TOPIC, "tx-p"), row.first.partition());
        assertEquals(poisonOffset, row.first.offset());
        assertEquals("StockDeducted", row.first.eventType());
        assertEquals(1, row.first.schemaVersion());
        assertTrue(row.first.detail().contains("settled attempt"));
        assertArrayEquals(bus.values(TOPIC).stream().filter(v -> EnvelopeCodec.decode(v, "tx-p").eventId()
                .equals("poison")).findFirst().orElseThrow(), row.first.recordBytes());
        int partition = bus.partitionFor(TOPIC, "tx-p");
        awaitTrue(() -> Long.valueOf(poisonOffset + 2).equals(bus.committedOffset(GROUP, TOPIC, partition)));

        send(bus, "poison", "tx-p");
        awaitTrue(() -> parked.byEventId("poison").parkCount == 2);
        assertEquals(1, parked.countParked(), "a re-parked event_id increments park_count instead of adding a row");
    }

    @Test
    void anUndecodableRecordIsParkedWithoutAnEventId() throws Exception {
        InMemoryEventBus bus = bus();
        InMemoryDeadLetterStore parked = new InMemoryDeadLetterStore();
        bus.send(TOPIC, "tx-g", "\u0000garbageÿ".getBytes(StandardCharsets.ISO_8859_1)).get();
        bus.send(TOPIC, "tx-g", "\u0000garbageÿ".getBytes(StandardCharsets.ISO_8859_1)).get();
        List<String> handled = new CopyOnWriteArrayList<>();
        start(bus, event -> handled.add(event.eventId()), parked, fastPermanent());
        awaitTrue(() -> parked.countParked() == 2);
        for (InMemoryDeadLetterStore.Row row : parked.rows()) {
            assertNull(row.first.eventId());
            assertEquals(ParkReason.UNDECODABLE, row.first.reason());
            assertEquals(1, row.parkCount, "null event_id rows never collide");
        }
        assertTrue(handled.isEmpty(), "the handler never sees an undecodable record");
    }

    @Test
    void storeFailuresAreTransientAndUnexpectedExceptionsAreParkedAsHandlerErrors() throws Exception {
        InMemoryEventBus bus = bus();
        InMemoryDeadLetterStore parked = new InMemoryDeadLetterStore();
        send(bus, "db-down", "tx-db");
        send(bus, "bug", "tx-bug");
        AtomicInteger dbAttempts = new AtomicInteger();
        start(bus, event -> {
            if (event.eventId().equals("db-down")) {
                int attempt = dbAttempts.incrementAndGet();
                if (attempt == 1) throw new StoreFailure("database unavailable");
                if (attempt == 2) throw StoreFailure.contended("deadlock retries exhausted", null);
                if (attempt <= 7) throw new StoreFailure("commit outcome unknown", null, true);
                return;
            }
            throw new IllegalStateException("unexpected null basket");
        }, parked, fastPermanent());
        awaitTrue(() -> parked.byEventId("bug") != null);
        InMemoryDeadLetterStore.Row bug = parked.byEventId("bug");
        assertEquals(ParkReason.HANDLER_ERROR, bug.first.reason());
        assertEquals("java.lang.IllegalStateException: unexpected null basket", bug.first.detail());
        awaitTrue(() -> dbAttempts.get() == 8);
        assertNull(parked.byEventId("db-down"), "store failures are retried past the permanent attempt limit, never parked");
    }

    @Test
    void aRecordInTransientRetryBeyondTheThresholdIsReportedAsStalled() throws Exception {
        InMemoryEventBus bus = bus();
        long offset = send(bus, "slow", "tx-slow");
        CountDownLatch healthy = new CountDownLatch(1);
        ConsumerLoop.Settings settings = settings().withPolicy(new ParkingPolicy(5, Duration.ofMillis(200),
                Duration.ofMillis(20), Duration.ofMillis(40), Duration.ofMillis(150))).withStatusInterval(Duration.ZERO);
        ConsumerLoop loop = start(bus, event -> {
            if (healthy.getCount() > 0) throw new TransientEventFailure("inventory not ready");
        }, settings);
        awaitTrue(() -> !loop.stalledPartitions().isEmpty());
        ConsumerLoop.StalledPartition stalled = loop.stalledPartitions().get(0);
        assertEquals(TOPIC, stalled.topic());
        assertEquals(bus.partitionFor(TOPIC, "tx-slow"), stalled.partition());
        assertEquals(offset, stalled.offset());
        assertTrue(stalled.age().toMillis() >= 150);
        healthy.countDown();
        awaitTrue(() -> loop.stalledPartitions().isEmpty());
    }

    @Test
    void revocationAndShutdownCommitWhatWasProcessedSoNothingIsHandledTwice() throws Exception {
        InMemoryEventBus bus = bus();
        Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();
        EventHandler counting = event -> counts.computeIfAbsent(event.eventId(), ignored -> new AtomicInteger()).incrementAndGet();
        for (int i = 0; i < 200; i++) send(bus, "a" + i, "tx-" + i);
        ConsumerLoop first = start(bus, counting, settings());
        awaitTrue(() -> counts.size() == 200);
        ConsumerLoop second = start(bus, counting, settings());
        for (int i = 0; i < 200; i++) send(bus, "b" + i, "tx-" + i);
        awaitTrue(() -> counts.size() == 400);
        first.stop(Duration.ofSeconds(2));
        for (int i = 0; i < 100; i++) send(bus, "c" + i, "tx-" + i);
        awaitTrue(() -> counts.size() == 500);
        second.stop(Duration.ofSeconds(2));
        counts.forEach((id, count) -> assertEquals(1, count.get(), id + " handled once across rebalances"));
        for (int p = 0; p < 16; p++) {
            assertEquals(Long.valueOf(bus.endOffset(TOPIC, p)), bus.committedOffset(GROUP, TOPIC, p),
                    "commitSync on revocation and shutdown covers partition " + p);
        }
    }

    @Test
    void aConsumerKilledBeforeItsCommitLeavesTheRecordsForRedelivery() throws Exception {
        InMemoryEventBus bus = bus();
        Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();
        for (int i = 0; i < 20; i++) send(bus, "k" + i, "tx-" + i);
        bus.killNextCommittingConsumer();
        start(bus, event -> counts.computeIfAbsent(event.eventId(), ignored -> new AtomicInteger()).incrementAndGet(),
                settings());
        awaitTrue(() -> counts.size() == 20 && counts.values().stream().allMatch(c -> c.get() >= 1));
        awaitTrue(() -> counts.values().stream().anyMatch(c -> c.get() == 2));
        for (int p = 0; p < 16; p++) {
            int partition = p;
            awaitTrue(() -> Long.valueOf(bus.endOffset(TOPIC, partition)).equals(bus.committedOffset(GROUP, TOPIC, partition))
                    || bus.endOffset(TOPIC, partition) == 0);
        }
    }

    @Test
    void manualAssignmentSeeksToTheSuppliedOffset() throws Exception {
        InMemoryEventBus bus = new InMemoryEventBus(7);
        bus.createTopic("test.catalog", 1);
        for (int i = 0; i < 10; i++) {
            CatalogItemPublished item = CatalogItemPublished.newBuilder().setSku("SKU-" + i).setName("n" + i)
                    .setPrice("1.00").setCatalogVersion("v").build();
            bus.send("test.catalog", "SKU-" + i, EnvelopeCodec.encode("c" + i, EventTypes.CATALOG_ITEM_PUBLISHED,
                    "SKU-" + i, "v", EventTypes.INVENTORY, 0, item)).get();
        }
        List<String> fromStart = new CopyOnWriteArrayList<>();
        List<String> fromSeven = new CopyOnWriteArrayList<>();
        RecordStream.Partition only = new RecordStream.Partition("test.catalog", 0);
        manual(bus, only, 0, event -> fromStart.add(event.eventId()));
        manual(bus, only, 7, event -> fromSeven.add(event.eventId()));
        awaitTrue(() -> fromStart.size() == 10 && fromSeven.size() == 3);
        assertEquals(List.of("c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"), fromStart);
        assertEquals(List.of("c7", "c8", "c9"), fromSeven);
    }

    /** The gauge is refreshed between polls, so a waiting partition keeps it current. */
    @Test
    void lagCountsRecordsBetweenTheCommittedOffsetAndTheEnd() throws Exception {
        InMemoryEventBus bus = bus();
        CountDownLatch healthy = new CountDownLatch(1);
        for (int i = 0; i < 5; i++) send(bus, "l" + i, "tx-lag");
        ConsumerLoop loop = start(bus, event -> {
            if (healthy.getCount() > 0) throw new TransientEventFailure("not ready");
        }, settings());
        awaitTrue(() -> loop.lag() == 5);
        healthy.countDown();
        awaitTrue(() -> loop.lag() == 0);
    }

    private static InMemoryEventBus bus() {
        InMemoryEventBus bus = new InMemoryEventBus(42);
        bus.createTopic(TOPIC, 16);
        return bus;
    }

    private static ConsumerLoop.Settings settings() {
        return ConsumerLoop.Settings.defaults().withPolicy(ParkingPolicy.defaults());
    }

    private static ConsumerLoop.Settings fastPermanent() {
        return settings().withPolicy(new ParkingPolicy(5, Duration.ofMillis(10), Duration.ofMillis(10),
                Duration.ofMillis(20), Duration.ofSeconds(60)));
    }

    private ConsumerLoop start(InMemoryEventBus bus, EventHandler handler, ConsumerLoop.Settings settings) {
        return start(bus, handler, new InMemoryDeadLetterStore(), settings);
    }

    private ConsumerLoop start(InMemoryEventBus bus, EventHandler handler, InMemoryDeadLetterStore parked,
                               ConsumerLoop.Settings settings) {
        ConsumerLoop loop = ConsumerLoop.group("test", GROUP, TOPIC, bus::stream, handler, parked, settings);
        loops.add(loop);
        loop.start();
        return loop;
    }

    private void manual(InMemoryEventBus bus, RecordStream.Partition partition, long offset, EventHandler handler) {
        ConsumerLoop loop = ConsumerLoop.manual("test", "catalog-" + offset, List.of(partition), ignored -> offset,
                bus::stream, handler, new InMemoryDeadLetterStore(), settings());
        loops.add(loop);
        loop.start();
    }

    /** Sends a {@code StockDeducted} with {@code eventId} for {@code key}; returns its offset. */
    private static long send(InMemoryEventBus bus, String eventId, String key) throws Exception {
        StockDeducted payload = StockDeducted.newBuilder()
                .setAttempt(AttemptId.newBuilder().setTransactionId(key).setAttemptNumber(1)).build();
        int partition = bus.partitionFor(TOPIC, key);
        long offset = bus.endOffset(TOPIC, partition);
        bus.send(TOPIC, key, EnvelopeCodec.encode(eventId, EventTypes.STOCK_DEDUCTED, key, key, EventTypes.INVENTORY,
                System.currentTimeMillis(), payload)).get();
        return offset;
    }

    private static String keyOnAnotherPartition(InMemoryEventBus bus, String key) {
        int partition = bus.partitionFor(TOPIC, key);
        for (int i = 0; ; i++) {
            if (bus.partitionFor(TOPIC, "other-" + i) != partition) return "other-" + i;
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("latch not released");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }

    private static void awaitTrue(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("condition not reached within 10 s");
            Thread.sleep(5);
        }
    }
}
