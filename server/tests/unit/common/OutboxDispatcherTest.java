package unit.common;

import database.StoreFailure;
import events.OutboxDispatcher;
import events.OutboxRow;
import events.RecordSender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import support.InMemoryOutboxStore;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** The outbox dispatcher's immediate path, batched marking, sweep, and re-send (D5). */
class OutboxDispatcherTest {
    private static final String CHECKOUT = "selfcheckout.checkout.v1";
    private static final String CATALOG = "selfcheckout.catalog.v1";
    private final List<OutboxDispatcher> started = new ArrayList<>();

    /** A sender whose futures the test completes, recording every send in order. */
    static final class ScriptedSender implements RecordSender {
        record Sent(String topic, String key, byte[] value, CompletableFuture<Void> future, long atNanos) { }

        final List<Sent> sent = new CopyOnWriteArrayList<>();
        volatile boolean autoConfirm = true;
        volatile boolean autoFail;
        volatile CountDownLatch blockFirst;

        @Override
        public CompletableFuture<Void> send(String topic, String key, byte[] value) {
            CountDownLatch gate = blockFirst;
            if (gate != null) {
                blockFirst = null;
                try {
                    gate.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            sent.add(new Sent(topic, key, value, future, System.nanoTime()));
            if (autoFail) future.completeExceptionally(new IllegalStateException("broker down"));
            else if (autoConfirm) future.complete(null);
            return future;
        }

        List<String> keys() {
            return sent.stream().map(Sent::key).toList();
        }

        void confirmAll() {
            for (Sent item : sent) item.future().complete(null);
        }
    }

    @AfterEach
    void stopDispatchers() {
        for (OutboxDispatcher dispatcher : started) dispatcher.stop(Duration.ofSeconds(2));
    }

    @Test
    void defaultsAreTheContractValues() {
        OutboxDispatcher.Settings defaults = OutboxDispatcher.Settings.defaults();
        assertEquals(Duration.ofMillis(1_000), defaults.sweepInterval());
        assertEquals(Duration.ofSeconds(1), defaults.sweepMinAge());
        assertEquals(500, defaults.sweepLimit());
        assertEquals(Duration.ofMillis(20), defaults.markInterval());
        assertEquals(200, defaults.markBatchSize());
        assertEquals(Duration.ofMillis(100), defaults.minBackoff());
        assertEquals(Duration.ofSeconds(5), defaults.maxBackoff());
        assertTrue(defaults.immediate());
        OutboxDispatcher.Settings tuned = OutboxDispatcher.Settings.fromEnvironment(
                Map.of("OUTBOX_SWEEP_MS", "250", "OUTBOX_MARK_BATCH_MS", "5"));
        assertEquals(Duration.ofMillis(250), tuned.sweepInterval());
        assertEquals(Duration.ofMillis(5), tuned.markInterval());
        assertEquals(defaults, OutboxDispatcher.Settings.fromEnvironment(Map.of()));
    }

    @Test
    void signalReturnsAtOnceAndRowsLeaveInSignalOrderWithTheirStoredBytes() throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        for (String id : List.of("a", "b", "c")) store.add(row(id, CHECKOUT, "tx-" + id, Instant.now()));
        ScriptedSender sender = new ScriptedSender();
        CountDownLatch release = new CountDownLatch(1);
        sender.blockFirst = release;
        OutboxDispatcher dispatcher = start(store, Map.of(CHECKOUT, sender), quietSweep());

        long before = System.nanoTime();
        dispatcher.signal(List.of("a", "b"));
        dispatcher.signal(List.of("c"));
        assertTrue(System.nanoTime() - before < TimeUnit.MILLISECONDS.toNanos(50), "signal never waits for the broker");
        release.countDown();

        await(() -> sender.sent.size() == 3);
        assertEquals(List.of("tx-a", "tx-b", "tx-c"), sender.keys());
        for (ScriptedSender.Sent item : sender.sent) {
            assertEquals(CHECKOUT, item.topic());
            assertArrayEquals(store.entry(item.key().substring(3)).row.envelope(), item.value());
        }
    }

    @Test
    void publishedAtIsMarkedOnlyAfterConfirmationInBatchesOfAtMost200() throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 450; i++) {
            ids.add("e" + i);
            store.add(row("e" + i, CHECKOUT, "tx-" + (i % 7), Instant.now()));
        }
        ScriptedSender sender = new ScriptedSender();
        sender.autoConfirm = false;
        OutboxDispatcher dispatcher = start(store, Map.of(CHECKOUT, sender), quietSweep());

        dispatcher.signal(ids);
        await(() -> sender.sent.size() == 450);
        Thread.sleep(100);
        assertTrue(store.markCalls().isEmpty(), "nothing is marked before the broker confirms");
        assertEquals(450, store.countUnpublished());

        sender.confirmAll();
        await(() -> store.countUnpublished() == 0);
        List<List<String>> calls = store.markCalls();
        assertTrue(calls.size() >= 3, "450 confirmations need at least three batches: " + calls.size());
        int marked = 0;
        for (List<String> call : calls) {
            assertTrue(call.size() <= 200, "a batch holds at most 200 IDs");
            marked += call.size();
        }
        assertEquals(450, marked, "each ID is marked once");
    }

    @Test
    void aLoneConfirmationIsMarkedWithinTheMarkingInterval() throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        store.add(row("solo", CHECKOUT, "tx-1", Instant.now()));
        ScriptedSender sender = new ScriptedSender();
        start(store, Map.of(CHECKOUT, sender), quietSweep()).signal(List.of("solo"));
        await(() -> store.entry("solo").publishedAt != null);
        assertEquals(List.of(List.of("solo")), store.markCalls(), "one store call for the one-ID batch");
    }

    @Test
    void aFailedSendStaysUnpublishedAndARestartedDispatcherResendsTheSameBytes() throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        store.add(row("lost", CHECKOUT, "tx-9", Instant.now()));
        ScriptedSender failing = new ScriptedSender();
        failing.autoFail = true;
        OutboxDispatcher first = start(store, Map.of(CHECKOUT, failing), quietSweep());
        first.signal(List.of("lost"));
        await(() -> failing.sent.size() == 1);
        Thread.sleep(50);
        assertNull(store.entry("lost").publishedAt, "a failed send never marks the row");
        first.stop(Duration.ofSeconds(1));

        ScriptedSender healthy = new ScriptedSender();
        start(store, Map.of(CHECKOUT, healthy), fastSweep(Duration.ZERO));
        await(() -> store.entry("lost").publishedAt != null);
        assertEquals(1, healthy.sent.size());
        assertArrayEquals(failing.sent.get(0).value(), healthy.sent.get(0).value(), "the re-send reuses the stored envelope");
        assertEquals("tx-9", healthy.sent.get(0).key());
        assertEquals(1, store.entry("lost").sendCount, "a sweep re-send counts in send_count");
    }

    @Test
    void theSweepSelectsAtMost500OldUnpublishedRowsInOutboxOrder() throws Exception {
        MutableClock clock = new MutableClock(Instant.parse("2026-10-29T12:00:00Z"));
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        for (int i = 0; i < 600; i++) store.add(row("old" + i, CHECKOUT, "k" + i, clock.instant().minusSeconds(5)));
        store.add(row("young", CHECKOUT, "young", clock.instant()));
        ScriptedSender sender = new ScriptedSender();
        sender.autoConfirm = false;
        OutboxDispatcher dispatcher = new OutboxDispatcher("test", store, Map.of(CHECKOUT, sender), List.of(CHECKOUT),
                OutboxDispatcher.Settings.defaults().withImmediate(false)
                        .withSweep(Duration.ofMillis(50), Duration.ofSeconds(1)), clock);
        started.add(dispatcher);
        dispatcher.start();

        await(() -> sender.sent.size() >= 500);
        Thread.sleep(150);
        assertEquals(500, sender.sent.size(), "one sweep re-sends at most 500 rows, and rows in flight are not repeated");
        for (int i = 0; i < 500; i++) assertEquals("k" + i, sender.sent.get(i).key(), "outbox_seq order");
        InMemoryOutboxStore.SelectCall call = store.selectCalls().get(0);
        assertEquals(500, call.limit());
        assertEquals(clock.instant().minusSeconds(1), call.olderThan(), "only rows older than 1 s");

        sender.confirmAll();
        await(() -> sender.sent.size() == 600);
        assertFalse(sender.keys().contains("young"), "a row younger than 1 s is left to the immediate path");
        clock.advance(Duration.ofSeconds(2));
        await(() -> sender.keys().contains("young"));
    }

    @Test
    void sweepResendsBackOffFrom100MsDoublingToTheCap() throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        store.add(row("stuck", CHECKOUT, "tx-s", Instant.now().minusSeconds(5)));
        ScriptedSender sender = new ScriptedSender();
        sender.autoFail = true;
        OutboxDispatcher.Settings settings = new OutboxDispatcher.Settings(Duration.ofMillis(10), Duration.ofSeconds(1),
                500, Duration.ofMillis(20), 200, Duration.ofMillis(100), Duration.ofMillis(400), false);
        start(store, Map.of(CHECKOUT, sender), settings);
        await(() -> sender.sent.size() >= 6);
        List<Long> gaps = new ArrayList<>();
        for (int i = 1; i < 6; i++) {
            gaps.add(TimeUnit.NANOSECONDS.toMillis(sender.sent.get(i).atNanos() - sender.sent.get(i - 1).atNanos()));
        }
        assertTrue(gaps.get(0) >= 95, "first retry after 100 ms: " + gaps);
        assertTrue(gaps.get(1) >= 195, "then 200 ms: " + gaps);
        assertTrue(gaps.get(2) >= 395 && gaps.get(3) >= 395, "then capped at the maximum: " + gaps);
        assertTrue(gaps.get(4) < 700, "the cap holds: " + gaps);
        assertNull(store.entry("stuck").publishedAt);
        assertTrue(store.entry("stuck").sendCount >= 6);
    }

    @Test
    void resendSendsARecordedRowAgainByItsEventId() throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        store.add(row("done", CHECKOUT, "tx-d", Instant.now()));
        ScriptedSender sender = new ScriptedSender();
        OutboxDispatcher dispatcher = start(store, Map.of(CHECKOUT, sender), quietSweep());
        dispatcher.signal(List.of("done"));
        await(() -> store.entry("done").publishedAt != null);
        Instant firstPublished = store.entry("done").publishedAt;

        dispatcher.resend("done");
        await(() -> sender.sent.size() == 2);
        assertArrayEquals(sender.sent.get(0).value(), sender.sent.get(1).value());
        assertEquals(1, store.entry("done").sendCount);
        Thread.sleep(60);
        assertEquals(firstPublished, store.entry("done").publishedAt, "the first publication time is kept");
        assertThrows(IllegalArgumentException.class, () -> dispatcher.resend("never-recorded"));
    }

    @Test
    void eachRowGoesToTheSenderOfItsTopicAndAMissingSenderFailsConstruction() throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        store.add(row("item", CATALOG, "SKU-1", Instant.now()));
        store.add(row("request", CHECKOUT, "tx-1", Instant.now()));
        ScriptedSender catalogSender = new ScriptedSender();
        ScriptedSender checkoutSender = new ScriptedSender();
        OutboxDispatcher dispatcher = start(store, Map.of(CATALOG, catalogSender, CHECKOUT, checkoutSender), quietSweep());
        dispatcher.signal(List.of("item", "request"));
        await(() -> catalogSender.sent.size() == 1 && checkoutSender.sent.size() == 1);
        assertEquals("SKU-1", catalogSender.sent.get(0).key());
        assertEquals("tx-1", checkoutSender.sent.get(0).key());

        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
                () -> new OutboxDispatcher("test", store, Map.of(CHECKOUT, checkoutSender), List.of(CHECKOUT, CATALOG),
                        OutboxDispatcher.Settings.defaults(), Clock.systemUTC()));
        assertTrue(missing.getMessage().contains(CATALOG));
    }

    @Test
    void withTheImmediatePathOffTheSweepAlonePublishesEveryRow() throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            ids.add("s" + i);
            store.add(row("s" + i, CHECKOUT, "tx-" + i, Instant.now()));
        }
        ScriptedSender sender = new ScriptedSender();
        OutboxDispatcher dispatcher = start(store, Map.of(CHECKOUT, sender),
                OutboxDispatcher.Settings.defaults().withImmediate(false).withSweep(Duration.ofMillis(20), Duration.ZERO));
        dispatcher.signal(ids);
        await(() -> store.countUnpublished() == 0);
        assertEquals(40, sender.sent.size(), "every row was published exactly once, by the sweep");
    }

    @Test
    void aStoreOutageDelaysPublicationWithoutLosingRows() throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        store.add(row("x", CHECKOUT, "tx-x", Instant.now()));
        ScriptedSender sender = new ScriptedSender();
        OutboxDispatcher dispatcher = start(store, Map.of(CHECKOUT, sender), fastSweep(Duration.ZERO));
        store.failWith(new StoreFailure("database unavailable"));
        dispatcher.signal(List.of("x"));
        Thread.sleep(100);
        assertTrue(sender.sent.isEmpty());
        store.failWith(null);
        await(() -> store.entry("x").publishedAt != null);
    }

    @Test
    void stopMarksEveryConfirmedRowBeforeReturning() throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            ids.add("f" + i);
            store.add(row("f" + i, CHECKOUT, "tx-" + i, Instant.now()));
        }
        ScriptedSender sender = new ScriptedSender();
        OutboxDispatcher dispatcher = start(store, Map.of(CHECKOUT, sender), quietSweep());
        dispatcher.signal(ids);
        dispatcher.stop(Duration.ofSeconds(2));
        assertEquals(30, sender.sent.size(), "queued work is sent before the dispatcher stops");
        assertEquals(0, store.countUnpublished(), "the final mark covers every confirmation");
        AtomicBoolean after = new AtomicBoolean();
        dispatcher.signal(List.of("f0"));
        after.set(sender.sent.size() == 30);
        assertTrue(after.get(), "a stopped dispatcher accepts no work");
    }

    private OutboxDispatcher start(InMemoryOutboxStore store, Map<String, RecordSender> senders,
                                   OutboxDispatcher.Settings settings) {
        OutboxDispatcher dispatcher = new OutboxDispatcher("test", store, senders, senders.keySet(), settings,
                Clock.systemUTC());
        started.add(dispatcher);
        dispatcher.start();
        return dispatcher;
    }

    /** A sweep that never finds the test's fresh rows. */
    private static OutboxDispatcher.Settings quietSweep() {
        return OutboxDispatcher.Settings.defaults().withSweep(Duration.ofSeconds(30), Duration.ofSeconds(30));
    }

    private static OutboxDispatcher.Settings fastSweep(Duration minAge) {
        return OutboxDispatcher.Settings.defaults().withSweep(Duration.ofMillis(20), minAge);
    }

    private static OutboxRow row(String eventId, String topic, String key, Instant createdAt) {
        return new OutboxRow(eventId, topic, key, "CheckoutRequested", key + ":1",
                ("envelope-" + eventId).getBytes(StandardCharsets.UTF_8), createdAt);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("condition not reached within 5 s");
            Thread.sleep(5);
        }
    }

    static final class MutableClock extends Clock {
        private volatile Instant now;

        MutableClock(Instant start) {
            now = start;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }
}
