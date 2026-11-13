package unit.notification;

import database.NotificationStore;
import database.NotificationStore.Pending;
import database.SubscriptionStore;
import events.EnvelopeCodec;
import events.EventIds;
import events.RecordSender;
import notification.Channels;
import notification.ChargingTracker;
import notification.NotificationDispatcher;
import org.junit.jupiter.api.Test;
import support.CountingRecordSender;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The dispatch table of contracts/notification-delivery.md (007 T042), with a {@link CountingRecordSender} over a
 * sender whose confirmations the test completes, a manual clock, and in-memory stores: the immediate path, lanes of
 * at most 64 in flight served round-robin, batched confirmations, the failure backoff, the sweep after a restart, and
 * provisioning before the first send.
 */
class NotificationDispatcherTest {
    private static final Instant T0 = Instant.parse("2026-11-11T12:00:00Z");

    @Test
    void theImmediatePathSendsRowsAsSoonAsTheyAreTakenIn() {
        Fixture fixture = new Fixture();
        fixture.dispatcher.runOnce();   // the startup sweep finds nothing; the next one is a second away
        List<Pending> rows = fixture.store.select("tx-1", "s1", "s2");
        fixture.dispatcher.accept(rows);
        fixture.dispatcher.runOnce();
        assertEquals(2, fixture.counting.sends());
        assertEquals(List.of("chan-s1", "chan-s2"), fixture.transport.topics());
        Map.Entry<String, byte[]> first = fixture.transport.sent.get(0);
        var event = EnvelopeCodec.decode(first.getValue(), "s1");
        assertEquals(EventIds.notificationId("tx-1", "A", "s1"), event.eventId(), "event_id = notification_id");
        assertEquals("tx-1", event.correlationId());
        assertEquals("notification", event.producer());
        assertEquals(T0.toEpochMilli(), event.occurredAtMs(), "occurred_at_ms = the selection time");
    }

    @Test
    void lanesHoldAtMost64InFlightAndAreServedRoundRobin() {
        Fixture fixture = new Fixture();
        List<Pending> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) rows.addAll(fixture.store.select("tx-" + i, "s1"));
        for (int i = 0; i < 5; i++) rows.addAll(fixture.store.select("tx-b" + i, "s2"));
        fixture.dispatcher.accept(rows);
        fixture.dispatcher.runOnce();
        assertEquals(64, fixture.dispatcher.inFlight("s1"));
        assertEquals(36, fixture.dispatcher.queued("s1"));
        assertEquals(5, fixture.dispatcher.inFlight("s2"));
        assertEquals(69, fixture.counting.sends());
        assertEquals(List.of("chan-s1", "chan-s2", "chan-s1", "chan-s2"), fixture.transport.topics().subList(0, 4),
                "the lanes take turns");

        fixture.transport.confirm(10);   // the first ten sends: five on each lane
        fixture.dispatcher.runOnce();
        assertEquals(64, fixture.dispatcher.inFlight("s1"), "confirmed sends make room for queued rows");
        assertEquals(31, fixture.dispatcher.queued("s1"));
        assertEquals(0, fixture.dispatcher.inFlight("s2"));
    }

    @Test
    void confirmationsAreMarkedInBatchesOf20MillisecondsOr200Ids() {
        Fixture fixture = new Fixture();
        fixture.dispatcher.accept(fixture.store.select("tx-1", "s1", "s2", "s3"));
        fixture.dispatcher.runOnce();
        fixture.transport.confirmAll();
        fixture.dispatcher.runOnce();
        assertTrue(fixture.store.markCalls.isEmpty(), "a batch younger than 20 ms waits");
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        assertEquals(List.of(3), fixture.store.markCalls);
        assertEquals(3, fixture.store.count("DELIVERED"));
        assertEquals(0, fixture.dispatcher.tracked());

        List<Pending> many = new ArrayList<>();
        for (int i = 0; i < 200; i++) many.addAll(fixture.store.select("tx-m" + i, "s" + (i % 4)));
        fixture.dispatcher.accept(many);
        fixture.dispatcher.runOnce();
        fixture.transport.confirmAll();
        fixture.dispatcher.runOnce();
        assertEquals(List.of(3, 200), fixture.store.markCalls, "200 IDs are written at once, in one batch");
    }

    @Test
    void failuresBackOffAndAreChargedOnlyWhenAnotherChannelConfirmed() {
        Fixture fixture = new Fixture();
        fixture.dispatcher.accept(fixture.store.select("tx-1", "s1"));
        fixture.dispatcher.runOnce();
        fixture.transport.failAll("broker unavailable");
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        FakeStore.Row row = fixture.store.only("s1");
        assertEquals(1, row.uncharged, "no other channel confirmed: a broker-wide failure");
        assertEquals(0, row.charged);
        assertEquals(fixture.clock.instant().plusMillis(400), row.nextAttemptAt, "200 ms x 2^1");
        assertTrue(row.lastError.contains("broker unavailable"));

        // s2 confirms after s1's next attempt started, so s1's failure is charged.
        fixture.clock.advance(Duration.ofSeconds(1));
        fixture.dispatcher.accept(fixture.store.select("tx-2", "s2"));
        fixture.dispatcher.runOnce();
        assertEquals(2, fixture.transport.pending(), "the sweep re-sent s1's due row and s2's row went out");
        fixture.clock.advance(Duration.ofMillis(1));
        fixture.transport.confirm("s2");
        fixture.transport.fail("s1", "rejected");
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        assertEquals(1, row.charged);
        assertEquals(1, row.uncharged);
        assertEquals(fixture.clock.instant().plusMillis(800), row.nextAttemptAt, "200 ms x 2^2");
    }

    @Test
    void theEighthChargedFailureParksTheRowWithAnAlarm() {
        Fixture fixture = new Fixture();
        FakeStore.Row row = fixture.store.rows.get(fixture.store.select("tx-1", "s1").get(0).notificationId());
        row.charged = 7;
        fixture.store.select("tx-2", "s2");
        fixture.dispatcher.runOnce();
        fixture.clock.advance(Duration.ofMillis(1));
        fixture.transport.confirm("s2");
        fixture.transport.fail("s1", "rejected");
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        assertEquals("PARKED", row.state);
        assertEquals(1, fixture.dispatcher.parkedSinceStart());
        assertTrue(fixture.dispatcher.alarms().stream().anyMatch(alarm -> alarm.startsWith("NOTIFICATIONS_PARKED")));
    }

    @Test
    void noConfirmedSendFor60SecondsWhileRowsArePendingRaisesTheStallAlarmButParksNothing() {
        Fixture fixture = new Fixture();
        fixture.dispatcher.accept(fixture.store.select("tx-1", "s1", "s2"));
        for (int second = 0; second <= 61; second++) {
            fixture.dispatcher.runOnce();
            fixture.transport.failAll("broker unavailable");
            fixture.clock.advance(Duration.ofSeconds(1));
        }
        fixture.dispatcher.runOnce();
        assertTrue(fixture.dispatcher.stalled());
        assertTrue(fixture.dispatcher.alarms().stream().anyMatch(alarm -> alarm.startsWith("DELIVERY_STALLED")));
        assertEquals(0, fixture.store.count("PARKED"), "a broker-wide outage parks nothing");
        assertEquals(0, fixture.store.rows.values().stream().mapToInt(r -> r.charged).sum());

        fixture.clock.advance(Duration.ofSeconds(6));   // past the 5 s backoff cap: the sweep sends both again
        fixture.dispatcher.runOnce();
        assertEquals(2, fixture.transport.pending());
        fixture.transport.confirmAll();
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        assertFalse(fixture.dispatcher.stalled(), "the broker is back");
        assertEquals(2, fixture.store.count("DELIVERED"));
    }

    @Test
    void theSweepResendsDueRowsAfterARestartWithTheSameIdentity() {
        Fixture fixture = new Fixture();
        List<Pending> rows = fixture.store.select("tx-1", "s1", "s2");
        fixture.dispatcher.accept(rows);
        fixture.dispatcher.runOnce();
        fixture.transport.confirmAll();
        fixture.dispatcher.crash();   // confirmed, but the marks were never written

        NotificationDispatcher restarted = fixture.newDispatcher();
        restarted.runOnce();
        assertEquals(4, fixture.counting.sends(), "both rows are sent again");
        List<String> resent = new ArrayList<>();
        for (Map.Entry<String, byte[]> send : fixture.transport.sent.subList(2, 4)) {
            resent.add(EnvelopeCodec.decode(send.getValue(), send.getKey().substring("chan-".length())).eventId());
        }
        assertEquals(rows.stream().map(Pending::notificationId).sorted().toList(), resent.stream().sorted().toList());
        fixture.transport.confirmAll();
        restarted.flushNow();
        assertEquals(2, fixture.store.count("DELIVERED"));
        fixture.clock.advance(Duration.ofSeconds(2));
        restarted.runOnce();
        assertEquals(4, fixture.counting.sends(), "a delivered row is never swept again");
    }

    @Test
    void aChannelThatIsNotReadyIsProvisionedBeforeItsFirstSend() {
        Fixture fixture = new Fixture();
        fixture.subscriptions.ready.put("s1", false);
        fixture.dispatcher.accept(fixture.store.select("tx-1", "s1"));
        fixture.dispatcher.runOnce();
        assertEquals(List.of("chan-s1"), fixture.provisioned);
        assertEquals(List.of("s1"), fixture.subscriptions.markedReady);
        assertEquals(1, fixture.counting.sends());

        fixture.subscriptions.ready.put("s2", false);
        fixture.failProvisioning = true;
        fixture.dispatcher.accept(fixture.store.select("tx-2", "s2"));
        fixture.dispatcher.runOnce();
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        assertEquals(1, fixture.counting.sends(), "nothing is sent to a channel that does not exist");
        assertEquals(1, fixture.store.only("s2").uncharged, "a provisioning failure is uncharged");
    }

    // ------------------------------------------------------------------ fixtures

    private static final class Fixture {
        final MutableClock clock = new MutableClock(T0);
        final FakeStore store = new FakeStore(clock);
        final FakeSubscriptions subscriptions = new FakeSubscriptions();
        final ControlledSender transport = new ControlledSender();
        final CountingRecordSender counting = new CountingRecordSender(transport);
        final List<String> provisioned = new CopyOnWriteArrayList<>();
        final ChargingTracker tracker = new ChargingTracker();
        volatile boolean failProvisioning;
        final NotificationDispatcher dispatcher = newDispatcher();

        NotificationDispatcher newDispatcher() {
            Channels channels = new Channels(subscriptions, topic -> {
                if (failProvisioning) throw new IllegalStateException("admin refused");
                provisioned.add(topic);
            });
            return new NotificationDispatcher(store, channels, counting, tracker, NotificationDispatcher.Settings.defaults(), clock);
        }
    }

    /** A sender whose futures stay open until the test confirms or fails them. */
    private static final class ControlledSender implements RecordSender {
        final List<Map.Entry<String, byte[]>> sent = new CopyOnWriteArrayList<>();
        final List<Map.Entry<String, CompletableFuture<Void>>> open = new ArrayList<>();

        @Override
        public synchronized CompletableFuture<Void> send(String topic, String key, byte[] value) {
            sent.add(Map.entry(topic, value));
            CompletableFuture<Void> future = new CompletableFuture<>();
            open.add(Map.entry(key, future));
            return future;
        }

        List<String> topics() { return sent.stream().map(Map.Entry::getKey).toList(); }

        synchronized int pending() { return open.size(); }

        synchronized void confirm(int count) {
            for (int i = 0; i < count && !open.isEmpty(); i++) open.remove(0).getValue().complete(null);
        }

        synchronized void confirmAll() { confirm(open.size()); }

        synchronized void confirm(String subscriber) {
            open.removeIf(entry -> entry.getKey().equals(subscriber) && entry.getValue().complete(null));
        }

        synchronized void fail(String subscriber, String error) {
            open.removeIf(entry -> entry.getKey().equals(subscriber)
                    && entry.getValue().completeExceptionally(new IllegalStateException(error)));
        }

        synchronized void failAll(String error) {
            for (var entry : open) entry.getValue().completeExceptionally(new IllegalStateException(error));
            open.clear();
        }
    }

    /** The {@code notification} table's rules in memory. */
    private static final class FakeStore implements NotificationStore {
        static final class Row {
            final Pending pending;
            String state = "PENDING";
            int charged;
            int uncharged;
            int attempts;
            Instant nextAttemptAt;
            String lastError;

            Row(Pending pending) {
                this.pending = pending;
                this.nextAttemptAt = pending.createdAt();
            }
        }

        final Clock clock;
        final Map<String, Row> rows = new LinkedHashMap<>();
        final List<Integer> markCalls = new ArrayList<>();

        FakeStore(Clock clock) {
            this.clock = clock;
        }

        /** A committed selection of SKU A x 1 for each subscriber, at the clock's time. */
        synchronized List<Pending> select(String saleId, String... subscribers) {
            List<Pending> created = new ArrayList<>();
            for (String subscriber : subscribers) {
                Pending pending = new Pending(EventIds.notificationId(saleId, "A", subscriber), saleId, "A", subscriber, 1,
                        clock.instant().minusSeconds(1), clock.instant(), 0, 0, 0);
                rows.put(pending.notificationId(), new Row(pending));
                created.add(pending);
            }
            return created;
        }

        synchronized Row only(String subscriber) {
            return rows.values().stream().filter(row -> row.pending.subscriberId().equals(subscriber)).findFirst().orElseThrow();
        }

        synchronized long count(String state) {
            return rows.values().stream().filter(row -> row.state.equals(state)).count();
        }

        @Override
        public synchronized int markDelivered(Collection<String> ids, Instant at) {
            markCalls.add(ids.size());
            int changed = 0;
            for (String id : ids) {
                Row row = rows.get(id);
                if (row != null && row.state.equals("PENDING")) {
                    row.state = "DELIVERED";
                    row.attempts++;
                    changed++;
                }
            }
            return changed;
        }

        @Override
        public synchronized int recordFailures(List<Failure> failures, Instant at, int parkAfterCharged) {
            int parked = 0;
            for (Failure failure : failures) {
                Row row = rows.get(failure.notificationId());
                if (row == null || !row.state.equals("PENDING")) continue;
                row.attempts++;
                if (failure.charged()) row.charged++;
                else row.uncharged++;
                row.lastError = failure.error();
                long micros = Math.min(200_000L << Math.min(row.charged + row.uncharged, 10), 5_000_000L);
                row.nextAttemptAt = at.plus(Duration.ofNanos(micros * 1_000));
                if (failure.charged() && row.charged >= parkAfterCharged) {
                    row.state = "PARKED";
                    parked++;
                }
            }
            return parked;
        }

        @Override
        public synchronized int park(Collection<String> ids, Instant at) {
            int changed = 0;
            for (String id : ids) {
                Row row = rows.get(id);
                if (row != null && row.state.equals("PENDING")) {
                    row.state = "PARKED";
                    changed++;
                }
            }
            return changed;
        }

        @Override
        public synchronized List<Pending> selectDue(Instant now, int limit, Collection<String> skip) {
            return rows.values().stream()
                    .filter(row -> row.state.equals("PENDING") && !row.nextAttemptAt.isAfter(now)
                            && !skip.contains(row.pending.subscriberId()))
                    .sorted((a, b) -> a.nextAttemptAt.compareTo(b.nextAttemptAt))
                    .limit(limit)
                    .map(row -> new Pending(row.pending.notificationId(), row.pending.saleId(), row.pending.sku(),
                            row.pending.subscriberId(), row.pending.quantity(), row.pending.completedAt(),
                            row.pending.createdAt(), row.pending.subscriptionVersion(), row.charged, row.uncharged))
                    .toList();
        }

        @Override
        public int retryParked(Optional<String> subscriberId, Instant now) { throw new UnsupportedOperationException(); }

        @Override
        public synchronized Counts counts() {
            return new Counts(count("PENDING"), count("DELIVERED"), count("PARKED"), 0, 0);
        }
    }

    /** Only the channel reads and the ready flag of the subscription store. */
    private static final class FakeSubscriptions implements SubscriptionStore {
        final Map<String, Boolean> ready = new LinkedHashMap<>();
        final List<String> markedReady = new CopyOnWriteArrayList<>();

        @Override
        public Optional<Channel> channel(String subscriberId) {
            return Optional.of(new Channel(subscriberId, "chan-" + subscriberId, ready.getOrDefault(subscriberId, true)));
        }

        @Override
        public void markChannelReady(String subscriberId) {
            markedReady.add(subscriberId);
            ready.put(subscriberId, true);
        }

        @Override
        public ChangeResult register(String id, SortedSet<String> skus, String topic, String unknown, Instant at) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChangeResult update(String id, SortedSet<String> add, SortedSet<String> remove, String unknown, Instant at) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChangeResult cancel(String id, Instant at) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<SubscriberState> find(String subscriberId) { throw new UnsupportedOperationException(); }

        @Override
        public IndexRows loadIndex() { throw new UnsupportedOperationException(); }

        @Override
        public List<ChangeRow> changesAfter(long after, long upTo) { throw new UnsupportedOperationException(); }

        @Override
        public long currentVersion() { throw new UnsupportedOperationException(); }
    }

    static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant start) {
            this.now = start;
        }

        synchronized void advance(Duration step) {
            now = now.plus(step);
        }

        @Override
        public synchronized Instant instant() { return now; }

        @Override
        public ZoneId getZone() { return ZoneOffset.UTC; }

        @Override
        public Clock withZone(ZoneId zone) { return this; }
    }
}
