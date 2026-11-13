package unit.notification;

import database.NotificationStore;
import database.NotificationStore.Pending;
import database.SubscriptionStore;
import events.EventIds;
import events.RecordSender;
import notification.Channels;
import notification.ChargingTracker;
import notification.NotificationDispatcher;
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

/**
 * The in-memory collaborators of {@link NotificationDispatcher} that its unit tests share (007 T042, T095): a fixture
 * with a manual clock, a sender whose confirmations the test completes, the {@code notification} table's rules in
 * memory, and the channel reads of the subscription store.
 */
final class DispatcherFakes {
    static final Instant T0 = Instant.parse("2026-11-11T12:00:00Z");

    private DispatcherFakes() { }

    static final class Fixture {
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
    static final class ControlledSender implements RecordSender {
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
    static final class FakeStore implements NotificationStore {
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
        public synchronized int retryParked(Optional<String> subscriberId, Instant now) {
            int retried = 0;
            for (Row row : rows.values()) {
                if (!row.state.equals("PARKED")) continue;
                if (subscriberId.isPresent() && !subscriberId.get().equals(row.pending.subscriberId())) continue;
                row.state = "PENDING";
                row.charged = 0;
                row.nextAttemptAt = now;
                retried++;
            }
            return retried;
        }

        @Override
        public synchronized List<Parked> listParked(Optional<String> subscriberId, int limit) {
            return rows.values().stream()
                    .filter(row -> row.state.equals("PARKED"))
                    .filter(row -> subscriberId.map(row.pending.subscriberId()::equals).orElse(true))
                    .limit(limit)
                    .map(row -> new Parked(row.pending.notificationId(), row.pending.saleId(), row.pending.sku(),
                            row.pending.subscriberId(), row.charged, row.lastError, clock.instant()))
                    .toList();
        }

        @Override
        public synchronized Counts counts() {
            return new Counts(count("PENDING"), count("DELIVERED"), count("PARKED"), 0, 0);
        }
    }

    /** Only the channel reads and the ready flag of the subscription store. */
    static final class FakeSubscriptions implements SubscriptionStore {
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
