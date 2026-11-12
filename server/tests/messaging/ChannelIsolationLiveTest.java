package messaging;

import database.JdbcDeadLetterStore;
import database.JdbcNotificationStore;
import database.JdbcSelectionStore;
import database.JdbcSubscriptionStore;
import events.ConsumerLoop;
import events.EnvelopeCodec;
import events.EventIds;
import events.EventTypes;
import events.TopicNames;
import faults.FaultHooks;
import notification.ChannelFaults;
import notification.Channels;
import notification.ChargingTracker;
import notification.KnownSkus;
import notification.NotificationDispatcher;
import notification.SaleSelectionHandler;
import notification.SubscriptionIndex;
import notification.SubscriptionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.SoldLine;
import selfcheckout.events.v1.SubscriberNotified;
import support.KafkaTestSupport;
import support.NotificationTestSchema;
import support.TestEvents;

import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 007 US2 acceptance scenario 5 (T045) on the live broker: three subscribers with their own channel topics; while
 * {@code notification.channel.reject:<s1>} and then {@code notification.channel.stall:<s1>} act on s1's channel
 * through {@link ChannelFaults}, s2 and s3 receive every notification on their own topics within 1 s of its
 * selection, and s1's rows stay {@code PENDING} with charged failures. Once the fault is cleared (and any parked
 * row retried), s1's notifications are delivered too. The stall is shortened to 2 s for the test.
 */
class ChannelIsolationLiveTest {
    private static final String SKU = "SKU-000001";
    private static final Duration WAIT = Duration.ofSeconds(60);

    @AfterEach
    void clearFaults() {
        System.clearProperty(FaultHooks.PROPERTY);
    }

    @Test
    void oneFaultyChannelDelaysNoOtherSubscriber() throws Exception {
        KafkaTestSupport.requireBroker();
        TopicNames names = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(names);
        String run = UUID.randomUUID().toString().substring(0, 6);
        String s1 = "iso-" + run + "-1", s2 = "iso-" + run + "-2", s3 = "iso-" + run + "-3";
        try (NotificationTestSchema schema = NotificationTestSchema.create();
             KafkaChannelProvisioner provisioner = new KafkaChannelProvisioner(KafkaTestSupport.bootstrap());
             KafkaRecordSender channelSender = ChannelSender.lazy(KafkaTestSupport.clientEnvironment());
             KafkaRecordSender saleSender = KafkaTestSupport.sender("iso-sales", 0)) {
            Clock clock = Clock.systemUTC();
            JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(schema.pool());
            KnownSkus knownSkus = new KnownSkus();
            knownSkus.handle(TestEvents.item(SKU, "Item", "1.00", "v1"));
            knownSkus.handle(TestEvents.snapshot("v1", 1));
            SubscriptionIndex index = new SubscriptionIndex();
            Channels channels = new Channels(subscriptions, provisioner);
            SubscriptionService service = new SubscriptionService(subscriptions, index, knownSkus, channels, names, clock);
            service.loadIndex();
            JdbcNotificationStore store = new JdbcNotificationStore(schema.pool());
            NotificationDispatcher dispatcher = new NotificationDispatcher(store, channels,
                    new ChannelFaults(channelSender, Duration.ofSeconds(2)), new ChargingTracker(),
                    NotificationDispatcher.Settings.defaults(), clock);
            String group = names.group(TopicNames.NOTIFICATION_SALE_COMPLETED_GROUP);
            ConsumerLoop sales = SaleCompletedConsumer.create(names, KafkaTestSupport.streams("iso-sale-consumer", group),
                    new SaleSelectionHandler(new JdbcSelectionStore(schema.pool(), clock), index, dispatcher::accept),
                    new JdbcDeadLetterStore(schema.pool(), "notification.deadLetter"), ConsumerLoop.Settings.defaults());
            dispatcher.start();
            sales.start();
            try {
                for (String subscriber : List.of(s1, s2, s3)) {
                    assertTrue(service.register(subscriber, List.of(SKU)).state().channelReady(), subscriber + "'s channel exists");
                }
                KafkaTestSupport.awaitGroupAssigned(group, names.saleCompleted(), 16, 1, Duration.ofSeconds(30));

                System.setProperty(FaultHooks.PROPERTY, FaultHooks.CHANNEL_REJECT + ":" + s1);
                sellFor(saleSender, names, 15, Duration.ofMillis(100));
                await(() -> delivered(schema, s2) == 15 && delivered(schema, s3) == 15, "s2 and s3 receive the first sales");

                System.setProperty(FaultHooks.PROPERTY, FaultHooks.CHANNEL_STALL + ":" + s1);
                sellFor(saleSender, names, 15, Duration.ofMillis(200));
                await(() -> delivered(schema, s2) == 30 && delivered(schema, s3) == 30, "s2 and s3 receive every sale");

                for (String healthy : List.of(s2, s3)) {
                    long slowest = schema.queryLong("SELECT MAX(TIMESTAMPDIFF(MICROSECOND, created_at, delivered_at)) "
                            + "FROM notification WHERE subscriber_id = ?", healthy);
                    assertTrue(slowest < 1_000_000, healthy + "'s slowest delivery took " + slowest + " us");
                    assertEquals(30, distinctOnChannel(names.channel(healthy), healthy, 30));
                }
                assertEquals(0, delivered(schema, s1), "nothing reaches the faulty channel");
                assertEquals(30, schema.queryLong("SELECT COUNT(*) FROM notification WHERE subscriber_id = ? "
                        + "AND state IN ('PENDING', 'PARKED')", s1));
                assertTrue(schema.queryLong("SELECT SUM(charged_failures) FROM notification WHERE subscriber_id = ?", s1) > 0,
                        "s1's failures are charged to its channel while the others confirm");
                assertEquals(0, schema.queryLong("SELECT COALESCE(SUM(charged_failures), 0) FROM notification "
                        + "WHERE subscriber_id <> ?", s1), "no other channel is charged");

                System.clearProperty(FaultHooks.PROPERTY);
                store.retryParked(Optional.of(s1), clock.instant());
                await(() -> delivered(schema, s1) == 30, "s1 receives its notifications once the fault is gone");
                assertEquals(30, distinctOnChannel(names.channel(s1), s1, 30));
            } finally {
                sales.stop(Duration.ofSeconds(5));
                dispatcher.stop(Duration.ofSeconds(5));
            }
        }
    }

    /** Publishes {@code count} completed sales of one unit of {@link #SKU}, {@code gap} apart. */
    private static void sellFor(KafkaRecordSender sender, TopicNames names, int count, Duration gap) throws Exception {
        for (int i = 0; i < count; i++) {
            String id = "iso-" + UUID.randomUUID();
            long now = System.currentTimeMillis();
            SaleCompleted sale = SaleCompleted.newBuilder().setSaleId(id).setTransactionId(id).setAttemptNumber(1)
                    .setCompletedAtMs(now).setStationId("iso-station")
                    .addLines(SoldLine.newBuilder().setSku(SKU).setQuantity(1)).build();
            sender.send(names.saleCompleted(), id, EnvelopeCodec.encode(EventIds.saleEventId(id), EventTypes.SALE_COMPLETED,
                    id, id, EventTypes.TRANSACTION, now, sale)).get();
            Thread.sleep(gap.toMillis());
        }
    }

    private static long delivered(NotificationTestSchema schema, String subscriber) {
        try {
            return schema.queryLong("SELECT COUNT(*) FROM notification WHERE subscriber_id = ? AND state = 'DELIVERED'", subscriber);
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    /** The distinct notification IDs on a channel topic once it holds {@code expected} of them. */
    private static int distinctOnChannel(String topic, String subscriber, int expected) {
        Set<String> ids = new HashSet<>();
        TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaConsumer<String, byte[]> consumer = KafkaClients.consumer(KafkaTestSupport.clientEnvironment(),
                "iso-reader-" + UUID.randomUUID(), null)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (ids.size() < expected && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    assertEquals(subscriber, record.key(), "a channel record is keyed by its subscriber");
                    var event = EnvelopeCodec.decode(record.value(), record.key());
                    assertEquals(EventTypes.SUBSCRIBER_NOTIFIED, event.type());
                    ids.add(event.payload(SubscriberNotified.class).getNotificationId());
                }
            }
        }
        return ids.size();
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "timed out waiting: " + what);
            Thread.sleep(50);
        }
    }
}
