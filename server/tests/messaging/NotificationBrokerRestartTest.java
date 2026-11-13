package messaging;

import events.EnvelopeCodec;
import events.EventIds;
import events.EventTypes;
import faults.FaultHooks;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.SubscriberNotified;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.LiveNotification;
import support.NotificationTestSchema;
import support.ServiceHttp;
import support.TransactionTestSchema;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 007 US5 acceptance scenario 3 (T097): with five subscribers and sales in flight, the native broker is stopped for
 * 30 s and started again. Transaction's {@code SaleCompleted} rows wait in its outbox and are published after the
 * restart, every owed notification reaches its channel exactly once in effect, no failure is charged to a channel
 * (the outage is broker-wide), and nothing is parked. Needs {@code KAFKA_TEST_BROKER_CONTROL=1}.
 */
class NotificationBrokerRestartTest {
    private static final int PRODUCTS = 5;
    private static final int STOCK = 1_000;
    private static final int BEFORE = 20;
    private static final int DURING = 30;
    private static final int SUBSCRIBERS = 5;
    private static final Duration OUTAGE = Duration.ofSeconds(30);
    private static final Duration WAIT = Duration.ofSeconds(180);

    @AfterEach
    void clearFaults() {
        System.clearProperty(FaultHooks.PROPERTY);
    }

    @Test
    void aThirtySecondBrokerOutageDeliversEveryNotificationOnceAndChargesNoChannel() throws Exception {
        KafkaTestSupport.requireBrokerControl();
        String run = UUID.randomUUID().toString().substring(0, 6);
        List<String> skus = new ArrayList<>();
        for (int n = 1; n <= PRODUCTS; n++) skus.add(InventoryTestSchema.sku(n));
        List<String> subscribers = new ArrayList<>();
        for (int s = 1; s <= SUBSCRIBERS; s++) subscribers.add("brk-" + run + "-" + s);
        try (CheckoutDeployment deployment = CheckoutDeployment.start(PRODUCTS, STOCK);
             NotificationTestSchema schema = NotificationTestSchema.create();
             LiveNotification notification = LiveNotification.start(schema, deployment.names, skus, "brk-sale-consumer", 4)) {
            for (String subscriber : subscribers) {
                assertTrue(notification.service.register(subscriber, skus).state().channelReady(), subscriber);
            }
            notification.awaitAssigned(16, Duration.ofSeconds(30));
            ServiceHttp http = deployment.transactionHttp;
            Map<String, String> soldSku = new HashMap<>();

            // Each of the first sales' selections waits 4 s before its commit, so they commit, and their notifications
            // are sent, while the broker is down: those sends certainly fail, and no channel is at fault.
            System.setProperty(FaultHooks.PROPERTY, "notification.selection.beforeCommit:delay:4000");
            for (int i = 0; i < BEFORE; i++) {
                String id = basket(http, "brk-before-" + i, skus.get(i % PRODUCTS), soldSku);
                assertEquals(200, http.complete(id).statusCode());
            }
            List<String> inFlight = new ArrayList<>();
            for (int i = 0; i < DURING; i++) inFlight.add(basket(http, "brk-during-" + i, skus.get(i % PRODUCTS), soldSku));
            ExecutorService stations = Executors.newFixedThreadPool(DURING);
            List<Future<Integer>> responses = new ArrayList<>();
            try {
                for (String id : inFlight) responses.add(stations.submit(() -> http.complete(id).statusCode()));
                Thread.sleep(50);
                KafkaTestSupport.stopBroker();
                assertTrue(schema.saleSelections() < BEFORE, "some of the first sales are selected during the outage");
                Thread.sleep(OUTAGE.toMillis());
                System.clearProperty(FaultHooks.PROPERTY);
                KafkaTestSupport.startBroker();
                for (Future<Integer> response : responses) {
                    int status = response.get(120, TimeUnit.SECONDS);
                    assertTrue(status == 200 || status == 500, "settled, or handed over within the budget: " + status);
                }
            } finally {
                stations.shutdownNow();
            }

            await(() -> query(() -> deployment.transactionSchema.pendingAttemptCount()) == 0,
                    "every pending attempt settles" + deployment.logs());
            for (String id : soldSku.keySet()) {
                List<TransactionTestSchema.AttemptRow> attempts = deployment.transactionSchema.attempts(id);
                assertEquals("COMPLETED", attempts.get(attempts.size() - 1).state(), id + " completed");
            }
            int sales = soldSku.size();
            await(() -> query(() -> deployment.transactionSchema.queryLong("SELECT COUNT(*) FROM outbox_event "
                    + "WHERE event_type = 'SaleCompleted' AND published_at IS NULL")) == 0, "saleEventsUnpublished returns to 0");
            assertEquals(sales, deployment.transactionSchema.queryLong(
                    "SELECT COUNT(*) FROM outbox_event WHERE event_type = 'SaleCompleted'"));
            await(() -> query(schema::saleSelections) == sales, "every sale is selected");
            await(() -> query(() -> schema.notifications("DELIVERED")) == (long) sales * SUBSCRIBERS,
                    "every owed notification is delivered");

            assertEquals((long) sales * SUBSCRIBERS, schema.notifications(), "one row per (sale, SKU, subscriber)");
            assertEquals(0, schema.notifications("PARKED"), "no row is parked");
            assertEquals(0, schema.queryLong("SELECT COALESCE(SUM(charged_failures), 0) FROM notification"),
                    "a broker-wide outage charges no channel");
            assertEquals(0, schema.queryLong("SELECT COALESCE(MAX(charged_failures), 0) FROM notification"));
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM dead_letter_event"), "no sale event is parked");
            long uncharged = schema.queryLong("SELECT COALESCE(SUM(uncharged_failures), 0) FROM notification");
            System.out.println("[broker-restart] " + sales + " sales, " + uncharged + " uncharged failures during the outage");
            assertTrue(uncharged > 0, "notifications were pending during the outage, and their failures were uncharged");

            for (String subscriber : subscribers) {
                Set<String> expected = new HashSet<>();
                soldSku.forEach((sale, sku) -> expected.add(EventIds.notificationId(sale, sku, subscriber)));
                Map<String, Integer> received = channel(deployment.names.channel(subscriber), subscriber, expected.size());
                assertEquals(expected, received.keySet(), subscriber + " received every owed notification and nothing else");
            }
        }
    }

    /** A basket of one unit of {@code sku}; records the sale's SKU. */
    private static String basket(ServiceHttp http, String station, String sku, Map<String, String> soldSku) throws Exception {
        String id = http.start(station);
        assertEquals(200, http.scan(id, sku).statusCode());
        soldSku.put(id, sku);
        return id;
    }

    /** The notification IDs on a channel with the number of copies of each, once {@code expected} distinct IDs arrived. */
    private static Map<String, Integer> channel(String topic, String subscriber, int expected) {
        Map<String, Integer> copies = new HashMap<>();
        TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaConsumer<String, byte[]> consumer = KafkaClients.consumer(KafkaTestSupport.clientEnvironment(),
                "brk-reader-" + UUID.randomUUID(), null)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (copies.size() < expected && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    var event = EnvelopeCodec.decode(record.value(), record.key());
                    assertEquals(EventTypes.SUBSCRIBER_NOTIFIED, event.type());
                    copies.merge(event.payload(SubscriberNotified.class).getNotificationId(), 1, Integer::sum);
                }
            }
            for (int extra = 0; extra < 5; extra++) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    copies.merge(EnvelopeCodec.decode(record.value(), record.key()).payload(SubscriberNotified.class)
                            .getNotificationId(), 1, Integer::sum);
                }
            }
        }
        return copies;
    }

    private interface Query {
        long get() throws Exception;
    }

    private static long query(Query query) {
        try {
            return query.get();
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "timed out waiting: " + what);
            Thread.sleep(250);
        }
    }
}
