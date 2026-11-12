package integration;

import events.EnvelopeCodec;
import events.EventIds;
import events.TopicNames;
import faults.FaultHooks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.SubscriberNotified;
import support.InProcessCheckout;
import support.InProcessNotification;
import support.InventoryTestSchema;
import support.NotificationTestSchema;
import transaction.TransactionOperations;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 007 US2 acceptance scenarios 1–4 and 6 (T044) on the real Transaction, Inventory, and Notification components over
 * one {@link support.InMemoryEventBus}: per-SKU notifications with the sold quantities on each subscriber's own
 * channel, a selection with zero recipients, the selection committed before the event is acknowledged, two sales of
 * one SKU, and one identity across a forced re-send.
 */
class NotificationFlowTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final String C = InventoryTestSchema.sku(3);
    private static final Duration WAIT = Duration.ofSeconds(30);

    @AfterEach
    void clearFaults() {
        System.clearProperty(FaultHooks.PROPERTY);
    }

    /** Scenarios 1 and 2: S1 = {A, B} and S2 = {B}; 3 x A + 1 x B + 2 x C; then a C-only sale. */
    @Test
    void eachSubscriberGetsOneNotificationPerSubscribedSkuWithItsQuantity() throws Exception {
        try (Flow flow = Flow.start()) {
            flow.register("s1", A, B);
            flow.register("s2", B);
            String sale = flow.sell(A, A, B, C, A, C);

            flow.await(() -> flow.delivered() == 3, "three notifications delivered");
            assertEquals(List.of(line("s1", sale, A, 3), line("s1", sale, B, 1)), flow.channel("s1"));
            assertEquals(List.of(line("s2", sale, B, 1)), flow.channel("s2"));
            assertEquals(2, flow.schema.queryLong("SELECT recipient_count FROM sale_selection WHERE sale_id = ?", sale));
            assertEquals(3, flow.schema.queryLong("SELECT notification_count FROM sale_selection WHERE sale_id = ?", sale));
            assertEquals(0, flow.schema.queryLong("SELECT COUNT(*) FROM notification WHERE sku = ?", C), "nobody subscribes to C");

            String cOnly = flow.sell(C, C);
            flow.await(() -> flow.selected(cOnly), "the C-only sale is selected");
            assertEquals(0, flow.schema.queryLong("SELECT recipient_count FROM sale_selection WHERE sale_id = ?", cOnly));
            assertEquals(0, flow.schema.queryLong("SELECT notification_count FROM sale_selection WHERE sale_id = ?", cOnly));
            assertEquals(2, flow.schema.queryLong("SELECT subscription_version FROM sale_selection WHERE sale_id = ?", cOnly));
            Thread.sleep(300);
            assertEquals(3, flow.schema.notifications(), "a zero-recipient sale creates no notification");
        }
    }

    /** Scenario 3: the selection commits before the sale event's offset is committed. */
    @Test
    void theSelectionIsRecordedBeforeTheEventIsAcknowledged() throws Exception {
        try (Flow flow = Flow.start()) {
            flow.register("s1", A);
            System.setProperty(FaultHooks.PROPERTY, "notification.selection.afterCommit:delay:2000");
            String sale = flow.sell(A);
            flow.await(() -> flow.selected(sale), "the sale is selected");
            String topic = flow.saga.names.saleCompleted();
            int partition = flow.saga.bus.partitionFor(topic, sale);
            String group = flow.saga.names.group(TopicNames.NOTIFICATION_SALE_COMPLETED_GROUP);
            Long during = flow.saga.bus.committedOffset(group, topic, partition);
            assertTrue(during == null || during == 0, "the offset is not committed while the handler has not returned: " + during);
            flow.await(() -> {
                Long committed = flow.saga.bus.committedOffset(group, topic, partition);
                return committed != null && committed == 1;
            }, "the offset is committed after the selection");
            assertEquals(1, flow.schema.saleSelections());
        }
    }

    /** Scenario 4: two sales of one SKU give the subscriber two notifications, one per sale. */
    @Test
    void twoSalesOfOneSkuGiveTwoNotifications() throws Exception {
        try (Flow flow = Flow.start()) {
            flow.register("s1", A);
            String first = flow.sell(A);
            String second = flow.sell(A, A);
            flow.await(() -> flow.delivered() == 2, "both notifications delivered");
            List<SubscriberNotified> received = flow.channel("s1");
            assertEquals(2, received.size());
            assertTrue(received.containsAll(List.of(line("s1", first, A, 1), line("s1", second, A, 2))));
            assertNotEquals(received.get(0).getNotificationId(), received.get(1).getNotificationId());
        }
    }

    /** Scenario 6: a re-send after a crash between the confirmed send and its mark keeps the identity. */
    @Test
    void aForcedResendKeepsTheNotificationIdentity() throws Exception {
        try (Flow flow = Flow.start()) {
            flow.register("s1", B);
            System.setProperty(FaultHooks.PROPERTY, "notification.delivery.afterSend:throw");
            String sale = flow.sell(B);
            flow.await(() -> flow.channel("s1").size() == 1, "the notification reaches the channel");
            Thread.sleep(200);
            assertEquals(1, flow.schema.notifications("PENDING"), "confirmed but never marked");

            flow.notification.crash();
            System.clearProperty(FaultHooks.PROPERTY);
            flow.await(() -> flow.delivered() == 1, "the restarted dispatcher sends and marks it");
            List<SubscriberNotified> received = flow.channel("s1");
            assertEquals(2, received.size(), "the subscriber receives a redelivery");
            assertEquals(received.get(0), received.get(1), "with the same identity and contents");
            assertEquals(EventIds.notificationId(sale, B, "s1"), received.get(0).getNotificationId());
            assertEquals(1, flow.schema.notifications());
        }
    }

    private static SubscriberNotified line(String subscriber, String sale, String sku, int quantity) {
        return SubscriberNotified.newBuilder().setNotificationId(EventIds.notificationId(sale, sku, subscriber))
                .setSubscriberId(subscriber).setSaleId(sale).setSku(sku).setQuantity(quantity).buildPartial();
    }

    /** Checkout, Inventory, and Notification on one bus, with the catalog loaded. */
    private static final class Flow implements AutoCloseable {
        final InProcessCheckout saga;
        final NotificationTestSchema schema;
        final InProcessNotification notification;

        private Flow(InProcessCheckout saga, NotificationTestSchema schema, InProcessNotification notification) {
            this.saga = saga;
            this.schema = schema;
            this.notification = notification;
        }

        static Flow start() throws Exception {
            InProcessCheckout saga = InProcessCheckout.start(5, 100);
            NotificationTestSchema schema = NotificationTestSchema.create();
            InProcessNotification notification = InProcessNotification.over(schema.pool(), saga.bus, saga.names);
            saga.inventory.publishCatalog();
            notification.awaitCatalog(WAIT);
            return new Flow(saga, schema, notification);
        }

        void register(String subscriber, String... skus) {
            notification.service().register(subscriber, List.of(skus));
        }

        String sell(String... skus) {
            TransactionOperations service = saga.checkout();
            String id = service.start(new TransactionOperations.StartCommand("flow-station")).transactionId();
            for (String sku : skus) service.scan(new TransactionOperations.ScanCommand(id, sku));
            service.complete(id);
            return id;
        }

        long delivered() {
            try {
                return schema.notifications("DELIVERED");
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        }

        boolean selected(String sale) {
            try {
                return schema.queryLong("SELECT COUNT(*) FROM sale_selection WHERE sale_id = ?", sale) == 1;
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        }

        /**
         * The subscriber's channel records, as the subscriber would compare them: without the fields the test does not
         * predict (completion time and history version).
         */
        List<SubscriberNotified> channel(String subscriber) {
            String topic = saga.names.channel(subscriber);
            List<SubscriberNotified> received = new ArrayList<>();
            if (!saga.bus.hasTopic(topic)) return received;
            for (byte[] value : saga.bus.values(topic)) {
                var event = EnvelopeCodec.decode(value, subscriber);
                SubscriberNotified notified = event.payload(SubscriberNotified.class);
                assertTrue(notified.getCompletedAtMs() > 0);
                received.add(notified.toBuilder().clearCompletedAtMs().clearSubscriptionVersion().buildPartial());
            }
            return received;
        }

        void await(BooleanSupplier condition, String what) throws InterruptedException {
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (!condition.getAsBoolean()) {
                assertTrue(System.nanoTime() < deadline, "timed out waiting: " + what);
                Thread.sleep(20);
            }
        }

        @Override
        public void close() throws Exception {
            try (saga; schema) {
                notification.close();
            }
        }
    }
}
