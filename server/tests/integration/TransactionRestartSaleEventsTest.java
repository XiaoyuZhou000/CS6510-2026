package integration;

import events.EnvelopeCodec;
import events.EventIds;
import faults.FaultHooks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.SubscriberNotified;
import support.InProcessCheckout;
import support.InProcessNotification;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.NotificationTestSchema;
import transaction.TransactionOperations;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 007 US5 acceptance scenario 5 (T099): sales completed while Transaction's dispatcher cannot publish
 * {@code SaleCompleted} stay as unpublished outbox rows; Transaction then crashes and restarts, its sweep publishes
 * every row, and Notification selects each sale once and notifies its subscriber once per sale.
 *
 * <p>The first Transaction's sender refuses every {@code SaleCompleted} send (the checkout messages still flow, so
 * each Complete commits T3), which leaves the rows exactly as a stopped dispatcher would.</p>
 */
class TransactionRestartSaleEventsTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final int SALES = 6;

    @AfterEach
    void clearFaults() {
        System.clearProperty(FaultHooks.PROPERTY);
    }

    @Test
    void completionsCommittedWhileTheDispatcherIsStoppedArePublishedAfterARestartAndSelectedOnce() throws Exception {
        InProcessCheckout saga = InProcessCheckout.start(5, 100);
        NotificationTestSchema schema = NotificationTestSchema.create();
        List<InProcessTransaction> transactions = new ArrayList<>();
        try (saga; schema; InProcessNotification notification = InProcessNotification.over(schema.pool(), saga.bus, saga.names);
             AutoCloseable stopTransactions = () -> { for (InProcessTransaction each : transactions) each.close(); }) {
            saga.inventory.publishCatalog();
            notification.awaitCatalog(WAIT);
            notification.service().register("s1", List.of(A));
            notification.service().register("s2", List.of(A, B));

            saga.transaction.close();
            String saleTopic = saga.names.saleCompleted();
            InProcessTransaction stopped = InProcessTransaction.over(saga.transactionSchema).pool(4, 24)
                    .bus(saga.bus, saga.names)
                    .wrapSender(sender -> (topic, key, value) -> topic.equals(saleTopic)
                            ? CompletableFuture.failedFuture(new IllegalStateException("dispatcher stopped"))
                            : sender.send(topic, key, value))
                    .build();
            transactions.add(stopped);

            List<String> sales = new ArrayList<>();
            for (int i = 0; i < SALES; i++) {
                TransactionOperations service = stopped.service;
                String id = service.start(new TransactionOperations.StartCommand("restart-" + i)).transactionId();
                service.scan(new TransactionOperations.ScanCommand(id, A));
                if (i % 2 == 0) service.scan(new TransactionOperations.ScanCommand(id, B));
                service.complete(id);
                sales.add(id);
            }
            assertEquals(SALES, unpublishedSaleEvents(saga), "every completion committed its SaleCompleted row");
            assertFalse(saga.bus.hasTopic(saleTopic) && !saga.bus.values(saleTopic).isEmpty(), "nothing was published");
            assertEquals(0, schema.saleSelections());

            stopped.close();   // the crash: the rows are still unpublished
            InProcessTransaction restarted = InProcessTransaction.over(saga.transactionSchema).pool(4, 24)
                    .bus(saga.bus, saga.names).build();
            transactions.add(restarted);

            await(() -> unpublishedSaleEvents(saga) == 0, "the restarted sweep publishes every SaleCompleted row");
            await(() -> delivered(schema) == SALES + SALES + SALES / 2, "every owed notification is delivered");
            Thread.sleep(300);

            assertEquals(SALES, schema.saleSelections(), "each sale is selected once");
            Map<String, Integer> events = new HashMap<>();
            for (byte[] value : saga.bus.values(saleTopic)) {
                SaleCompleted sale = SaleCompleted.parseFrom(EventEnvelope.parseFrom(value).getPayload());
                events.merge(sale.getSaleId(), 1, Integer::sum);
            }
            assertEquals(new TreeSet<>(sales), new TreeSet<>(events.keySet()), "one event per completed sale");
            for (String sale : sales) {
                assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM sale_selection WHERE sale_id = ?", sale));
            }
            assertEquals(Set.copyOf(expectedIds(sales, "s1", false)), Set.copyOf(channelIds(saga, "s1")));
            assertEquals(SALES, channelIds(saga, "s1").size(), "s1: once per sale, no redelivery");
            assertEquals(Set.copyOf(expectedIds(sales, "s2", true)), Set.copyOf(channelIds(saga, "s2")));
            assertEquals(SALES + SALES / 2, channelIds(saga, "s2").size(), "s2: once per sold SKU, no redelivery");
        }
    }

    private static List<String> expectedIds(List<String> sales, String subscriber, boolean withB) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < sales.size(); i++) {
            ids.add(EventIds.notificationId(sales.get(i), A, subscriber));
            if (withB && i % 2 == 0) ids.add(EventIds.notificationId(sales.get(i), B, subscriber));
        }
        return ids;
    }

    private static List<String> channelIds(InProcessCheckout saga, String subscriber) {
        List<String> ids = new ArrayList<>();
        String topic = saga.names.channel(subscriber);
        if (!saga.bus.hasTopic(topic)) return ids;
        for (byte[] value : saga.bus.values(topic)) {
            ids.add(EnvelopeCodec.decode(value, subscriber).payload(SubscriberNotified.class).getNotificationId());
        }
        return ids;
    }

    private static long unpublishedSaleEvents(InProcessCheckout saga) {
        try {
            return saga.transactionSchema.queryLong(
                    "SELECT COUNT(*) FROM outbox_event WHERE event_type = 'SaleCompleted' AND published_at IS NULL");
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static long delivered(NotificationTestSchema schema) {
        try {
            return schema.notifications("DELIVERED");
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "timed out waiting: " + what);
            Thread.sleep(20);
        }
    }
}
