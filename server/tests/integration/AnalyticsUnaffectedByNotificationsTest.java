package integration;

import analytics.AnalyticsService;
import database.JdbcRelayStore;
import events.TopicNames;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.InMemoryEventBus;
import support.InProcessAnalytics;
import support.InProcessInventory;
import support.InProcessNotification;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.NotificationTestSchema;
import support.TestSchema;
import support.TransactionTestSchema;
import transaction.ScanEventRelay;
import transaction.TransactionOperations;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;
import static support.PipelineTestSupport.awaitOrFail;

/**
 * 007 US6 (T105, FR-033, SC-009): Notification changes nothing Analytics publishes. One fixed, seeded stream of sales
 * (1,600 scans, 320 completed baskets) runs through the real Transaction, its scan relay, and Analytics, once without
 * Notification and once with Notification running on the same bus with 100 subscribers. The published windows, their
 * ends, and their ranks are identical, and they match an independent recomputation from the stored positions.
 */
class AnalyticsUnaffectedByNotificationsTest {
    private static final int PRODUCTS = 20;
    private static final int STOCK = 1_000;
    private static final int SALES = 320;
    private static final int SCANS_PER_SALE = 5;
    private static final int SUBSCRIBERS = 100;
    private static final long SEED = 20261112L;
    private static final Duration WAIT = Duration.ofSeconds(60);

    @Test
    void theWindowsAndRanksAreIdenticalWithAndWithoutNotification() throws Exception {
        Map<Long, List<String>> without = run(false);
        Map<Long, List<String>> with = run(true);
        assertEquals(List.of(1_000L, 1_500L), new ArrayList<>(without.keySet()), "1,600 scans publish two windows");
        assertEquals(without, with, "the same windows and ranks with Notification running");
    }

    /** The published windows, by end, as {@code sku:count} in rank order. */
    private static Map<Long, List<String>> run(boolean withNotification) throws Exception {
        TopicNames names = TopicNames.unprefixed();
        InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 6510);
        try (TransactionTestSchema transactions = TransactionTestSchema.create(TestSchema.Options.defaults().pool(4, 24));
             InventoryTestSchema stock = InventoryTestSchema.create(TestSchema.Options.defaults().pool(4, 24));
             AnalyticsTestSchema analyticsSchema = AnalyticsTestSchema.create();
             NotificationTestSchema notificationSchema = withNotification ? NotificationTestSchema.create() : null;
             AutoCloseable seeded = seed(transactions, stock);
             InProcessInventory inventory = InProcessInventory.over(stock, bus, names);
             InProcessAnalytics analytics = InProcessAnalytics.over(analyticsSchema.pool(), bus, names)) {
            InProcessTransaction checkout = InProcessTransaction.over(transactions).pool(4, 24).bus(bus, names).build();
            ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(checkout.pool), analytics.sink(),
                    ScanEventRelay.Settings.defaults());
            relay.start();
            AnalyticsService pipeline = analytics.startPipeline();
            InProcessNotification notification = null;
            try {
                List<String> skus = new ArrayList<>();
                for (int n = 1; n <= PRODUCTS; n++) skus.add(InventoryTestSchema.sku(n));
                if (withNotification) {
                    notification = InProcessNotification.over(notificationSchema.pool(), bus, names);
                    inventory.publishCatalog();
                    notification.awaitCatalog(WAIT);
                    Random subscriptions = new Random(SEED + 1);
                    for (int s = 1; s <= SUBSCRIBERS; s++) {
                        List<String> held = new ArrayList<>();
                        for (String sku : skus) if (subscriptions.nextInt(4) == 0) held.add(sku);
                        if (held.isEmpty()) held.add(skus.get(s % PRODUCTS));
                        assertTrue(notification.service().register("an-" + s, held).state().channelReady());
                    }
                }

                Random stream = new Random(SEED);
                for (int sale = 0; sale < SALES; sale++) {
                    String id = checkout.service.start(new TransactionOperations.StartCommand("analytics-" + sale % 10))
                            .transactionId();
                    for (int scan = 0; scan < SCANS_PER_SALE; scan++) {
                        // A skewed stream, so that the ranks are distinct for most SKUs.
                        int product = Math.min(stream.nextInt(PRODUCTS), stream.nextInt(PRODUCTS));
                        checkout.service.scan(new TransactionOperations.ScanCommand(id, skus.get(product)));
                    }
                    assertEquals(SCANS_PER_SALE, checkout.service.complete(id).itemCount());
                }

                awaitOrFail(WAIT, "the 1,500 window is published", () -> analytics.windows.readMaxWindowEnd() >= 1_500);
                if (withNotification) {
                    NotificationTestSchema schema = notificationSchema;
                    awaitOrFail(WAIT, "every sale is selected", () -> count(schema::saleSelections) == SALES);
                    awaitOrFail(WAIT, "every notification is delivered",
                            () -> count(() -> schema.notifications("DELIVERED")) == count(schema::notifications));
                    assertTrue(schema.notifications() > 0, "Notification did its work alongside Analytics");
                }
                Map<Long, List<String>> windows = new LinkedHashMap<>();
                for (long end : analyticsSchema.windowEnds()) {
                    List<String> ranks = analyticsSchema.storedRanks(end);
                    assertEquals(analyticsSchema.expectedRanks(end), ranks, "independent recomputation at " + end);
                    windows.put(end, ranks);
                }
                return windows;
            } finally {
                if (notification != null) notification.close();
                pipeline.shutdown();
                relay.close();
                checkout.close();
            }
        }
    }

    /** Seeds Inventory and the replica before Inventory starts, so that its catalog publisher finds the catalog. */
    private static AutoCloseable seed(TransactionTestSchema transactions, InventoryTestSchema stock) throws Exception {
        transactions.seedReplica(PRODUCTS, stock.seed(PRODUCTS, STOCK));
        return () -> { };
    }

    private interface Count {
        long get() throws Exception;
    }

    private static long count(Count count) {
        try {
            return count.get();
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
