package integration;

import analytics.AnalyticsService;
import database.JdbcRelayStore;
import events.TopicNames;
import messaging.KafkaScanEventSink;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.InMemoryEventBus;
import support.InProcessAnalytics;
import support.InProcessInventory;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.SwitchableScanEventSink;
import support.TransactionTestSchema;
import transaction.ScanEventRelay;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Transaction keeps accepting scans and checkouts while Analytics is down and while the broker is unreachable
 * (FR-041, FR-048, FR-049, FR-051, FR-052, FR-054): during an Analytics outage the backlog waits in the scan
 * topic, during a broker outage in the outbox. After each, the backlog is published and ingested in order, and the
 * 1,000-position prewarm of the restarted Analytics restores the overlap so every window is exact. Scans reach
 * Analytics only through the relay and the broker.
 */
class AnalyticsBacklogIsolationTest {
    private static final String SKU = "SKU-000001";
    private static final String OTHER_SKU = "SKU-000002";

    @Test
    void transactionContinuesDuringAnalyticsOutageAndPrewarmRestoresOverlap() throws Exception {
        try (TransactionTestSchema transactions = TransactionTestSchema.create();
             InventoryTestSchema stock = InventoryTestSchema.create();
             AnalyticsTestSchema analyticsSchema = AnalyticsTestSchema.create();
             InProcessInventory inventory = InProcessInventory.over(stock)) {
            transactions.seedReplica(2, stock.seed(2, 2_000));
            InMemoryEventBus bus = inventory.bus;
            TopicNames names = inventory.names;
            InProcessTransaction checkout = InProcessTransaction.over(transactions).bus(bus, names).build();
            SwitchableScanEventSink broker = new SwitchableScanEventSink();
            broker.up(new KafkaScanEventSink(bus, names));
            ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(checkout.pool), broker,
                    ScanEventRelay.Settings.defaults());
            relay.start();
            try {
                String id = checkout.start();
                for (int position = 1; position <= 1000; position++) {
                    checkout.scan(id, position % 4 == 0 ? OTHER_SKU : SKU);
                }
                assertEquals(1000, transactions.scanCount());
                assertEquals(1000, checkout.service.get(id).itemCount());
                assertEquals(2000, stock.stock(SKU), "Analytics outage cannot deduct inventory");
                awaitPublished(transactions);
                assertEquals(1000, bus.endOffset(names.scanAccepted(), 0), "the backlog waits durably in the topic");
                String paid = checkout.start();
                checkout.scan(paid, SKU);
                checkout.scan(paid, SKU);
                assertEquals(2, checkout.complete(paid).itemCount(), "checkouts continue during the outage");
                assertEquals(1998, stock.stock(SKU));

                try (InProcessAnalytics first = InProcessAnalytics.over(
                        analyticsSchema.newPool("test.analytics.first", 4, 8, null), bus, names)) {
                    AnalyticsService pipeline = first.startPipeline();
                    try { await(first, 1000); }
                    finally { pipeline.shutdown(); }
                }
                analyticsSchema.assertWindowsMatchPositions(1000);

                broker.down();
                for (int position = 1001; position <= 1500; position++) {
                    checkout.scan(id, position % 4 == 0 ? OTHER_SKU : SKU);
                }
                assertEquals(1502, transactions.scanCount());
                assertEquals(1500, checkout.service.get(id).itemCount(), "scans continue while the broker is unreachable");
                assertEquals(500, transactions.undeliveredCount(), "during a broker outage the backlog waits in the outbox");
                broker.up(new KafkaScanEventSink(bus, names));

                try (InProcessAnalytics restarted = InProcessAnalytics.over(
                        analyticsSchema.newPool("test.analytics.restarted", 4, 8, null), bus, names)) {
                    assertEquals(2, restarted.names.size(), "the restarted name map is loaded from product_name");
                    AnalyticsService pipeline = restarted.startPipeline();
                    try { await(restarted, 1500); }
                    finally { pipeline.shutdown(); }
                    assertEquals(1500, restarted.windows.readLatest(10).orElseThrow().windowEnd());
                    awaitPublished(transactions);
                    restarted.awaitConsumed(java.time.Duration.ofSeconds(10));
                }
                analyticsSchema.assertWindowsMatchPositions(1500);
                assertEquals(transactions.identities(), analyticsSchema.identities(), "every scan counted once");
                analyticsSchema.assertPositionsContiguous();
                List<Integer> basket = analyticsSchema.events().stream()
                        .filter(event -> event.sourceTransactionId().equals(id))
                        .map(AnalyticsTestSchema.StoredEvent::sourceScanSequence).toList();
                assertEquals(java.util.stream.IntStream.rangeClosed(1, 1500).boxed().toList(), basket,
                        "both backlogs were published and ingested in acceptance order");
            } finally {
                relay.close();
            }
        }
    }

    private static void awaitPublished(TransactionTestSchema transactions) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (transactions.undeliveredCount() > 0) {
            if (System.nanoTime() >= deadline) throw new AssertionError("The relay did not publish the backlog");
            Thread.sleep(20);
        }
    }

    private static void await(InProcessAnalytics analytics, long end) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (analytics.windows.readMaxWindowEnd() < end) {
            if (System.nanoTime() >= deadline) throw new AssertionError("Analytics did not catch up to " + end);
            Thread.sleep(20);
        }
    }
}
