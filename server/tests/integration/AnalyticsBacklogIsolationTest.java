package integration;

import analytics.AnalyticsService;
import database.JdbcRelayStore;
import org.junit.jupiter.api.Test;
import rpc.GrpcAnalyticsIngestClient;
import support.AnalyticsTestSchema;
import support.InProcessAnalytics;
import support.InProcessInventory;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.SwitchableScanEventSink;
import support.TransactionTestSchema;
import transaction.ScanEventRelay;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Transaction keeps accepting scans and checkouts while Analytics is down; its outbox holds the backlog, and
 * after Analytics restarts the 1,000-position prewarm restores the overlap so every window is exact (FR-041,
 * FR-048, FR-049). Events reach Analytics only through the relay.
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
            InProcessTransaction checkout = InProcessTransaction.over(transactions).inventory(inventory.gateway()).build();
            SwitchableScanEventSink analyticsLink = new SwitchableScanEventSink();
            ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(checkout.pool), analyticsLink,
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
                Thread.sleep(300);
                assertEquals(1000, transactions.undeliveredCount(), "the backlog waits durably in the outbox");
                String paid = checkout.start();
                checkout.scan(paid, SKU);
                checkout.scan(paid, SKU);
                assertEquals(2, checkout.complete(paid).itemCount(), "checkouts continue during the outage");
                assertEquals(1998, stock.stock(SKU));

                try (InProcessAnalytics first = InProcessAnalytics.over(analyticsSchema.newPool("test.analytics.first", 4, 8, null))) {
                    analyticsLink.up(new GrpcAnalyticsIngestClient(first.channel(), Duration.ofSeconds(5)));
                    AnalyticsService pipeline = first.startPipeline();
                    try { await(first, 1000); }
                    finally { pipeline.shutdown(); }
                    analyticsLink.down();
                }
                analyticsSchema.assertWindowsMatchPositions(1000);

                for (int position = 1001; position <= 1500; position++) {
                    checkout.scan(id, position % 4 == 0 ? OTHER_SKU : SKU);
                }
                assertEquals(1502, transactions.scanCount());
                assertEquals(1500, checkout.service.get(id).itemCount());

                try (InProcessAnalytics restarted = InProcessAnalytics.over(analyticsSchema.newPool("test.analytics.restarted", 4, 8, null))) {
                    assertEquals(2, restarted.names.size(), "the restarted name map is loaded from product_name");
                    analyticsLink.up(new GrpcAnalyticsIngestClient(restarted.channel(), Duration.ofSeconds(5)));
                    AnalyticsService pipeline = restarted.startPipeline();
                    try { await(restarted, 1500); }
                    finally { pipeline.shutdown(); }
                    assertEquals(1500, restarted.windows.readLatest(10).orElseThrow().windowEnd());
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (transactions.undeliveredCount() > 0 && System.nanoTime() < deadline) Thread.sleep(20);
                }
                analyticsSchema.assertWindowsMatchPositions(1500);
                assertEquals(transactions.identities(), analyticsSchema.identities(), "every scan counted once");
                analyticsSchema.assertPositionsContiguous();
            } finally {
                relay.close();
            }
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
