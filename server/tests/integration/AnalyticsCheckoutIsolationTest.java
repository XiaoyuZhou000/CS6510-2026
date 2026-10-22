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
import support.PipelineTestSupport.FakePopularWindowStore;
import support.TransactionTestSchema;
import transaction.ScanEventRelay;
import transaction.TransactionOperations;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static support.PipelineTestSupport.awaitOrFail;

/**
 * Failing Analytics persistence cannot disturb concurrent Scan and Complete traffic (FR-049). Events reach
 * Analytics through Transaction's relay and Analytics' ingestion, never through a shared table.
 */
class AnalyticsCheckoutIsolationTest {
    private static final String SKU = "SKU-000001";

    @Test
    void failingAnalyticsPersistenceCannotCorruptConcurrentScanAndCompletionTraffic() throws Exception {
        FakePopularWindowStore analyticsStore = new FakePopularWindowStore();
        analyticsStore.writeGate().fail();
        int stations = 20;
        int scansPerStation = 75;
        try (TransactionTestSchema transactions = TransactionTestSchema.create(TransactionTestSchema.Options.defaults().pool(8, 32));
             InventoryTestSchema stock = InventoryTestSchema.create(InventoryTestSchema.Options.defaults().pool(4, 16));
             AnalyticsTestSchema analyticsSchema = AnalyticsTestSchema.create();
             InProcessInventory inventory = InProcessInventory.over(stock);
             InProcessAnalytics analytics = InProcessAnalytics.over(analyticsSchema)) {
            transactions.seedReplica(2, stock.seed(2, 2_000));
            InProcessTransaction checkout = InProcessTransaction.over(transactions).pool(8, 32)
                    .inventory(inventory.gateway()).build();
            ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(checkout.pool),
                    new GrpcAnalyticsIngestClient(analytics.channel(), Duration.ofSeconds(5)), ScanEventRelay.Settings.defaults());
            relay.start();
            AnalyticsService pipeline = analytics.startPipeline(analyticsStore);
            try {
                List<String> transactionIds = new ArrayList<>();
                for (int station = 0; station < stations; station++) {
                    transactionIds.add(checkout.service.start(new TransactionOperations.StartCommand("station-" + station))
                            .transactionId());
                }

                try (var executor = Executors.newFixedThreadPool(stations)) {
                    List<Future<TransactionOperations.ReceiptView>> results = new ArrayList<>();
                    for (String transactionId : transactionIds) {
                        results.add(executor.submit(() -> {
                            for (int scan = 0; scan < scansPerStation; scan++) checkout.scan(transactionId, SKU);
                            return checkout.complete(transactionId);
                        }));
                    }
                    for (Future<TransactionOperations.ReceiptView> result : results) {
                        assertEquals(scansPerStation, result.get().itemCount());
                    }
                }

                awaitOrFail(Duration.ofSeconds(30), "oldest analytics window to reach failing persistence",
                        () -> analyticsStore.writeAttemptCount() > 0);
                assertEquals(2_000 - stations * scansPerStation, stock.stock(SKU));
                for (String transactionId : transactionIds) {
                    assertEquals(TransactionOperations.TransactionStatus.COMPLETED, checkout.service.get(transactionId).status());
                }
                assertTrue(analyticsStore.committedWindows().isEmpty());

                analyticsStore.writeGate().pass();
                awaitOrFail(Duration.ofSeconds(30), "both retained windows to commit",
                        () -> analyticsStore.committedWindows().size() == 2);
                assertEquals(List.of(1000L, 1500L), analyticsStore.committedWindows().stream()
                        .map(window -> window.windowEnd()).toList());
                assertEquals(transactions.identities(), analyticsSchema.identities(), "every scan arrived through the relay once");
            } finally {
                pipeline.shutdown();
                relay.close();
            }
        }
    }
}
