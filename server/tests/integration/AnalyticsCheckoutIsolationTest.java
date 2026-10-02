package integration;

import analytics.AnalyticsService;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.CheckoutDatabase;
import support.PipelineTestSupport.FakePopularWindowStore;
import transaction.TransactionOperations;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static support.PipelineTestSupport.awaitOrFail;

class AnalyticsCheckoutIsolationTest {
    @Test
    void failingAnalyticsPersistenceCannotCorruptConcurrentScanAndCompletionTraffic() throws Exception {
        FakePopularWindowStore analyticsStore = new FakePopularWindowStore();
        analyticsStore.writeGate().fail();
        AnalyticsService analytics = new AnalyticsService(analyticsStore);
        int stations = 20;
        int scansPerStation = 75;
        try (CheckoutDatabase db = open(2_000, analytics::recordAcceptedScan)) {
            List<String> transactionIds = new ArrayList<>();
            for (int station = 0; station < stations; station++) {
                transactionIds.add(db.checkout.start(
                        new TransactionOperations.StartCommand("station-" + station)).transactionId());
            }

            try (var executor = Executors.newFixedThreadPool(stations)) {
                List<Future<TransactionOperations.ReceiptView>> results = new ArrayList<>();
                for (String transactionId : transactionIds) {
                    results.add(executor.submit(() -> {
                        for (int scan = 0; scan < scansPerStation; scan++) {
                            db.checkout.scan(new TransactionOperations.ScanCommand(
                                    transactionId, CheckoutDatabase.SKU));
                        }
                        return db.checkout.complete(transactionId);
                    }));
                }
                for (Future<TransactionOperations.ReceiptView> result : results) {
                    assertEquals(scansPerStation, result.get().itemCount());
                }
            }

            awaitOrFail("oldest analytics window to reach failing persistence",
                    () -> analyticsStore.writeAttemptCount() > 0);
            assertEquals(2_000 - stations * scansPerStation, db.stock());
            for (String transactionId : transactionIds) {
                assertEquals(TransactionOperations.TransactionStatus.COMPLETED,
                        db.checkout.get(transactionId).status());
            }
            assertTrue(analyticsStore.committedWindows().isEmpty());

            analyticsStore.writeGate().pass();
            awaitOrFail("both retained windows to commit",
                    () -> analyticsStore.committedWindows().size() == 2);
            assertEquals(List.of(1000L, 1500L), analyticsStore.committedWindows().stream()
                    .map(window -> window.windowEnd()).toList());
        } finally {
            analytics.shutdown();
        }
    }

    private static CheckoutDatabase open(int stock, transaction.AcceptedScanSink sink) throws Exception {
        try {
            return new CheckoutDatabase(stock, sink);
        } catch (SQLException unavailable) {
            throw new TestAbortedException("MySQL unavailable for isolation integration test", unavailable);
        }
    }
}
