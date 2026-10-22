package performance;

import analytics.ProductNameMap;
import database.JdbcScanIngestStore;
import database.ScanIngestStore;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.CountingConnections;
import support.FaultingConnections;
import support.FaultingConnections.CommitFault;
import support.StatementBudgets;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Analytics ingestion's atomicity and acknowledgement order (strict) and its statement budget, which does not
 * grow with the batch (R22; contracts/scan-event-delivery.md section 3).
 */
class AnalyticsIngestStatementBudgetTest {

    @Test
    void eventsPositionsAndSequenceShareOneCommitThatPrecedesTheResult() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(AnalyticsTestSchema.Options.defaults().decorator(faults::wrap))) {
            JdbcScanIngestStore store = store(schema);
            CompletableFuture<ScanIngestStore.IngestResult> result = new CompletableFuture<>();
            AtomicLong commitsReturnedAtResult = new AtomicLong(-1);
            Thread ingesting = Thread.ofPlatform().start(() -> {
                faults.armForCurrentThread(CommitFault.delay(300));
                try {
                    ScanIngestStore.IngestResult stored = store.ingest(events("tx-1", 1, 100, 5));
                    commitsReturnedAtResult.set(faults.commitsReturned());
                    result.complete(stored);
                } catch (Throwable failure) {
                    result.completeExceptionally(failure);
                }
            });
            Thread.sleep(150);
            assertFalse(result.isDone(), "no result exists while the ingestion commit is still in progress");
            assertEquals(0, schema.storedEvents(), "nothing is visible before the commit");
            assertEquals(0, schema.lastPosition());
            assertEquals(100, result.get(10, TimeUnit.SECONDS).stored());
            ingesting.join(5_000);
            assertEquals(1, faults.commitsStarted(), "one commit covers events, positions, names, and scan_sequence");
            assertEquals(1, commitsReturnedAtResult.get(), "the commit returned before the result");
            assertEquals(100, schema.storedEvents());
            assertEquals(100, schema.lastPosition());
            assertEquals(5, schema.queryLong("SELECT COUNT(*) FROM product_name"));
        }
    }

    @Test
    void statementsAndCommitsPerBatchDoNotGrowWithBatchSize() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(AnalyticsTestSchema.Options.defaults().decorator(counting::wrap))) {
            JdbcScanIngestStore store = store(schema);
            int transaction = 0;
            for (int size : new int[]{1, 100, 500, 1_000}) {
                // A cold batch writes its names; the warm batch after it names the same SKUs and writes none.
                CountingConnections.Counts cold = measure(counting, store, events("cold-" + transaction, 1, size, 20));
                assertEquals(1, cold.commits(), "one commit per batch of " + size);
                StatementBudgets.assertWithin("analytics.ingest.statements", cold.statements());
                StatementBudgets.assertWithin("analytics.ingest.commits", cold.commits());

                CountingConnections.Counts warm = measure(counting, store, events("warm-" + transaction, 1, size, 20));
                assertEquals(1, warm.commits());
                StatementBudgets.assertWithin("analytics.ingest.statements", warm.statements());
                StatementBudgets.assertWithin("analytics.ingest.commits", warm.commits());
                StatementBudgets.assertWithin("analytics.ingest.warmNameWrites", warm.inserts() - 1);
                assertEquals(1, warm.inserts(), "a warm batch inserts its events and no name");
                transaction++;
            }
            CountingConnections.Counts duplicates = measure(counting, store, events("warm-0", 1, 1, 20));
            StatementBudgets.assertWithin("analytics.ingest.statements", duplicates.statements());
            assertEquals(1, duplicates.commits(), "a batch of duplicates still ends in its one commit");
            assertEquals(0, duplicates.inserts() + duplicates.updates(), "and writes nothing");
            schema.assertPositionsContiguous();
        }
    }

    private static CountingConnections.Counts measure(CountingConnections counting, JdbcScanIngestStore store,
                                                      List<ScanIngestStore.ScanEvent> batch) {
        try (CountingConnections.Scope scope = counting.scope()) {
            store.ingest(batch);
            return scope.counts();
        }
    }

    private static JdbcScanIngestStore store(AnalyticsTestSchema schema) throws Exception {
        ProductNameMap names = new ProductNameMap();
        JdbcScanIngestStore store = new JdbcScanIngestStore(schema.pool(), names);
        names.loadFrom(store);
        return store;
    }

    /** Events {@code from..to} of one transaction over {@code skus} distinct SKUs. */
    private static List<ScanIngestStore.ScanEvent> events(String transactionId, int from, int to, int skus) {
        List<ScanIngestStore.ScanEvent> events = new ArrayList<>();
        for (int sequence = from; sequence <= to; sequence++) {
            int sku = sequence % skus + 1;
            events.add(new ScanIngestStore.ScanEvent(transactionId, sequence, "sku" + sku, "Item " + sku, Instant.now()));
        }
        return events;
    }
}
