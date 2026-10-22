package database;

import analytics.ProductNameMap;
import org.junit.jupiter.api.Test;
import rpc.FaultHooks;
import support.AnalyticsTestSchema;
import support.CountingConnections;
import support.FaultInjection;
import support.FaultingConnections;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Position assignment at ingestion (R11; contracts/scan-event-delivery.md sections 3 and 4). Migrates
 * {@code JdbcScanMaterializerTest}: the materializer is gone, and its guarantees (consecutive positions under
 * the singleton lock, the checkpoint advancing in the same commit, crash safety) now belong to ingestion.
 */
class JdbcScanIngestStoreTest {

    @Test
    void positionsAreUniqueContiguousFromOneAndImmutable() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            JdbcScanIngestStore store = store(schema.pool());
            assertEquals(new ScanIngestStore.IngestResult(3, 0, 1, 3), store.ingest(events("tx-1", 1, 3, "sku1")));
            List<AnalyticsTestSchema.StoredEvent> first = schema.events();
            assertEquals(new ScanIngestStore.IngestResult(2, 1, 4, 5), store.ingest(events("tx-1", 3, 5, "sku2")));
            List<AnalyticsTestSchema.StoredEvent> all = schema.events();
            assertEquals(first, all.subList(0, 3), "stored positions never change");
            schema.assertPositionsContiguous();
            assertEquals(5, schema.lastPosition());
        }
    }

    @Test
    void rollbackLeavesNoPositionAndNoCheckpointAdvance() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(AnalyticsTestSchema.Options.defaults().decorator(faults::wrap))) {
            ProductNameMap names = new ProductNameMap();
            JdbcScanIngestStore store = new JdbcScanIngestStore(schema.pool(), names);
            faults.armForCurrentThread(FaultingConnections.CommitFault.failBeforeCommit());
            StoreFailure failure = assertThrows(StoreFailure.class, () -> store.ingest(events("tx-1", 1, 4, "sku1")));
            assertTrue(failure.commitOutcomeUnknown(), "a failed commit is reported as an unknown outcome");
            assertEquals(0, schema.lastPosition());
            assertEquals(0, schema.storedEvents());
            assertNull(schema.productName("sku1"));
            assertNull(names.name("sku1"), "the name map changes only after a commit");
        }
    }

    @Test
    void crashBeforeCommitRollsBackWholeBatchAndRetryKeepsItGapless() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            JdbcScanIngestStore store = store(schema.pool());
            try (FaultInjection.Armed armed = FaultInjection.spec().fail("analytics.ingest.beforeCommit").armInThisJvm()) {
                assertThrows(FaultHooks.InjectedFault.class, () -> store.ingest(events("tx-1", 1, 5, "sku1")));
            }
            assertEquals(0, schema.lastPosition());
            assertEquals(0, schema.storedEvents());
            assertEquals(3, store.ingest(events("tx-1", 1, 3, "sku1")).stored());
            assertEquals(2, store.ingest(events("tx-1", 1, 5, "sku1")).stored());
            assertEquals(0, store.ingest(events("tx-1", 1, 5, "sku1")).stored());
            assertEquals(5, schema.lastPosition());
            assertEquals(5, schema.queryLong("SELECT COUNT(DISTINCT position) FROM accepted_scan"));
            assertEquals(1, schema.queryLong("SELECT MIN(position) FROM accepted_scan"));
            schema.assertPositionsContiguous();
        }
    }

    @Test
    void crashAfterCommitAndConcurrentRetriesNeverReassignPositions() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); ExecutorService workers = Executors.newFixedThreadPool(2)) {
            JdbcScanIngestStore store = store(schema.pool());
            try (FaultInjection.Armed armed = FaultInjection.spec().fail("analytics.ingest.afterCommit").armInThisJvm()) {
                assertThrows(FaultHooks.InjectedFault.class, () -> store.ingest(events("tx-1", 1, 4, "sku1")));
            }
            assertEquals(4, schema.lastPosition());
            List<AnalyticsTestSchema.StoredEvent> firstFour = schema.events();
            assertEquals(4, firstFour.size());
            List<Callable<Integer>> retries = List.of(
                    () -> store.ingest(events("tx-1", 1, 12, "sku1")).stored(),
                    () -> store.ingest(events("tx-1", 1, 12, "sku1")).stored());
            int stored = 0;
            for (Future<Integer> retry : workers.invokeAll(retries)) stored += retry.get();
            assertEquals(8, stored, "only the eight new identities are stored, once");
            assertEquals(0, store.ingest(events("tx-1", 1, 12, "sku1")).stored());
            assertEquals(12, schema.lastPosition());
            assertEquals(firstFour, schema.events().subList(0, 4), "positions of the committed batch never move");
            assertEquals(12, schema.queryLong("SELECT COUNT(DISTINCT position) FROM accepted_scan"));
            assertEquals(12, schema.queryLong("SELECT MAX(position) FROM accepted_scan"));
        }
    }

    /** Analytics half of {@code JdbcScanMaterializerTest#lowerEventIdCommittingLateIsStillDiscovered}. */
    @Test
    void eventDeliveredLateAfterHigherNumberedEventsTakesTheNextPositionWithoutGap() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            JdbcScanIngestStore store = store(schema.pool());
            store.ingest(List.of(event("tx-b", 1, "sku2"), event("tx-b", 2, "sku2")));
            // tx-a's first scan committed in Transaction before tx-b's but reaches Analytics afterwards.
            assertEquals(new ScanIngestStore.IngestResult(1, 0, 3, 3), store.ingest(List.of(event("tx-a", 1, "sku1"))));
            List<AnalyticsTestSchema.StoredEvent> stored = schema.events();
            assertEquals(new AnalyticsTestSchema.StoredEvent(3, "tx-a", 1, "sku1"), stored.get(2));
            schema.assertPositionsContiguous();
        }
    }

    @Test
    void concurrentIngestionCallsSerializeWithoutGaps() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(AnalyticsTestSchema.Options.defaults().pool(8, 8));
             ExecutorService workers = Executors.newFixedThreadPool(8)) {
            JdbcScanIngestStore store = store(schema.pool());
            CountDownLatch go = new CountDownLatch(1);
            List<Callable<Integer>> callers = new ArrayList<>();
            for (int thread = 0; thread < 8; thread++) {
                String transactionId = "tx-" + thread;
                callers.add(() -> {
                    go.await();
                    int stored = 0;
                    for (int batch = 0; batch < 20; batch++) {
                        stored += store.ingest(events(transactionId, batch * 10 + 1, batch * 10 + 10, "sku" + batch % 3)).stored();
                    }
                    return stored;
                });
            }
            List<Future<Integer>> results = new ArrayList<>();
            for (Callable<Integer> caller : callers) results.add(workers.submit(caller));
            go.countDown();
            int stored = 0;
            for (Future<Integer> result : results) stored += result.get();
            assertEquals(1_600, stored);
            schema.assertPositionsContiguous();
            assertEquals(1_600, schema.identities().size());
        }
    }

    /** Constitution II: two Analytics instances, each with its own pool and name map, over one schema. */
    @Test
    void twoInstancesIngestOverlappingBatchesConcurrentlyAndStoreEveryIdentityOnce() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); ExecutorService workers = Executors.newFixedThreadPool(2)) {
            JdbcScanIngestStore one = store(schema.newPool("test.ingest.one", 2, 4, null));
            JdbcScanIngestStore two = store(schema.newPool("test.ingest.two", 2, 4, null));
            CountDownLatch go = new CountDownLatch(1);
            // Both instances receive the same 30 batches of 50, shifted by 25 events, as two relays redelivering would.
            Callable<Integer> first = () -> {
                go.await();
                int stored = 0;
                for (int batch = 0; batch < 30; batch++) stored += one.ingest(events("tx", batch * 50 + 1, batch * 50 + 50, "sku1")).stored();
                return stored;
            };
            Callable<Integer> second = () -> {
                go.await();
                int stored = 0;
                for (int batch = 0; batch < 30; batch++) stored += two.ingest(events("tx", batch * 50 + 26, batch * 50 + 75, "sku2")).stored();
                return stored;
            };
            Future<Integer> a = workers.submit(first);
            Future<Integer> b = workers.submit(second);
            go.countDown();
            int stored = a.get() + b.get();
            assertEquals(1_525, stored, "identities 1..1,525 are each stored exactly once");
            Set<String> identities = schema.identities();
            assertEquals(1_525, identities.size());
            schema.assertPositionsContiguous();
            List<AnalyticsTestSchema.StoredEvent> sample = schema.events();
            assertEquals(0, one.ingest(events("tx", 1, 1_000, "sku1")).stored());
            assertEquals(0, two.ingest(events("tx", 501, 1_500, "sku2")).stored());
            assertEquals(sample, schema.events(), "positions are immutable whichever instance sees a redelivery");
        }
    }

    @Test
    void restartReloadsTheNameMapAndRewritesNoExistingName() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            JdbcScanIngestStore before = store(schema.pool());
            before.ingest(List.of(event("tx-1", 1, "sku1"), event("tx-1", 2, "sku2"), event("tx-1", 3, "sku3")));
            schema.update("UPDATE product_name SET updated_at = '2001-01-01 00:00:00.000'");

            CountingConnections counting = new CountingConnections();
            ProductNameMap reloaded = new ProductNameMap();
            JdbcScanIngestStore after = new JdbcScanIngestStore(schema.newPool("test.ingest.restart", 1, 2, counting::wrap), reloaded);
            reloaded.loadFrom(after);
            assertEquals(3, reloaded.size());
            assertEquals("Item 2", reloaded.name("sku2"));
            try (CountingConnections.Scope scope = counting.scope()) {
                after.ingest(List.of(event("tx-2", 1, "sku1"), event("tx-2", 2, "sku3")));
                assertEquals(1, scope.counts().inserts(), "only the events are inserted; no name is written");
            }
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM product_name WHERE updated_at <> '2001-01-01 00:00:00.000'"));
            Map<String, String> names = new HashMap<>();
            for (String sku : List.of("sku1", "sku2", "sku3")) names.put(sku, schema.productName(sku));
            assertEquals(Map.of("sku1", "Item 1", "sku2", "Item 2", "sku3", "Item 3"), names);
        }
    }

    private static JdbcScanIngestStore store(ConnectionPool pool) {
        ProductNameMap names = new ProductNameMap();
        JdbcScanIngestStore store = new JdbcScanIngestStore(pool, names);
        names.loadFrom(store);
        return store;
    }

    /** Events {@code from..to} of one transaction, all one SKU. */
    private static List<ScanIngestStore.ScanEvent> events(String transactionId, int from, int to, String sku) {
        List<ScanIngestStore.ScanEvent> events = new ArrayList<>();
        for (int sequence = from; sequence <= to; sequence++) events.add(event(transactionId, sequence, sku));
        return events;
    }

    private static ScanIngestStore.ScanEvent event(String transactionId, int sequence, String sku) {
        return new ScanIngestStore.ScanEvent(transactionId, sequence, sku, "Item " + sku.substring(3), Instant.now());
    }
}
