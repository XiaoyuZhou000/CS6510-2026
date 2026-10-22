package unit.transaction;

import database.CheckoutAttemptStore;
import database.ScanStore;
import database.StoreFailure;
import database.TransactionStore;
import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import support.FakeInventoryGateway;
import transaction.CheckoutOrchestrator;
import transaction.CheckoutPendingFailure;
import transaction.InventoryGateway;
import transaction.ProductCatalogReplica;
import transaction.ProductCatalogUnavailableFailure;
import transaction.RecoverySchedule;
import transaction.SettlementRegistry;
import transaction.TransactionFailure;
import transaction.TransactionLocks;
import transaction.TransactionOperations;
import transaction.TransactionService;
import transaction.TransactionStateCache;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Start, Scan, Lookup, and the delegated Complete through the state cache, the catalog replica, and
 * guarded writes, with fakes (R19; contracts/checkout-saga.md).
 */
class TransactionServiceTest {
    private final FakeDatabase database = new FakeDatabase();
    private final FakeInventoryGateway inventory = FakeInventoryGateway.applying();
    private final ProductCatalogReplica replica = replica("v1", "1.25", "2.50");
    private final TransactionService service = newService(replica, 100);

    @Test
    void startReturnsAnOpenEmptyTransactionWithAMillisecondStart() {
        var started = service.start(new TransactionOperations.StartCommand("station-1"));
        assertEquals(TransactionOperations.TransactionStatus.OPEN, started.status());
        assertEquals(0, started.itemCount());
        assertEquals(new BigDecimal("0.00"), started.runningTotal());
        assertEquals("station-1", started.stationId());
        assertEquals(0, started.startedAt().getNano() % 1_000_000, "millisecond precision, as stored");
        assertEquals(started.startedAt(), database.rows.get(started.transactionId()).startedAt(),
                "the service supplies the stored timestamp, so nothing is read back");
        assertEquals(0, database.finds + database.loads, "Start reads nothing");
    }

    @Test
    void startScanAndDurableLookupAcrossServiceReconstruction() {
        var started = service.start(new TransactionOperations.StartCommand("station-1"));
        var scan = service.scan(new TransactionOperations.ScanCommand(started.transactionId(), "A"));
        assertEquals(1, scan.itemCount());
        assertEquals(new BigDecimal("1.25"), scan.runningTotal());
        assertEquals("Apple", scan.name());
        assertEquals(1, database.appends);

        TransactionService restarted = newService(replica("v1", "1.25", "2.50"), 100);
        var lookup = restarted.get(started.transactionId());
        assertEquals(started.startedAt(), lookup.startedAt());
        assertEquals(1, lookup.itemCount());
        assertEquals(new BigDecimal("1.25"), lookup.runningTotal());
        var next = restarted.scan(new TransactionOperations.ScanCommand(started.transactionId(), "B"));
        assertEquals(2, next.itemCount(), "the rebuilt service continues from the durable basket");
        assertEquals(new BigDecimal("3.75"), next.runningTotal());
        assertEquals(List.of(1, 2), database.sequences(started.transactionId()));
    }

    @Test
    void scanPrecedenceIsNotFoundThenNotOpenThenUnknownSkuAndEveryRejectionIsConfirmed() {
        assertFailure(TransactionFailure.Code.NOT_FOUND, () -> service.get("missing"));
        assertFailure(TransactionFailure.Code.NOT_FOUND,
                () -> service.scan(new TransactionOperations.ScanCommand("missing", "missing")));
        assertThrows(IllegalArgumentException.class, () -> new TransactionOperations.ScanCommand("missing", " "),
                "request validation (400) comes first");

        String id = service.start(new TransactionOperations.StartCommand("station-2")).transactionId();
        assertFailure(TransactionFailure.Code.UNKNOWN_SKU,
                () -> service.scan(new TransactionOperations.ScanCommand(id, "missing")));
        assertEquals(0, database.appends, "a rejected Scan writes nothing");

        service.scan(new TransactionOperations.ScanCommand(id, "A"));
        database.complete(id);
        int loads = database.loads;
        assertFailure(TransactionFailure.Code.NOT_OPEN,
                () -> service.scan(new TransactionOperations.ScanCommand(id, "missing")),
                "a stale OPEN entry plus an unknown SKU still answers 409 once the store says COMPLETED");
        assertTrue(database.loads > loads, "the rejection was confirmed against the store");
        assertFailure(TransactionFailure.Code.NOT_OPEN, () -> service.scan(new TransactionOperations.ScanCommand(id, "A")));
        assertEquals(1, database.appends);

        String fresh = service.start(new TransactionOperations.StartCommand("station-3")).transactionId();
        loads = database.loads;
        assertFailure(TransactionFailure.Code.UNKNOWN_SKU,
                () -> service.scan(new TransactionOperations.ScanCommand(fresh, "missing")));
        assertEquals(loads + 1, database.loads, "a cached 404 is confirmed with one load");
    }

    @Test
    void aRepeatedSkuKeepsItsFirstCapturedPriceAfterAReplicaSwap() {
        String id = service.start(new TransactionOperations.StartCommand("station-1")).transactionId();
        assertEquals(new BigDecimal("1.25"), service.scan(new TransactionOperations.ScanCommand(id, "A")).unitPrice());
        replica.install("v2", List.of(product("A", "Apple", "1.10"), product("B", "Bread", "2.50")));
        var again = service.scan(new TransactionOperations.ScanCommand(id, "A"));
        assertEquals(new BigDecimal("1.25"), again.unitPrice(), "the line's captured price stands");
        assertEquals(new BigDecimal("2.50"), again.runningTotal());

        String another = service.start(new TransactionOperations.StartCommand("station-2")).transactionId();
        assertEquals(new BigDecimal("1.10"), service.scan(new TransactionOperations.ScanCommand(another, "A")).unitPrice(),
                "a new basket captures the current price");
        assertEquals(List.of("1.25", "1.25"), database.prices(id));
    }

    @Test
    void failedStoreWriteCannotAcknowledgeScanOrCorruptAnotherBasket() {
        String first = service.start(new TransactionOperations.StartCommand("one")).transactionId();
        String second = service.start(new TransactionOperations.StartCommand("two")).transactionId();
        database.appendFailure = new StoreFailure("database unavailable");
        assertThrows(StoreFailure.class, () -> service.scan(new TransactionOperations.ScanCommand(first, "A")));
        assertEquals(0, service.get(first).itemCount());
        database.appendFailure = null;
        assertEquals(1, service.scan(new TransactionOperations.ScanCommand(second, "B")).itemCount());
        assertEquals(0, service.get(first).itemCount());
        assertEquals(1, service.scan(new TransactionOperations.ScanCommand(first, "A")).itemCount(),
                "the failed basket resumes from the durable state");
    }

    @Test
    void concurrentScansOnOneTransactionAllSucceedWithContiguousSequences() throws Exception {
        String id = service.start(new TransactionOperations.StartCommand("station-1")).transactionId();
        List<Callable<TransactionOperations.ScanView>> scans = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            String sku = i % 2 == 0 ? "A" : "B";
            scans.add(() -> service.scan(new TransactionOperations.ScanCommand(id, sku)));
        }
        List<Integer> counts = new ArrayList<>();
        try (ExecutorService workers = Executors.newFixedThreadPool(8)) {
            for (Future<TransactionOperations.ScanView> result : workers.invokeAll(scans)) counts.add(result.get().itemCount());
        }
        counts.sort(Integer::compare);
        List<Integer> expected = new ArrayList<>();
        for (int i = 1; i <= 32; i++) expected.add(i);
        assertEquals(expected, counts, "every response names a distinct position");
        assertEquals(expected, database.sequences(id), "one row per acknowledged scan");
        assertEquals(new BigDecimal("60.00"), service.get(id).runningTotal());
        assertEquals(0, database.guardMisses, "one service instance never misses its own guard");
    }

    @Test
    void aGuardMissReloadsAndReEvaluates() {
        String id = service.start(new TransactionOperations.StartCommand("station-1")).transactionId();
        service.scan(new TransactionOperations.ScanCommand(id, "A"));
        database.foreignScan(id, "B", "Bread", "2.50");
        long misses = PerfMetrics.counterValue("transaction.cache.guardMisses");
        int loads = database.loads;

        var scan = service.scan(new TransactionOperations.ScanCommand(id, "A"));
        assertEquals(3, scan.itemCount(), "the stale cache cannot lose the other writer's unit");
        assertEquals(new BigDecimal("5.00"), scan.runningTotal());
        assertEquals(1, database.guardMisses);
        assertEquals(misses + 1, PerfMetrics.counterValue("transaction.cache.guardMisses"));
        assertEquals(loads + 1, database.loads, "one reload, then the guarded write succeeds");
        assertEquals(List.of(1, 2, 3), database.sequences(id));
    }

    @Test
    void anUnusableReplicaIsAnInternalFailureNotAnUnknownSku() {
        TransactionService empty = newService(new ProductCatalogReplica(), 100);
        String id = empty.start(new TransactionOperations.StartCommand("station-1")).transactionId();
        assertThrows(ProductCatalogUnavailableFailure.class,
                () -> empty.scan(new TransactionOperations.ScanCommand(id, "A")));
        assertThrows(ProductCatalogUnavailableFailure.class,
                () -> empty.scan(new TransactionOperations.ScanCommand(id, "missing")));
        assertFailure(TransactionFailure.Code.NOT_FOUND,
                () -> empty.scan(new TransactionOperations.ScanCommand("missing", "A")),
                "an unknown transaction still answers 404 first");
        assertEquals(0, empty.get(id).itemCount(), "Lookup works without product data");
        assertEquals(0, database.appends);
    }

    @Test
    void lookupReadsTheStoreNotTheCache() {
        String id = service.start(new TransactionOperations.StartCommand("station-1")).transactionId();
        service.scan(new TransactionOperations.ScanCommand(id, "A"));
        database.foreignScan(id, "B", "Bread", "2.50");
        var lookup = service.get(id);
        assertEquals(2, lookup.itemCount());
        assertEquals(new BigDecimal("3.75"), lookup.runningTotal());
        database.complete(id);
        assertEquals(TransactionOperations.TransactionStatus.COMPLETED, service.get(id).status());
    }

    @Test
    void aPendingCheckoutFreezesTheBasket() {
        String id = service.start(new TransactionOperations.StartCommand("station-1")).transactionId();
        service.scan(new TransactionOperations.ScanCommand(id, "A"));
        database.beginAttempt(id);
        assertThrows(CheckoutPendingFailure.class, () -> service.scan(new TransactionOperations.ScanCommand(id, "A")),
                "no unit joins a basket that is being completed (FR-035)");
        assertEquals(1, database.sequences(id).size());
        assertEquals(TransactionOperations.TransactionStatus.OPEN, service.get(id).status(), "Lookup still says OPEN");
    }

    @Test
    void theCacheCanBeDisabledWithoutChangingOutcomes() {
        TransactionService uncached = newService(replica, 0);
        String id = uncached.start(new TransactionOperations.StartCommand("station-1")).transactionId();
        assertEquals(1, uncached.scan(new TransactionOperations.ScanCommand(id, "A")).itemCount());
        assertEquals(2, uncached.scan(new TransactionOperations.ScanCommand(id, "A")).itemCount());
        assertEquals(2, database.loads, "every Scan loads the durable state when caching is off");
    }

    @Test
    void completeAfterServiceReconstructionReturnsTheReceiptWithCapturedPrices() {
        var started = service.start(new TransactionOperations.StartCommand("station-1"));
        String id = started.transactionId();
        service.scan(new TransactionOperations.ScanCommand(id, "A"));
        service.scan(new TransactionOperations.ScanCommand(id, "A"));
        service.scan(new TransactionOperations.ScanCommand(id, "B"));

        TransactionService restarted = newService(replica("v2", "9.99", "9.99"), 100);
        var receipt = restarted.complete(id);
        assertTrue(database.completedBeforeReturn(id), "the receipt follows the committed T3");
        assertEquals(id, receipt.transactionId());
        assertEquals("station-1", receipt.stationId());
        assertEquals(3, receipt.itemCount());
        assertEquals(new BigDecimal("5.00"), receipt.totalAmount(), "the captured prices, not the new replica's");
        assertEquals(started.startedAt(), receipt.startedAt());
        assertEquals(2, receipt.lines().size());
        assertEquals("A", receipt.lines().get(0).sku());
        assertEquals(2, receipt.lines().get(0).quantity());
        assertEquals(new BigDecimal("1.25"), receipt.lines().get(0).unitPrice());
        assertEquals("Apple", receipt.lines().get(0).name());
        assertEquals(new BigDecimal("2.50"), receipt.lines().get(1).unitPrice());
        assertEquals(database.completedAt(id), receipt.completedAt(), "the receipt carries the stored completed_at");
        assertEquals(TransactionOperations.TransactionStatus.COMPLETED, restarted.get(id).status());
        assertEquals(1, inventory.applyCalls());
        assertEquals(List.of(new InventoryGateway.CheckoutLine("A", 2), new InventoryGateway.CheckoutLine("B", 1)),
                inventory.calls().get(0).lines(), "one batched call with the frozen basket");
    }

    @Test
    void completeReportsMissingEmptyInsufficientAndDuplicateCases() {
        assertFailure(TransactionFailure.Code.NOT_FOUND, () -> service.complete("missing"));
        String id = service.start(new TransactionOperations.StartCommand("station-2")).transactionId();
        assertFailure(TransactionFailure.Code.EMPTY_BASKET, () -> service.complete(id));
        assertEquals(0, inventory.applyCalls(), "an empty basket never reaches Inventory");

        service.scan(new TransactionOperations.ScanCommand(id, "A"));
        inventory.then(new InventoryGateway.Rejected(InventoryGateway.RejectionReason.INSUFFICIENT_STOCK, "A"));
        TransactionFailure insufficient = assertThrows(TransactionFailure.class, () -> service.complete(id));
        assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK, insufficient.code());
        assertEquals("Insufficient stock for SKU: A", insufficient.getMessage());
        assertEquals(TransactionOperations.TransactionStatus.OPEN, service.get(id).status(), "the basket reopened");
        assertEquals(2, service.scan(new TransactionOperations.ScanCommand(id, "A")).itemCount(),
                "the reopened basket accepts another scan");

        var receipt = service.complete(id);
        assertEquals(2, receipt.itemCount());
        assertEquals(List.of(1, 2), inventory.calls().stream().map(call -> call.key().attemptNumber()).toList(),
                "the retry is attempt 2");
        assertFailure(TransactionFailure.Code.NOT_OPEN, () -> service.complete(id));
        assertFailure(TransactionFailure.Code.NOT_OPEN,
                () -> service.scan(new TransactionOperations.ScanCommand(id, "A")));
    }

    @Test
    void aRepeatedCompleteAfterCompletionNeverCallsInventory() {
        String id = service.start(new TransactionOperations.StartCommand("station-3")).transactionId();
        service.scan(new TransactionOperations.ScanCommand(id, "B"));
        service.complete(id);
        assertEquals(1, inventory.applyCalls());
        for (int i = 0; i < 3; i++) assertFailure(TransactionFailure.Code.NOT_OPEN, () -> service.complete(id));
        assertEquals(1, inventory.applyCalls(), "FR-036: no Inventory call after COMPLETED");
        TransactionService restarted = newService(replica, 100);
        assertFailure(TransactionFailure.Code.NOT_OPEN, () -> restarted.complete(id));
        assertEquals(1, inventory.applyCalls());
    }

    private TransactionService newService(ProductCatalogReplica catalog, int cacheEntries) {
        TransactionLocks locks = new TransactionLocks();
        TransactionStateCache cache = new TransactionStateCache(locks, cacheEntries);
        CheckoutOrchestrator checkout = new CheckoutOrchestrator(database, database, inventory, cache,
                new SettlementRegistry(locks), new RecoverySchedule(), CheckoutOrchestrator.Settings.defaults());
        return new TransactionService(database, database, checkout, catalog, locks, cache);
    }

    private static ProductCatalogReplica replica(String version, String applePrice, String breadPrice) {
        ProductCatalogReplica replica = new ProductCatalogReplica();
        replica.install(version, List.of(product("A", "Apple", applePrice), product("B", "Bread", breadPrice)));
        return replica;
    }

    private static ProductCatalogReplica.Product product(String sku, String name, String price) {
        return new ProductCatalogReplica.Product(sku, name, new BigDecimal(price));
    }

    private static void assertFailure(TransactionFailure.Code code, Runnable action) {
        assertEquals(code, assertThrows(TransactionFailure.class, action::run).code());
    }

    private static void assertFailure(TransactionFailure.Code code, Runnable action, String message) {
        assertEquals(code, assertThrows(TransactionFailure.class, action::run, message).code(), message);
    }

    /** A durable store that applies the same guards as the real one. */
    private static final class FakeDatabase implements TransactionStore, ScanStore, CheckoutAttemptStore {
        record Row(String stationId, TransactionStatus status, int itemCount, BigDecimal total, int attemptCount,
                   Integer activeAttempt, Instant startedAt, Instant completedAt) {
            Row(String stationId, TransactionStatus status, int itemCount, BigDecimal total, int attemptCount,
                Integer activeAttempt, Instant startedAt) {
                this(stationId, status, itemCount, total, attemptCount, activeAttempt, startedAt, null);
            }
        }

        final Map<String, Row> rows = new HashMap<>();
        final Map<String, List<AcceptedScan>> scans = new HashMap<>();
        final Map<AttemptKey, AttemptRecord> attempts = new HashMap<>();
        StoreFailure appendFailure;
        int finds;
        int loads;
        int appends;
        int guardMisses;

        @Override
        public synchronized void insertOpen(String id, String stationId, Instant startedAt) {
            rows.put(id, new Row(stationId, TransactionStatus.OPEN, 0, new BigDecimal("0.00"), 0, null, startedAt));
            scans.put(id, new ArrayList<>());
        }

        @Override
        public synchronized Optional<TransactionRecord> findById(String id) {
            finds++;
            Row row = rows.get(id);
            return row == null ? Optional.empty() : Optional.of(record(id, row));
        }

        @Override
        public synchronized Optional<TransactionState> loadState(String id) {
            loads++;
            Row row = rows.get(id);
            if (row == null) return Optional.empty();
            Map<String, TransactionLine> lines = new LinkedHashMap<>();
            for (AcceptedScan scan : scans.get(id)) {
                TransactionLine line = lines.get(scan.sku());
                lines.put(scan.sku(), new TransactionLine(scan.sku(), scan.productName(),
                        line == null ? 1 : line.quantity() + 1, line == null ? scan.unitPrice() : line.unitPrice()));
            }
            return Optional.of(new TransactionState(record(id, row), row.attemptCount(), row.activeAttempt(),
                    lines.values().stream().sorted(java.util.Comparator.comparing(TransactionLine::sku)).toList()));
        }

        @Override
        public synchronized AppendResult appendScan(String id, int expectedItemCount, BigDecimal newTotal, AcceptedScan scan) {
            if (appendFailure != null) throw appendFailure;
            Row row = rows.get(id);
            if (row == null || row.status() != TransactionStatus.OPEN || row.activeAttempt() != null
                    || row.itemCount() != expectedItemCount) {
                guardMisses++;
                return AppendResult.GUARD_MISS;
            }
            assertEquals(expectedItemCount + 1, scan.scanSequence());
            rows.put(id, new Row(row.stationId(), row.status(), expectedItemCount + 1, newTotal, row.attemptCount(),
                    null, row.startedAt()));
            scans.get(id).add(scan);
            appends++;
            return AppendResult.OK;
        }

        /** Another writer's committed Scan, which this service's cache has not seen. */
        synchronized void foreignScan(String id, String sku, String name, String price) {
            Row row = rows.get(id);
            BigDecimal unit = new BigDecimal(price);
            scans.get(id).add(new AcceptedScan(row.itemCount() + 1, sku, name, unit, Instant.now()));
            rows.put(id, new Row(row.stationId(), row.status(), row.itemCount() + 1, row.total().add(unit),
                    row.attemptCount(), row.activeAttempt(), row.startedAt()));
        }

        synchronized void complete(String id) {
            Row row = rows.get(id);
            rows.put(id, new Row(row.stationId(), TransactionStatus.COMPLETED, row.itemCount(), row.total(),
                    row.attemptCount(), null, row.startedAt(), row.startedAt().plusSeconds(1)));
        }

        synchronized boolean completedBeforeReturn(String id) {
            return rows.get(id).status() == TransactionStatus.COMPLETED && attempts.values().stream()
                    .anyMatch(attempt -> attempt.key().transactionId().equals(id) && attempt.state() == State.COMPLETED);
        }

        synchronized Instant completedAt(String id) {
            return rows.get(id).completedAt();
        }

        @Override
        public synchronized WriteResult begin(Begin begin) {
            AttemptKey key = begin.key();
            Row row = rows.get(key.transactionId());
            if (row == null || row.status() != TransactionStatus.OPEN || row.activeAttempt() != null
                    || row.itemCount() != begin.itemCount() || row.attemptCount() != key.attemptNumber() - 1) {
                return WriteResult.GUARD_MISS;
            }
            rows.put(key.transactionId(), new Row(row.stationId(), row.status(), row.itemCount(), row.total(),
                    key.attemptNumber(), key.attemptNumber(), row.startedAt()));
            attempts.put(key, new AttemptRecord(key, State.PENDING, null, null, begin.lines(), begin.itemCount(),
                    begin.totalAmount(), ClientOutcome.NONE, begin.createdAt(), null, 0, begin.nextRetryAt(), null));
            return WriteResult.OK;
        }

        @Override
        public synchronized WriteResult complete(AttemptKey key, Instant completedAt, boolean answering) {
            Row row = rows.get(key.transactionId());
            if (row == null || !Integer.valueOf(key.attemptNumber()).equals(row.activeAttempt())) {
                return WriteResult.GUARD_MISS;
            }
            rows.put(key.transactionId(), new Row(row.stationId(), TransactionStatus.COMPLETED, row.itemCount(),
                    row.total(), row.attemptCount(), null, row.startedAt(), completedAt));
            settle(key, State.COMPLETED, null, null, completedAt, answering ? ClientOutcome.SUCCESS : null);
            return WriteResult.OK;
        }

        @Override
        public synchronized WriteResult reopen(AttemptKey key, RejectionReason reason, String rejectedSku,
                                               Instant settledAt, boolean answering) {
            Row row = rows.get(key.transactionId());
            if (row == null || !Integer.valueOf(key.attemptNumber()).equals(row.activeAttempt())) {
                return WriteResult.GUARD_MISS;
            }
            rows.put(key.transactionId(), new Row(row.stationId(), row.status(), row.itemCount(), row.total(),
                    row.attemptCount(), null, row.startedAt()));
            settle(key, State.REJECTED, reason, rejectedSku, settledAt, answering ? ClientOutcome.REJECTED : null);
            return WriteResult.OK;
        }

        private void settle(AttemptKey key, State state, RejectionReason reason, String sku, Instant at,
                            ClientOutcome answer) {
            AttemptRecord a = attempts.get(key);
            ClientOutcome outcome = answer != null && a.clientOutcome() == ClientOutcome.NONE ? answer : a.clientOutcome();
            attempts.put(key, new AttemptRecord(key, state, reason, sku, a.lines(), a.itemCount(), a.totalAmount(),
                    outcome, a.createdAt(), at, a.retryCount(), a.nextRetryAt(), a.lastError()));
        }

        @Override
        public synchronized boolean recordClientOutcome(AttemptKey key, ClientOutcome outcome) {
            AttemptRecord a = attempts.get(key);
            if (a == null || a.clientOutcome() != ClientOutcome.NONE) return false;
            attempts.put(key, new AttemptRecord(key, a.state(), a.rejectionReason(), a.rejectedSku(), a.lines(),
                    a.itemCount(), a.totalAmount(), outcome, a.createdAt(), a.settledAt(), a.retryCount(),
                    a.nextRetryAt(), a.lastError()));
            return true;
        }

        @Override
        public synchronized boolean markPendingError(AttemptKey key, Instant now) {
            AttemptRecord a = attempts.get(key);
            if (a == null || a.state() != State.PENDING || a.clientOutcome() != ClientOutcome.NONE) return false;
            attempts.put(key, new AttemptRecord(key, a.state(), null, null, a.lines(), a.itemCount(), a.totalAmount(),
                    ClientOutcome.PENDING_ERROR, a.createdAt(), null, a.retryCount(), now, a.lastError()));
            return true;
        }

        @Override
        public synchronized List<AttemptRecord> findPending(Instant dueBefore, PageCursor after, int limit) {
            throw new UnsupportedOperationException("recovery is not part of this test");
        }

        @Override
        public synchronized Optional<AttemptRecord> findAttempt(AttemptKey key) {
            return Optional.ofNullable(attempts.get(key));
        }

        @Override
        public synchronized void scheduleRetry(AttemptKey key, Instant nextRetryAt, String lastError) {
            throw new UnsupportedOperationException("recovery is not part of this test");
        }

        synchronized void beginAttempt(String id) {
            Row row = rows.get(id);
            rows.put(id, new Row(row.stationId(), row.status(), row.itemCount(), row.total(), row.attemptCount() + 1,
                    row.attemptCount() + 1, row.startedAt()));
        }

        synchronized List<Integer> sequences(String id) {
            return scans.get(id).stream().map(AcceptedScan::scanSequence).toList();
        }

        synchronized List<String> prices(String id) {
            return scans.get(id).stream().map(scan -> scan.unitPrice().toPlainString()).toList();
        }

        private static TransactionRecord record(String id, Row row) {
            return new TransactionRecord(id, row.stationId(), row.status(), row.total(), row.startedAt(),
                    row.completedAt(), row.itemCount());
        }
    }
}
