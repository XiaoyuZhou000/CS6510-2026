package unit.transaction;

import database.CatalogStore;
import database.CheckoutCompletionStore;
import database.StoreFailure;
import database.TransactionStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import transaction.CatalogCache;
import transaction.TransactionFailure;
import transaction.TransactionOperations;
import transaction.TransactionService;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TransactionServiceTest {
    private final FakeTransactionStore transactions = new FakeTransactionStore();
    private final FakeCompletionStore completions = new FakeCompletionStore();
    private final AtomicInteger acceptedScans = new AtomicInteger();
    private TransactionService service;
    private CatalogCache catalog;

    @BeforeEach
    void setUp() {
        CatalogStore catalogStore = () -> List.of(
                new CatalogStore.CatalogItem("A", "Apple", new BigDecimal("1.25")),
                new CatalogStore.CatalogItem("B", "Bread", new BigDecimal("2.50")));
        catalog = CatalogCache.load(catalogStore);
        service = new TransactionService(transactions, completions,
                catalog, sku -> acceptedScans.incrementAndGet());
    }

    @Test
    void startScanCompleteAndDurableLookup() {
        TransactionOperations.TransactionView started =
                service.start(new TransactionOperations.StartCommand("station-1"));
        assertEquals(TransactionOperations.TransactionStatus.OPEN, started.status());

        TransactionOperations.ScanView scan = service.scan(
                new TransactionOperations.ScanCommand(started.transactionId(), "A"));
        assertEquals(1, scan.itemCount());
        assertEquals(new BigDecimal("1.25"), scan.runningTotal());
        assertEquals(1, acceptedScans.get());

        TransactionOperations.ReceiptView receipt = service.complete(started.transactionId());
        assertTrue(completions.committedBeforeReturn);
        assertEquals(1, receipt.itemCount());
        assertEquals(new BigDecimal("1.25"), receipt.totalAmount());
        assertEquals(1, receipt.lines().size());
        assertEquals(TransactionOperations.TransactionStatus.COMPLETED,
                service.get(started.transactionId()).status());
    }

    @Test
    void reportsMissingUnknownEmptyInsufficientAndDuplicateCases() {
        assertFailure(TransactionFailure.Code.NOT_FOUND, () -> service.get("missing"));

        String id = service.start(new TransactionOperations.StartCommand("station-2")).transactionId();
        assertFailure(TransactionFailure.Code.UNKNOWN_SKU,
                () -> service.scan(new TransactionOperations.ScanCommand(id, "missing")));
        assertEquals(0, acceptedScans.get());
        assertFailure(TransactionFailure.Code.EMPTY_BASKET, () -> service.complete(id));

        service.scan(new TransactionOperations.ScanCommand(id, "A"));
        completions.result = new CheckoutCompletionStore.InsufficientStock("A");
        assertFailure(TransactionFailure.Code.INSUFFICIENT_STOCK, () -> service.complete(id));
        completions.result = new CheckoutCompletionStore.Completed(Instant.now());
        service.complete(id);
        assertFailure(TransactionFailure.Code.NOT_OPEN, () -> service.complete(id));
    }

    @Test
    void storeAndScanSinkFailuresDoNotCorruptOtherBaskets() {
        TransactionService isolated = new TransactionService(transactions, completions,
                catalog, sku -> { throw new IllegalStateException("analytics unavailable"); });
        String first = isolated.start(new TransactionOperations.StartCommand("one")).transactionId();
        String second = isolated.start(new TransactionOperations.StartCommand("two")).transactionId();
        assertDoesNotThrow(() -> isolated.scan(new TransactionOperations.ScanCommand(first, "A")));

        completions.failure = new StoreFailure("database unavailable");
        assertThrows(StoreFailure.class, () -> isolated.complete(first));
        completions.failure = null;
        assertEquals(1, isolated.scan(new TransactionOperations.ScanCommand(second, "B")).itemCount());
    }

    @Test
    void analyticsRunsOnlyAfterAdmissionAndSlowOrThrowingSinksCannotUndoCheckout() throws Exception {
        CountDownLatch sinkEntered = new CountDownLatch(1);
        CountDownLatch releaseSink = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        TransactionService isolated = new TransactionService(transactions, completions, catalog, sku -> {
            calls.incrementAndGet();
            sinkEntered.countDown();
            try {
                assertTrue(releaseSink.await(2, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("analytics unavailable");
        });
        String id = isolated.start(new TransactionOperations.StartCommand("station-slow")).transactionId();
        Thread scan = new Thread(() -> isolated.scan(new TransactionOperations.ScanCommand(id, "A")));

        scan.start();
        assertTrue(sinkEntered.await(1, TimeUnit.SECONDS));
        assertEquals(1, isolated.get(id).itemCount(), "basket admission precedes analytics callback");
        releaseSink.countDown();
        scan.join(1_000);

        assertFalse(scan.isAlive());
        assertEquals(1, calls.get());
        assertEquals(1, isolated.complete(id).itemCount());
        assertEquals(TransactionOperations.TransactionStatus.COMPLETED, isolated.get(id).status());
        assertFailure(TransactionFailure.Code.NOT_OPEN,
                () -> isolated.scan(new TransactionOperations.ScanCommand(id, "A")));
        assertEquals(1, calls.get(), "a rejected scan must never reach analytics");
    }

    private static void assertFailure(TransactionFailure.Code code, Runnable action) {
        TransactionFailure failure = assertThrows(TransactionFailure.class, action::run);
        assertEquals(code, failure.code());
    }

    private static final class FakeTransactionStore implements TransactionStore {
        private final Map<String, TransactionRecord> rows = new LinkedHashMap<>();

        @Override
        public void insertOpen(String transactionId, String stationId) {
            rows.put(transactionId, new TransactionRecord(transactionId, stationId,
                    TransactionStatus.OPEN, new BigDecimal("0.00"), Instant.now(), null, 0));
        }

        @Override
        public Optional<TransactionRecord> findById(String transactionId) {
            return Optional.ofNullable(rows.get(transactionId));
        }
    }

    private final class FakeCompletionStore implements CheckoutCompletionStore {
        private CompletionResult result;
        private StoreFailure failure;
        private boolean committedBeforeReturn;
        private final List<CompletionCommand> commands = new ArrayList<>();

        @Override
        public CompletionResult completeAtomically(CompletionCommand command) {
            if (failure != null) throw failure;
            commands.add(command);
            CompletionResult actual = result == null ? new Completed(Instant.now()) : result;
            if (actual instanceof Completed completed) {
                TransactionStore.TransactionRecord open = transactions.rows.get(command.transactionId());
                transactions.rows.put(command.transactionId(), new TransactionStore.TransactionRecord(
                        open.transactionId(), open.stationId(), TransactionStore.TransactionStatus.COMPLETED,
                        command.totalAmount(), open.startedAt(), completed.completedAt(),
                        command.lines().stream().mapToInt(CompletionLine::quantity).sum()));
                committedBeforeReturn = true;
            }
            return actual;
        }
    }
}
