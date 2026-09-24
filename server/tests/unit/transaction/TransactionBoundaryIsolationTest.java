package unit.transaction;

import database.CatalogStore;
import database.CheckoutCompletionStore;
import database.TransactionStore;
import org.junit.jupiter.api.Test;
import transaction.CatalogCache;
import transaction.TransactionFailure;
import transaction.TransactionOperations;
import transaction.TransactionService;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class TransactionBoundaryIsolationTest {
    @Test
    void publicBusinessContractExposesNoHttpOrJdbcTypes() {
        for (Method method : TransactionOperations.class.getDeclaredMethods()) {
            assertBoundaryType(method.getReturnType());
            for (Class<?> parameter : method.getParameterTypes()) assertBoundaryType(parameter);
        }
        for (Class<?> nested : TransactionOperations.class.getDeclaredClasses()) {
            for (var component : nested.isRecord() ? nested.getRecordComponents() : new java.lang.reflect.RecordComponent[0]) {
                assertBoundaryType(component.getType());
            }
        }
    }

    @Test
    void acceptedScanSinkRunsExactlyOnceAndOnlyAfterBasketAdmission() {
        FakeTransactions rows = new FakeTransactions();
        CatalogCache catalog = CatalogCache.load(() -> List.of(
                new CatalogStore.CatalogItem("A", "Apple", new BigDecimal("1.25"))));
        AtomicReference<TransactionService> serviceRef = new AtomicReference<>();
        AtomicReference<String> transactionId = new AtomicReference<>();
        AtomicInteger sinkCalls = new AtomicInteger();
        TransactionService service = new TransactionService(rows,
                command -> new CheckoutCompletionStore.Completed(Instant.now()), catalog, sku -> {
                    sinkCalls.incrementAndGet();
                    assertEquals(1, serviceRef.get().get(transactionId.get()).itemCount(),
                            "the basket must contain the scan before the sink is invoked");
                });
        serviceRef.set(service);
        transactionId.set(service.start(new TransactionOperations.StartCommand("station")).transactionId());

        service.scan(new TransactionOperations.ScanCommand(transactionId.get(), "A"));

        assertEquals(1, sinkCalls.get());
        assertEquals(1, service.get(transactionId.get()).itemCount());
    }

    @Test
    void rejectedScansNeverReachTheSinkAndSinkFailureDoesNotUndoAdmission() {
        FakeTransactions rows = new FakeTransactions();
        CatalogCache catalog = CatalogCache.load(() -> List.of(
                new CatalogStore.CatalogItem("A", "Apple", new BigDecimal("1.25"))));
        AtomicInteger sinkCalls = new AtomicInteger();
        TransactionService service = new TransactionService(rows,
                command -> new CheckoutCompletionStore.Completed(Instant.now()), catalog, sku -> {
                    sinkCalls.incrementAndGet();
                    throw new IllegalStateException("analytics unavailable");
                });
        String id = service.start(new TransactionOperations.StartCommand("station")).transactionId();

        TransactionFailure unknown = assertThrows(TransactionFailure.class,
                () -> service.scan(new TransactionOperations.ScanCommand(id, "missing")));
        assertEquals(TransactionFailure.Code.UNKNOWN_SKU, unknown.code());
        assertEquals(0, sinkCalls.get());

        assertDoesNotThrow(() -> service.scan(new TransactionOperations.ScanCommand(id, "A")));
        assertEquals(1, sinkCalls.get());
        assertEquals(1, service.get(id).itemCount());
    }

    private static void assertBoundaryType(Class<?> type) {
        String name = type.getName();
        assertFalse(name.startsWith("com.sun.net.httpserver"), name);
        assertFalse(name.startsWith("java.sql"), name);
    }

    private static final class FakeTransactions implements TransactionStore {
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
}
