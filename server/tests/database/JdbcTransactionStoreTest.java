package database;

import org.junit.jupiter.api.Test;
import support.TransactionTestSchema;

import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Transaction rows, their service-supplied timestamps, and the cold-path state load (R5, R19).
 * Migrates the transaction parts of JdbcStoreAdaptersTest.
 */
class JdbcTransactionStoreTest {

    @Test
    void insertOpenStoresTheSuppliedStartAndFindByIdReadsTheRow() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcTransactionStore transactions = new JdbcTransactionStore(schema.pool());
            Instant startedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            transactions.insertOpen("tx-adapter", "station-1", startedAt);

            TransactionStore.TransactionRecord transaction = transactions.findById("tx-adapter").orElseThrow();
            assertEquals("tx-adapter", transaction.transactionId());
            assertEquals("station-1", transaction.stationId());
            assertEquals(TransactionStore.TransactionStatus.OPEN, transaction.status());
            assertEquals(new BigDecimal("0.00"), transaction.totalAmount());
            assertEquals(0, transaction.itemCount());
            assertEquals(startedAt, transaction.startedAt(), "the service's own timestamp, not the server's");
            assertNull(transaction.completedAt());
            assertTrue(transaction.startedAt().isBefore(Instant.now().plusSeconds(1)));

            JdbcScanStore scans = new JdbcScanStore(schema.pool());
            scans.appendScan("tx-adapter", 0, new BigDecimal("0.85"), scan(1, "SKU-000001", "Item 1", "0.85"));
            scans.appendScan("tx-adapter", 1, new BigDecimal("3.35"), scan(2, "SKU-000002", "Item 2", "2.50"));
            TransactionStore.TransactionRecord scanned = transactions.findById("tx-adapter").orElseThrow();
            assertEquals(2, scanned.itemCount());
            assertEquals(new BigDecimal("3.35"), scanned.totalAmount());
            assertEquals(startedAt, scanned.startedAt());

            Instant completedAt = startedAt.plusSeconds(5);
            schema.update("UPDATE `transaction` SET status = 'COMPLETED', completed_at = ? WHERE transaction_id = ?",
                    java.sql.Timestamp.from(completedAt), "tx-adapter");
            TransactionStore.TransactionRecord completed = transactions.findById("tx-adapter").orElseThrow();
            assertEquals(TransactionStore.TransactionStatus.COMPLETED, completed.status());
            assertEquals(completedAt, completed.completedAt());
        }
    }

    @Test
    void loadStateGroupsAcceptedScansIntoLinesWithTheFirstCapturedPriceAndName() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            schema.seedReplica(3, UUID.randomUUID().toString());
            JdbcTransactionStore transactions = new JdbcTransactionStore(schema.pool());
            JdbcScanStore scans = new JdbcScanStore(schema.pool());
            Instant startedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            transactions.insertOpen("tx-lines", "station-1", startedAt);
            scans.appendScan("tx-lines", 0, new BigDecimal("0.85"), scan(1, "SKU-000001", "Item 1", "0.85"));

            schema.update("UPDATE product_replica SET price = 1.10, name = 'Renamed' WHERE sku = 'SKU-000001'");
            scans.appendScan("tx-lines", 1, new BigDecimal("1.70"), scan(2, "SKU-000001", "Item 1", "0.85"));
            scans.appendScan("tx-lines", 2, new BigDecimal("2.90"), scan(3, "SKU-000002", "Item 2", "1.20"));

            TransactionStore.TransactionState state = transactions.loadState("tx-lines").orElseThrow();
            assertEquals(3, state.transaction().itemCount());
            assertEquals(new BigDecimal("2.90"), state.transaction().totalAmount());
            assertEquals(TransactionStore.TransactionStatus.OPEN, state.transaction().status());
            assertEquals(startedAt, state.transaction().startedAt());
            assertEquals(0, state.attemptCount());
            assertNull(state.activeAttemptNumber());
            assertEquals(List.of(
                    new TransactionStore.TransactionLine("SKU-000001", "Item 1", 2, new BigDecimal("0.85")),
                    new TransactionStore.TransactionLine("SKU-000002", "Item 2", 1, new BigDecimal("1.20"))),
                    state.lines(), "quantity is the row count; the captured price and name survive a catalog change");

            schema.update("UPDATE `transaction` SET attempt_count = 2, active_attempt_number = 2 WHERE transaction_id = ?",
                    "tx-lines");
            TransactionStore.TransactionState pending = transactions.loadState("tx-lines").orElseThrow();
            assertEquals(2, pending.attemptCount());
            assertEquals(2, pending.activeAttemptNumber());

            transactions.insertOpen("tx-empty", "station-2", startedAt);
            assertEquals(List.of(), transactions.loadState("tx-empty").orElseThrow().lines());
        }
    }

    @Test
    void anUnknownIdIsEmpty() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcTransactionStore transactions = new JdbcTransactionStore(schema.pool());
            assertTrue(transactions.findById("missing").isEmpty());
            assertTrue(transactions.loadState("missing").isEmpty());
        }
    }

    @Test
    void sqlFailuresSurfaceAsStoreFailuresAndTheStoreInterfaceHasNoJdbcTypes() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcTransactionStore transactions = new JdbcTransactionStore(schema.pool());
            schema.setTableUnavailable("transaction", true);

            StoreFailure transactionFailure = assertThrows(StoreFailure.class, () -> transactions.findById("missing"));
            assertInstanceOf(SQLException.class, transactionFailure.getCause());
            assertEquals("Failed to read transaction", transactionFailure.getMessage());
            StoreFailure stateFailure = assertThrows(StoreFailure.class, () -> transactions.loadState("missing"));
            assertInstanceOf(SQLException.class, stateFailure.getCause());
            assertThrows(StoreFailure.class,
                    () -> transactions.insertOpen("tx", "station", Instant.now().truncatedTo(ChronoUnit.MILLIS)));
        }

        for (Class<?> type : List.of(TransactionStore.class, ScanStore.class)) {
            for (Method method : type.getDeclaredMethods()) {
                assertNoJdbc(method.getReturnType());
                for (Class<?> parameter : method.getParameterTypes()) assertNoJdbc(parameter);
            }
            for (Class<?> nested : type.getDeclaredClasses()) {
                if (!nested.isRecord()) continue;
                for (RecordComponent component : nested.getRecordComponents()) assertNoJdbc(component.getType());
            }
        }
    }

    private static void assertNoJdbc(Class<?> type) {
        assertFalse(type.getName().startsWith("java.sql"), type.getName());
    }

    private static ScanStore.AcceptedScan scan(int sequence, String sku, String name, String price) {
        return new ScanStore.AcceptedScan(sequence, sku, name, new BigDecimal(price),
                Instant.now().truncatedTo(ChronoUnit.MILLIS));
    }
}
