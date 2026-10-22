package database;

import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.FaultingConnections;
import support.FaultingConnections.CommitFault;
import support.TransactionTestSchema;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Scan transaction: one guarded UPDATE of the transaction row plus one accepted_scan INSERT in one
 * commit (R10, R19; contracts/scan-event-delivery.md section 1). Migrates JdbcAcceptedScanStoreTest.
 */
class JdbcScanStoreTest {
    private static final String SKU = "SKU-000001";
    private static final BigDecimal PRICE = new BigDecimal("0.85");

    @Test
    void eachSuccessfulScanCommitsTheTotalsAndTheUndeliveredEventTogether() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.newPool("test.scan." + suffix(), 2, 4, counting::wrap);
            JdbcScanStore scans = new JdbcScanStore(pool);
            String id = open(pool);
            Instant acceptedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

            try (CountingConnections.Scope scope = counting.scope()) {
                assertEquals(ScanStore.AppendResult.OK,
                        scans.appendScan(id, 0, PRICE, new ScanStore.AcceptedScan(1, SKU, "Item 1", PRICE, acceptedAt)));
                assertEquals(1, scope.counts().updates());
                assertEquals(1, scope.counts().inserts());
                assertEquals(0, scope.counts().selects());
                assertEquals(1, scope.counts().commits(), "the UPDATE and the INSERT share one commit");
            }
            var row = schema.transaction(id).orElseThrow();
            assertEquals(1, row.itemCount());
            assertEquals(PRICE, row.totalAmount());
            List<TransactionTestSchema.ScanRow> events = schema.scans(id);
            assertEquals(1, events.size());
            assertEquals(1, events.get(0).scanSequence());
            assertEquals(SKU, events.get(0).sku());
            assertEquals("Item 1", events.get(0).productName());
            assertEquals(PRICE, events.get(0).unitPrice());
            assertFalse(events.get(0).delivered(), "a new event is undelivered");
            try (PreparedStatement read = schema.admin().prepareStatement(
                    "SELECT accepted_at FROM accepted_scan WHERE transaction_id = ?")) {
                read.setString(1, id);
                try (var result = read.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(acceptedAt, result.getTimestamp(1).toInstant(), "the service-supplied time, to the ms");
                }
            }
        }
    }

    @Test
    void aFailureInEitherStatementLeavesNeither() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.pool();
            JdbcScanStore scans = new JdbcScanStore(pool);
            String id = open(pool);

            BigDecimal overflow = new BigDecimal("99999999999.99").multiply(BigDecimal.TEN);
            assertThrows(StoreFailure.class, () -> scans.appendScan(id, 0, overflow, scan(1)),
                    "the guarded UPDATE fails on an out-of-range total");
            assertUntouched(schema, id);
            try (PreparedStatement nowait = schema.admin().prepareStatement(
                    "SELECT item_count FROM `transaction` WHERE transaction_id = ? FOR UPDATE NOWAIT")) {
                nowait.setString(1, id);
                try (var row = nowait.executeQuery()) {
                    assertTrue(row.next(), "the failed write left no row lock behind in the pool");
                }
            }

            schema.setTableUnavailable("accepted_scan", true);
            StoreFailure insertFailure = assertThrows(StoreFailure.class, () -> scans.appendScan(id, 0, PRICE, scan(1)));
            assertInstanceOf(SQLException.class, insertFailure.getCause());
            assertFalse(insertFailure.commitOutcomeUnknown(), "a statement failure is definitive");
            schema.setTableUnavailable("accepted_scan", false);
            assertUntouched(schema, id);

            assertEquals(ScanStore.AppendResult.OK, scans.appendScan(id, 0, PRICE, scan(1)),
                    "the same scan then succeeds, because nothing partial was left");
            assertEquals(1, schema.transaction(id).orElseThrow().itemCount());
        }
    }

    @Test
    void aStaleExpectationAClosedTransactionOrAPendingAttemptIsAGuardMissThatWritesNothing() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.newPool("test.guard." + suffix(), 2, 4, counting::wrap);
            JdbcScanStore scans = new JdbcScanStore(pool);

            String stale = open(pool);
            assertEquals(ScanStore.AppendResult.OK, scans.appendScan(stale, 0, PRICE, scan(1)));
            try (CountingConnections.Scope scope = counting.scope()) {
                assertEquals(ScanStore.AppendResult.GUARD_MISS, scans.appendScan(stale, 0, PRICE, scan(1)),
                        "the expected item_count is stale");
                assertEquals(1, scope.counts().updates());
                assertEquals(0, scope.counts().inserts(), "a miss never inserts");
                assertEquals(0, scope.counts().commits());
                assertEquals(1, scope.counts().rollbacks());
            }
            assertEquals(1, schema.transaction(stale).orElseThrow().itemCount());
            assertEquals(1, schema.scans(stale).size());

            String completed = open(pool);
            schema.update("UPDATE `transaction` SET status = 'COMPLETED', completed_at = NOW(3) WHERE transaction_id = ?",
                    completed);
            assertEquals(ScanStore.AppendResult.GUARD_MISS, scans.appendScan(completed, 0, PRICE, scan(1)));
            assertEquals(0, schema.scans(completed).size());

            String pending = open(pool);
            schema.update("UPDATE `transaction` SET active_attempt_number = 1, attempt_count = 1 WHERE transaction_id = ?",
                    pending);
            assertEquals(ScanStore.AppendResult.GUARD_MISS, scans.appendScan(pending, 0, PRICE, scan(1)),
                    "no unit joins a frozen basket (FR-035)");
            assertEquals(0, schema.scans(pending).size());
            assertEquals(0, schema.transaction(pending).orElseThrow().itemCount());

            assertEquals(ScanStore.AppendResult.GUARD_MISS, scans.appendScan("missing", 0, PRICE, scan(1)));
        }
    }

    @Test
    void concurrentScansWithOneExpectationCommitExactlyOnceAndSequencesStayDistinct() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create(
                TransactionTestSchema.Options.defaults().pool(4, 8));
             ExecutorService workers = Executors.newFixedThreadPool(8)) {
            JdbcScanStore scans = new JdbcScanStore(schema.pool());
            String id = open(schema.pool());
            String other = open(schema.pool());

            List<Callable<ScanStore.AppendResult>> racing = new ArrayList<>();
            for (int i = 0; i < 8; i++) racing.add(() -> scans.appendScan(id, 0, PRICE, scan(1)));
            int ok = 0;
            for (Future<ScanStore.AppendResult> result : workers.invokeAll(racing)) {
                if (result.get() == ScanStore.AppendResult.OK) ok++;
            }
            assertEquals(1, ok, "the row lock and the guard admit exactly one of the racing writes");
            assertEquals(1, schema.transaction(id).orElseThrow().itemCount());

            for (int expected = 1; expected < 5; expected++) {
                assertEquals(ScanStore.AppendResult.OK, scans.appendScan(id, expected,
                        PRICE.multiply(BigDecimal.valueOf(expected + 1)), scan(expected + 1)));
            }
            for (int expected = 0; expected < 4; expected++) {
                assertEquals(ScanStore.AppendResult.OK, scans.appendScan(other, expected,
                        PRICE.multiply(BigDecimal.valueOf(expected + 1)), scan(expected + 1)));
            }
            Set<String> identities = new HashSet<>();
            for (String transaction : List.of(id, other)) {
                List<TransactionTestSchema.ScanRow> rows = schema.scans(transaction);
                for (int i = 0; i < rows.size(); i++) {
                    assertEquals(i + 1, rows.get(i).scanSequence(), "contiguous from 1");
                    assertTrue(identities.add(transaction + "#" + rows.get(i).scanSequence()));
                }
            }
            assertEquals(9, schema.scanCount());
            assertEquals(9, schema.undeliveredCount());
            assertEquals(9, schema.queryLong("SELECT COUNT(DISTINCT scan_id) FROM accepted_scan"));
        }
    }

    @Test
    void theSequenceIsTheNewItemCountAndTheRowKeepsTheSuppliedCapturedValues() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcScanStore scans = new JdbcScanStore(schema.pool());
            String id = open(schema.pool());
            assertEquals(ScanStore.AppendResult.OK, scans.appendScan(id, 0, PRICE, scan(1)));
            schema.update("UPDATE product_replica SET price = 1.10 WHERE sku = ?", SKU);
            assertEquals(ScanStore.AppendResult.OK, scans.appendScan(id, 1, new BigDecimal("1.70"), scan(2)),
                    "the store writes the captured price it is given, whatever the replica says now");
            List<TransactionTestSchema.ScanRow> rows = schema.scans(id);
            assertEquals(List.of(1, 2), rows.stream().map(TransactionTestSchema.ScanRow::scanSequence).toList());
            assertTrue(rows.stream().allMatch(row -> row.unitPrice().equals(PRICE) && row.productName().equals("Item 1")));
            assertEquals(2, schema.transaction(id).orElseThrow().itemCount());
            assertEquals(new BigDecimal("1.70"), schema.transaction(id).orElseThrow().totalAmount());
            assertThrows(IllegalArgumentException.class, () -> scans.appendScan(id, 2, PRICE, scan(5)),
                    "the sequence must be the expected item count plus one");
        }
    }

    @Test
    void aDeadlockRetriesOnlyBeforeCommitAndAFailingCommitIsNeverRetried() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        DeadlockOnce deadlock = new DeadlockOnce();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.newPool("test.scan.faults." + suffix(), 2, 4,
                    connection -> faults.wrap(deadlock.wrap(connection)));
            JdbcScanStore scans = new JdbcScanStore(pool);
            String id = open(pool);

            deadlock.armed.set(true);
            long commits = faults.commitsStarted();
            assertEquals(ScanStore.AppendResult.OK, scans.appendScan(id, 0, PRICE, scan(1)));
            assertFalse(deadlock.armed.get(), "the injected deadlock fired");
            assertEquals(commits + 1, faults.commitsStarted(), "the retried unit committed once");
            assertEquals(1, schema.scans(id).size());

            faults.armForCurrentThread(CommitFault.failBeforeCommit());
            commits = faults.commitsStarted();
            StoreFailure lost = assertThrows(StoreFailure.class, () -> scans.appendScan(id, 1, PRICE.add(PRICE), scan(2)));
            assertTrue(lost.commitOutcomeUnknown());
            assertEquals(commits + 1, faults.commitsStarted(), "no retry after a commit was attempted");
            assertEquals(1, schema.scans(id).size(), "this commit did not happen");

            faults.armForCurrentThread(CommitFault.failAfterCommit());
            commits = faults.commitsStarted();
            StoreFailure durable = assertThrows(StoreFailure.class, () -> scans.appendScan(id, 1, PRICE.add(PRICE), scan(2)));
            assertTrue(durable.commitOutcomeUnknown());
            assertEquals(commits + 1, faults.commitsStarted(), "no retry after a commit was attempted");
            assertEquals(2, schema.scans(id).size(), "this commit happened although the caller saw a failure");
            assertEquals(2, schema.transaction(id).orElseThrow().itemCount());
        }
    }

    private static void assertUntouched(TransactionTestSchema schema, String id) throws Exception {
        var row = schema.transaction(id).orElseThrow();
        assertEquals(0, row.itemCount());
        assertEquals(new BigDecimal("0.00"), row.totalAmount());
        assertEquals(0, schema.scans(id).size());
    }

    private static ScanStore.AcceptedScan scan(int sequence) {
        return new ScanStore.AcceptedScan(sequence, SKU, "Item 1", PRICE, Instant.now().truncatedTo(ChronoUnit.MILLIS));
    }

    private static String open(ConnectionPool pool) {
        String id = UUID.randomUUID().toString();
        new JdbcTransactionStore(pool).insertOpen(id, "scan-store-test", Instant.now().truncatedTo(ChronoUnit.MILLIS));
        return id;
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    /** Fails the next guarded UPDATE with a MySQL deadlock (error 1213) without running it. */
    private static final class DeadlockOnce {
        final AtomicBoolean armed = new AtomicBoolean();

        Connection wrap(Connection connection) {
            return proxy(Connection.class, connection, (method, args) -> {
                Object result = invoke(method, connection, args);
                if (result instanceof PreparedStatement prepared && method.getName().equals("prepareStatement")
                        && ((String) args[0]).stripLeading().startsWith("UPDATE")) {
                    return proxy(PreparedStatement.class, prepared, (statementMethod, statementArgs) -> {
                        if (statementMethod.getName().equals("executeUpdate") && armed.compareAndSet(true, false)) {
                            throw new SQLException("Deadlock found when trying to get lock", "40001", 1213);
                        }
                        return invoke(statementMethod, prepared, statementArgs);
                    });
                }
                return result;
            });
        }

        private interface Handler {
            Object handle(java.lang.reflect.Method method, Object[] args) throws Throwable;
        }

        private static <T> T proxy(Class<T> type, T target, Handler handler) {
            return type.cast(Proxy.newProxyInstance(JdbcScanStoreTest.class.getClassLoader(), new Class<?>[]{type},
                    (self, method, args) -> switch (method.getName()) {
                        case "equals" -> args[0] == self;
                        case "hashCode" -> System.identityHashCode(self);
                        default -> handler.handle(method, args);
                    }));
        }

        private static Object invoke(java.lang.reflect.Method method, Object target, Object[] args) throws Throwable {
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException failure) {
                throw failure.getCause();
            }
        }
    }
}
