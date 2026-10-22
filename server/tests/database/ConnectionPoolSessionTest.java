package database;

import metrics.LatencyHistogram;
import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.FaultingConnections;
import support.FaultingConnections.CommitFault;
import support.TransactionTestSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Managed pool sessions and the unit-of-work helper (R20, R21). */
class ConnectionPoolSessionTest {
    private static final String INSERT =
            "INSERT INTO product_replica (sku, name, price, catalog_version) VALUES (?, 'n', 1.00, 'v')";

    @Test
    void managedPoolsAreManualCommitAtReadCommittedAndLegacyPoolsAreUnchanged() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool managed = schema.newPool("test.session." + suffix(), 2, 2, null);
            Connection connection = managed.borrow();
            try {
                assertFalse(connection.getAutoCommit());
                assertEquals(Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());
                try (Statement sql = connection.createStatement();
                     ResultSet row = sql.executeQuery("SELECT @@session.transaction_isolation, @@session.autocommit")) {
                    assertTrue(row.next());
                    assertEquals("READ-COMMITTED", row.getString(1), "the server session matches");
                    assertEquals(0, row.getInt(2));
                }
            } finally {
                managed.release(connection);
            }

            try (ConnectionPool legacy = new ConnectionPool(schema.host(), schema.port(), schema.name(),
                    schema.user(), schema.password(), 1)) {
                Connection plain = legacy.borrow();
                try {
                    assertTrue(plain.getAutoCommit(), "legacy pools keep driver defaults");
                    assertEquals(Connection.TRANSACTION_REPEATABLE_READ, plain.getTransactionIsolation());
                } finally {
                    legacy.release(plain);
                }
            }
        }
    }

    @Test
    void releaseRollsBackOpenWorkAndRestoresTheSessionSettings() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.newPool("test.release." + suffix(), 1, 1, null);
            Connection connection = pool.borrow();
            insert(connection, "SKU-UNCOMMITTED");
            pool.release(connection);
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM product_replica WHERE sku = 'SKU-UNCOMMITTED'"),
                    "work left open by a borrower is rolled back on release");

            Connection again = pool.borrow();
            assertSame(connection, again, "the single connection is reused");
            again.setAutoCommit(true);
            again.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            pool.release(again);
            Connection restored = pool.borrow();
            try {
                assertFalse(restored.getAutoCommit());
                assertEquals(Connection.TRANSACTION_READ_COMMITTED, restored.getTransactionIsolation());
            } finally {
                pool.release(restored);
            }
            assertThrows(IllegalStateException.class, () -> pool.release(restored), "double release is refused");
        }
    }

    @Test
    void unitsOfWorkNeverToggleAutocommitOrIsolation() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.newPool("test.counting." + suffix(), 2, 2, counting::wrap);
            for (int i = 0; i < 3; i++) {
                try (CountingConnections.Scope scope = counting.scope()) {
                    String sku = "SKU-" + i;
                    JdbcUnitOfWork.inTransaction(pool, "test.unit", connection -> insert(connection, sku));
                    assertEquals(1, scope.counts().inserts());
                    assertEquals(1, scope.counts().statements());
                    assertEquals(1, scope.counts().commits());
                    assertEquals(0, scope.counts().rollbacks());
                    assertEquals(0, scope.counts().setAutoCommitCalls());
                    assertEquals(0, scope.counts().setTransactionIsolationCalls());
                }
                try (CountingConnections.Scope scope = counting.scope()) {
                    long rows = JdbcUnitOfWork.read(pool, "test.read", connection -> count(connection));
                    assertEquals(i + 1, rows);
                    assertEquals(1, scope.counts().selects());
                    assertEquals(0, scope.counts().commits());
                    assertEquals(1, scope.counts().rollbacks(), "a read ends with one rollback");
                    assertEquals(0, scope.counts().setAutoCommitCalls());
                    assertEquals(0, scope.counts().setTransactionIsolationCalls());
                }
            }
            assertEquals(0, counting.total().setAutoCommitCalls(), "session setup happens once, beneath the decorator");
            assertEquals(0, counting.total().setTransactionIsolationCalls());
            assertTrue(PerfMetrics.timerSnapshot("test.unit").count() >= 3, "units are timed");

            String rolledBack = JdbcUnitOfWork.inTransaction(pool, "test.unit", connection -> {
                insert(connection, "SKU-GUARD-MISS");
                throw JdbcUnitOfWork.rollbackAndReturn("GUARD_MISS");
            });
            assertEquals("GUARD_MISS", rolledBack);
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM product_replica WHERE sku = 'SKU-GUARD-MISS'"));

            assertThrows(IllegalStateException.class, () -> JdbcUnitOfWork.inTransaction(pool, "test.unit", connection -> {
                insert(connection, "SKU-THROWN");
                throw new IllegalStateException("business failure");
            }));
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM product_replica WHERE sku = 'SKU-THROWN'"));
            StoreFailure sqlFailure = assertThrows(StoreFailure.class, () -> JdbcUnitOfWork.inTransaction(pool, "test.unit",
                    connection -> { insert(connection, "SKU-0"); return null; }));
            assertFalse(sqlFailure.commitOutcomeUnknown(), "a duplicate key fails before commit");
        }
    }

    @Test
    void commitFailuresOfBothKindsAreReportedAsUnknownAndNeverRetried() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.newPool("test.faults." + suffix(), 1, 1, faults::wrap);

            faults.armForCurrentThread(CommitFault.failBeforeCommit());
            StoreFailure before = assertThrows(StoreFailure.class, () -> JdbcUnitOfWork.inTransaction(pool, "test.fault",
                    connection -> insert(connection, "SKU-BEFORE")));
            assertTrue(before.commitOutcomeUnknown());
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM product_replica WHERE sku = 'SKU-BEFORE'"));

            faults.armForCurrentThread(CommitFault.failAfterCommit());
            StoreFailure after = assertThrows(StoreFailure.class, () -> JdbcUnitOfWork.inTransaction(pool, "test.fault",
                    connection -> insert(connection, "SKU-AFTER")));
            assertTrue(after.commitOutcomeUnknown());
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM product_replica WHERE sku = 'SKU-AFTER'"),
                    "the commit happened although the caller saw a failure");

            assertEquals(2, faults.commitsStarted(), "neither unit was retried after its commit failed");
            assertEquals(0, faults.commitsReturned());

            JdbcUnitOfWork.inTransaction(pool, "test.fault", connection -> insert(connection, "SKU-HEALTHY"));
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM product_replica WHERE sku = 'SKU-HEALTHY'"),
                    "the pool still serves a working connection afterwards");
        }
    }

    @Test
    void deadlocksRetryTheWholeUnitOnlyBeforeCommit() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.newPool("test.deadlock." + suffix(), 2, 2, null);
            long retriesBefore = PerfMetrics.counterValue("test.deadlock.deadlockRetries");
            AtomicInteger attempts = new AtomicInteger();
            int result = JdbcUnitOfWork.inTransaction(pool, "test.deadlock", connection -> {
                insert(connection, "SKU-RETRIED-" + attempts.incrementAndGet());
                if (attempts.get() < 3) throw new SQLTransactionRollbackException("Deadlock found", "40001", 1213);
                return attempts.get();
            });
            assertEquals(3, result, "two deadlocks, then success on the last allowed attempt");
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM product_replica WHERE sku LIKE 'SKU-RETRIED-%'"),
                    "each failed attempt rolled back");
            assertEquals(retriesBefore + 2, PerfMetrics.counterValue("test.deadlock.deadlockRetries"));

            AtomicInteger exhausted = new AtomicInteger();
            StoreFailure failure = assertThrows(StoreFailure.class, () -> JdbcUnitOfWork.inTransaction(pool, "test.deadlock",
                    connection -> {
                        exhausted.incrementAndGet();
                        throw new SQLException("Deadlock found", "40001", 1213);
                    }));
            assertEquals(JdbcUnitOfWork.MAX_ATTEMPTS, exhausted.get());
            assertFalse(failure.commitOutcomeUnknown());

            AtomicInteger notDeadlock = new AtomicInteger();
            assertThrows(StoreFailure.class, () -> JdbcUnitOfWork.inTransaction(pool, "test.deadlock", connection -> {
                notDeadlock.incrementAndGet();
                throw new SQLException("Lock wait timeout exceeded", "HY000", 1205);
            }));
            assertEquals(1, notDeadlock.get(), "other failures are not retried");

            FaultingConnections faults = new FaultingConnections();
            ConnectionPool faulting = schema.newPool("test.deadlock.commit." + suffix(), 1, 1, faults::wrap);
            faults.armForCurrentThread(CommitFault.failBeforeCommit());
            AtomicInteger commitAttempts = new AtomicInteger();
            assertThrows(StoreFailure.class, () -> JdbcUnitOfWork.inTransaction(faulting, "test.deadlock",
                    connection -> insert(connection, "SKU-COMMIT-" + commitAttempts.incrementAndGet())));
            assertEquals(1, commitAttempts.get(), "a failed commit is never retried, deadlock or not");
        }
    }

    @Test
    void aRealInnoDbDeadlockIsRetriedAndBothUnitsCommit() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create();
             var workers = Executors.newFixedThreadPool(2)) {
            schema.update("INSERT INTO product_replica VALUES ('SKU-A', 'a', 1.00, 'v'), ('SKU-B', 'b', 1.00, 'v')");
            ConnectionPool pool = schema.newPool("test.innodb." + suffix(), 2, 2, null);
            CountDownLatch bothLocked = new CountDownLatch(2);
            Future<Integer> first = workers.submit(() -> crossLock(pool, "SKU-A", "SKU-B", bothLocked));
            Future<Integer> second = workers.submit(() -> crossLock(pool, "SKU-B", "SKU-A", bothLocked));
            int attempts = first.get(30, TimeUnit.SECONDS) + second.get(30, TimeUnit.SECONDS);
            assertTrue(attempts >= 3, "one unit was chosen as the deadlock victim and retried");
            assertEquals(2, schema.queryLong("SELECT COUNT(*) FROM product_replica WHERE name = 'locked'"));
        }
    }

    @Test
    void poolRecordsBorrowWaitHoldTimeAndPeakOccupancy() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create();
             var worker = Executors.newSingleThreadExecutor()) {
            String name = "test.metrics." + suffix();
            ConnectionPool pool = schema.newPool(name, 1, 2, null);
            Connection a = pool.borrow();
            Connection b = pool.borrow();
            assertEquals(2L, PerfMetrics.gaugeValue(name + ".pool.occupancy"));
            Future<Connection> waiting = worker.submit(pool::borrow);
            Thread.sleep(200);
            pool.release(a);
            Connection c = waiting.get(5, TimeUnit.SECONDS);
            Thread.sleep(50);
            pool.release(b);
            pool.release(c);

            LatencyHistogram.Snapshot wait = PerfMetrics.timerSnapshot(name + ".pool.borrowWait");
            assertEquals(3, wait.count());
            assertTrue(wait.maxNanos() >= 150_000_000L, "the third borrow waited for a release: " + wait);
            LatencyHistogram.Snapshot hold = PerfMetrics.timerSnapshot(name + ".pool.holdTime");
            assertEquals(3, hold.count());
            assertTrue(hold.maxNanos() >= 200_000_000L, "a connection was held through the wait: " + hold);
            assertEquals(2L, PerfMetrics.gaugeValue(name + ".pool.occupancyHighWater"));
            assertEquals(0L, PerfMetrics.gaugeValue(name + ".pool.occupancy"));
            assertEquals(2L, PerfMetrics.gaugeValue(name + ".pool.size"));
            assertEquals(2, pool.activeHighWater());
        }
    }

    /**
     * With {@code useLocalTransactionState=true} the driver skips {@code rollback()} after a failed first
     * statement, because the error packet never told it a transaction had begun. The unit must still end
     * that transaction, or the session carries its row and metadata locks back into the pool.
     */
    @Test
    void aFailedFirstStatementLeavesNoTransactionOrLockInThePool() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.newPool("test.abort." + suffix(), 1, 1, null);
            JdbcUnitOfWork.inTransaction(pool, "test.abort", connection -> insert(connection, "SKU-LOCK"));

            StoreFailure failure = assertThrows(StoreFailure.class, () -> JdbcUnitOfWork.inTransaction(pool, "test.abort",
                    connection -> {
                        try (PreparedStatement update = connection.prepareStatement(
                                "UPDATE product_replica SET price = 123456789012 WHERE sku = 'SKU-LOCK'")) {
                            return update.executeUpdate();
                        }
                    }), "an out-of-range value fails the first statement after it locked the row");
            assertFalse(failure.commitOutcomeUnknown());

            try (Statement admin = schema.admin().createStatement()) {
                admin.execute("SET SESSION lock_wait_timeout = 2");
                try (ResultSet row = admin.executeQuery(
                        "SELECT sku FROM product_replica WHERE sku = 'SKU-LOCK' FOR UPDATE NOWAIT")) {
                    assertTrue(row.next(), "no pooled session still holds the row lock");
                }
                admin.executeUpdate("RENAME TABLE product_replica TO product_replica_moved");
                admin.executeUpdate("RENAME TABLE product_replica_moved TO product_replica");
            }
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM information_schema.INNODB_TRX t "
                    + "JOIN information_schema.PROCESSLIST p ON p.id = t.trx_mysql_thread_id WHERE p.db = ?", schema.name()),
                    "no open transaction remains in the schema's sessions");
            assertEquals(1L, JdbcUnitOfWork.read(pool, "test.abort", ConnectionPoolSessionTest::count),
                    "the same pooled connection keeps working");
        }
    }

    /** Locks {@code first}, waits until the peer locked its row, then locks {@code second}. */
    private static int crossLock(ConnectionPool pool, String first, String second, CountDownLatch bothLocked) {
        AtomicInteger attempts = new AtomicInteger();
        JdbcUnitOfWork.inTransaction(pool, "test.innodb", connection -> {
            boolean firstAttempt = attempts.incrementAndGet() == 1;
            lockAndMark(connection, first);
            if (firstAttempt) {
                bothLocked.countDown();
                try {
                    if (!bothLocked.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("peer never locked");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
            lockAndMark(connection, second);
            return null;
        });
        return attempts.get();
    }

    private static void lockAndMark(Connection connection, String sku) throws SQLException {
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE product_replica SET name = 'locked' WHERE sku = ?")) {
            update.setString(1, sku);
            update.executeUpdate();
        }
    }

    private static Void insert(Connection connection, String sku) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement(INSERT)) {
            insert.setString(1, sku);
            insert.executeUpdate();
        }
        return null;
    }

    private static long count(Connection connection) throws SQLException {
        try (Statement sql = connection.createStatement();
             ResultSet row = sql.executeQuery("SELECT COUNT(*) FROM product_replica")) {
            row.next();
            return row.getLong(1);
        }
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
