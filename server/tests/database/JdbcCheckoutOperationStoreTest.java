package database;

import database.CheckoutOperationStore.ApplyOutcome;
import database.CheckoutOperationStore.ApplyResult;
import database.CheckoutOperationStore.Attempt;
import database.CheckoutOperationStore.Line;
import database.CheckoutOperationStore.RestoreOutcome;
import database.CheckoutOperationStore.State;
import inventory.CheckoutStockService;
import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import support.InventoryTestSchema;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Inventory's checkout operation store over a disposable schema (R8; contracts/grpc-internal-contracts.md
 * §ApplyCheckout, §RestoreCheckout). Migrates the inventory side of JdbcCheckoutCompletionStoreTest.
 */
class JdbcCheckoutOperationStoreTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final String C = InventoryTestSchema.sku(3);

    @Test
    void appliedDeductsEveryLineOnceStoresTheCanonicalLinesAndAReplayDeductsNothing() throws Exception {
        try (InventoryTestSchema schema = seeded(5)) {
            JdbcCheckoutOperationStore store = new JdbcCheckoutOperationStore(schema.pool());
            Attempt attempt = attempt();
            List<Line> lines = List.of(new Line(A, 2), new Line(B, 1));

            ApplyResult applied = store.apply(attempt, lines, canonical(lines));
            assertEquals(new ApplyResult(ApplyOutcome.APPLIED, null, false), applied);
            assertEquals(3, schema.stock(A), "two units of A deducted once");
            assertEquals(4, schema.stock(B));
            assertEquals(5, schema.stock(C), "unreferenced stock is untouched");
            assertEquals("[{\"sku\":\"SKU-000001\",\"quantity\":2},{\"sku\":\"SKU-000002\",\"quantity\":1}]",
                    schema.queryString("SELECT `lines` FROM checkout_operation WHERE transaction_id = ?",
                            attempt.transactionId()), "the canonical payload is stored with the outcome");
            assertEquals("APPLIED", schema.queryString("SELECT state FROM checkout_operation WHERE transaction_id = ?",
                    attempt.transactionId()));

            ApplyResult replay = store.apply(attempt, lines, canonical(lines));
            assertEquals(new ApplyResult(ApplyOutcome.APPLIED, null, true), replay, "the recorded outcome, replayed");
            assertEquals(3, schema.stock(A), "a replay deducts nothing");
            assertEquals(4, schema.stock(B));
            assertEquals(1, schema.operationCount("APPLIED"));
            assertEquals(State.APPLIED, store.find(attempt).orElseThrow().state());
        }
    }

    @Test
    void insufficientStockRecordsTheRejectionAndLeavesEveryStockRowUnchanged() throws Exception {
        try (InventoryTestSchema schema = seeded(1)) {
            JdbcCheckoutOperationStore store = new JdbcCheckoutOperationStore(schema.pool());
            Attempt attempt = attempt();
            List<Line> lines = List.of(new Line(A, 2), new Line(B, 1), new Line(C, 2));

            ApplyResult rejected = store.apply(attempt, lines, canonical(lines));
            assertEquals(new ApplyResult(ApplyOutcome.INSUFFICIENT_STOCK, A, false), rejected,
                    "the first short SKU in SKU order");
            assertEquals(1, schema.stock(A));
            assertEquals(1, schema.stock(B), "the line that had stock rolled back with the rest");
            assertEquals(1, schema.stock(C));
            var recorded = store.find(attempt).orElseThrow();
            assertEquals(State.REJECTED, recorded.state());
            assertEquals("INSUFFICIENT_STOCK", recorded.rejectionReason());
            assertEquals(A, recorded.rejectedSku());
            assertEquals(new ApplyResult(ApplyOutcome.INSUFFICIENT_STOCK, A, true), store.apply(attempt, lines,
                    canonical(lines)), "a rejection replays even after stock arrives");
            schema.setStock(A, 10);
            assertEquals(new ApplyResult(ApplyOutcome.INSUFFICIENT_STOCK, A, true), store.apply(attempt, lines,
                    canonical(lines)));
            assertEquals(10, schema.stock(A));

            Attempt unknown = attempt();
            List<Line> withUnknown = List.of(new Line(A, 50), new Line("NO-SUCH-SKU", 1), new Line("ZZ-UNKNOWN", 1));
            assertEquals(new ApplyResult(ApplyOutcome.INVALID_REQUEST, "NO-SUCH-SKU", false),
                    store.apply(unknown, withUnknown, canonical(withUnknown)),
                    "an unknown SKU takes precedence over a shortfall, and the first one is named");
            assertEquals(10, schema.stock(A));
            assertEquals("INVALID_REQUEST", store.find(unknown).orElseThrow().rejectionReason());
        }
    }

    @Test
    void twoAttemptsRacingForTheLastUnitGiveOneAppliedAndOneInsufficient() throws Exception {
        try (InventoryTestSchema schema = seeded(1);
             ExecutorService workers = Executors.newFixedThreadPool(2)) {
            JdbcCheckoutOperationStore store = new JdbcCheckoutOperationStore(schema.newPool("test.race." + suffix(), 2, 4, null));
            for (int round = 0; round < 5; round++) {
                schema.setStock(A, 1);
                List<Line> lines = List.of(new Line(A, 1));
                CountDownLatch go = new CountDownLatch(1);
                List<Callable<ApplyResult>> racers = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    Attempt attempt = attempt();
                    racers.add(() -> {
                        go.await();
                        return store.apply(attempt, lines, canonical(lines));
                    });
                }
                List<Future<ApplyResult>> results = new ArrayList<>();
                for (Callable<ApplyResult> racer : racers) results.add(workers.submit(racer));
                go.countDown();
                int applied = 0;
                int insufficient = 0;
                for (Future<ApplyResult> result : results) {
                    ApplyOutcome outcome = result.get(30, TimeUnit.SECONDS).outcome();
                    if (outcome == ApplyOutcome.APPLIED) applied++;
                    if (outcome == ApplyOutcome.INSUFFICIENT_STOCK) insufficient++;
                }
                assertEquals(1, applied, "round " + round);
                assertEquals(1, insufficient, "round " + round + ": contention is never reported as anything else");
                assertEquals(0, schema.stock(A), "round " + round + ": the last unit sold once, never negative");
            }
        }
    }

    @Test
    void theCheckConstraintBlocksNegativeStock() throws Exception {
        try (InventoryTestSchema schema = seeded(1)) {
            SQLException refused = assertThrows(SQLException.class,
                    () -> schema.update("UPDATE inventory SET stock_quantity = stock_quantity - 2 WHERE sku = ?", A));
            assertTrue(String.valueOf(refused.getMessage()).contains("chk_stock_nonnegative"), refused.getMessage());
            assertEquals(1, schema.stock(A));
        }
    }

    @Test
    void deadlocksAndLockWaitTimeoutsRetryBeforeCommitAndPersistentContentionIsReported() throws Exception {
        try (InventoryTestSchema schema = seeded(5)) {
            FailLocking failing = new FailLocking();
            JdbcCheckoutOperationStore store = new JdbcCheckoutOperationStore(
                    schema.newPool("test.contention." + suffix(), 1, 2, failing::wrap));
            List<Line> lines = List.of(new Line(A, 1));
            long retries = PerfMetrics.counterValue("inventory.apply.retries");

            failing.next(1, 1213);
            assertEquals(ApplyOutcome.APPLIED, store.apply(attempt(), lines, canonical(lines)).outcome());
            failing.next(2, 1205);
            assertEquals(ApplyOutcome.APPLIED, store.apply(attempt(), lines, canonical(lines)).outcome(),
                    "a lock-wait timeout retries like a deadlock");
            assertEquals(3, schema.stock(A), "each retried attempt deducted exactly once");
            assertEquals(retries + 3, PerfMetrics.counterValue("inventory.apply.retries"));

            failing.next(Integer.MAX_VALUE, 1213);
            Attempt exhausted = attempt();
            StoreFailure contention = assertThrows(StoreFailure.class, () -> store.apply(exhausted, lines, canonical(lines)));
            assertTrue(contention.contended(), "exhausted retries are contention, never a business outcome");
            assertFalse(contention.commitOutcomeUnknown());
            failing.next(0, 0);
            assertTrue(store.find(exhausted).isEmpty(), "nothing was recorded");
            assertEquals(3, schema.stock(A));
        }
    }

    @Test
    void restoreFollowsTheStateTable() throws Exception {
        try (InventoryTestSchema schema = seeded(5)) {
            JdbcCheckoutOperationStore store = new JdbcCheckoutOperationStore(schema.pool());
            List<Line> lines = List.of(new Line(A, 2), new Line(B, 1));

            Attempt applied = attempt();
            store.apply(applied, lines, canonical(lines));
            assertEquals(3, schema.stock(A));
            var restored = store.restore(applied);
            assertEquals(RestoreOutcome.RESTORED, restored.outcome());
            assertEquals(canonical(lines), restored.lines());
            assertEquals(5, schema.stock(A), "APPLIED → RESTORED adds the recorded lines back");
            assertEquals(5, schema.stock(B));
            assertEquals(State.RESTORED, store.find(applied).orElseThrow().state());
            assertNotNull(schema.queryString("SELECT restored_at FROM checkout_operation WHERE transaction_id = ?",
                    applied.transactionId()));

            var again = store.restore(applied);
            assertEquals(RestoreOutcome.ALREADY_RESTORED, again.outcome());
            assertEquals(canonical(lines), again.lines());
            assertEquals(5, schema.stock(A), "a repeated restore changes nothing");

            Attempt rejected = attempt();
            List<Line> tooMany = List.of(new Line(A, 50));
            store.apply(rejected, tooMany, canonical(tooMany));
            assertEquals(RestoreOutcome.NOTHING_TO_RESTORE, store.restore(rejected).outcome(), "REJECTED applied nothing");
            assertEquals(State.REJECTED, store.find(rejected).orElseThrow().state());

            Attempt absent = attempt();
            var blocked = store.restore(absent);
            assertEquals(RestoreOutcome.NOTHING_TO_RESTORE, blocked.outcome());
            assertNull(blocked.lines());
            var tombstone = store.find(absent).orElseThrow();
            assertEquals(State.BLOCKED, tombstone.state(), "absent → a BLOCKED tombstone");
            assertNull(tombstone.lines());
            assertEquals(RestoreOutcome.NOTHING_TO_RESTORE, store.restore(absent).outcome(), "BLOCKED → nothing");
            assertEquals(5, schema.stock(A));
        }
    }

    @Test
    void applyAfterRestoredOrBlockedReturnsStockRestored() throws Exception {
        try (InventoryTestSchema schema = seeded(5)) {
            JdbcCheckoutOperationStore store = new JdbcCheckoutOperationStore(schema.pool());
            List<Line> lines = List.of(new Line(A, 1));

            Attempt restored = attempt();
            store.apply(restored, lines, canonical(lines));
            store.restore(restored);
            assertEquals(new ApplyResult(ApplyOutcome.STOCK_RESTORED, null, true), store.apply(restored, lines,
                    canonical(lines)));
            assertEquals(5, schema.stock(A), "a restored attempt is never applied again");

            Attempt blocked = attempt();
            store.restore(blocked);
            assertEquals(new ApplyResult(ApplyOutcome.STOCK_RESTORED, null, true), store.apply(blocked, lines,
                    canonical(lines)), "a tombstone refuses a late first apply");
            assertEquals(5, schema.stock(A));
            assertEquals(State.BLOCKED, store.find(blocked).orElseThrow().state());
        }
    }

    @Test
    void aReusedIdentifierWithOtherLinesIsAMismatchAndASecondDeductionIsAnIntegrityFailure() throws Exception {
        try (InventoryTestSchema schema = seeded(5)) {
            JdbcCheckoutOperationStore store = new JdbcCheckoutOperationStore(schema.pool());
            Attempt attempt = attempt();
            List<Line> lines = List.of(new Line(A, 1));
            store.apply(attempt, lines, canonical(lines));

            List<Line> other = List.of(new Line(A, 2));
            assertEquals(new ApplyResult(ApplyOutcome.PAYLOAD_MISMATCH, null, true),
                    store.apply(attempt, other, canonical(other)));
            assertEquals(4, schema.stock(A), "a mismatch changes nothing");

            Attempt second = new Attempt(attempt.transactionId(), 2);
            assertThrows(CheckoutOperationStore.IntegrityFailure.class, () -> store.apply(second, lines, canonical(lines)),
                    "the unique applied index allows one effective deduction per transaction (FR-029)");
            assertEquals(4, schema.stock(A));
            assertTrue(store.find(second).isEmpty(), "the refused operation rolled back entirely");
            assertEquals(1, schema.operationCount("APPLIED"));

            Attempt rejectedSecond = new Attempt(attempt.transactionId(), 3);
            List<Line> tooMany = List.of(new Line(A, 99));
            assertEquals(ApplyOutcome.INSUFFICIENT_STOCK,
                    store.apply(rejectedSecond, tooMany, canonical(tooMany)).outcome(),
                    "a rejection records no deduction, so it never meets the applied index");
        }
    }

    private static String canonical(List<Line> lines) {
        return CheckoutStockService.canonical(lines);
    }

    private static Attempt attempt() {
        return new Attempt("tx-" + suffix(), 1);
    }

    private static InventoryTestSchema seeded(int stock) throws Exception {
        InventoryTestSchema schema = InventoryTestSchema.create();
        schema.seed(3, stock);
        return schema;
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    /** Fails the next locking SELECTs with a MySQL error code before they run. */
    private static final class FailLocking {
        private final AtomicInteger remaining = new AtomicInteger();
        private volatile int errorCode;

        void next(int times, int code) {
            errorCode = code;
            remaining.set(times);
        }

        Connection wrap(Connection connection) {
            return proxy(Connection.class, connection, (method, args) -> {
                Object result = invoke(method, connection, args);
                if (result instanceof PreparedStatement prepared && method.getName().equals("prepareStatement")
                        && ((String) args[0]).contains("FOR UPDATE")) {
                    return proxy(PreparedStatement.class, prepared, (statementMethod, statementArgs) -> {
                        if (statementMethod.getName().equals("executeQuery") && remaining.getAndUpdate(
                                left -> left > 0 && left != Integer.MAX_VALUE ? left - 1 : left) > 0) {
                            throw errorCode == 1213 ? new SQLException("Deadlock found when trying to get lock", "40001", 1213)
                                    : new SQLException("Lock wait timeout exceeded", "HY000", errorCode);
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
            return type.cast(Proxy.newProxyInstance(JdbcCheckoutOperationStoreTest.class.getClassLoader(),
                    new Class<?>[]{type}, (self, method, args) -> switch (method.getName()) {
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
