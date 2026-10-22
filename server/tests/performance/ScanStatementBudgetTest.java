package performance;

import database.JdbcProductReplicaStore;
import database.StoreFailure;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.FaultingConnections;
import support.FaultingConnections.CommitFault;
import support.InProcessTransaction;
import support.StatementBudgets;
import support.TransactionTestSchema;
import transaction.CatalogReplicaRefresher;
import transaction.InventoryGateway;
import transaction.ProductCatalogReplica;
import transaction.TransactionOperations;
import transaction.TransactionService;

import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Start and Scan ordering, atomicity, and remote-call rules (strict, exact, gating) plus their measured
 * statement budgets (R22; plan.md "Key flows and statement budgets").
 */
class ScanStatementBudgetTest {

    @Test
    void aScanIsAcknowledgedOnlyAfterItsCommitReturns() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(faults::wrap).build();
            String id = transaction.start();
            transaction.scan(id, "SKU-000001");

            CompletableFuture<TransactionOperations.ScanView> result = new CompletableFuture<>();
            AtomicLong commitsReturnedAtResult = new AtomicLong(-1);
            long commitsBefore = faults.commitsReturned();
            long started = System.nanoTime();
            Thread scanning = Thread.ofPlatform().start(() -> {
                faults.armForCurrentThread(CommitFault.delay(300));
                try {
                    TransactionOperations.ScanView view = transaction.scan(id, "SKU-000002");
                    commitsReturnedAtResult.set(faults.commitsReturned());
                    result.complete(view);
                } catch (Throwable failure) {
                    result.completeExceptionally(failure);
                }
            });
            Thread.sleep(150);
            assertFalse(result.isDone(), "no result exists while the commit is still in progress");
            TransactionOperations.ScanView view = result.get(10, TimeUnit.SECONDS);
            scanning.join(5000);
            assertTrue(System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(300));
            assertEquals(commitsBefore + 1, commitsReturnedAtResult.get(), "the commit had returned before the result");
            assertEquals(2, view.itemCount());
            assertEquals(2, schema.scans(id).size());
        }
    }

    @Test
    void eitherKindOfCommitFailureAcknowledgesNothing() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(faults::wrap).build();
            String id = transaction.start();
            for (CommitFault fault : new CommitFault[]{CommitFault.failBeforeCommit(), CommitFault.failAfterCommit()}) {
                faults.armForCurrentThread(fault);
                StoreFailure failure = assertThrows(StoreFailure.class, () -> transaction.scan(id, "SKU-000001"),
                        fault.kind() + " yields a failure, never an acknowledgement");
                assertTrue(failure.commitOutcomeUnknown());
            }
            assertEquals(1, schema.scans(id).size(), "only the commit that failed after succeeding is durable");
            assertEquals(2, transaction.scan(id, "SKU-000001").itemCount(), "and the next Scan builds on it");
        }
    }

    @Test
    void theGuardedUpdateAndTheEventShareOneCommitAndStartAndScanMakeNoRemoteCall() throws Exception {
        CountingConnections counting = new CountingConnections();
        FailingInventory inventory = new FailingInventory();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(counting::wrap).build();
            new CatalogReplicaRefresher(inventory, new JdbcProductReplicaStore(transaction.pool), new ProductCatalogReplica(),
                    Duration.ofSeconds(30)).loadDurable();
            String id;
            try (CountingConnections.Scope scope = counting.scope()) {
                id = transaction.start();
                assertEquals(1, scope.counts().inserts());
                assertEquals(1, scope.counts().commits());
            }
            for (int i = 1; i <= 3; i++) {
                try (CountingConnections.Scope scope = counting.scope()) {
                    transaction.scan(id, "SKU-00000" + i);
                    assertEquals(1, scope.counts().updates(), "exactly one guarded UPDATE");
                    assertEquals(1, scope.counts().inserts(), "exactly one accepted_scan INSERT");
                    assertEquals(1, scope.counts().commits(), "both in one commit");
                    assertEquals(0, scope.counts().rollbacks());
                }
            }
            assertEquals(0, inventory.calls.get(), "Start and Scan never call Inventory");
            assertTrue(Arrays.stream(TransactionService.class.getConstructors()[0].getParameterTypes())
                    .noneMatch(InventoryGateway.class::isAssignableFrom), "the Scan path has no route to Inventory");
        }
    }

    @Test
    void warmScanStartAndColdScanStayWithinTheirStatementBudgets() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(counting::wrap).build();
            String id;
            try (CountingConnections.Scope scope = counting.scope()) {
                id = transaction.start();
                StatementBudgets.assertWithin("start.statements", scope.counts().statements());
                StatementBudgets.assertWithin("start.selects", scope.counts().selects());
                StatementBudgets.assertWithin("start.commits", scope.counts().commits());
            }
            transaction.scan(id, "SKU-000001");
            try (CountingConnections.Scope scope = counting.scope()) {
                transaction.scan(id, "SKU-000001");
                StatementBudgets.assertWithin("scan.warm.statements", scope.counts().statements());
                StatementBudgets.assertWithin("scan.warm.selects", scope.counts().selects());
                StatementBudgets.assertWithin("scan.warm.commits", scope.counts().commits());
                StatementBudgets.assertWithin("scan.warm.sessionCalls",
                        scope.counts().setAutoCommitCalls() + scope.counts().setTransactionIsolationCalls());
            }

            InProcessTransaction restarted = InProcessTransaction.over(schema).pool(transaction.pool).build();
            try (CountingConnections.Scope scope = counting.scope()) {
                assertEquals(3, restarted.scan(id, "SKU-000002").itemCount());
                StatementBudgets.assertWithin("scan.cold.extraReads", scope.counts().selects());
                assertEquals(2, scope.counts().statements() - scope.counts().selects(), "the writes stay the warm two");
            }
            try (CountingConnections.Scope scope = counting.scope()) {
                restarted.scan(id, "SKU-000002");
                assertEquals(0, scope.counts().selects(), "the cold load happens once per load");
            }
        }
    }

    private static TransactionTestSchema seeded() throws Exception {
        TransactionTestSchema schema = TransactionTestSchema.create();
        schema.seedReplica(20, UUID.randomUUID().toString());
        return schema;
    }

    /** An Inventory that fails the test if anything calls it. */
    private static final class FailingInventory implements InventoryGateway {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public CatalogVersion catalogVersion() {
            calls.incrementAndGet();
            throw new AssertionError("Inventory was called");
        }

        @Override
        public Catalog catalog() {
            calls.incrementAndGet();
            throw new AssertionError("Inventory was called");
        }

        @Override
        public ApplyResult applyCheckout(database.CheckoutAttemptStore.AttemptKey key, java.util.List<CheckoutLine> lines,
                                         java.time.Duration deadline) {
            calls.incrementAndGet();
            throw new AssertionError("Inventory was called");
        }

        @Override
        public RecordedOutcome checkoutOutcome(database.CheckoutAttemptStore.AttemptKey key) {
            calls.incrementAndGet();
            throw new AssertionError("Inventory was called");
        }
    }
}
