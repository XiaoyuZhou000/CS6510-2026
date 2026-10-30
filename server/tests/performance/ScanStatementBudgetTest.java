package performance;

import database.JdbcProductReplicaStore;
import database.StoreFailure;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.FaultingConnections;
import support.FaultingConnections.CommitFault;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.StatementBudgets;
import support.TestEvents;
import support.TransactionTestSchema;
import transaction.CatalogEventHandler;
import transaction.ProductCatalogReplica;
import transaction.TransactionOperations;
import transaction.TransactionService;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(counting::wrap).build();
            assertTrue(new CatalogEventHandler(new JdbcProductReplicaStore(transaction.pool), new ProductCatalogReplica())
                    .loadDurable(), "the durable replica the catalog events left is what Scan prices from");
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
            assertThrows(ClassNotFoundException.class, () -> Class.forName("transaction.InventoryGateway"),
                    "Transaction has no synchronous route to Inventory at all");
            assertTrue(Arrays.stream(TransactionService.class.getConstructors()[0].getParameterTypes())
                    .noneMatch(type -> type.getPackageName().equals("messaging") || type.getPackageName().equals("rpc")
                            || type.getSimpleName().contains("Inventory")), "the Scan path has no route to Inventory");
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

    /** A schema whose replica holds 20 products, installed from a complete catalog snapshot as at runtime (D9). */
    private static TransactionTestSchema seeded() throws Exception {
        TransactionTestSchema schema = TransactionTestSchema.create();
        String version = UUID.randomUUID().toString();
        CatalogEventHandler catalog = new CatalogEventHandler(new JdbcProductReplicaStore(schema.pool()),
                new ProductCatalogReplica());
        for (int n = 1; n <= 20; n++) {
            catalog.handle(TestEvents.item(InventoryTestSchema.sku(n), "Item " + n, InventoryTestSchema.price(n).toPlainString(),
                    version));
        }
        catalog.handle(TestEvents.snapshot(version, 20));
        return schema;
    }
}
