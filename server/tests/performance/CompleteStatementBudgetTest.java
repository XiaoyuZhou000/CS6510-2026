package performance;

import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.FakeInventoryGateway;
import support.FaultingConnections;
import support.FaultingConnections.CommitFault;
import support.InProcessTransaction;
import support.StatementBudgets;
import support.TransactionTestSchema;
import transaction.InventoryGateway;
import transaction.TransactionFailure;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Complete's ordering rules, exact and gating, and its measured statement budgets (R22; plan.md "Key
 * flows and statement budgets"): T1 commits before Inventory is called, no 200 precedes the T3 commit,
 * a warm Complete makes one ApplyCheckout call, and nothing is read after T3.
 */
class CompleteStatementBudgetTest {
    private static final long DELAY_MS = 300;

    @Test
    void t1CommitsBeforeApplyCheckoutIsSent() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        FakeInventoryGateway inventory = FakeInventoryGateway.applying();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(faults::wrap)
                    .inventory(inventory).build();
            String id = transaction.start();
            transaction.scan(id, "SKU-000001");
            AtomicLong returnedAtCall = new AtomicLong(-1);
            AtomicLong calledAt = new AtomicLong();
            long returnedBefore = faults.commitsReturned();
            inventory.thenAnswer(call -> {
                calledAt.set(System.nanoTime());
                returnedAtCall.set(faults.commitsReturned());
                assertEquals(1, schema.pendingAttemptCount(), "the attempt is durable before Inventory hears of it");
                return new InventoryGateway.Applied(false);
            });

            long started = System.nanoTime();
            faults.armForCurrentThread(CommitFault.delay(DELAY_MS));
            transaction.complete(id);
            assertEquals(returnedBefore + 1, returnedAtCall.get(), "T1's commit had returned when the call was sent");
            assertTrue(TimeUnit.NANOSECONDS.toMillis(calledAt.get() - started) >= DELAY_MS,
                    "the call waited for the delayed T1 commit");
        }
    }

    @Test
    void no200IsProducedBeforeTheT3CommitReturns() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        FakeInventoryGateway inventory = FakeInventoryGateway.applying();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(faults::wrap)
                    .inventory(inventory).build();
            String id = transaction.start();
            transaction.scan(id, "SKU-000001");
            AtomicLong appliedAt = new AtomicLong();
            AtomicLong returnedAtApply = new AtomicLong();
            inventory.thenAnswer(call -> {
                appliedAt.set(System.nanoTime());
                returnedAtApply.set(faults.commitsReturned());
                faults.armForCurrentThread(CommitFault.delay(DELAY_MS));
                return new InventoryGateway.Applied(false);
            });

            var receipt = transaction.complete(id);
            long answeredAt = System.nanoTime();
            assertNotNull(receipt);
            assertEquals(returnedAtApply.get() + 1, faults.commitsReturned(), "T3's commit returned before the receipt");
            assertTrue(TimeUnit.NANOSECONDS.toMillis(answeredAt - appliedAt.get()) >= DELAY_MS,
                    "the receipt waited for the delayed T3 commit");
            assertEquals("COMPLETED", schema.transaction(id).orElseThrow().status(), "durable before the 200");
        }
    }

    @Test
    void aWarmCompleteMakesOneApplyCheckoutCallAndReadsNothingAfterT3() throws Exception {
        CountingConnections counting = new CountingConnections();
        FakeInventoryGateway inventory = FakeInventoryGateway.applying();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(counting::wrap)
                    .inventory(inventory).build();
            String id = transaction.start();
            transaction.scan(id, "SKU-000001");
            transaction.scan(id, "SKU-000002");

            CountingConnections.Scope[] scope = {counting.scope()};
            CountingConnections.Counts[] t1 = new CountingConnections.Counts[1];
            inventory.thenAnswer(call -> {
                t1[0] = scope[0].counts();
                scope[0].close();
                scope[0] = counting.scope();
                return new InventoryGateway.Applied(false);
            });
            try {
                transaction.complete(id);
            } finally {
                scope[0].close();
            }
            CountingConnections.Counts t3 = scope[0].counts();
            assertEquals(1, inventory.applyCalls(), "exactly one ApplyCheckout on the warm path");

            assertEquals(0, t1[0].selects(), "pre-checks read nothing on the warm path");
            assertEquals(1, t1[0].updates());
            assertEquals(1, t1[0].inserts());
            assertEquals(0, t3.selects(), "the receipt comes from memory: no read follows T3");
            assertEquals(2, t3.updates());
            StatementBudgets.assertWithin("complete.t1.statements", t1[0].statements());
            StatementBudgets.assertWithin("complete.t1.commits", t1[0].commits());
            StatementBudgets.assertWithin("complete.t3.statements", t3.statements());
            StatementBudgets.assertWithin("complete.t3.commits", t3.commits());
        }
    }

    @Test
    void theReopenAfterARejectionStaysWithinItsBudget() throws Exception {
        CountingConnections counting = new CountingConnections();
        FakeInventoryGateway inventory = FakeInventoryGateway.applying();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(counting::wrap)
                    .inventory(inventory).build();
            String id = transaction.start();
            transaction.scan(id, "SKU-000001");

            CountingConnections.Scope[] scope = {counting.scope()};
            inventory.thenAnswer(call -> {
                scope[0].close();
                scope[0] = counting.scope();
                return new InventoryGateway.Rejected(InventoryGateway.RejectionReason.INSUFFICIENT_STOCK, "SKU-000001");
            });
            try {
                assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK,
                        assertThrows(TransactionFailure.class, () -> transaction.complete(id)).code());
            } finally {
                scope[0].close();
            }
            CountingConnections.Counts reopen = scope[0].counts();
            assertEquals(0, reopen.selects());
            assertEquals(2, reopen.updates());
            StatementBudgets.assertWithin("complete.reopen.statements", reopen.statements());
            StatementBudgets.assertWithin("complete.reopen.commits", reopen.commits());
        }
    }

    private static TransactionTestSchema seeded() throws Exception {
        TransactionTestSchema schema = TransactionTestSchema.create();
        schema.seedReplica(20, UUID.randomUUID().toString());
        return schema;
    }
}
