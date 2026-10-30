package performance;

import database.CheckoutAttemptStore;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.FakeInventoryReactor;
import support.FakeInventoryReactor.Reply;
import support.FaultingConnections;
import support.FaultingConnections.CommitFault;
import support.InProcessTransaction;
import support.StatementBudgets;
import support.TransactionTestSchema;
import transaction.TransactionFailure;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Complete's ordering rules, exact and gating, and its measured statement budgets (R22, D7; plan.md "Key
 * flows and statement budgets"): T1 commits the attempt and its {@code CheckoutRequested} outbox row before
 * the request event is sent, no 200 precedes the T3 commit, a warm Complete records one request event and
 * its request thread runs T1 alone, and nothing is read after T3.
 */
class CompleteStatementBudgetTest {
    private static final long DELAY_MS = 300;

    @Test
    void t1CommitsBeforeApplyCheckoutIsSent() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(faults::wrap).build();
            String id = transaction.start();
            transaction.scan(id, "SKU-000001");
            AtomicLong returnedAtRequest = new AtomicLong(-1);
            AtomicLong requestedAt = new AtomicLong();
            AtomicLong pendingAtRequest = new AtomicLong(-1);
            AtomicLong outboxAtRequest = new AtomicLong(-1);
            long returnedBefore = faults.commitsReturned();
            try (FakeInventoryReactor inventory = transaction.reactor().thenAnswer(call -> {
                requestedAt.set(System.nanoTime());
                returnedAtRequest.set(faults.commitsReturned());
                try {
                    pendingAtRequest.set(schema.pendingAttemptCount());
                    outboxAtRequest.set(schema.queryLong("SELECT COUNT(*) FROM outbox_event WHERE event_id = ?",
                            call.eventId()));
                } catch (Exception failure) {
                    throw new AssertionError(failure);
                }
                return Reply.deduct();
            }).start()) {
                long started = System.nanoTime();
                faults.armForCurrentThread(CommitFault.delay(DELAY_MS));
                transaction.complete(id);
                assertTrue(returnedAtRequest.get() >= returnedBefore + 1,
                        "T1's commit had returned when the request event was sent");
                assertEquals(1, pendingAtRequest.get(), "the attempt is durable before Inventory hears of it");
                assertEquals(1, outboxAtRequest.get(), "and so is the outbox row the event was sent from (FR-010)");
                assertTrue(TimeUnit.NANOSECONDS.toMillis(requestedAt.get() - started) >= DELAY_MS,
                        "the event waited for the delayed T1 commit");
            }
        }
    }

    @Test
    void no200IsProducedBeforeTheT3CommitReturns() throws Exception {
        AtomicLong t3Returned = new AtomicLong();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema)
                    .wrapAttempts(store -> tap(store, "complete", () -> sleep(DELAY_MS), () -> t3Returned.set(System.nanoTime())))
                    .build();
            String id = transaction.start();
            transaction.scan(id, "SKU-000001");
            try (FakeInventoryReactor inventory = transaction.reactor().start()) {
                long started = System.nanoTime();
                var receipt = transaction.complete(id);
                long answeredAt = System.nanoTime();
                assertNotNull(receipt);
                assertTrue(t3Returned.get() > 0 && t3Returned.get() <= answeredAt, "T3's commit returned before the receipt");
                assertTrue(TimeUnit.NANOSECONDS.toMillis(answeredAt - started) >= DELAY_MS,
                        "the receipt waited for the delayed T3 commit");
                assertEquals("COMPLETED", schema.transaction(id).orElseThrow().status(), "durable before the 200");
            }
        }
    }

    @Test
    void aWarmCompleteMakesOneApplyCheckoutCallAndReadsNothingAfterT3() throws Exception {
        CountingConnections counting = new CountingConnections();
        AtomicReference<CountingConnections.Counts> t3 = new AtomicReference<>();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(counting::wrap)
                    .wrapAttempts(store -> measured(store, "complete", counting, t3)).build();
            String id = transaction.start();
            transaction.scan(id, "SKU-000001");
            transaction.scan(id, "SKU-000002");

            try (FakeInventoryReactor inventory = transaction.reactor().start()) {
                CountingConnections.Counts request;
                try (CountingConnections.Scope scope = counting.scope()) {
                    transaction.complete(id);
                    request = scope.counts();
                }
                assertEquals(1, inventory.requests(), "exactly one CheckoutRequested on the warm path");
                assertEquals(0, request.selects(), "pre-checks read nothing on the warm path, and no read follows T3");
                assertEquals(1, request.updates(), "the guarded UPDATE");
                assertEquals(2, request.inserts(), "the attempt INSERT and the outbox INSERT");
                assertEquals(1, request.commits(), "the request thread commits T1 only");
                StatementBudgets.assertWithin("complete.t1.statements", request.statements());
                StatementBudgets.assertWithin("complete.t1.commits", request.commits());

                CountingConnections.Counts settled = t3.get();
                assertNotNull(settled, "T3 ran on the outcome consumer");
                assertEquals(0, settled.selects(), "T3 reads nothing");
                assertEquals(2, settled.updates());
                StatementBudgets.assertWithin("complete.t3.statements", settled.statements());
                StatementBudgets.assertWithin("complete.t3.commits", settled.commits());
            }
        }
    }

    @Test
    void theReopenAfterARejectionStaysWithinItsBudget() throws Exception {
        CountingConnections counting = new CountingConnections();
        AtomicReference<CountingConnections.Counts> reopen = new AtomicReference<>();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(counting::wrap)
                    .wrapAttempts(store -> measured(store, "reopen", counting, reopen)).build();
            String id = transaction.start();
            transaction.scan(id, "SKU-000001");
            try (FakeInventoryReactor inventory = transaction.reactor().then(Reply.insufficient("SKU-000001")).start()) {
                assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK,
                        assertThrows(TransactionFailure.class, () -> transaction.complete(id)).code());
            }
            CountingConnections.Counts counts = reopen.get();
            assertEquals(0, counts.selects());
            assertEquals(2, counts.updates());
            StatementBudgets.assertWithin("complete.reopen.statements", counts.statements());
            StatementBudgets.assertWithin("complete.reopen.commits", counts.commits());
        }
    }

    private static TransactionTestSchema seeded() throws Exception {
        TransactionTestSchema schema = TransactionTestSchema.create();
        schema.seedReplica(20, UUID.randomUUID().toString());
        return schema;
    }

    /** Counts the statements of {@code method} on the thread that runs it, the outcome consumer's. */
    static CheckoutAttemptStore measured(CheckoutAttemptStore store, String method, CountingConnections counting,
                                         AtomicReference<CountingConnections.Counts> counts) {
        CountingConnections.Scope[] scope = new CountingConnections.Scope[1];
        return tap(store, method, () -> scope[0] = counting.scope(), () -> {
            counts.set(scope[0].counts());
            scope[0].close();
        });
    }

    /** Runs {@code before} and {@code after} around every call of {@code method}. */
    static CheckoutAttemptStore tap(CheckoutAttemptStore store, String method, Runnable before, Runnable after) {
        return (CheckoutAttemptStore) Proxy.newProxyInstance(CompleteStatementBudgetTest.class.getClassLoader(),
                new Class<?>[]{CheckoutAttemptStore.class}, (self, called, args) -> {
                    boolean tapped = called.getName().equals(method);
                    if (tapped) before.run();
                    try {
                        return called.invoke(store, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    } finally {
                        if (tapped) after.run();
                    }
                });
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
