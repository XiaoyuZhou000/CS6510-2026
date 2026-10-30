package performance;

import database.CheckoutAttemptStore.AttemptKey;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.FakeInventoryReactor;
import support.FakeInventoryReactor.Reply;
import support.InProcessTransaction;
import support.RecordingOutboxSignal;
import support.StatementBudgets;
import support.TransactionTestSchema;
import transaction.CheckoutOrchestrator;
import transaction.CheckoutPendingFailure;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Transaction's outcome handler in a fixed number of round trips (R22, D7; plan.md "Key flows"): for the
 * PENDING active attempt, T3 or the reopen is one guarded UPDATE of the transaction and one UPDATE of the
 * attempt in one commit, with no read, and {@code client_outcome} rides in the same UPDATE.
 */
class OutcomeHandlerStatementBudgetTest {

    @Test
    void t3AndTheReopenAreTwoStatementsAndOneCommitWithNoRead() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            schema.seedReplica(5, UUID.randomUUID().toString());
            // Messaging stays stopped: the test hands each outcome to the handler on its own thread.
            InProcessTransaction transaction = InProcessTransaction.over(schema).decorator(counting::wrap)
                    .settings(CheckoutOrchestrator.Settings.defaults().withResponseBudget(Duration.ofMillis(200)))
                    .signal(new RecordingOutboxSignal()).messagingStopped().build();

            AttemptKey deducted = pendingAttempt(transaction);
            try (CountingConnections.Scope scope = counting.scope()) {
                transaction.outcomes.handle(FakeInventoryReactor.outcomeEvent(deducted, Reply.deduct()));
                CountingConnections.Counts counts = scope.counts();
                assertEquals(0, counts.selects(), "T3 reads nothing");
                assertEquals(2, counts.updates());
                assertEquals(1, counts.commits());
                StatementBudgets.assertWithin("transaction.outcome.statements", counts.statements());
                StatementBudgets.assertWithin("transaction.outcome.commits", counts.commits());
            }
            assertEquals("COMPLETED", schema.transaction(deducted.transactionId()).orElseThrow().status());

            AttemptKey rejected = pendingAttempt(transaction);
            try (CountingConnections.Scope scope = counting.scope()) {
                transaction.outcomes.handle(FakeInventoryReactor.outcomeEvent(rejected, Reply.insufficient("SKU-000001")));
                CountingConnections.Counts counts = scope.counts();
                assertEquals(0, counts.selects(), "the reopen reads nothing");
                assertEquals(2, counts.updates());
                StatementBudgets.assertWithin("transaction.outcome.statements", counts.statements());
                StatementBudgets.assertWithin("transaction.outcome.commits", counts.commits());
            }
            assertEquals("REJECTED", schema.attempts(rejected.transactionId()).get(0).state());
        }
    }

    /** A transaction with one unit whose Complete timed out, leaving attempt 1 PENDING with no responder. */
    private static AttemptKey pendingAttempt(InProcessTransaction transaction) throws Exception {
        String id = transaction.start();
        transaction.scan(id, "SKU-000001");
        CompletableFuture<Void> complete = CompletableFuture.runAsync(() ->
                assertThrows(CheckoutPendingFailure.class, () -> transaction.complete(id)));
        complete.get();
        return new AttemptKey(id, 1);
    }
}
