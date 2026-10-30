package performance;

import database.ConnectionPool;
import org.junit.jupiter.api.Test;
import support.FakeInventoryReactor;
import support.FakeInventoryReactor.Reply;
import support.FaultInjection;
import support.InProcessCheckout;
import support.InProcessTransaction;
import support.TestSchema;
import support.TransactionTestSchema;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * No connection, lock, or transaction is held while a request waits for an event (FR-040; 006
 * contracts/checkout-saga.md "Invariants"; replaces HeldConnectionDuringRpcTest). While a Complete waits for
 * its delayed outcome event, its pool is at the baseline, the transaction's stripe is free, and the
 * transaction row can be locked at once by anyone else; Inventory's consumer holds a connection only inside
 * its apply transaction.
 */
class HeldResourcesDuringEventWaitTest {

    @Test
    void whileCompleteWaitsForItsOutcomeTheRequestHoldsNoConnectionLockOrTransaction() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            schema.seedReplica(5, UUID.randomUUID().toString());
            InProcessTransaction transaction = InProcessTransaction.over(schema).pool(2, 4).build();
            String id = transaction.start();
            transaction.scan(id, "SKU-000001");
            int baseline = transaction.pool.occupancy().active();
            try (FakeInventoryReactor inventory = transaction.reactor().then(Reply.deduct().delayed(Duration.ofSeconds(3)))
                    .start()) {
                CompletableFuture<?> complete = CompletableFuture.supplyAsync(() -> transaction.complete(id));
                awaitRequests(inventory);
                long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(2_200);
                int sampled = 0;
                while (System.nanoTime() < until) {
                    assertFalse(complete.isDone(), "the outcome is still on its way");
                    assertEquals(baseline, settledOccupancy(transaction.pool, baseline),
                            "no pooled connection is borrowed while the request waits");
                    assertTrue(transaction.locks.lockFor(id).tryLock(), "the transaction's stripe is free");
                    transaction.locks.lockFor(id).unlock();
                    assertRowLockable(schema, id);
                    sampled++;
                    Thread.sleep(200);
                }
                assertTrue(sampled >= 5, "the wait was sampled throughout: " + sampled);
                complete.get(10, TimeUnit.SECONDS);
            }
            assertEquals("COMPLETED", schema.transaction(id).orElseThrow().status());
        }
    }

    @Test
    void inventorysConsumerHoldsAConnectionOnlyInsideItsApplyTransaction() throws Exception {
        try (InProcessCheckout checkout = InProcessCheckout.start(5, 10);
             var armed = FaultInjection.spec().delay("inventory.apply.afterCommit", 2_000).armInThisJvm()) {
            ConnectionPool pool = checkout.inventory.pool;
            int baseline = pool.occupancy().active();
            String id = checkout.transaction.start();
            checkout.transaction.scan(id, "SKU-000001");
            CompletableFuture<?> complete = CompletableFuture.supplyAsync(() -> checkout.transaction.complete(id));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (checkout.inventorySchema.queryLong("SELECT COUNT(*) FROM checkout_operation WHERE transaction_id = ?",
                    id) == 0) {
                assertTrue(System.nanoTime() < deadline, "Inventory never applied the request");
                Thread.sleep(5);
            }
            // The consumer thread now sleeps in inventory.apply.afterCommit, after its commit.
            for (int i = 0; i < 5; i++) {
                assertEquals(baseline, settledOccupancy(pool, baseline),
                        "Inventory's consumer returned its connection with its apply commit");
                Thread.sleep(150);
            }
            complete.get(15, TimeUnit.SECONDS);
            assertEquals(9, checkout.stock("SKU-000001"));
        }
    }

    /**
     * The pool's active count, re-sampled once after 100 ms when it differs: a sample can land in a brief
     * borrow of a dispatcher sweep, which returns at once, while a connection held through the wait would
     * still be held.
     */
    private static int settledOccupancy(ConnectionPool pool, int baseline) throws InterruptedException {
        int active = pool.occupancy().active();
        if (active != baseline) {
            Thread.sleep(100);
            active = pool.occupancy().active();
        }
        return active;
    }

    private static void awaitRequests(FakeInventoryReactor inventory) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (inventory.requests() == 0) {
            assertTrue(System.nanoTime() < deadline, "Complete never published its request");
            Thread.sleep(5);
        }
    }

    private static void assertRowLockable(TestSchema schema, String id) throws Exception {
        schema.admin().setAutoCommit(false);
        try (PreparedStatement lock = schema.admin().prepareStatement(
                "SELECT status, active_attempt_number FROM `transaction` WHERE transaction_id = ? FOR UPDATE NOWAIT")) {
            lock.setString(1, id);
            long started = System.nanoTime();
            try (ResultSet row = lock.executeQuery()) {
                assertTrue(row.next(), "NOWAIT got the row lock at once, so nobody holds it");
                assertEquals("OPEN", row.getString(1));
                assertEquals(1, row.getInt(2), "T1 is committed and visible");
            }
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 1_000);
        } finally {
            schema.admin().rollback();
            schema.admin().setAutoCommit(true);
        }
    }
}
