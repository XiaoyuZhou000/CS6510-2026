package performance;

import org.junit.jupiter.api.Test;
import support.FakeInventoryGateway;
import support.InProcessTransaction;
import support.TransactionTestSchema;
import transaction.InventoryGateway;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * No database connection, row lock, or transaction is held across {@code ApplyCheckout} (FR-033;
 * contracts/checkout-saga.md "Invariants"): while Inventory blocks, the pool is back at its baseline and
 * the transaction row can be locked at once by anyone else.
 */
class HeldConnectionDuringRpcTest {

    @Test
    void whileApplyCheckoutBlocksNoConnectionOrRowLockIsHeld() throws Exception {
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        FakeInventoryGateway inventory = FakeInventoryGateway.applying().thenAnswer(call -> {
            inside.countDown();
            assertTrue(release.await(30, TimeUnit.SECONDS), "the test never released Inventory");
            return new InventoryGateway.Applied(false);
        });
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            schema.seedReplica(5, UUID.randomUUID().toString());
            InProcessTransaction transaction = InProcessTransaction.over(schema).pool(2, 4).inventory(inventory).build();
            String id = transaction.start();
            transaction.scan(id, "SKU-000001");
            int baseline = transaction.pool.occupancy().active();

            CompletableFuture<?> complete = CompletableFuture.supplyAsync(() -> transaction.complete(id));
            try {
                assertTrue(inside.await(30, TimeUnit.SECONDS), "Complete never reached Inventory");
                assertEquals(baseline, transaction.pool.occupancy().active(),
                        "no pooled connection is borrowed while ApplyCheckout runs");
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
            } finally {
                release.countDown();
            }
            complete.get(30, TimeUnit.SECONDS);
            assertEquals("COMPLETED", schema.transaction(id).orElseThrow().status());
        }
    }
}
