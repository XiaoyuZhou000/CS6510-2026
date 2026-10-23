package performance;

import database.ConnectionPool;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import support.FakeInventoryGateway;
import support.InProcessTransaction;
import support.TransactionTestSchema;
import transaction.CheckoutOrchestrator;
import transaction.CheckoutPendingFailure;
import transaction.InventoryGateway;
import transaction.TransactionFailure;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A request in a bounded wait holds no connection, row lock, or transaction (FR-033, FR-035, R7): between
 * its database checks the pool is at its baseline and the transaction row can be locked at once; the
 * checks are at most one per {@code CHECKOUT_WAIT_BACKSTOP_MS} plus one at the bound, none before the
 * settlement for an attempt owned in the same process, and each holds its connection for one read.
 */
class HeldConnectionDuringWaitTest {
    private static final String DB_CHECKS = "transaction.wait.dbChecks";

    @Test
    void aWaiterInAnotherInstanceHoldsNothingBetweenItsBoundedChecks() throws Exception {
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        FakeInventoryGateway inventory = FakeInventoryGateway.applying().thenAnswer(call -> {
            inside.countDown();
            assertTrue(release.await(30, TimeUnit.SECONDS), "the test never released Inventory");
            return new InventoryGateway.Applied(false);
        });
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            schema.seedReplica(5, UUID.randomUUID().toString());
            InProcessTransaction owner = InProcessTransaction.over(schema).pool(2, 4).inventory(inventory).build();
            String metrics = "test.wait." + UUID.randomUUID().toString().substring(0, 8);
            ConnectionPool waiterPool = schema.newPool(metrics, 2, 4, null);
            Duration budget = Duration.ofMillis(3_500);
            InProcessTransaction waiter = InProcessTransaction.over(schema).pool(waiterPool)
                    .settings(CheckoutOrchestrator.Settings.defaults().withResponseBudget(budget)).build();
            String id = owner.start();
            owner.scan(id, "SKU-000001");
            int baseline = waiterPool.occupancy().active();

            CompletableFuture<?> complete = CompletableFuture.supplyAsync(() -> owner.complete(id));
            try {
                assertTrue(inside.await(30, TimeUnit.SECONDS), "Complete never reached Inventory");
                long checks = PerfMetrics.counterValue(DB_CHECKS);
                LatencyHistogram.Snapshot holdsBefore = PerfMetrics.timerSnapshot(metrics + ".pool.holdTime");
                long started = System.nanoTime();
                CompletableFuture<String> scan = CompletableFuture.supplyAsync(() -> {
                    try {
                        return "OK " + waiter.scan(id, "SKU-000002").itemCount();
                    } catch (CheckoutPendingFailure pending) {
                        return "PENDING";
                    } catch (TransactionFailure failure) {
                        return failure.code().name();
                    }
                });
                awaitWaiting(waiter);

                int sampled = 0;
                while (!scan.isDone()) {
                    int active = waiterPool.occupancy().active();
                    if (active != baseline) {
                        // A sample can land inside one of the bounded checks, which borrows a connection
                        // for one read. A connection held between checks would still be held 100 ms
                        // later, far inside the 1 s backstop; the hold-time assertions below bound every
                        // borrow.
                        Thread.sleep(100);
                        active = waiterPool.occupancy().active();
                    }
                    assertEquals(baseline, active, "no pooled connection is held between the waiter's checks");
                    assertRowLockable(schema, id);
                    sampled++;
                    Thread.sleep(150);
                }
                assertTrue(sampled >= 5, "the wait was sampled throughout: " + sampled);
                assertEquals("PENDING", scan.get(), "still pending at the bound: 500");
                long elapsed = System.nanoTime() - started;
                long allowed = elapsed / waiter.checkout.settings().waitBackstop().toNanos() + 1;
                long made = PerfMetrics.counterValue(DB_CHECKS) - checks;
                assertTrue(made >= 1 && made <= allowed, "checks " + made + ", at most " + allowed);
                LatencyHistogram.Snapshot holds = PerfMetrics.timerSnapshot(metrics + ".pool.holdTime");
                assertEquals(made + 1, holds.count() - holdsBefore.count(),
                        "one borrow per check, plus the Scan's one cold load that found the attempt pending");
                assertTrue(holds.maxNanos() < TimeUnit.MILLISECONDS.toNanos(500),
                        "each borrow lasted one read: " + LatencyHistogram.Snapshot.millis(holds.maxNanos()) + " ms");
            } finally {
                release.countDown();
            }
            complete.get(30, TimeUnit.SECONDS);
            assertEquals(1, schema.scans(id).size());
        }
    }

    @Test
    void aWaiterInTheOwningProcessMakesNoCheckBeforeTheSettlementSignal() throws Exception {
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        FakeInventoryGateway inventory = FakeInventoryGateway.applying().thenAnswer(call -> {
            inside.countDown();
            assertTrue(release.await(30, TimeUnit.SECONDS), "the test never released Inventory");
            return new InventoryGateway.Applied(false);
        });
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            schema.seedReplica(5, UUID.randomUUID().toString());
            InProcessTransaction service = InProcessTransaction.over(schema).pool(2, 4).inventory(inventory).build();
            String id = service.start();
            service.scan(id, "SKU-000001");
            int baseline = service.pool.occupancy().active();

            CompletableFuture<?> complete = CompletableFuture.supplyAsync(() -> service.complete(id));
            CompletableFuture<String> scan;
            long checks;
            try {
                assertTrue(inside.await(30, TimeUnit.SECONDS));
                checks = PerfMetrics.counterValue(DB_CHECKS);
                scan = CompletableFuture.supplyAsync(() -> {
                    try {
                        return "OK " + service.scan(id, "SKU-000002").itemCount();
                    } catch (TransactionFailure failure) {
                        return failure.code().name();
                    }
                });
                awaitWaiting(service);
                for (int i = 0; i < 12; i++) {
                    assertEquals(baseline, service.pool.occupancy().active(), "nothing is held during the wait");
                    assertRowLockable(schema, id);
                    Thread.sleep(200);
                }
                assertEquals(checks, PerfMetrics.counterValue(DB_CHECKS),
                        "no check for 2.4 s: this process's live request will signal");
            } finally {
                release.countDown();
            }
            complete.get(30, TimeUnit.SECONDS);
            assertEquals("NOT_OPEN", scan.get(10, TimeUnit.SECONDS));
            assertEquals(checks, PerfMetrics.counterValue(DB_CHECKS), "woken by the signal, not a check");
        }
    }

    private static void assertRowLockable(TransactionTestSchema schema, String id) throws Exception {
        schema.admin().setAutoCommit(false);
        try (PreparedStatement lock = schema.admin().prepareStatement(
                "SELECT active_attempt_number FROM `transaction` WHERE transaction_id = ? FOR UPDATE NOWAIT")) {
            lock.setString(1, id);
            try (ResultSet row = lock.executeQuery()) {
                assertTrue(row.next(), "NOWAIT got the row lock at once, so nobody holds it");
                assertEquals(1, row.getInt(1), "the attempt is still pending");
            }
        } finally {
            schema.admin().rollback();
            schema.admin().setAutoCommit(true);
        }
    }

    private static void awaitWaiting(InProcessTransaction service) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (service.registry.waiting() == 0) {
            assertTrue(System.nanoTime() < deadline, "the Scan never started waiting");
            Thread.sleep(5);
        }
    }
}
