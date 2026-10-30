package integration;

import database.CheckoutAttemptStore.AttemptKey;
import database.InventoryStatusQueries;
import database.TransactionStatusQueries;
import org.junit.jupiter.api.Test;
import support.FakeInventoryReactor;
import support.FakeInventoryReactor.Reply;
import support.InProcessCheckout;
import support.InventoryTestSchema;
import support.TestSchema;
import transaction.CheckoutPendingFailure;
import transaction.CheckoutRecoveryWorker;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integrity faults are parked and alarmed, never settled on an assumed outcome (FR-038, US3 scenario 8; D6):
 * a reused attempt key with other lines is parked in Inventory as {@code PAYLOAD_MISMATCH}, and its attempt
 * stays PENDING and is reported as stuck after {@code CHECKOUT_STUCK_ALARM_SECONDS} (2 s here); a deduction for
 * a REJECTED attempt, a restoration of a COMPLETED transaction, and an outcome for an unknown attempt are parked
 * in Transaction as {@code INTEGRITY} or {@code UNKNOWN_ATTEMPT} with {@code last_error = INTEGRITY:<reason>}.
 * Other transactions keep flowing, and every parked row shows in the status queries.
 */
class CheckoutIntegrityAlarmTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final Duration STUCK_AFTER = Duration.ofSeconds(2);

    @Test
    void aMismatchedRequestIsParkedInInventoryAndItsAttemptStaysPendingUntilReportedStuck() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 10)) {
            TransactionOperations service = saga.checkout();
            String id = basket(service, A);
            // Inventory already recorded attempt 1 of this transaction with other lines.
            saga.inventorySchema.update("INSERT INTO checkout_operation (transaction_id, attempt_number, state, "
                    + "rejection_reason, rejected_sku, `lines`, outcome_event_id) VALUES (?, 1, 'REJECTED', "
                    + "'INSUFFICIENT_STOCK', ?, ?, UUID())", id, B, "[{\"sku\":\"" + B + "\",\"quantity\":9}]");
            CheckoutRecoveryWorker recovery = new CheckoutRecoveryWorker(saga.transaction.attempts, saga.transaction.dispatcher,
                    saga.transaction.cache, saga.transaction.registry, saga.transaction.schedule,
                    CheckoutRecoveryWorker.Settings.defaults().withStuckAfter(STUCK_AFTER), Clock.systemUTC());
            recovery.start();
            try {
                assertInstanceOf(CheckoutPendingFailure.class, assertThrows(RuntimeException.class, () -> service.complete(id)),
                        "nothing answers a parked request: 500 at the budget");
                awaitParked(saga.inventorySchema, 1);
                assertEquals("PAYLOAD_MISMATCH", saga.inventorySchema.queryString("SELECT reason FROM dead_letter_event"));

                String other = basket(service, A);
                assertEquals(1, service.complete(other).itemCount(), "other transactions' events keep flowing");

                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (!"STUCK".equals(saga.transactionSchema.attempts(id).get(0).lastError())) {
                    assertTrue(System.nanoTime() < deadline, "the republished attempt was never alarmed as stuck");
                    Thread.sleep(100);
                }
            } finally {
                recovery.close();
            }
            assertEquals("PENDING", saga.transactionSchema.attempts(id).get(0).state(),
                    "never settled on an assumed outcome");
            TransactionStatusQueries.Status status = new TransactionStatusQueries(saga.transaction.pool, Clock.systemUTC(),
                    STUCK_AFTER).read();
            assertEquals(1, status.stuckAttempts(), status.line());
            InventoryStatusQueries.Status inventory = new InventoryStatusQueries(saga.inventory.pool).read();
            assertEquals(1, inventory.parkedEvents(), "the republished request is the same parked row: " + inventory.line());
            assertTrue(saga.inventorySchema.queryLong("SELECT park_count FROM dead_letter_event") >= 1);
        }
    }

    @Test
    void contradictingOutcomesAreParkedInTransactionWithTheirReasonAndChangeNoState() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 1)) {
            TransactionOperations service = saga.checkout();
            FakeInventoryReactor forger = FakeInventoryReactor.deducting(saga.bus, saga.names);

            String rejected = basket(service, A);
            service.scan(new TransactionOperations.ScanCommand(rejected, A));
            assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK,
                    assertThrows(TransactionFailure.class, () -> service.complete(rejected)).code());
            forger.publish(new AttemptKey(rejected, 1), Reply.deduct());

            String completed = basket(service, B);
            assertEquals(1, service.complete(completed).itemCount());
            forger.publish(new AttemptKey(completed, 1), Reply.restored());

            String unknown = "tx-unknown-" + UUID.randomUUID().toString().substring(0, 8);
            forger.publish(new AttemptKey(unknown, 1), Reply.deduct());

            awaitParked(saga.transactionSchema, 3);
            assertEquals(1, count(saga.transactionSchema, "INTEGRITY", rejected));
            assertEquals(1, count(saga.transactionSchema, "INTEGRITY", completed));
            assertEquals(1, count(saga.transactionSchema, "UNKNOWN_ATTEMPT", unknown));
            assertEquals("INTEGRITY:deduction-after-reject", saga.transactionSchema.attempts(rejected).get(0).lastError());
            assertEquals("INTEGRITY:restored-after-completion", saga.transactionSchema.attempts(completed).get(0).lastError());
            assertEquals("REJECTED", saga.transactionSchema.attempts(rejected).get(0).state(), "no state change");
            assertEquals(TransactionOperations.TransactionStatus.OPEN, service.get(rejected).status());
            assertEquals(TransactionOperations.TransactionStatus.COMPLETED, service.get(completed).status(),
                    "a completed sale is never reversed");
            assertEquals(1, saga.stock(A), "nothing deducted for the rejected attempt");
            assertEquals(0, saga.stock(B));

            String later = basket(service, B);
            saga.inventorySchema.setStock(B, 1);
            assertEquals(1, service.complete(later).itemCount(), "other transactions keep flowing after parking");
            TransactionStatusQueries.Status status = new TransactionStatusQueries(saga.transaction.pool).read();
            assertEquals(3, status.parkedEvents(), status.line());
        }
    }

    private static long count(TestSchema schema, String reason, String transactionId) throws Exception {
        long parked = 0;
        try (var select = schema.admin().prepareStatement("SELECT record_bytes FROM dead_letter_event WHERE reason = ?")) {
            select.setString(1, reason);
            try (var rows = select.executeQuery()) {
                while (rows.next()) {
                    var envelope = selfcheckout.events.v1.EventEnvelope.parseFrom(rows.getBytes(1));
                    if (envelope.getRoutingKey().equals(transactionId)) parked++;
                }
            }
        }
        return parked;
    }

    private static void awaitParked(TestSchema schema, long rows) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (schema.parkedCount() < rows) {
            assertTrue(System.nanoTime() < deadline, "only " + schema.parkedCount() + " of " + rows + " records were parked");
            Thread.sleep(50);
        }
    }

    private static String basket(TransactionOperations service, String sku) {
        String id = service.start(new TransactionOperations.StartCommand("integrity")).transactionId();
        service.scan(new TransactionOperations.ScanCommand(id, sku));
        return id;
    }
}
