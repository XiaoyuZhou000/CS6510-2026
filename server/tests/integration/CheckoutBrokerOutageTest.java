package integration;

import api.ApiErrorMapper;
import events.EnvelopeCodec;
import org.junit.jupiter.api.Test;
import support.InProcessCheckout;
import support.InventoryTestSchema;
import support.TransactionTestSchema;
import transaction.CheckoutPendingFailure;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checkout while the broker is down or refuses sends (US3 scenarios 5-7; FR-015, FR-037, FR-042, FR-044), over
 * an {@link support.InMemoryEventBus} between the real Transaction and Inventory. Complete answers 500 within
 * the client's 10 s and leaves the attempt PENDING with {@code client_outcome = PENDING_ERROR}; once the broker
 * is back the attempt settles from its outbox rows, COMPLETED or, when the stock was taken meanwhile, OPEN; a
 * repeated Complete is 409 with no stock change; Scan keeps working; and each outbox row reaches the broker
 * exactly once after sends are accepted again.
 */
class CheckoutBrokerOutageTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);

    @Test
    void withTheBrokerDownCompleteAnswers500AndTheAttemptSettlesOnceTheBrokerReturns() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 10)) {
            TransactionOperations service = saga.checkout();
            String paid = basket(service, A);
            String starved = basket(service, B);
            saga.bus.pauseBroker(true);
            for (String id : List.of(paid, starved)) {
                long started = System.nanoTime();
                RuntimeException failure = assertThrows(RuntimeException.class, () -> service.complete(id));
                assertInstanceOf(CheckoutPendingFailure.class, failure);
                assertEquals(500, ApiErrorMapper.map(failure).httpStatus());
                assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(10), "within the client's 10 s timeout");
                TransactionTestSchema.AttemptRow attempt = saga.transactionSchema.attempts(id).get(0);
                assertEquals("PENDING", attempt.state(), "a 500 is never a cancellation");
                assertEquals("PENDING_ERROR", attempt.clientOutcome());
            }
            assertEquals(2, saga.transactionSchema.unpublishedOutboxCount(), "both requests wait in the outbox");
            assertEquals(2, scanOutcome(service, basket(service, A), A), "Scan works while the broker is down");
            saga.inventorySchema.setStock(B, 0);

            saga.bus.pauseBroker(false);
            awaitSettled(saga, paid);
            awaitSettled(saga, starved);
            assertEquals("COMPLETED", saga.transactionSchema.attempts(paid).get(0).state(), "settled from the outbox");
            assertEquals(TransactionOperations.TransactionStatus.COMPLETED, service.get(paid).status());
            assertEquals("PENDING_ERROR", saga.transactionSchema.attempts(paid).get(0).clientOutcome(),
                    "a background completion");
            assertEquals("REJECTED", saga.transactionSchema.attempts(starved).get(0).state(),
                    "the stock was taken meanwhile");
            assertEquals(TransactionOperations.TransactionStatus.OPEN, service.get(starved).status());
            assertEquals(9, saga.stock(A));
            assertEquals(TransactionFailure.Code.NOT_OPEN,
                    assertThrows(TransactionFailure.class, () -> service.complete(paid)).code());
            assertEquals(9, saga.stock(A), "a repeated Complete changes no stock");
        }
    }

    @Test
    void aScanOrCompleteForAPendingTransactionWaitsUpToTheBoundAndHoldsNoConnection() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 10)) {
            TransactionOperations service = saga.checkout();
            String id = basket(service, A);
            saga.bus.pauseBroker(true);
            CompletableFuture<String> owner = CompletableFuture.supplyAsync(() -> outcome(() -> service.complete(id)));
            awaitPending(saga, id);
            int baseline = saga.transaction.pool.occupancy().active();
            long started = System.nanoTime();
            CompletableFuture<String> scan = CompletableFuture.supplyAsync(() -> outcome(() ->
                    service.scan(new TransactionOperations.ScanCommand(id, B))));
            CompletableFuture<String> complete = CompletableFuture.supplyAsync(() -> outcome(() -> service.complete(id)));
            Thread.sleep(1_500);
            int active = saga.transaction.pool.occupancy().active();
            if (active > baseline) {
                Thread.sleep(100);
                active = saga.transaction.pool.occupancy().active();
            }
            // The baseline may itself catch a brief borrow of the dispatcher's sweep, so only growth counts.
            assertTrue(active <= baseline, "the waiters hold no connection: " + active + " active, baseline " + baseline);
            assertEquals("PENDING", owner.get(20, TimeUnit.SECONDS));
            assertEquals("PENDING", scan.get(20, TimeUnit.SECONDS), "still pending at the bound: 500 with no change");
            assertEquals("PENDING", complete.get(20, TimeUnit.SECONDS));
            assertTrue(System.nanoTime() - started >= TimeUnit.SECONDS.toNanos(7), "they waited for their budget");
            assertEquals(1, saga.transactionSchema.scans(id).size(), "no unit joined the frozen basket");
            assertEquals(1, saga.transactionSchema.attempts(id).size(), "no second attempt");

            saga.bus.pauseBroker(false);
            awaitSettled(saga, id);
            assertEquals("NOT_OPEN", outcome(() -> service.scan(new TransactionOperations.ScanCommand(id, B))),
                    "after the settlement: 409");
        }
    }

    @Test
    void whileTheBrokerRefusesSendsRowsStayUnpublishedAndEachIsSentExactlyOnceAfterwards() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 10)) {
            TransactionOperations service = saga.checkout();
            List<String> ids = List.of(basket(service, A), basket(service, B), basket(service, A));
            saga.bus.refuseSends(true);
            for (String id : ids) {
                assertInstanceOf(CheckoutPendingFailure.class, assertThrows(RuntimeException.class, () -> service.complete(id)),
                        "Complete behaves as with the broker down");
            }
            assertEquals(2, scanOutcome(service, basket(service, B), B), "Scan keeps succeeding");
            assertEquals(3, saga.transactionSchema.unpublishedOutboxCount(), "the requests stay unpublished");
            assertTrue(saga.bus.values(saga.names.checkout()).isEmpty(), "nothing reached the broker");

            saga.bus.refuseSends(false);
            for (String id : ids) awaitSettled(saga, id);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (saga.transactionSchema.unpublishedOutboxCount() + saga.inventorySchema.unpublishedOutboxCount() > 0) {
                assertTrue(System.nanoTime() < deadline, "every row is confirmed once sends are accepted");
                Thread.sleep(100);
            }
            assertEquals(Map.of(3L, 1L), copiesPerEvent(saga.bus.values(saga.names.checkout())),
                    "each CheckoutRequested reached the broker exactly once");
            assertEquals(Map.of(3L, 1L), copiesPerEvent(saga.bus.values(saga.names.checkoutOutcome())),
                    "and so did each outcome");
            for (String id : ids) assertEquals("COMPLETED", saga.transactionSchema.attempts(id).get(0).state());
        }
    }

    /** How many distinct event IDs were stored how many times: {events: copies} collapsed to {count: copies}. */
    private static Map<Long, Long> copiesPerEvent(List<byte[]> values) {
        Map<String, Long> copies = new HashMap<>();
        for (byte[] value : values) {
            String key = parseKey(value);
            copies.merge(EnvelopeCodec.decode(value, key).eventId(), 1L, Long::sum);
        }
        Map<Long, Long> result = new HashMap<>();
        result.put((long) copies.size(), copies.values().stream().mapToLong(Long::longValue).max().orElse(0));
        return result;
    }

    private static String parseKey(byte[] value) {
        try {
            return selfcheckout.events.v1.EventEnvelope.parseFrom(value).getRoutingKey();
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static String basket(TransactionOperations service, String sku) {
        String id = service.start(new TransactionOperations.StartCommand("outage")).transactionId();
        service.scan(new TransactionOperations.ScanCommand(id, sku));
        return id;
    }

    private static int scanOutcome(TransactionOperations service, String id, String sku) {
        return service.scan(new TransactionOperations.ScanCommand(id, sku)).itemCount();
    }

    private static String outcome(java.util.function.Supplier<?> action) {
        try {
            action.get();
            return "OK";
        } catch (TransactionFailure failure) {
            return failure.code().name();
        } catch (CheckoutPendingFailure pending) {
            return "PENDING";
        }
    }

    private static void awaitPending(InProcessCheckout saga, String id) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (saga.transactionSchema.transaction(id).orElseThrow().activeAttemptNumber() == null) {
            assertTrue(System.nanoTime() < deadline, "the checkout never became pending");
            Thread.sleep(5);
        }
    }

    private static void awaitSettled(InProcessCheckout saga, String id) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (saga.transactionSchema.attempts(id).get(0).state().equals("PENDING")) {
            assertTrue(System.nanoTime() < deadline, id + " never settled");
            Thread.sleep(50);
        }
    }
}
