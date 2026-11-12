package integration;

import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.SoldLine;
import support.InProcessCheckout;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import transaction.CheckoutPendingFailure;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 007 US1 acceptance scenarios 1 to 5 and 7 (T022) on the real Transaction and Inventory over an
 * {@link support.InMemoryEventBus}, with no Notification Service: T3 records exactly one {@code SaleCompleted} per
 * completed sale, with the sale identity and SKU quantities, and the dispatcher publishes it once.
 */
class SaleCompletedEventTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final Duration WAIT = Duration.ofSeconds(30);

    /** Scenario 1: 3 x A + 1 x B gives one event with A = 3 and B = 1. */
    @Test
    void aCompletedBasketGivesOneEventWithItsSkuQuantities() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 50)) {
            TransactionOperations service = saga.checkout();
            String id = basket(service, "sale-station", A, A, B, A);
            service.complete(id);

            List<SaleCompleted> events = awaitSales(saga, 1);
            SaleCompleted sale = events.get(0);
            assertEquals(id, sale.getSaleId());
            assertEquals(id, sale.getTransactionId());
            assertEquals(1, sale.getAttemptNumber());
            assertEquals("sale-station", sale.getStationId());
            assertEquals(List.of(line(A, 3), line(B, 1)), sale.getLinesList());
            assertEquals(1, saleRows(saga, id));
            assertEquals(id, saleKeys(saga).get(0), "keyed by the transaction ID");
        }
    }

    /** Scenario 2: a rejection followed by a successful second attempt gives one event, for attempt 2. */
    @Test
    void aRejectionThenASuccessGivesOneEventForTheSecondAttempt() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 1)) {
            TransactionOperations service = saga.checkout();
            String id = basket(service, "retry-station", A, A);
            assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK,
                    assertThrows(TransactionFailure.class, () -> service.complete(id)).code());
            assertEquals(0, saleRows(saga, id), "a rejected attempt records no event");

            saga.inventorySchema.setStock(A, 2);
            service.complete(id);
            SaleCompleted sale = awaitSales(saga, 1).get(0);
            assertEquals(2, sale.getAttemptNumber());
            assertEquals(List.of(line(A, 2)), sale.getLinesList());
            assertEquals(1, saleRows(saga, id));
        }
    }

    /**
     * Scenario 3: a repeated Complete, a redelivered {@code StockDeducted}, and a Transaction restart give no
     * additional event.
     */
    @Test
    void repeatsRedeliveriesAndARestartGiveNoAdditionalEvent() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 50)) {
            TransactionOperations service = saga.checkout();
            String repeated = basket(service, "repeat-station", A);
            service.complete(repeated);
            assertEquals(TransactionFailure.Code.NOT_OPEN,
                    assertThrows(TransactionFailure.class, () -> service.complete(repeated)).code(), "a repeated Complete");

            String redelivered = basket(service, "redelivery-station", B);
            saga.bus.duplicateNextSends(2);
            service.complete(redelivered);
            awaitSales(saga, 2);
            awaitOutcomeEvents(saga, 3);

            saga.transaction.close();
            try (InProcessTransaction restarted = InProcessTransaction.over(saga.transactionSchema).pool(4, 24)
                    .bus(saga.bus, saga.names).build()) {
                restarted.service.get(repeated);
                Thread.sleep(1_500);
                assertEquals(2, sales(saga).size(), "no additional event after duplicates or the restart");
            }
            assertEquals(1, saleRows(saga, repeated));
            assertEquals(1, saleRows(saga, redelivered));
            assertEquals(2, saga.transactionSchema.queryLong(
                    "SELECT COUNT(*) FROM outbox_event WHERE event_type = 'SaleCompleted'"));
        }
    }

    /** Scenario 4: scans, a pending attempt, and a rejected attempt give no event. */
    @Test
    void scansPendingAndRejectedAttemptsGiveNoEvent() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 1)) {
            TransactionOperations service = saga.checkout();
            String scanned = basket(service, "scan-station", A);
            String rejected = basket(service, "reject-station", B, B);
            assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK,
                    assertThrows(TransactionFailure.class, () -> service.complete(rejected)).code());

            saga.bus.pauseBroker(true);
            String pending = basket(service, "pending-station", A);
            assertInstanceOf(CheckoutPendingFailure.class, assertThrows(RuntimeException.class, () -> service.complete(pending)));
            assertEquals("PENDING", saga.transactionSchema.attempts(pending).get(0).state());
            for (String id : List.of(scanned, rejected, pending)) assertEquals(0, saleRows(saga, id), id);
            assertTrue(sales(saga).isEmpty());
            saga.bus.pauseBroker(false);
        }
    }

    /** Scenario 5: a background completion after a 500 gives one event. */
    @Test
    void aBackgroundCompletionAfterA500GivesOneEvent() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 50)) {
            TransactionOperations service = saga.checkout();
            String id = basket(service, "background-station", A, B);
            saga.bus.pauseBroker(true);
            assertInstanceOf(CheckoutPendingFailure.class, assertThrows(RuntimeException.class, () -> service.complete(id)));
            saga.bus.pauseBroker(false);

            long deadline = System.nanoTime() + WAIT.toNanos();
            while (!saga.transactionSchema.attempts(id).get(0).state().equals("COMPLETED")) {
                assertTrue(System.nanoTime() < deadline, "the attempt never completed in the background");
                Thread.sleep(50);
            }
            assertEquals("PENDING_ERROR", saga.transactionSchema.attempts(id).get(0).clientOutcome(), "a background completion");
            SaleCompleted sale = awaitSales(saga, 1).get(0);
            assertEquals(id, sale.getSaleId());
            assertEquals(List.of(line(A, 1), line(B, 1)), sale.getLinesList());
            assertEquals(1, saleRows(saga, id));
        }
    }

    /** Scenario 7: the payload names no subscriber, recipient, or channel (FR-004). */
    @Test
    void thePayloadHasNoRecipientField() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 50)) {
            TransactionOperations service = saga.checkout();
            service.complete(basket(service, "fields-station", A));
            SaleCompleted sale = awaitSales(saga, 1).get(0);
            for (var field : sale.getAllFields().keySet()) {
                String name = field.getName().toLowerCase(java.util.Locale.ROOT);
                assertFalse(name.contains("subscri") || name.contains("recipient") || name.contains("channel"), name);
            }
            assertTrue(sale.getUnknownFields().asMap().isEmpty(), "no field beyond the contract");
        }
    }

    private static String basket(TransactionOperations service, String station, String... skus) {
        String id = service.start(new TransactionOperations.StartCommand(station)).transactionId();
        for (String sku : skus) service.scan(new TransactionOperations.ScanCommand(id, sku));
        return id;
    }

    private static SoldLine line(String sku, int quantity) {
        return SoldLine.newBuilder().setSku(sku).setQuantity(quantity).build();
    }

    private static long saleRows(InProcessCheckout saga, String id) throws Exception {
        return saga.transactionSchema.queryLong("SELECT COUNT(*) FROM outbox_event WHERE event_type = 'SaleCompleted' "
                + "AND aggregate_key = ?", "sale:" + id);
    }

    /** Every {@code SaleCompleted} on the bus's sale-completed topic, partition by partition. */
    private static List<SaleCompleted> sales(InProcessCheckout saga) throws Exception {
        List<SaleCompleted> sales = new ArrayList<>();
        for (byte[] value : saga.bus.values(saga.names.saleCompleted())) {
            EventEnvelope envelope = EventEnvelope.parseFrom(value);
            assertEquals("SaleCompleted", envelope.getEventType());
            sales.add(SaleCompleted.parseFrom(envelope.getPayload()));
        }
        return sales;
    }

    private static List<String> saleKeys(InProcessCheckout saga) {
        String prefix = saga.names.saleCompleted() + "/";
        return saga.bus.sendLog().stream().filter(entry -> entry.startsWith(prefix))
                .map(entry -> entry.substring(prefix.length())).toList();
    }

    /** Waits for {@code count} records on the sale topic, then a moment more so a duplicate would show. */
    private static List<SaleCompleted> awaitSales(InProcessCheckout saga, int count) throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (sales(saga).size() < count) {
            assertTrue(System.nanoTime() < deadline, "only " + sales(saga).size() + " of " + count + " SaleCompleted records");
            Thread.sleep(20);
        }
        Thread.sleep(300);
        List<SaleCompleted> sales = sales(saga);
        Map<String, Integer> perSale = new TreeMap<>();
        for (SaleCompleted sale : sales) perSale.merge(sale.getSaleId(), 1, Integer::sum);
        for (var entry : perSale.entrySet()) assertEquals(1, entry.getValue(), "one record for sale " + entry.getKey());
        assertEquals(count, sales.size());
        return sales;
    }

    private static void awaitOutcomeEvents(InProcessCheckout saga, int count) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (saga.bus.values(saga.names.checkoutOutcome()).size() < count) {
            assertTrue(System.nanoTime() < deadline, "the outcome events never arrived");
            Thread.sleep(20);
        }
    }
}
