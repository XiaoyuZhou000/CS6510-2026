package integration;

import api.ApiErrorMapper;
import events.EnvelopeCodec;
import events.EventTypes;
import inventory.CheckoutStockService;
import org.junit.jupiter.api.Test;
import support.InProcessCheckout;
import support.InventoryTestSchema;
import support.TransactionTestSchema;
import transaction.ProductCatalogReplica;
import transaction.TransactionOperations;
import transaction.TransactionOperations.ReceiptView;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The choreographed checkout Saga end to end (US1 scenarios 1, 2, 5, and 6; 006 D7, D8): the real Transaction
 * and Inventory business classes, each over its own disposable schema, exchanging only {@code CheckoutRequested}
 * and its outcome event through an {@link support.InMemoryEventBus}. Every compensation a normal Saga path can
 * trigger is covered: insufficient stock and an unknown SKU (Constitution IX). Public status codes are those
 * the HTTP mapper gives the failures.
 */
class CheckoutSagaTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final String C = InventoryTestSchema.sku(3);

    @Test
    void aBasketOfTwoAAndOneBCompletesWithCapturedPricesAndExactDeductions() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(10, 50)) {
            TransactionOperations service = saga.checkout();
            String id = service.start(new TransactionOperations.StartCommand("saga-station")).transactionId();
            scan(saga, id, A);
            scan(saga, id, A);
            scan(saga, id, B);

            ReceiptView receipt = service.complete(id);
            assertEquals(id, receipt.transactionId());
            assertEquals("saga-station", receipt.stationId());
            assertEquals(3, receipt.itemCount());
            BigDecimal total = InventoryTestSchema.price(1).multiply(BigDecimal.TWO).add(InventoryTestSchema.price(2));
            assertEquals(0, total.compareTo(receipt.totalAmount()));
            assertEquals(2, receipt.lines().size());
            assertEquals(A, receipt.lines().get(0).sku());
            assertEquals(2, receipt.lines().get(0).quantity());
            assertEquals(0, InventoryTestSchema.price(1).compareTo(receipt.lines().get(0).unitPrice()));
            assertNotNull(receipt.completedAt());

            assertEquals(48, saga.stock(A), "two units of A");
            assertEquals(49, saga.stock(B), "one unit of B");
            assertEquals(50, saga.stock(C));
            assertEquals(TransactionOperations.TransactionStatus.COMPLETED, service.get(id).status());
            assertEquals("COMPLETED", saga.transactionSchema.attempts(id).get(0).state());
            assertEquals(CheckoutStockService.Outcome.APPLIED, saga.inventory.stock.outcome(id, 1).outcome());
            assertEquals(List.of("CheckoutRequested"), types(saga.bus.values(saga.names.checkout())));
            assertEquals(List.of("StockDeducted"), types(saga.bus.values(saga.names.checkoutOutcome())));
        }
    }

    /** Migrates ScanCompletionConvergenceTest#failedCompletionLeavesOpenBasketForScanningAndRetry. */
    @Test
    void insufficientStockLeavesTheOpenBasketForScanningAndRetry() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(10, 1)) {
            TransactionOperations service = saga.checkout();
            String id = service.start(new TransactionOperations.StartCommand("retry-station")).transactionId();
            scan(saga, id, A);
            scan(saga, id, A);

            RuntimeException insufficient = failure(() -> service.complete(id));
            assertError(insufficient, 409, "INSUFFICIENT_STOCK");
            assertEquals("Insufficient stock for SKU: " + A, ApiErrorMapper.map(insufficient).message());
            assertEquals(1, saga.stock(A), "nothing was deducted");
            assertEquals(TransactionOperations.TransactionStatus.OPEN, service.get(id).status());
            TransactionTestSchema.AttemptRow rejected = saga.transactionSchema.attempts(id).get(0);
            assertEquals("REJECTED", rejected.state());
            assertEquals("INSUFFICIENT_STOCK", rejected.rejectionReason());
            assertEquals(A, rejected.rejectedSku());

            saga.inventorySchema.setStock(A, 3);
            assertEquals(3, scan(saga, id, A).itemCount(), "the reopened basket accepts another scan");
            ReceiptView receipt = service.complete(id);
            assertEquals(3, receipt.itemCount());
            assertEquals(3, receipt.lines().get(0).quantity());
            assertEquals(0, saga.stock(A));
            assertEquals(List.of(1, 2), saga.transactionSchema.attempts(id).stream()
                    .map(TransactionTestSchema.AttemptRow::attemptNumber).toList());
            assertEquals(List.of("StockRejected", "StockDeducted"), types(saga.bus.values(saga.names.checkoutOutcome())),
                    "the compensation and the later success, each one outcome event");
        }
    }

    @Test
    void anUnknownTransactionIs404AndAnEmptyBasketIs409WithoutReachingInventory() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(10, 5)) {
            TransactionOperations service = saga.checkout();
            assertError(failure(() -> service.complete("no-such-transaction")), 404, "TRANSACTION_NOT_FOUND");
            String id = service.start(new TransactionOperations.StartCommand("empty-station")).transactionId();
            assertError(failure(() -> service.complete(id)), 409, "EMPTY_BASKET");
            assertEquals(CheckoutStockService.Outcome.NOT_PROCESSED, saga.inventory.stock.outcome(id, 1).outcome(),
                    "Inventory never heard of the attempt");
            assertEquals(0, saga.transactionSchema.attempts(id).size());
            assertEquals(0, saga.transactionSchema.queryLong("SELECT COUNT(*) FROM outbox_event"), "no event was recorded");
            assertTrue(saga.bus.values(saga.names.checkout()).isEmpty(), "and none was published");
            assertEquals(TransactionOperations.TransactionStatus.OPEN, service.get(id).status());
        }
    }

    @Test
    void aPriceChangeInInventoryAfterScanningLeavesTheReceiptAtTheCapturedPrice() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(10, 5)) {
            TransactionOperations service = saga.checkout();
            String id = service.start(new TransactionOperations.StartCommand("price-station")).transactionId();
            scan(saga, id, A);

            BigDecimal changed = new BigDecimal("9.99");
            saga.inventorySchema.update("UPDATE catalog_item SET price = ? WHERE sku = ?", changed, A);
            String version = saga.inventorySchema.newCatalogVersion();
            List<ProductCatalogReplica.Product> products = new ArrayList<>();
            for (int n = 1; n <= 10; n++) {
                products.add(new ProductCatalogReplica.Product(InventoryTestSchema.sku(n), "Item " + n,
                        n == 1 ? changed : InventoryTestSchema.price(n)));
            }
            // The replica takes Inventory's new version, as replication would deliver it.
            saga.transaction.replica.install(version, products);

            assertEquals(0, InventoryTestSchema.price(1).compareTo(scan(saga, id, A).unitPrice()),
                    "a repeated SKU keeps the line's captured price");
            String fresh = service.start(new TransactionOperations.StartCommand("price-station-2")).transactionId();
            assertEquals(0, changed.compareTo(scan(saga, fresh, A).unitPrice()),
                    "a new basket captures the new price, so the replica did change");

            ReceiptView receipt = service.complete(id);
            assertEquals(0, InventoryTestSchema.price(1).multiply(BigDecimal.TWO).compareTo(receipt.totalAmount()),
                    "the receipt uses the captured price");
            assertEquals(3, saga.stock(A));
        }
    }

    @Test
    void anInvalidRequestFromInventoryReopensTheBasketEndToEnd() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(10, 5)) {
            TransactionOperations service = saga.checkout();
            String id = service.start(new TransactionOperations.StartCommand("invalid-station")).transactionId();
            scan(saga, id, A);
            scan(saga, id, C);
            // Inventory forgets C after the scan, while Transaction's replica still has it; its stock row goes
            // with it (ON DELETE CASCADE).
            saga.inventorySchema.update("DELETE FROM catalog_item WHERE sku = ?", C);

            assertError(failure(() -> service.complete(id)), 500, "INTERNAL_ERROR");
            var lookup = service.get(id);
            assertEquals(TransactionOperations.TransactionStatus.OPEN, lookup.status(), "the transaction reopened");
            assertEquals(2, lookup.itemCount(), "with its basket");
            TransactionTestSchema.AttemptRow attempt = saga.transactionSchema.attempts(id).get(0);
            assertEquals("REJECTED", attempt.state());
            assertEquals("INVALID_REQUEST", attempt.rejectionReason());
            assertEquals(C, attempt.rejectedSku());
            assertNull(saga.transactionSchema.transaction(id).orElseThrow().activeAttemptNumber());
            assertEquals(5, saga.stock(A), "no stock changed");
            assertEquals(5, saga.stock(B));
            assertEquals(CheckoutStockService.Outcome.INVALID_REQUEST, saga.inventory.stock.outcome(id, 1).outcome());
        }
    }

    private static TransactionOperations.ScanView scan(InProcessCheckout saga, String id, String sku) {
        return saga.checkout().scan(new TransactionOperations.ScanCommand(id, sku));
    }

    private static List<String> types(List<byte[]> values) {
        List<String> types = new ArrayList<>();
        for (byte[] value : values) {
            var envelope = parse(value);
            types.add(envelope.getEventType());
            EventTypes type = EventTypes.byName(envelope.getEventType()).orElseThrow();
            assertEquals(type.producer(), envelope.getProducer(), "each event carries its owner as producer");
            EnvelopeCodec.decode(value, envelope.getRoutingKey());
        }
        return types;
    }

    private static selfcheckout.events.v1.EventEnvelope parse(byte[] value) {
        try {
            return selfcheckout.events.v1.EventEnvelope.parseFrom(value);
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static RuntimeException failure(Supplier<?> action) {
        return assertThrows(RuntimeException.class, action::get);
    }

    private static void assertError(RuntimeException failure, int status, String code) {
        ApiErrorMapper.ApiError error = ApiErrorMapper.map(failure);
        assertEquals(status, error.httpStatus(), String.valueOf(failure));
        assertEquals(code, error.error());
        assertNotNull(error.message());
    }
}
