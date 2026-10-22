package integration;

import org.junit.jupiter.api.Test;
import support.InProcessCheckout;
import support.InventoryTestSchema;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Preserves the former Basket assertions against the durable checkout boundary, in the per-schema
 * topology: Transaction over its own schema and Inventory over its own, with every Complete going
 * through the Saga and Inventory's real checkout path.
 */
class DurableBasketTest {
    private static final String SKU = InventoryTestSchema.sku(5);
    private static final String OTHER_SKU = InventoryTestSchema.sku(6);
    private static final BigDecimal PRICE = InventoryTestSchema.price(5);
    private static final BigDecimal OTHER_PRICE = InventoryTestSchema.price(6);

    @Test
    void completedBasketKeepsCapturedPriceAndRejectsFurtherScansOrCompletion() throws Exception {
        try (InProcessCheckout db = InProcessCheckout.start(10, 5)) {
            var checkout = db.checkout();
            String id = checkout.start(new TransactionOperations.StartCommand("station-1")).transactionId();
            checkout.scan(new TransactionOperations.ScanCommand(id, SKU));
            var second = checkout.scan(new TransactionOperations.ScanCommand(id, SKU));
            assertEquals(PRICE, second.unitPrice());
            assertEquals(2, second.itemCount());
            assertEquals(PRICE.add(PRICE), second.runningTotal());

            var open = checkout.get(id);
            assertEquals(TransactionOperations.TransactionStatus.OPEN, open.status());
            assertEquals(2, open.itemCount());
            var receipt = checkout.complete(id);
            assertEquals(2, receipt.itemCount());
            assertEquals(PRICE.add(PRICE), receipt.totalAmount());
            assertEquals(open.startedAt(), receipt.startedAt());
            assertEquals(2, receipt.lines().getFirst().quantity());
            assertEquals(PRICE, receipt.lines().getFirst().unitPrice());
            assertEquals(3, db.stock(SKU));
            assertEquals(TransactionOperations.TransactionStatus.COMPLETED, checkout.get(id).status());
            assertEquals(TransactionFailure.Code.NOT_OPEN, assertThrows(TransactionFailure.class,
                    () -> checkout.scan(new TransactionOperations.ScanCommand(id, SKU))).code());
            assertEquals(TransactionFailure.Code.NOT_OPEN, assertThrows(TransactionFailure.class,
                    () -> checkout.complete(id)).code());
            assertEquals(3, db.stock(SKU), "a rejected repeat changes no stock");
        }
    }

    @Test
    void failedCompletionKeepsTheSameOpenBasketAndAllowsAnotherScan() throws Exception {
        try (InProcessCheckout db = InProcessCheckout.start(10, 0)) {
            var checkout = db.checkout();
            String id = checkout.start(new TransactionOperations.StartCommand("station-2")).transactionId();
            checkout.scan(new TransactionOperations.ScanCommand(id, OTHER_SKU));
            assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK, assertThrows(TransactionFailure.class,
                    () -> checkout.complete(id)).code());
            assertEquals(TransactionOperations.TransactionStatus.OPEN, checkout.get(id).status());
            var second = checkout.scan(new TransactionOperations.ScanCommand(id, OTHER_SKU));
            assertEquals(2, second.itemCount());
            assertEquals(OTHER_PRICE.add(OTHER_PRICE), second.runningTotal());
            assertEquals(2, checkout.get(id).itemCount());
            assertEquals(0, db.stock(OTHER_SKU));
        }
    }

    @Test
    void concurrentScansCommitPositiveQuantitiesAndDistinctEvents() throws Exception {
        try (InProcessCheckout db = InProcessCheckout.start(10, 20);
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var checkout = db.checkout();
            String id = checkout.start(new TransactionOperations.StartCommand("station-3")).transactionId();
            int count = 20;
            CountDownLatch start = new CountDownLatch(1);
            Future<?>[] results = new Future<?>[count];
            for (int i = 0; i < count; i++) {
                results[i] = workers.submit(() -> {
                    start.await();
                    checkout.scan(new TransactionOperations.ScanCommand(id, SKU));
                    return null;
                });
            }
            start.countDown();
            for (Future<?> result : results) result.get(30, TimeUnit.SECONDS);
            assertEquals(count, db.transactionSchema.scans(id).size(), "one distinct event per accepted unit");
            assertEquals(count, checkout.get(id).itemCount());
            var receipt = checkout.complete(id);
            assertEquals(count, receipt.itemCount());
            assertEquals(count, receipt.lines().getFirst().quantity());
            assertEquals(PRICE.multiply(BigDecimal.valueOf(count)), receipt.totalAmount());
            assertEquals(0, db.stock(SKU));
        }
    }
}
