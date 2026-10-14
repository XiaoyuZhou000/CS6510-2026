package integration;

import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.CheckoutDatabase;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Preserves the former Basket assertions against the durable checkout boundary. */
class DurableBasketTest {
    @Test
    void completedBasketKeepsCapturedPriceAndRejectsFurtherScansOrCompletion() throws Exception {
        try (CheckoutDatabase db = open(5)) {
            String id = db.checkout.start(new TransactionOperations.StartCommand("station-1")).transactionId();
            db.checkout.scan(new TransactionOperations.ScanCommand(id, CheckoutDatabase.SKU));
            var second = db.checkout.scan(new TransactionOperations.ScanCommand(id, CheckoutDatabase.SKU));
            assertEquals(new BigDecimal("2.50"), second.unitPrice());
            assertEquals(2, second.itemCount());
            assertEquals(new BigDecimal("5.00"), second.runningTotal());

            var open = db.checkout.get(id);
            assertEquals(TransactionOperations.TransactionStatus.OPEN, open.status());
            assertEquals(2, open.itemCount());
            var receipt = db.checkout.complete(id);
            assertEquals(2, receipt.itemCount());
            assertEquals(new BigDecimal("5.00"), receipt.totalAmount());
            assertEquals(open.startedAt(), receipt.startedAt());
            assertEquals(2, receipt.lines().getFirst().quantity());
            assertEquals(new BigDecimal("2.50"), receipt.lines().getFirst().unitPrice());
            assertEquals(3, db.stock());
            assertEquals(TransactionOperations.TransactionStatus.COMPLETED, db.checkout.get(id).status());
            assertEquals(TransactionFailure.Code.NOT_OPEN, assertThrows(TransactionFailure.class,
                    () -> db.checkout.scan(new TransactionOperations.ScanCommand(id, CheckoutDatabase.SKU))).code());
            assertEquals(TransactionFailure.Code.NOT_OPEN, assertThrows(TransactionFailure.class,
                    () -> db.checkout.complete(id)).code());
        }
    }

    @Test
    void failedCompletionKeepsTheSameOpenBasketAndAllowsAnotherScan() throws Exception {
        try (CheckoutDatabase db = open(0)) {
            String id = db.checkout.start(new TransactionOperations.StartCommand("station-2")).transactionId();
            db.checkout.scan(new TransactionOperations.ScanCommand(id, CheckoutDatabase.OTHER_SKU));
            assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK, assertThrows(TransactionFailure.class,
                    () -> db.checkout.complete(id)).code());
            assertEquals(TransactionOperations.TransactionStatus.OPEN, db.checkout.get(id).status());
            var second = db.checkout.scan(new TransactionOperations.ScanCommand(id, CheckoutDatabase.OTHER_SKU));
            assertEquals(2, second.itemCount());
            assertEquals(new BigDecimal("2.50"), second.runningTotal());
            assertEquals(2, db.checkout.get(id).itemCount());
        }
    }

    @Test
    void concurrentScansCommitPositiveQuantitiesAndOneContiguousPositionEach() throws Exception {
        try (CheckoutDatabase db = open(20);
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            String id = db.checkout.start(new TransactionOperations.StartCommand("station-3")).transactionId();
            int count = 20;
            CountDownLatch start = new CountDownLatch(1);
            Future<?>[] results = new Future<?>[count];
            for (int i = 0; i < count; i++) {
                results[i] = workers.submit(() -> {
                    start.await();
                    db.checkout.scan(new TransactionOperations.ScanCommand(id, CheckoutDatabase.SKU));
                    return null;
                });
            }
            start.countDown();
            for (Future<?> result : results) result.get(30, TimeUnit.SECONDS);
            assertEquals(count, db.position());
            assertEquals(count, db.checkout.get(id).itemCount());
            var receipt = db.checkout.complete(id);
            assertEquals(count, receipt.itemCount());
            assertEquals(count, receipt.lines().getFirst().quantity());
            assertEquals(new BigDecimal("50.00"), receipt.totalAmount());
            assertEquals(0, db.stock());
        }
    }

    private static CheckoutDatabase open(int stock) throws Exception {
        try { return new CheckoutDatabase(stock); }
        catch (SQLException unavailable) { throw new TestAbortedException("MySQL unavailable", unavailable); }
    }
}