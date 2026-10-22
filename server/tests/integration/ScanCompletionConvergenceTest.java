package integration;

import org.junit.jupiter.api.Test;
import support.FaultInjection;
import support.InProcessCheckout;
import support.InventoryTestSchema;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A racing Scan and Complete on one transaction have one serial outcome (FR-035; US3 scenario 6). The
 * legacy test held the transaction row's lock to make them overlap; here Inventory's first reply is
 * delayed past the deadline by its {@code inventory.apply.afterCommit} hook, so a Scan that loses the race
 * waits for the pending checkout to settle. The file's other legacy method moved to
 * {@code CheckoutSagaTest#insufficientStockLeavesTheOpenBasketForScanningAndRetry} (T059).
 */
class ScanCompletionConvergenceTest {
    private static final String SKU = InventoryTestSchema.sku(1);

    /** Migrates racingScanAndCompletionHaveOneSerialOutcome with every assertion. */
    @Test
    void racingScanAndCompletionHaveOneSerialOutcome() throws Exception {
        try (InProcessCheckout db = InProcessCheckout.start(5, 10);
             var delayed = FaultInjection.spec().delay("inventory.apply.afterCommit", 3000).armInThisJvm();
             ExecutorService workers = Executors.newFixedThreadPool(2)) {
            TransactionOperations checkout = db.checkout();
            String id = checkout.start(new TransactionOperations.StartCommand("race-station")).transactionId();
            checkout.scan(new TransactionOperations.ScanCommand(id, SKU));
            long before = db.transactionSchema.scans(id).size();

            CountDownLatch go = new CountDownLatch(1);
            Future<Boolean> scan = workers.submit(() -> {
                go.await();
                try {
                    checkout.scan(new TransactionOperations.ScanCommand(id, SKU));
                    return true;
                } catch (TransactionFailure e) {
                    assertEquals(TransactionFailure.Code.NOT_OPEN, e.code());
                    return false;
                }
            });
            Future<TransactionOperations.ReceiptView> completion = workers.submit(() -> {
                go.await();
                return checkout.complete(id);
            });
            go.countDown();
            boolean accepted = scan.get(20, TimeUnit.SECONDS);
            var receipt = completion.get(20, TimeUnit.SECONDS);
            int count = accepted ? 2 : 1;
            assertEquals(count, receipt.itemCount());
            assertEquals(count, receipt.lines().getFirst().quantity());
            assertEquals(10 - count, db.stock(SKU));
            assertEquals(before + (accepted ? 1 : 0), db.transactionSchema.scans(id).size());
            assertThrows(TransactionFailure.class, () -> checkout.scan(new TransactionOperations.ScanCommand(id, SKU)));
        }
    }
}
