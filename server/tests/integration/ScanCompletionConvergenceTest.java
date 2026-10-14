package integration;

import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.CheckoutDatabase;
import transaction.TransactionFailure;
import transaction.TransactionOperations;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class ScanCompletionConvergenceTest {
    @Test
    void racingScanAndCompletionHaveOneSerialOutcome() throws Exception {
        try (CheckoutDatabase db = open(10); ExecutorService workers = Executors.newFixedThreadPool(2)) {
            String id = db.checkout.start(new TransactionOperations.StartCommand("race-station")).transactionId();
            db.checkout.scan(new TransactionOperations.ScanCommand(id,CheckoutDatabase.SKU));
            long before = db.position();
            Connection blocker = db.pool.borrow();
            blocker.setAutoCommit(false);
            try (PreparedStatement lock=blocker.prepareStatement(
                    "SELECT status FROM `transaction` WHERE transaction_id=? FOR UPDATE")) {
                lock.setString(1,id);
                try (var ignored=lock.executeQuery()) {
                    Future<Boolean> scan=workers.submit(() -> {
                        try { db.checkout.scan(new TransactionOperations.ScanCommand(id,CheckoutDatabase.SKU)); return true; }
                        catch (TransactionFailure e) {
                            assertEquals(TransactionFailure.Code.NOT_OPEN,e.code());
                            return false;
                        }
                    });
                    Future<TransactionOperations.ReceiptView> completion=workers.submit(() -> db.checkout.complete(id));
                    blocker.commit();
                    boolean accepted=scan.get(5,TimeUnit.SECONDS);
                    var receipt=completion.get(5,TimeUnit.SECONDS);
                    int count=accepted ? 2 : 1;
                    assertEquals(count,receipt.itemCount());
                    assertEquals(count,receipt.lines().getFirst().quantity());
                    assertEquals(10-count,db.stock());
                    assertEquals(before+(accepted ? 1 : 0),db.position());
                    assertThrows(TransactionFailure.class,
                        () -> db.checkout.scan(new TransactionOperations.ScanCommand(id,CheckoutDatabase.SKU)));
                }
            } finally {
                blocker.setAutoCommit(true);
                db.pool.release(blocker);
            }
        }
    }

    @Test
    void failedCompletionLeavesOpenBasketForScanningAndRetry() throws Exception {
        try (CheckoutDatabase db = open(1)) {
            String id=db.checkout.start(new TransactionOperations.StartCommand("retry-station")).transactionId();
            db.checkout.scan(new TransactionOperations.ScanCommand(id,CheckoutDatabase.SKU));
            db.checkout.scan(new TransactionOperations.ScanCommand(id,CheckoutDatabase.SKU));
            TransactionFailure insufficient=assertThrows(TransactionFailure.class,() -> db.checkout.complete(id));
            assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK,insufficient.code());
            assertEquals(1,db.stock());
            assertEquals(TransactionOperations.TransactionStatus.OPEN,db.checkout.get(id).status());
            db.setStock(3);
            db.checkout.scan(new TransactionOperations.ScanCommand(id,CheckoutDatabase.SKU));
            var receipt=db.checkout.complete(id);
            assertEquals(3,receipt.itemCount());
            assertEquals(3,receipt.lines().getFirst().quantity());
            assertEquals(0,db.stock());
        }
    }
    private static CheckoutDatabase open(int stock) throws Exception {
        try { return new CheckoutDatabase(stock); }
        catch (java.sql.SQLException unavailable) { throw new TestAbortedException("MySQL unavailable",unavailable); }
    }
}
