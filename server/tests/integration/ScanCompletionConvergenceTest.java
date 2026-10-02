package integration;

import org.junit.jupiter.api.Test;
import support.CheckoutDatabase;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ScanCompletionConvergenceTest {
    // Direct layered-service fixture: no HTTP or analytics implementation participates.
    @Test
    void everyScanAcceptedBeforeCompletionReservationIsSoldExactlyOnce() throws Exception {
        final int initialStock = 1_000_000;
        try (CheckoutDatabase db = new CheckoutDatabase(initialStock)) {
            String tx = db.checkout.start(new TransactionOperations.StartCommand("race-station")).transactionId();
            db.checkout.scan(new TransactionOperations.ScanCommand(tx, CheckoutDatabase.SKU));

            // Exhausting the one-connection pool lets completion reserve its basket snapshot,
            // then pause before any durable work. A rejected scan proves reservation occurred.
            Connection blocker = db.pool.borrow();
            try (ExecutorService worker = Executors.newSingleThreadExecutor()) {
                Future<TransactionOperations.ReceiptView> completion = worker.submit(() -> db.checkout.complete(tx));
                int acceptedDuringRace = 0;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (true) {
                    try {
                        db.checkout.scan(new TransactionOperations.ScanCommand(tx, CheckoutDatabase.SKU));
                        acceptedDuringRace++;
                    } catch (TransactionFailure e) {
                        assertEquals(TransactionFailure.Code.NOT_OPEN, e.code());
                        break;
                    }
                    assertTrue(System.nanoTime() < deadline, "completion never reserved its snapshot");
                }

                db.pool.release(blocker);
                blocker = null;
                TransactionOperations.ReceiptView receipt = completion.get(5, TimeUnit.SECONDS);
                int accepted = 1 + acceptedDuringRace;
                assertEquals(accepted, receipt.itemCount());
                assertEquals(accepted, receipt.lines().getFirst().quantity());
                assertEquals(initialStock - accepted, db.stock());
                assertThrows(TransactionFailure.class,
                    () -> db.checkout.scan(new TransactionOperations.ScanCommand(tx, CheckoutDatabase.SKU)));
            } finally {
                if (blocker != null) db.pool.release(blocker);
            }
        }
    }

    @Test
    void failedCompletionReopensTheSameBasketForScanningAndRetry() throws Exception {
        try (CheckoutDatabase db = new CheckoutDatabase(1)) {
            String tx = db.checkout.start(new TransactionOperations.StartCommand("retry-station")).transactionId();
            db.checkout.scan(new TransactionOperations.ScanCommand(tx, CheckoutDatabase.SKU));
            db.checkout.scan(new TransactionOperations.ScanCommand(tx, CheckoutDatabase.SKU));

            TransactionFailure insufficient = assertThrows(TransactionFailure.class,
                () -> db.checkout.complete(tx));
            assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK, insufficient.code());
            assertEquals(1, db.stock(), "failed completion must roll back inventory");

            db.setStock(3);
            db.checkout.scan(new TransactionOperations.ScanCommand(tx, CheckoutDatabase.SKU));
            TransactionOperations.ReceiptView receipt = db.checkout.complete(tx);
            assertEquals(3, receipt.itemCount());
            assertEquals(3, receipt.lines().getFirst().quantity());
            assertEquals(0, db.stock());
        }
    }
}
