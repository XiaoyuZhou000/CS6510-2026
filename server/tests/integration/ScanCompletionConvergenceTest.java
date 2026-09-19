package integration;

import checkout.CheckoutException;
import checkout.CheckoutService;
import org.junit.jupiter.api.Test;
import support.CheckoutDatabase;

import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ScanCompletionConvergenceTest {
    @Test
    void everyScanAcceptedBeforeCompletionReservationIsSoldExactlyOnce() throws Exception {
        try (CheckoutDatabase db = new CheckoutDatabase(100)) {
            String tx = db.checkout.start("race-station").transactionId();
            db.checkout.scan(tx, CheckoutDatabase.SKU);

            // Exhausting the one-connection pool lets completion reserve its basket snapshot,
            // then pause before any durable work. A rejected scan proves reservation occurred.
            Connection blocker = db.pool.borrow();
            try (ExecutorService worker = Executors.newSingleThreadExecutor()) {
                Future<CheckoutService.ReceiptView> completion = worker.submit(() -> db.checkout.complete(tx));
                int acceptedDuringRace = 0;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (true) {
                    try {
                        db.checkout.scan(tx, CheckoutDatabase.SKU);
                        acceptedDuringRace++;
                    } catch (CheckoutException e) {
                        assertEquals(409, e.httpStatus);
                        break;
                    }
                    assertTrue(System.nanoTime() < deadline, "completion never reserved its snapshot");
                }

                db.pool.release(blocker);
                blocker = null;
                CheckoutService.ReceiptView receipt = completion.get(5, TimeUnit.SECONDS);
                int accepted = 1 + acceptedDuringRace;
                assertEquals(accepted, receipt.itemCount());
                assertEquals(accepted, receipt.lines().getFirst().quantity());
                assertEquals(100 - accepted, db.stock());
                assertThrows(CheckoutException.class,
                    () -> db.checkout.scan(tx, CheckoutDatabase.SKU));
            } finally {
                if (blocker != null) db.pool.release(blocker);
            }
        }
    }

    @Test
    void failedCompletionReopensTheSameBasketForScanningAndRetry() throws Exception {
        try (CheckoutDatabase db = new CheckoutDatabase(1)) {
            String tx = db.checkout.start("retry-station").transactionId();
            db.checkout.scan(tx, CheckoutDatabase.SKU);
            db.checkout.scan(tx, CheckoutDatabase.SKU);

            CheckoutException insufficient = assertThrows(CheckoutException.class,
                () -> db.checkout.complete(tx));
            assertEquals("INSUFFICIENT_STOCK", insufficient.errorCode);
            assertEquals(1, db.stock(), "failed completion must roll back inventory");

            db.setStock(3);
            db.checkout.scan(tx, CheckoutDatabase.SKU);
            CheckoutService.ReceiptView receipt = db.checkout.complete(tx);
            assertEquals(3, receipt.itemCount());
            assertEquals(3, receipt.lines().getFirst().quantity());
            assertEquals(0, db.stock());
        }
    }
}
