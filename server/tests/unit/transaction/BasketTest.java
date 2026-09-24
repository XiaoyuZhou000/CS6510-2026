package unit.transaction;

import org.junit.jupiter.api.Test;
import transaction.Basket;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class BasketTest {
    @Test
    void completionReservationSerializesBasketAndSuccessfulCompletionIsTerminal() {
        Basket basket = new Basket("tx-1", "station-1");
        assertNotNull(basket.addScanIfOpen("B", "Bread", new BigDecimal("2.50")));
        assertNotNull(basket.addScanIfOpen("B", "Bread changed", new BigDecimal("9.99")));

        Basket.CompletionSnapshot snapshot = basket.beginCompletion();
        assertEquals(Basket.Status.COMPLETING, basket.status());
        assertNull(basket.addScanIfOpen("A", "Apple", BigDecimal.ONE));
        assertNull(basket.beginCompletion());
        assertEquals(2, snapshot.itemCount());
        assertEquals(new BigDecimal("5.00"), snapshot.totalAmount());
        assertEquals(2, snapshot.lines().get("B").quantity());
        assertEquals(new BigDecimal("2.50"), snapshot.lines().get("B").unitPrice());

        basket.completionSucceeded();
        assertEquals(Basket.Status.COMPLETED, basket.status());
        assertNull(basket.addScanIfOpen("A", "Apple", BigDecimal.ONE));
    }

    @Test
    void failedCompletionReopensTheSameBasket() {
        Basket basket = new Basket("tx-2", "station-2");
        basket.addScanIfOpen("A", "Apple", new BigDecimal("1.25"));
        basket.beginCompletion();
        basket.completionFailed();
        assertEquals(Basket.Status.OPEN, basket.status());
        assertNotNull(basket.addScanIfOpen("A", "Apple", new BigDecimal("1.25")));
        assertEquals(2, basket.itemCount());
        assertEquals(new BigDecimal("2.50"), basket.runningTotal());
    }

    @Test
    void concurrentScansAreSerializedAndQuantitiesStayPositive() throws Exception {
        Basket basket = new Basket("tx-3", "station-3");
        int workers = 20;
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?>[] futures = new Future<?>[workers];
            for (int i = 0; i < workers; i++) {
                futures[i] = executor.submit(() -> {
                    start.await();
                    assertNotNull(basket.addScanIfOpen("A", "Apple", new BigDecimal("0.50")));
                    return null;
                });
            }
            start.countDown();
            for (Future<?> future : futures) future.get();
        }
        Basket.CompletionSnapshot snapshot = basket.beginCompletion();
        assertEquals(workers, snapshot.itemCount());
        assertEquals(workers, snapshot.lines().get("A").quantity());
        assertEquals(new BigDecimal("10.00"), snapshot.totalAmount());
    }
}
