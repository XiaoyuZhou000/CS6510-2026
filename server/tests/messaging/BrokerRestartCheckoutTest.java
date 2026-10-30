package messaging;

import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.ServiceHttp;
import support.TransactionTestSchema;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The broker restarts while checkouts are in flight (US3 scenario 3, SC-005; D1, D5): with 50 Completes in
 * progress, the native broker is restarted at once and, separately, stopped for 10 s and started again (on
 * Windows every stop is a forced kill). No confirmed event is lost, unconfirmed rows are re-sent from the
 * outboxes, every pending attempt settles without manual steps, and no stock effect applies twice. Needs
 * {@code KAFKA_TEST_BROKER_CONTROL=1}.
 */
class BrokerRestartCheckoutTest {
    private static final int CHECKOUTS = 50;
    private static final int PRODUCTS = 10;
    private static final int STOCK = 100;

    @Test
    void anImmediateRestartLosesNoCheckoutAndAppliesNoneTwice() throws Exception {
        KafkaTestSupport.requireBrokerControl();
        restartDuringCheckouts(Duration.ZERO);
    }

    @Test
    void aTenSecondOutageLosesNoCheckoutAndAppliesNoneTwice() throws Exception {
        KafkaTestSupport.requireBrokerControl();
        restartDuringCheckouts(Duration.ofSeconds(10));
    }

    private static void restartDuringCheckouts(Duration down) throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(PRODUCTS, STOCK)) {
            ServiceHttp http = deployment.transactionHttp;
            List<String> ids = new ArrayList<>();
            Map<String, Integer> expected = new HashMap<>();
            for (int i = 0; i < CHECKOUTS; i++) {
                String id = http.start("restart-" + i);
                String sku = InventoryTestSchema.sku(1 + i % PRODUCTS);
                assertEquals(200, http.scan(id, sku).statusCode());
                ids.add(id);
                expected.merge(sku, 1, Integer::sum);
            }
            List<Future<Integer>> responses = new ArrayList<>();
            ExecutorService stations = Executors.newFixedThreadPool(CHECKOUTS);
            try {
                for (String id : ids) responses.add(stations.submit(() -> http.complete(id).statusCode()));
                Thread.sleep(50);
                KafkaTestSupport.stopBroker();
                if (!down.isZero()) Thread.sleep(down.toMillis());
                KafkaTestSupport.startBroker();
                for (Future<Integer> response : responses) {
                    int status = response.get(60, TimeUnit.SECONDS);
                    assertTrue(status == 200 || status == 500, "settled, or handed over within the budget: " + status);
                }
            } finally {
                stations.shutdownNow();
            }

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(180);
            while (deployment.transactionSchema.pendingAttemptCount() > 0) {
                assertTrue(System.nanoTime() < deadline, "every pending attempt settles automatically" + deployment.logs());
                Thread.sleep(500);
            }
            for (String id : ids) {
                List<TransactionTestSchema.AttemptRow> attempts = deployment.transactionSchema.attempts(id);
                assertEquals(1, attempts.size(), id + ": one attempt");
                assertEquals("COMPLETED", attempts.get(0).state(), id + ": stock sufficed, so it completed");
            }
            for (int n = 1; n <= PRODUCTS; n++) {
                String sku = InventoryTestSchema.sku(n);
                assertEquals(STOCK - expected.getOrDefault(sku, 0), deployment.inventorySchema.stock(sku),
                        sku + ": each checkout deducted exactly once");
            }
            assertEquals(CHECKOUTS, deployment.inventorySchema.operationCount("APPLIED"));
            long deadlineOutbox = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (deployment.transactionSchema.unpublishedOutboxCount() + deployment.inventorySchema.unpublishedOutboxCount() > 0) {
                assertTrue(System.nanoTime() < deadlineOutbox, "every outbox row is confirmed after the restart");
                Thread.sleep(200);
            }
            assertEquals(0, deployment.transactionSchema.parkedCount() + deployment.inventorySchema.parkedCount());
        }
    }
}
