package integration;

import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.FaultInjection;
import support.InventoryTestSchema;
import support.ServiceHttp;
import support.TransactionTestSchema;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The checkout Saga's crash matrix over events (006 contracts/checkout-saga.md "Crash, message, and broker
 * fault matrix"; US3 scenarios 1, 2, and 4; replaces CheckoutRecoveryFaultTest): a Transaction process and an
 * Inventory process over their own schemas, talking through the live broker, each fault a {@code halt} armed in
 * one process at one of the Saga's hooks, and the halted service restarted without it. Every attempt settles
 * without manual steps: COMPLETED exactly when Inventory applied it, stock changes once, no stock is restored,
 * the client never sees success for an unsettled attempt, and lookup shows the settled state.
 */
class CheckoutEventFaultTest {
    private static final int STOCK = 50;
    private static final Duration SETTLE = Duration.ofSeconds(90);

    /** A halt at {@code hook} in {@code service}; the Transaction halts drop the waiting request's connection. */
    private record Fault(String service, String hook) { }

    private static final List<Fault> FAULTS = List.of(
            new Fault("transaction", "checkout.afterBegin"),
            new Fault("transaction", "outbox.beforeSend"),
            new Fault("transaction", "outbox.afterConfirm"),
            new Fault("inventory", "outbox.beforeSend"),
            new Fault("inventory", "inventory.apply.afterCommit"),
            new Fault("inventory", "outbox.afterConfirm"),
            new Fault("inventory", "consumer.afterHandlerCommit"),
            new Fault("transaction", "consumer.afterHandlerCommit"),
            new Fault("transaction", "checkout.outcome.beforeCommit"),
            new Fault("transaction", "checkout.afterRecordCompletion"));

    @Test
    void everyHaltAtEachSagaStepSettlesExactlyOnceWithoutManualSteps() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(20, STOCK)) {
            ServiceHttp http = deployment.transactionHttp;
            Map<String, Integer> sold = new LinkedHashMap<>();
            int product = 0;
            for (Fault fault : FAULTS) {
                String label = fault.service() + " " + fault.hook();
                String sku = InventoryTestSchema.sku(++product);
                String spec = FaultInjection.haltAt(fault.hook());
                if (fault.service().equals("transaction")) deployment.startTransaction(spec);
                else deployment.startInventory(spec);

                String id;
                try {
                    id = http.start("fault-" + product);
                } catch (java.io.IOException dropped) {
                    throw new AssertionError(label + ": Transaction died before the checkout began" + deployment.logs(),
                            dropped);
                }
                assertEquals(200, http.scan(id, sku).statusCode());
                assertEquals(200, http.scan(id, sku).statusCode());
                Integer status = completeStatus(http, id);
                if (fault.service().equals("transaction")) {
                    assertNull(status, label + ": the halted process drops the waiting request" + deployment.logs());
                    assertEquals(137, deployment.transactionProcess().awaitExit(Duration.ofSeconds(30)), label + " halts");
                    deployment.startTransaction();
                } else {
                    assertNotNull(status, label + ": Transaction still answers");
                    if (status == 200) {
                        assertEquals("COMPLETED", deployment.transactionSchema.attempts(id).get(0).state(),
                                label + ": a 200 only for a durably completed attempt");
                    } else {
                        assertEquals(500, (int) status, label + ": 500 at the budget, never a cancellation");
                    }
                    assertEquals(137, deployment.inventoryProcess().awaitExit(Duration.ofSeconds(60)), label + " halts");
                    deployment.startInventory();
                }
                awaitSettled(deployment, id, label);
                assertCompletedOnce(deployment, http, id, sku, label);
                sold.put(sku, 2);
            }

            for (Map.Entry<String, Integer> entry : sold.entrySet()) {
                assertEquals(STOCK - entry.getValue(), deployment.inventorySchema.stock(entry.getKey()),
                        entry.getKey() + ": initial minus final stock equals the COMPLETED units");
            }
            for (int n = product + 1; n <= 20; n++) {
                assertEquals(STOCK, deployment.inventorySchema.stock(InventoryTestSchema.sku(n)), "untouched SKUs");
            }
            assertEquals(0, deployment.inventorySchema.operationCount("RESTORED"), "no fault restored stock");
            assertEquals(0, deployment.inventorySchema.operationCount("BLOCKED"));
            assertEquals(0, deployment.transactionSchema.pendingAttemptCount(), "every attempt settled");
            assertEquals(0, deployment.inventorySchema.queryLong("SELECT COUNT(*) FROM inventory WHERE stock_quantity < 0"));
            assertEquals(0, deployment.transactionSchema.parkedCount() + deployment.inventorySchema.parkedCount(),
                    "a crash is never a poison record");
        }
    }

    /** The Complete's status, or null when the process halted and dropped the connection. */
    private static Integer completeStatus(ServiceHttp http, String id) throws InterruptedException {
        try {
            HttpResponse<String> response = http.complete(id);
            return response.statusCode();
        } catch (IOException dropped) {
            return null;
        }
    }

    private static void assertCompletedOnce(CheckoutDeployment deployment, ServiceHttp http, String id, String sku,
                                            String label) throws Exception {
        List<TransactionTestSchema.AttemptRow> attempts = deployment.transactionSchema.attempts(id);
        assertEquals(1, attempts.size(), label + ": one attempt");
        assertEquals("COMPLETED", attempts.get(0).state(), label);
        assertEquals("COMPLETED", ServiceHttp.json(http.lookup(id)).get("status"), label + ": lookup shows COMPLETED");
        HttpResponse<String> repeated = http.complete(id);
        assertEquals(409, repeated.statusCode(), label + ": a repeated Complete");
        assertEquals("TRANSACTION_NOT_OPEN", ServiceHttp.json(repeated).get("error"));
        assertEquals(STOCK - 2, deployment.inventorySchema.stock(sku), label + ": stock changed exactly once");
        assertEquals(1, deployment.inventorySchema.queryLong(
                "SELECT COUNT(*) FROM checkout_operation WHERE transaction_id = ? AND state = 'APPLIED'", id),
                label + ": Inventory applied it once");
    }

    private static void awaitSettled(CheckoutDeployment deployment, String id, String label) throws Exception {
        long deadline = System.nanoTime() + SETTLE.toNanos();
        while (true) {
            List<TransactionTestSchema.AttemptRow> attempts = deployment.transactionSchema.attempts(id);
            if (!attempts.isEmpty() && attempts.stream().noneMatch(attempt -> attempt.state().equals("PENDING"))) return;
            if (System.nanoTime() > deadline) fail(label + ": the attempt never settled " + attempts + deployment.logs());
            TimeUnit.MILLISECONDS.sleep(100);
        }
    }
}
