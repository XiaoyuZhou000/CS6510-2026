package integration;

import contracts.inventory.v1.CheckoutAttemptKey;
import contracts.inventory.v1.CheckoutOutcome;
import contracts.inventory.v1.GetCheckoutOutcomeRequest;
import contracts.inventory.v1.InventoryServiceGrpc;
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
 * The checkout Saga's crash and fault matrix (contracts/checkout-saga.md; US3 scenarios 1-4 and 8):
 * a Transaction process and an Inventory process over their own schemas, each fault armed in one
 * process through its {@code selfcheckout.faults} hook, and the halted service restarted without it.
 * Every attempt settles without manual steps: COMPLETED exactly when Inventory recorded APPLIED, OPEN
 * otherwise; stock changes at most once; no stock is ever restored; and per SKU, initial stock minus
 * final stock equals the units of COMPLETED transactions.
 */
class CheckoutRecoveryFaultTest {
    private static final int STOCK = 50;
    private static final Duration SETTLE = Duration.ofSeconds(60);

    @Test
    void everyInjectedFaultSettlesExactlyOnceWithoutManualSteps() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(20, STOCK)) {
            ServiceHttp http = deployment.transactionHttp;
            Map<String, Integer> initial = new LinkedHashMap<>();
            Map<String, Integer> sold = new LinkedHashMap<>();
            int product = 0;

            // US3 scenario 2: Transaction crashes at each step and resumes from its durable progress.
            for (String hook : List.of("checkout.afterBegin", "checkout.beforeApply", "checkout.afterApplyReply",
                    "checkout.afterRecordCompletion")) {
                String sku = InventoryTestSchema.sku(++product);
                deployment.startTransaction(FaultInjection.spec().halt(hook).build());
                String id = http.start("fault-" + hook);
                assertEquals(200, http.scan(id, sku).statusCode());
                assertEquals(200, http.scan(id, sku).statusCode());
                assertNoResponse(http, id, hook, deployment);
                assertEquals(137, deployment.transactionProcess().awaitExit(Duration.ofSeconds(30)), hook + " halts");
                deployment.startTransaction();
                awaitSettled(deployment, id, hook);
                assertCompletedOnce(deployment, http, id, sku, 2, hook);
                initial.put(sku, STOCK);
                sold.put(sku, 2);
            }

            // A Transaction crash after Inventory's definitive rejection reopens the basket after the restart.
            String short_ = InventoryTestSchema.sku(++product);
            deployment.inventorySchema.setStock(short_, 1);
            deployment.startTransaction(FaultInjection.spec().halt("checkout.afterApplyReply").build());
            String rejected = http.start("fault-rejected");
            assertEquals(200, http.scan(rejected, short_).statusCode());
            assertEquals(200, http.scan(rejected, short_).statusCode());
            assertNoResponse(http, rejected, "checkout.afterApplyReply (rejected)", deployment);
            deployment.transactionProcess().awaitExit(Duration.ofSeconds(30));
            deployment.startTransaction();
            awaitSettled(deployment, rejected, "rejected");
            assertEquals("REJECTED", deployment.transactionSchema.attempts(rejected).get(0).state());
            assertEquals("OPEN", ServiceHttp.json(http.lookup(rejected)).get("status"), "OPEN, with its basket");
            assertEquals(2, ((Number) ServiceHttp.json(http.lookup(rejected)).get("itemCount")).intValue());
            assertEquals(1, deployment.inventorySchema.stock(short_), "nothing deducted");
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_INSUFFICIENT_STOCK, outcome(deployment, rejected));
            initial.put(short_, 1);
            sold.put(short_, 0);

            // US3 scenarios 1 and 3: Inventory commits, then its reply is delayed past the deadline.
            String delayed = InventoryTestSchema.sku(++product);
            deployment.startInventory(FaultInjection.spec().delay("inventory.apply.afterCommit", 3000).build());
            String late = http.start("fault-delayed-reply");
            assertEquals(200, http.scan(late, delayed).statusCode());
            HttpResponse<String> retried = http.complete(late);
            assertEquals(200, retried.statusCode(), "the in-request retry replays the recorded outcome: "
                    + retried.body() + deployment.logs());
            assertCompletedOnce(deployment, http, late, delayed, 1, "delayed reply");
            initial.put(delayed, STOCK);
            sold.put(delayed, 1);

            // US3 scenarios 3 and 4: Inventory halts after its commit; the client gets 500 within the budget.
            for (boolean enough : List.of(true, false)) {
                String sku = InventoryTestSchema.sku(++product);
                if (!enough) deployment.inventorySchema.setStock(sku, 0);
                initial.put(sku, enough ? STOCK : 0);
                deployment.startInventory(FaultInjection.spec().halt("inventory.apply.afterCommit").build());
                String id = http.start("fault-inventory-halt-" + enough);
                assertEquals(200, http.scan(id, sku).statusCode());
                long started = System.nanoTime();
                HttpResponse<String> pending = http.complete(id);
                long elapsed = System.nanoTime() - started;
                assertEquals(500, pending.statusCode(), pending.body());
                assertEquals("INTERNAL_ERROR", ServiceHttp.json(pending).get("error"));
                assertTrue(elapsed < TimeUnit.SECONDS.toNanos(10), "within the client's 10 s timeout");
                assertEquals(137, deployment.inventoryProcess().awaitExit(Duration.ofSeconds(30)));
                TransactionTestSchema.AttemptRow handedOver = deployment.transactionSchema.attempts(id).get(0);
                assertEquals("PENDING", handedOver.state(), "a 500 is never a cancellation");
                assertEquals("PENDING_ERROR", handedOver.clientOutcome());
                assertEquals("OPEN", ServiceHttp.json(http.lookup(id)).get("status"));

                deployment.startInventory();
                awaitSettled(deployment, id, "inventory halt " + enough);
                if (enough) {
                    assertCompletedOnce(deployment, http, id, sku, 1, "inventory halt");
                    sold.put(sku, 1);
                } else {
                    assertEquals("REJECTED", deployment.transactionSchema.attempts(id).get(0).state());
                    assertEquals("OPEN", ServiceHttp.json(http.lookup(id)).get("status"), "OPEN with its basket");
                    assertEquals(0, deployment.inventorySchema.stock(sku));
                    sold.put(sku, 0);
                }
            }

            // US3 scenario 8: per-SKU reconciliation, and no restoration on any path.
            for (Map.Entry<String, Integer> entry : sold.entrySet()) {
                assertEquals(initial.get(entry.getKey()) - entry.getValue(), deployment.inventorySchema.stock(entry.getKey()),
                        entry.getKey() + ": initial minus final stock equals the COMPLETED units");
            }
            for (int n = product + 1; n <= 20; n++) {
                assertEquals(STOCK, deployment.inventorySchema.stock(InventoryTestSchema.sku(n)), "untouched SKUs");
            }
            assertEquals(0, deployment.inventorySchema.operationCount("RESTORED"), "no timeout restored stock");
            assertEquals(0, deployment.inventorySchema.operationCount("BLOCKED"));
            assertEquals(0, deployment.transactionSchema.pendingAttemptCount(), "every attempt settled");
            assertEquals(0, deployment.inventorySchema.queryLong("SELECT COUNT(*) FROM inventory WHERE stock_quantity < 0"),
                    "no stock is negative");
        }
    }

    /** The halted process drops the connection: the client sees no response at all. */
    private static void assertNoResponse(ServiceHttp http, String id, String hook, CheckoutDeployment deployment)
            throws InterruptedException {
        try {
            HttpResponse<String> response = http.complete(id);
            fail(hook + ": expected the process to halt, but it answered " + response.statusCode() + " "
                    + response.body() + deployment.logs());
        } catch (IOException dropped) {
            // Expected: Runtime.halt closes the socket without a response.
        }
    }

    private static void assertCompletedOnce(CheckoutDeployment deployment, ServiceHttp http, String id, String sku,
                                            int units, String label) throws Exception {
        List<TransactionTestSchema.AttemptRow> attempts = deployment.transactionSchema.attempts(id);
        assertEquals(1, attempts.size(), label + ": one attempt");
        assertEquals("COMPLETED", attempts.get(0).state(), label);
        assertEquals("COMPLETED", ServiceHttp.json(http.lookup(id)).get("status"), label + ": lookup shows COMPLETED");
        HttpResponse<String> repeated = http.complete(id);
        assertEquals(409, repeated.statusCode(), label + ": a repeated Complete");
        assertEquals("TRANSACTION_NOT_OPEN", ServiceHttp.json(repeated).get("error"));
        assertEquals(STOCK - units, deployment.inventorySchema.stock(sku), label + ": stock changed exactly once");
        assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED, outcome(deployment, id), label + ": Inventory APPLIED");
    }

    private static void awaitSettled(CheckoutDeployment deployment, String id, String label) throws Exception {
        long deadline = System.nanoTime() + SETTLE.toNanos();
        while (true) {
            List<TransactionTestSchema.AttemptRow> attempts = deployment.transactionSchema.attempts(id);
            if (!attempts.isEmpty() && attempts.stream().noneMatch(attempt -> attempt.state().equals("PENDING"))) return;
            if (System.nanoTime() > deadline) fail(label + ": the attempt never settled " + attempts + deployment.logs());
            Thread.sleep(100);
        }
    }

    private static CheckoutOutcome outcome(CheckoutDeployment deployment, String id) {
        // Waits for ready: the diagnostic channel reconnects after an Inventory restart.
        return InventoryServiceGrpc.newBlockingStub(deployment.inventoryChannel()).withWaitForReady()
                .withDeadlineAfter(20, TimeUnit.SECONDS).getCheckoutOutcome(GetCheckoutOutcomeRequest.newBuilder().setAttempt(CheckoutAttemptKey.newBuilder()
                        .setTransactionId(id).setAttemptNumber(1)).build()).getOutcome();
    }
}
