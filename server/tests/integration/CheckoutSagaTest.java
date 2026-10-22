package integration;

import api.json.Json;
import contracts.inventory.v1.CheckoutAttemptKey;
import contracts.inventory.v1.CheckoutOutcome;
import contracts.inventory.v1.GetCheckoutOutcomeRequest;
import contracts.inventory.v1.InventoryServiceGrpc;
import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.ServiceHttp;
import support.TransactionTestSchema;

import java.math.BigDecimal;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The checkout Saga end to end (US1 scenarios 1, 2, 5, and 6): a Transaction process and an Inventory
 * process, each over its own disposable schema and user, talking only through gRPC. Every compensation a
 * normal Saga path can trigger is covered: insufficient stock and an unknown SKU (Constitution IX).
 */
class CheckoutSagaTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final String C = InventoryTestSchema.sku(3);

    @Test
    void aBasketOfTwoAAndOneBCompletesWithCapturedPricesAndExactDeductions() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(10, 50)) {
            ServiceHttp http = deployment.transactionHttp;
            String id = http.start("saga-station");
            assertEquals(200, http.scan(id, A).statusCode());
            assertEquals(200, http.scan(id, A).statusCode());
            assertEquals(200, http.scan(id, B).statusCode());

            HttpResponse<String> completed = http.complete(id);
            assertEquals(200, completed.statusCode(), completed.body() + deployment.logs());
            Map<String, Object> receipt = ServiceHttp.json(completed);
            assertEquals(id, receipt.get("transactionId"));
            assertEquals("saga-station", receipt.get("stationId"));
            assertEquals(3, ((Number) receipt.get("itemCount")).intValue());
            BigDecimal total = InventoryTestSchema.price(1).multiply(BigDecimal.TWO).add(InventoryTestSchema.price(2));
            assertEquals(0, total.compareTo(new BigDecimal(receipt.get("totalAmount").toString())));
            List<Object> lines = Json.getList(receipt, "lines");
            assertEquals(2, lines.size());
            Map<String, Object> first = Json.asObject(lines.get(0));
            assertEquals(A, first.get("sku"));
            assertEquals(2, ((Number) first.get("quantity")).intValue());
            assertEquals(0, InventoryTestSchema.price(1).compareTo(new BigDecimal(first.get("unitPrice").toString())));
            assertNotNull(receipt.get("completedAt"));

            assertEquals(48, deployment.inventorySchema.stock(A), "two units of A");
            assertEquals(49, deployment.inventorySchema.stock(B), "one unit of B");
            assertEquals(50, deployment.inventorySchema.stock(C));
            assertEquals("COMPLETED", ServiceHttp.json(http.lookup(id)).get("status"));
            assertEquals("COMPLETED", deployment.transactionSchema.attempts(id).get(0).state());
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED, outcome(deployment, id, 1));
        }
    }

    /** Migrates ScanCompletionConvergenceTest#failedCompletionLeavesOpenBasketForScanningAndRetry. */
    @Test
    void insufficientStockLeavesTheOpenBasketForScanningAndRetry() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(10, 1)) {
            ServiceHttp http = deployment.transactionHttp;
            String id = http.start("retry-station");
            assertEquals(200, http.scan(id, A).statusCode());
            assertEquals(200, http.scan(id, A).statusCode());

            HttpResponse<String> insufficient = http.complete(id);
            assertError(insufficient, 409, "INSUFFICIENT_STOCK");
            assertEquals("Insufficient stock for SKU: " + A, ServiceHttp.json(insufficient).get("message"));
            assertEquals(1, deployment.inventorySchema.stock(A), "nothing was deducted");
            assertEquals("OPEN", ServiceHttp.json(http.lookup(id)).get("status"));
            TransactionTestSchema.AttemptRow rejected = deployment.transactionSchema.attempts(id).get(0);
            assertEquals("REJECTED", rejected.state());
            assertEquals("INSUFFICIENT_STOCK", rejected.rejectionReason());
            assertEquals(A, rejected.rejectedSku());

            deployment.inventorySchema.setStock(A, 3);
            assertEquals(200, http.scan(id, A).statusCode(), "the reopened basket accepts another scan");
            HttpResponse<String> completed = http.complete(id);
            assertEquals(200, completed.statusCode(), completed.body());
            Map<String, Object> receipt = ServiceHttp.json(completed);
            assertEquals(3, ((Number) receipt.get("itemCount")).intValue());
            assertEquals(3, ((Number) Json.asObject(Json.getList(receipt, "lines").get(0)).get("quantity")).intValue());
            assertEquals(0, deployment.inventorySchema.stock(A));
            assertEquals(List.of(1, 2), deployment.transactionSchema.attempts(id).stream()
                    .map(TransactionTestSchema.AttemptRow::attemptNumber).toList());
        }
    }

    @Test
    void anUnknownTransactionIs404AndAnEmptyBasketIs409WithoutReachingInventory() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(10, 5)) {
            ServiceHttp http = deployment.transactionHttp;
            assertError(http.complete("no-such-transaction"), 404, "TRANSACTION_NOT_FOUND");
            String id = http.start("empty-station");
            assertError(http.complete(id), 409, "EMPTY_BASKET");
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_NOT_PROCESSED, outcome(deployment, id, 1),
                    "Inventory never heard of the attempt");
            assertEquals(0, deployment.transactionSchema.attempts(id).size());
            assertEquals("OPEN", ServiceHttp.json(http.lookup(id)).get("status"));
        }
    }

    @Test
    void aPriceChangeInInventoryAfterScanningLeavesTheReceiptAtTheCapturedPrice() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(10, 5, Map.of("CATALOG_REFRESH_SECONDS", "1"))) {
            ServiceHttp http = deployment.transactionHttp;
            String id = http.start("price-station");
            assertEquals(200, http.scan(id, A).statusCode());

            BigDecimal changed = new BigDecimal("9.99");
            deployment.inventorySchema.update("UPDATE catalog_item SET price = ? WHERE sku = ?", changed, A);
            String version = deployment.inventorySchema.newCatalogVersion();
            awaitReplica(deployment, version);

            assertEquals(0, InventoryTestSchema.price(1).compareTo(
                    new BigDecimal(ServiceHttp.json(http.scan(id, A)).get("unitPrice").toString())),
                    "a repeated SKU keeps the line's captured price");
            String fresh = http.start("price-station-2");
            assertEquals(0, changed.compareTo(new BigDecimal(ServiceHttp.json(http.scan(fresh, A)).get("unitPrice").toString())),
                    "a new basket captures the new price, so the replica did change");

            HttpResponse<String> completed = http.complete(id);
            assertEquals(200, completed.statusCode(), completed.body());
            Map<String, Object> receipt = ServiceHttp.json(completed);
            assertEquals(0, InventoryTestSchema.price(1).multiply(BigDecimal.TWO)
                    .compareTo(new BigDecimal(receipt.get("totalAmount").toString())), "the receipt uses the captured price");
            assertEquals(3, deployment.inventorySchema.stock(A));
        }
    }

    @Test
    void anInvalidRequestFromInventoryReopensTheBasketEndToEnd() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(10, 5)) {
            ServiceHttp http = deployment.transactionHttp;
            String id = http.start("invalid-station");
            assertEquals(200, http.scan(id, A).statusCode());
            assertEquals(200, http.scan(id, C).statusCode());
            // Inventory forgets C after the scan; its stock row goes with it (ON DELETE CASCADE).
            deployment.inventorySchema.update("DELETE FROM catalog_item WHERE sku = ?", C);

            assertError(http.complete(id), 500, "INTERNAL_ERROR");
            Map<String, Object> lookup = ServiceHttp.json(http.lookup(id));
            assertEquals("OPEN", lookup.get("status"), "the transaction reopened");
            assertEquals(2, ((Number) lookup.get("itemCount")).intValue(), "with its basket");
            TransactionTestSchema.AttemptRow attempt = deployment.transactionSchema.attempts(id).get(0);
            assertEquals("REJECTED", attempt.state());
            assertEquals("INVALID_REQUEST", attempt.rejectionReason());
            assertEquals(C, attempt.rejectedSku());
            assertNull(deployment.transactionSchema.transaction(id).orElseThrow().activeAttemptNumber());
            assertEquals(5, deployment.inventorySchema.stock(A), "no stock changed");
            assertEquals(5, deployment.inventorySchema.stock(B));
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_INVALID_REQUEST, outcome(deployment, id, 1));
        }
    }

    private static void awaitReplica(CheckoutDeployment deployment, String version) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!version.equals(deployment.transactionSchema.queryString(
                "SELECT catalog_version FROM product_replica_meta WHERE meta_key = 1"))) {
            if (System.nanoTime() > deadline) fail("The replica never reached version " + version + deployment.logs());
            Thread.sleep(100);
        }
        // The in-memory snapshot swaps right after the replica's commit.
        Thread.sleep(Duration.ofMillis(200));
    }

    private static CheckoutOutcome outcome(CheckoutDeployment deployment, String id, int attempt) {
        return InventoryServiceGrpc.newBlockingStub(deployment.inventoryChannel()).withDeadlineAfter(10, TimeUnit.SECONDS)
                .getCheckoutOutcome(GetCheckoutOutcomeRequest.newBuilder().setAttempt(CheckoutAttemptKey.newBuilder()
                        .setTransactionId(id).setAttemptNumber(attempt)).build()).getOutcome();
    }

    private static void assertError(HttpResponse<String> response, int status, String code) {
        assertEquals(status, response.statusCode(), response.body());
        Map<String, Object> body = ServiceHttp.json(response);
        assertEquals(Set.of("error", "message"), body.keySet());
        assertEquals(code, body.get("error"));
        assertNotNull(body.get("message"));
    }
}
