package rpc;

import contracts.inventory.v1.ApplyCheckoutRequest;
import contracts.inventory.v1.ApplyCheckoutResponse;
import contracts.inventory.v1.CheckoutAttemptKey;
import contracts.inventory.v1.CheckoutLine;
import contracts.inventory.v1.CheckoutOutcome;
import contracts.inventory.v1.GetCheckoutOutcomeRequest;
import contracts.inventory.v1.GetCheckoutOutcomeResponse;
import contracts.inventory.v1.InventoryServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import support.InProcessInventory;
import support.InventoryTestSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code ApplyCheckout} and {@code GetCheckoutOutcome} over the real adapters and a disposable schema
 * (contracts/grpc-internal-contracts.md §ApplyCheckout; FR-026, FR-028, FR-029). Every outcome, replay,
 * validation fault, and race is checked at the gRPC boundary, also across two Inventory instances.
 */
class InventoryCheckoutContractTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final String C = InventoryTestSchema.sku(3);

    @Test
    void appliedDeductsEveryLineOnceAndARepeatReplaysTheRecordedOutcome() throws Exception {
        try (InventoryTestSchema schema = seeded(5); InProcessInventory inventory = InProcessInventory.over(schema)) {
            var stub = stub(inventory);
            ApplyCheckoutRequest request = request(tx(), 1, line(B, 1), line(A, 2));
            ApplyCheckoutResponse applied = stub.applyCheckout(request);
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED, applied.getOutcome());
            assertFalse(applied.getReplayed());
            assertEquals("", applied.getRejectedSku());
            assertEquals(3, schema.stock(A));
            assertEquals(4, schema.stock(B));

            ApplyCheckoutResponse replay = stub.applyCheckout(request);
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED, replay.getOutcome());
            assertTrue(replay.getReplayed(), "the outcome recorded by the first request");
            assertEquals(3, schema.stock(A), "no stock change on a replay");
            assertEquals(4, schema.stock(B));
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED, outcome(stub, request.getAttempt()).getOutcome());
        }
    }

    @Test
    void insufficientStockNamesTheFirstShortSkuInSkuOrderAndDeductsNothing() throws Exception {
        try (InventoryTestSchema schema = seeded(1); InProcessInventory inventory = InProcessInventory.over(schema)) {
            var stub = stub(inventory);
            ApplyCheckoutRequest request = request(tx(), 1, line(C, 2), line(A, 1), line(B, 2));
            ApplyCheckoutResponse rejected = stub.applyCheckout(request);
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_INSUFFICIENT_STOCK, rejected.getOutcome());
            assertEquals(B, rejected.getRejectedSku(), "B is the first short SKU in SKU order, whatever the request order");
            assertEquals(1, schema.stock(A), "nothing deducted, not even the line that had stock");
            assertEquals(1, schema.stock(B));
            assertEquals(1, schema.stock(C));
            ApplyCheckoutResponse replay = stub.applyCheckout(request);
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_INSUFFICIENT_STOCK, replay.getOutcome());
            assertTrue(replay.getReplayed());
            GetCheckoutOutcomeResponse recorded = outcome(stub, request.getAttempt());
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_INSUFFICIENT_STOCK, recorded.getOutcome());
            assertEquals(B, recorded.getRejectedSku());
        }
    }

    @Test
    void anUnknownSkuIsAnInvalidRequestNamingTheFirstUnknownSkuAndDeductsNothing() throws Exception {
        try (InventoryTestSchema schema = seeded(5); InProcessInventory inventory = InProcessInventory.over(schema)) {
            var stub = stub(inventory);
            ApplyCheckoutRequest request = request(tx(), 1, line("ZZ-UNKNOWN", 1), line(A, 1), line("NO-SUCH-SKU", 1));
            ApplyCheckoutResponse rejected = stub.applyCheckout(request);
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_INVALID_REQUEST, rejected.getOutcome());
            assertEquals("NO-SUCH-SKU", rejected.getRejectedSku());
            assertEquals(5, schema.stock(A));
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_INVALID_REQUEST, outcome(stub, request.getAttempt()).getOutcome());
        }
    }

    @Test
    void theSameKeyWithOtherLinesIsFailedPreconditionAndChangesNothing() throws Exception {
        try (InventoryTestSchema schema = seeded(5); InProcessInventory inventory = InProcessInventory.over(schema)) {
            var stub = stub(inventory);
            String tx = tx();
            stub.applyCheckout(request(tx, 1, line(A, 1)));
            for (ApplyCheckoutRequest other : List.of(request(tx, 1, line(A, 2)), request(tx, 1, line(A, 1), line(B, 1)))) {
                assertStatus(Status.Code.FAILED_PRECONDITION, () -> stub.applyCheckout(other));
            }
            assertEquals(4, schema.stock(A));
            assertEquals(5, schema.stock(B));
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED, outcome(stub, key(tx, 1)).getOutcome());
        }
    }

    @Test
    void twentyConcurrentDuplicatesOfOneAttemptDeductOnce() throws Exception {
        try (InventoryTestSchema schema = seeded(50); InProcessInventory inventory = InProcessInventory.over(schema)) {
            ApplyCheckoutRequest request = request(tx(), 1, line(A, 3), line(B, 2));
            List<ApplyCheckoutResponse> responses = race(20, n -> stub(inventory).applyCheckout(request));
            assertTrue(responses.stream().allMatch(r -> r.getOutcome() == CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED));
            assertEquals(1, responses.stream().filter(r -> !r.getReplayed()).count(), "one request applied it");
            assertEquals(47, schema.stock(A), "deducted once");
            assertEquals(48, schema.stock(B));
            assertEquals(1, schema.operationCount("APPLIED"));
        }
    }

    @Test
    void twoAttemptsRacingForTheLastUnitGiveOneAppliedAndOneInsufficientStock() throws Exception {
        try (InventoryTestSchema schema = seeded(1); InProcessInventory inventory = InProcessInventory.over(schema)) {
            for (int round = 0; round < 5; round++) {
                schema.setStock(A, 1);
                List<ApplyCheckoutRequest> requests = List.of(request(tx(), 1, line(A, 1)), request(tx(), 1, line(A, 1)));
                List<ApplyCheckoutResponse> responses = race(2, n -> stub(inventory).applyCheckout(requests.get(n)));
                assertLastUnitSoldOnce(schema, responses, "round " + round);
            }
        }
    }

    @Test
    void contentionNeverYieldsInsufficientStockWhenStockSuffices() throws Exception {
        try (InventoryTestSchema schema = seeded(30); InProcessInventory inventory = InProcessInventory.over(schema)) {
            // Overlapping baskets list B and C in either order; locking in SKU order serializes them without deadlock.
            List<ApplyCheckoutRequest> requests = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                requests.add(i % 2 == 0 ? request(tx(), 1, line(C, 1), line(B, 1))
                        : request(tx(), 1, line(B, 1), line(C, 1), line(A, 1)));
            }
            List<ApplyCheckoutResponse> responses = race(20, n -> stub(inventory).applyCheckout(requests.get(n)));
            assertTrue(responses.stream().allMatch(r -> r.getOutcome() == CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED),
                    "every request had stock, so every request applied");
            assertEquals(10, schema.stock(B));
            assertEquals(10, schema.stock(C));
            assertEquals(20, schema.stock(A));
        }
    }

    @Test
    void malformedRequestsAreInvalidArgumentAndRecordNothing() throws Exception {
        try (InventoryTestSchema schema = seeded(5); InProcessInventory inventory = InProcessInventory.over(schema)) {
            var stub = stub(inventory);
            List<ApplyCheckoutRequest> malformed = List.of(
                    ApplyCheckoutRequest.newBuilder().setAttempt(key(tx(), 1)).build(),
                    request(tx(), 1, line(A, 1), line(A, 1)),
                    request(tx(), 1, line(A, 0)),
                    request("t".repeat(41), 1, line(A, 1)),
                    request(tx(), 0, line(A, 1)),
                    request("", 1, line(A, 1)),
                    request(tx(), 1, line("", 1)),
                    request(tx(), 1, line("S".repeat(21), 1)),
                    ApplyCheckoutRequest.newBuilder().addLines(line(A, 1)).build());
            for (ApplyCheckoutRequest request : malformed) {
                assertStatus(Status.Code.INVALID_ARGUMENT, () -> stub.applyCheckout(request));
            }
            List<CheckoutLine> tooMany = new ArrayList<>();
            for (int n = 0; n <= 2000; n++) tooMany.add(line("X-" + n, 1));
            assertStatus(Status.Code.INVALID_ARGUMENT, () -> stub.applyCheckout(
                    ApplyCheckoutRequest.newBuilder().setAttempt(key(tx(), 1)).addAllLines(tooMany).build()));
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM checkout_operation"), "nothing is recorded");
            assertEquals(5, schema.stock(A));
            assertStatus(Status.Code.INVALID_ARGUMENT, () -> outcome(stub, key("", 1)));
        }
    }

    @Test
    void aSecondAppliedAttemptForOneTransactionIsFailedPrecondition() throws Exception {
        try (InventoryTestSchema schema = seeded(5); InProcessInventory inventory = InProcessInventory.over(schema)) {
            var stub = stub(inventory);
            String tx = tx();
            stub.applyCheckout(request(tx, 1, line(A, 1)));
            assertStatus(Status.Code.FAILED_PRECONDITION, () -> stub.applyCheckout(request(tx, 2, line(A, 1))));
            assertEquals(4, schema.stock(A), "one effective deduction per transaction (FR-029)");
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_NOT_PROCESSED, outcome(stub, key(tx, 2)).getOutcome());
        }
    }

    @Test
    void getCheckoutOutcomeReportsNotProcessedForUnknownKeysAndNeverChangesState() throws Exception {
        try (InventoryTestSchema schema = seeded(5); InProcessInventory inventory = InProcessInventory.over(schema)) {
            var stub = stub(inventory);
            GetCheckoutOutcomeResponse unknown = outcome(stub, key(tx(), 1));
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_NOT_PROCESSED, unknown.getOutcome());
            assertEquals("", unknown.getRejectedSku());
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM checkout_operation"), "a query records nothing");
            String tx = tx();
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_NOT_PROCESSED, outcome(stub, key(tx, 1)).getOutcome());
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED, stub.applyCheckout(request(tx, 1, line(A, 1))).getOutcome(),
                    "a queried attempt is still processed fresh");
            assertFalse(stub.applyCheckout(request(tx(), 1, line(A, 1))).getReplayed());
        }
    }

    @Test
    void twoInventoryInstancesOverOneSchemaApplyEachAttemptOnceAndNeverOversell() throws Exception {
        try (InventoryTestSchema schema = seeded(50);
             InProcessInventory one = InProcessInventory.overNewPool(schema, "test.inv.one." + suffix(), 4, 8);
             InProcessInventory two = InProcessInventory.overNewPool(schema, "test.inv.two." + suffix(), 4, 8)) {
            assertNotSame(one.catalog, two.catalog, "each instance has its own catalog cache and pool");
            ApplyCheckoutRequest duplicate = request(tx(), 1, line(A, 2));
            List<ApplyCheckoutResponse> duplicates = race(20,
                    n -> stub(n % 2 == 0 ? one : two).applyCheckout(duplicate));
            assertTrue(duplicates.stream().allMatch(r -> r.getOutcome() == CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED));
            assertEquals(1, duplicates.stream().filter(r -> !r.getReplayed()).count());
            assertEquals(48, schema.stock(A), "one deduction across both instances");

            for (int round = 0; round < 5; round++) {
                schema.setStock(B, 1);
                List<ApplyCheckoutRequest> requests = List.of(request(tx(), 1, line(B, 1)), request(tx(), 1, line(B, 1)));
                List<ApplyCheckoutResponse> responses = race(2, n -> stub(n == 0 ? one : two).applyCheckout(requests.get(n)));
                assertEquals(1, responses.stream().filter(r -> r.getOutcome() == CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED).count(),
                        "round " + round);
                assertEquals(1, responses.stream()
                        .filter(r -> r.getOutcome() == CheckoutOutcome.CHECKOUT_OUTCOME_INSUFFICIENT_STOCK).count());
                assertEquals(0, schema.stock(B), "never oversold across instances");
            }
        }
    }

    private static void assertLastUnitSoldOnce(InventoryTestSchema schema, List<ApplyCheckoutResponse> responses,
                                               String context) throws Exception {
        assertEquals(1, responses.stream().filter(r -> r.getOutcome() == CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED).count(),
                context);
        assertEquals(1, responses.stream()
                .filter(r -> r.getOutcome() == CheckoutOutcome.CHECKOUT_OUTCOME_INSUFFICIENT_STOCK).count(), context);
        assertEquals(0, schema.stock(A), context + ": final stock 0");
    }

    @FunctionalInterface
    private interface Call {
        ApplyCheckoutResponse run(int index) throws Exception;
    }

    /** Starts {@code count} calls together and returns their responses in index order. */
    private static List<ApplyCheckoutResponse> race(int count, Call call) throws Exception {
        CountDownLatch go = new CountDownLatch(1);
        List<Future<ApplyCheckoutResponse>> futures = new ArrayList<>();
        try (ExecutorService workers = Executors.newFixedThreadPool(count)) {
            for (int i = 0; i < count; i++) {
                int index = i;
                Callable<ApplyCheckoutResponse> task = () -> {
                    go.await();
                    return call.run(index);
                };
                futures.add(workers.submit(task));
            }
            go.countDown();
            List<ApplyCheckoutResponse> responses = new ArrayList<>();
            for (Future<ApplyCheckoutResponse> future : futures) responses.add(future.get(60, TimeUnit.SECONDS));
            return responses;
        }
    }

    private static void assertStatus(Status.Code expected, org.junit.jupiter.api.function.Executable call) {
        StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, call);
        assertEquals(expected, failure.getStatus().getCode(), String.valueOf(failure.getStatus().getDescription()));
    }

    private static GetCheckoutOutcomeResponse outcome(InventoryServiceGrpc.InventoryServiceBlockingStub stub,
                                                      CheckoutAttemptKey key) {
        return stub.getCheckoutOutcome(GetCheckoutOutcomeRequest.newBuilder().setAttempt(key).build());
    }

    private static ApplyCheckoutRequest request(String tx, int attempt, CheckoutLine... lines) {
        return ApplyCheckoutRequest.newBuilder().setAttempt(key(tx, attempt)).addAllLines(List.of(lines)).build();
    }

    private static CheckoutAttemptKey key(String tx, int attempt) {
        return CheckoutAttemptKey.newBuilder().setTransactionId(tx).setAttemptNumber(attempt).build();
    }

    private static CheckoutLine line(String sku, int quantity) {
        return CheckoutLine.newBuilder().setSku(sku).setQuantity(quantity).build();
    }

    private static InventoryServiceGrpc.InventoryServiceBlockingStub stub(InProcessInventory inventory) {
        return InventoryServiceGrpc.newBlockingStub(inventory.channel()).withDeadlineAfter(30, TimeUnit.SECONDS);
    }

    private static InventoryTestSchema seeded(int stock) throws Exception {
        InventoryTestSchema schema = InventoryTestSchema.create(InventoryTestSchema.Options.defaults().pool(4, 24));
        schema.seed(3, stock);
        return schema;
    }

    private static String tx() {
        return UUID.randomUUID().toString();
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
