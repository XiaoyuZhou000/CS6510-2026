package rpc;

import contracts.inventory.v1.ApplyCheckoutRequest;
import contracts.inventory.v1.ApplyCheckoutResponse;
import contracts.inventory.v1.CheckoutAttemptKey;
import contracts.inventory.v1.CheckoutLine;
import contracts.inventory.v1.CheckoutOutcome;
import contracts.inventory.v1.InventoryServiceGrpc;
import contracts.inventory.v1.RestoreCheckoutRequest;
import contracts.inventory.v1.RestoreCheckoutResponse;
import contracts.inventory.v1.RestoreResult;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;
import support.FaultInjection;
import support.GrpcTestSupport;
import support.InProcessInventory;
import support.InventoryTestSchema;
import support.ServiceProcesses;
import support.TestSchema;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The reserved {@code RestoreCheckout} operation, exercised directly against Inventory (FR-032, SC-012;
 * US3 scenario 7; contracts/grpc-internal-contracts.md §RestoreCheckout).
 *
 * <p>This is a direct Inventory test. It is <b>not</b> an end-to-end Saga compensation test: no normal
 * checkout path calls {@code RestoreCheckout}, and its results are reported separately from the Saga's
 * fault and recovery results. Units are restored at most once and only if they were deducted, and an
 * {@code ApplyCheckout} that arrives after a restore request has no effect.</p>
 */
class RestoreCheckoutDirectTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);

    @Test
    void anAppliedAttemptIsRestoredExactlyOnceAndEveryRepeatIsAlreadyRestored() throws Exception {
        try (InventoryTestSchema schema = seeded(5); InProcessInventory inventory = InProcessInventory.over(schema)) {
            var stub = stub(inventory.channel());
            String tx = tx();
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED,
                    stub.applyCheckout(apply(tx, 1, line(A, 2), line(B, 1))).getOutcome());
            assertEquals(3, schema.stock(A));
            assertEquals(4, schema.stock(B));

            RestoreCheckoutResponse restored = stub.restoreCheckout(restore(tx, 1));
            assertEquals(RestoreResult.RESTORE_RESULT_RESTORED, restored.getResult());
            assertEquals(List.of(line(A, 2), line(B, 1)), restored.getRestoredLinesList(), "exactly the recorded lines");
            assertEquals(5, schema.stock(A));
            assertEquals(5, schema.stock(B));

            RestoreCheckoutResponse repeated = stub.restoreCheckout(restore(tx, 1));
            assertEquals(RestoreResult.RESTORE_RESULT_ALREADY_RESTORED, repeated.getResult());
            assertEquals(restored.getRestoredLinesList(), repeated.getRestoredLinesList());
            assertEquals(5, schema.stock(A), "no further change");

            List<RestoreCheckoutResponse> concurrent = race(10, () -> stub.restoreCheckout(restore(tx, 1)));
            assertTrue(concurrent.stream().allMatch(r -> r.getResult() == RestoreResult.RESTORE_RESULT_ALREADY_RESTORED),
                    "10 concurrent repeats");
            try (InProcessInventory another = InProcessInventory.overNewPool(schema, "test.restore." + suffix(), 2, 4)) {
                assertEquals(RestoreResult.RESTORE_RESULT_ALREADY_RESTORED,
                        stub(another.channel()).restoreCheckout(restore(tx, 1)).getResult(), "through another instance");
            }
            assertEquals(5, schema.stock(A));
            assertEquals(5, schema.stock(B));
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_STOCK_RESTORED,
                    stub.applyCheckout(apply(tx, 1, line(A, 2), line(B, 1))).getOutcome(),
                    "a restored attempt never deducts again");
            assertEquals(5, schema.stock(A));
            assertEquals(1, schema.operationCount("RESTORED"));
        }
    }

    @Test
    void tenConcurrentFirstRestoresRestoreOnce() throws Exception {
        try (InventoryTestSchema schema = seeded(5); InProcessInventory inventory = InProcessInventory.over(schema)) {
            var stub = stub(inventory.channel());
            String tx = tx();
            stub.applyCheckout(apply(tx, 1, line(A, 3)));
            assertEquals(2, schema.stock(A));
            List<RestoreCheckoutResponse> responses = race(10, () -> stub.restoreCheckout(restore(tx, 1)));
            assertEquals(1, responses.stream().filter(r -> r.getResult() == RestoreResult.RESTORE_RESULT_RESTORED).count(),
                    "one restore wins");
            assertEquals(9, responses.stream().filter(r -> r.getResult() == RestoreResult.RESTORE_RESULT_ALREADY_RESTORED)
                    .count(), "the others replay it");
            assertEquals(5, schema.stock(A), "the units came back once");
        }
    }

    @Test
    void aRejectedAttemptHasNothingToRestore() throws Exception {
        try (InventoryTestSchema schema = seeded(1); InProcessInventory inventory = InProcessInventory.over(schema)) {
            var stub = stub(inventory.channel());
            String tx = tx();
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_INSUFFICIENT_STOCK,
                    stub.applyCheckout(apply(tx, 1, line(A, 2))).getOutcome());
            RestoreCheckoutResponse nothing = stub.restoreCheckout(restore(tx, 1));
            assertEquals(RestoreResult.RESTORE_RESULT_NOTHING_TO_RESTORE, nothing.getResult());
            assertEquals(0, nothing.getRestoredLinesCount());
            assertEquals(1, schema.stock(A), "nothing was deducted, so nothing is added");
        }
    }

    @Test
    void anAbsentAttemptIsBlockedAndALaterDelayedApplyDeductsNothing() throws Exception {
        try (InventoryTestSchema schema = seeded(5); InProcessInventory inventory = InProcessInventory.over(schema)) {
            var stub = stub(inventory.channel());
            String tx = tx();
            RestoreCheckoutResponse blocked = stub.restoreCheckout(restore(tx, 1));
            assertEquals(RestoreResult.RESTORE_RESULT_NOTHING_TO_RESTORE, blocked.getResult(),
                    "the restore arrived before its stock request");
            assertEquals(1, schema.operationCount("BLOCKED"), "a tombstone blocks the attempt");
            ApplyCheckoutResponse late = stub.applyCheckout(apply(tx, 1, line(A, 2)));
            assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_STOCK_RESTORED, late.getOutcome(), "the delayed request has no effect");
            assertEquals(5, schema.stock(A), "no deduction");
            assertEquals(RestoreResult.RESTORE_RESULT_NOTHING_TO_RESTORE, stub.restoreCheckout(restore(tx, 1)).getResult(),
                    "a repeat stays a no-op");
            assertEquals(5, schema.stock(A));
        }
    }

    @Test
    void aRestoreInterruptedByAnInventoryCrashAfterItsCommitIsRetriedAndRestoresExactlyOnce() throws Exception {
        try (InventoryTestSchema schema = InventoryTestSchema.create(TestSchema.Options.defaults().withOwnUser())) {
            schema.seed(3, 5);
            int httpPort = GrpcTestSupport.freePort();
            int grpcPort = GrpcTestSupport.freePort();
            ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
            try {
                String tx = tx();
                try (ServiceProcesses.ServiceProcess halting = inventory(schema, httpPort, grpcPort,
                        FaultInjection.spec().halt("inventory.restore.afterCommit").build())) {
                    var stub = stub(channel).withWaitForReady();
                    assertEquals(CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED,
                            stub.applyCheckout(apply(tx, 1, line(A, 2), line(B, 1))).getOutcome());
                    assertEquals(3, schema.stock(A));
                    StatusRuntimeException lost = assertThrows(StatusRuntimeException.class,
                            () -> stub(channel).restoreCheckout(restore(tx, 1)), "Inventory halts before replying");
                    assertEquals(Status.Code.UNAVAILABLE, lost.getStatus().getCode());
                    assertEquals(137, halting.awaitExit(Duration.ofSeconds(30)));
                }
                assertEquals(5, schema.stock(A), "the restore committed before the crash");
                assertEquals(5, schema.stock(B));

                try (ServiceProcesses.ServiceProcess restarted = inventory(schema, httpPort, grpcPort, null)) {
                    var stub = stub(channel).withWaitForReady();
                    RestoreCheckoutResponse retried = stub.restoreCheckout(restore(tx, 1));
                    assertEquals(RestoreResult.RESTORE_RESULT_ALREADY_RESTORED, retried.getResult(),
                            "the retry after the restart finds it restored");
                    assertEquals(List.of(line(A, 2), line(B, 1)), retried.getRestoredLinesList());
                    assertEquals(RestoreResult.RESTORE_RESULT_ALREADY_RESTORED,
                            stub(channel).withWaitForReady().restoreCheckout(restore(tx, 1)).getResult());
                    assertTrue(restarted.isAlive());
                }
                assertEquals(5, schema.stock(A), "restored exactly once");
                assertEquals(5, schema.stock(B));
                assertEquals(1, schema.operationCount("RESTORED"));
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    private static ServiceProcesses.ServiceProcess inventory(InventoryTestSchema schema, int httpPort, int grpcPort,
                                                             String faults) throws Exception {
        ServiceProcesses.ServiceProcess process = ServiceProcesses.service("inventory").database(schema)
                .httpPort(httpPort).grpcPort(grpcPort).faults(faults).start();
        process.awaitHttp("/items", 200, Duration.ofSeconds(60));
        process.awaitPort(grpcPort, Duration.ofSeconds(30));
        return process;
    }

    @FunctionalInterface
    private interface Call {
        RestoreCheckoutResponse run();
    }

    private static List<RestoreCheckoutResponse> race(int count, Call call) throws Exception {
        ExecutorService threads = Executors.newFixedThreadPool(count);
        try {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<RestoreCheckoutResponse>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                futures.add(threads.submit(() -> {
                    go.await();
                    return call.run();
                }));
            }
            go.countDown();
            List<RestoreCheckoutResponse> responses = new ArrayList<>();
            for (Future<RestoreCheckoutResponse> future : futures) responses.add(future.get(60, TimeUnit.SECONDS));
            return responses;
        } finally {
            threads.shutdownNow();
        }
    }

    private static ApplyCheckoutRequest apply(String tx, int attempt, CheckoutLine... lines) {
        return ApplyCheckoutRequest.newBuilder().setAttempt(key(tx, attempt)).addAllLines(List.of(lines)).build();
    }

    private static RestoreCheckoutRequest restore(String tx, int attempt) {
        return RestoreCheckoutRequest.newBuilder().setAttempt(key(tx, attempt)).build();
    }

    private static CheckoutAttemptKey key(String tx, int attempt) {
        return CheckoutAttemptKey.newBuilder().setTransactionId(tx).setAttemptNumber(attempt).build();
    }

    private static CheckoutLine line(String sku, int quantity) {
        return CheckoutLine.newBuilder().setSku(sku).setQuantity(quantity).build();
    }

    private static InventoryServiceGrpc.InventoryServiceBlockingStub stub(ManagedChannel channel) {
        return InventoryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
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
