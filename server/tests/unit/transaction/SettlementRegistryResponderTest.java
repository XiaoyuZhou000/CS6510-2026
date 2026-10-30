package unit.transaction;

import database.CheckoutAttemptStore.AttemptKey;
import org.junit.jupiter.api.Test;
import transaction.SettlementRegistry;
import transaction.SettlementRegistry.Responder;
import transaction.SettlementRegistry.Settlement;
import transaction.TransactionLocks;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The responder claim of D7 (contracts/checkout-saga.md "Response budget and claiming the response"):
 * exactly one of the outcome handler and the request's budget end writes {@code client_outcome}.
 */
class SettlementRegistryResponderTest {
    private final TransactionLocks locks = new TransactionLocks(64);
    private final SettlementRegistry registry = new SettlementRegistry(locks);

    @Test
    void aRegisteredResponderStartsWaitingAndOwnsTheAttempt() {
        AttemptKey key = new AttemptKey("txn-register", 1);
        assertNull(registry.responder(key), "no responder before registration");
        assertFalse(registry.owned(key));
        CompletableFuture<Settlement> future;
        try (var held = locks.acquire(key.transactionId())) {
            future = registry.register(key);
        }
        assertEquals(Responder.WAITING, registry.responder(key));
        assertTrue(registry.owned(key), "a waiting responder owns the attempt");
        assertFalse(future.isDone());
        assertThrows(IllegalStateException.class, () -> registry.register(key), "registration needs the stripe");
    }

    @Test
    void exactlyOneOfTheHandlerClaimAndTheTimeoutClaimSucceedsUnderRaces() throws Exception {
        int races = 10_000;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            int handlerWins = 0;
            int timeoutWins = 0;
            for (int i = 0; i < races; i++) {
                AttemptKey key = new AttemptKey("txn-race-" + i, 1);
                try (var held = locks.acquire(key.transactionId())) {
                    registry.register(key);
                }
                CountDownLatch start = new CountDownLatch(1);
                Future<Boolean> handler = pool.submit(() -> {
                    start.await();
                    return registry.claimForHandler(key);
                });
                Future<Boolean> timeout = pool.submit(() -> {
                    start.await();
                    return registry.claimTimeout(key);
                });
                start.countDown();
                boolean handlerWon = handler.get(5, TimeUnit.SECONDS);
                boolean timeoutWon = timeout.get(5, TimeUnit.SECONDS);
                assertTrue(handlerWon ^ timeoutWon, "exactly one claim wins in race " + i);
                assertEquals(handlerWon ? Responder.ANSWERING : Responder.TIMED_OUT, registry.responder(key));
                assertFalse(registry.claimForHandler(key), "a decided responder cannot be claimed again");
                assertFalse(registry.claimTimeout(key), "a decided responder cannot time out again");
                if (handlerWon) handlerWins++; else timeoutWins++;
                try (var held = locks.acquire(key.transactionId())) {
                    registry.complete(key, Settlement.completed(Instant.EPOCH, handlerWon));
                }
            }
            assertEquals(races, handlerWins + timeoutWins);
            assertEquals(0, registry.waiting(), "every entry was dropped when completed");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void anAnsweringResponderReceivesTheSettlementEvenAfterItsBudgetExpired() throws Exception {
        AttemptKey key = new AttemptKey("txn-late", 2);
        CompletableFuture<Settlement> future;
        try (var held = locks.acquire(key.transactionId())) {
            future = registry.register(key);
        }
        assertTrue(registry.claimForHandler(key), "the handler claims before its commit");
        assertFalse(registry.claimTimeout(key), "the budget ends during the handler's commit, so the timeout claim fails");
        assertTrue(registry.owned(key), "an answering responder still owns the attempt");
        Instant completedAt = Instant.parse("2026-10-29T12:00:00.123Z");
        Thread settler = Thread.ofPlatform().start(() -> {
            try (var held = locks.acquire(key.transactionId())) {
                registry.complete(key, Settlement.completed(completedAt, true));
            }
        });
        Settlement settlement = future.get(5, TimeUnit.SECONDS);
        settler.join();
        assertEquals(SettlementRegistry.Kind.COMPLETED, settlement.kind());
        assertEquals(completedAt, settlement.completedAt(), "the request answers with the supplied completion time");
        assertTrue(settlement.outcomeRecorded(), "the handler's commit recorded the claimed outcome");
    }

    @Test
    void anEntryWithoutAResponderCanStillBeCompleted() throws Exception {
        AttemptKey key = new AttemptKey("txn-waiter", 1);
        CompletableFuture<Settlement> waiter;
        try (var held = locks.acquire(key.transactionId())) {
            waiter = registry.futureFor(key);
        }
        assertNull(registry.responder(key), "a waiter for another request's attempt is no responder");
        assertFalse(registry.claimForHandler(key), "nothing to claim without a responder");
        assertFalse(registry.owned(key));
        try (var held = locks.acquire(key.transactionId())) {
            registry.complete(key, Settlement.reopened(database.CheckoutAttemptStore.RejectionReason.INSUFFICIENT_STOCK,
                    "SKU-000001", false));
        }
        Settlement settlement = waiter.get(1, TimeUnit.SECONDS);
        assertEquals(SettlementRegistry.Kind.REOPENED, settlement.kind());
        assertEquals("SKU-000001", settlement.rejectedSku());
        // Completing an attempt nobody registered is a no-op.
        try (var held = locks.acquire("txn-nobody")) {
            registry.complete(new AttemptKey("txn-nobody", 1), Settlement.failed());
        }
    }

    @Test
    void releasingAWaitingResponderTimesItOutSoRecoveryNoLongerTreatsItAsOwned() {
        AttemptKey key = new AttemptKey("txn-release", 1);
        try (var held = locks.acquire(key.transactionId())) {
            registry.register(key);
        }
        registry.release(key);
        assertEquals(Responder.TIMED_OUT, registry.responder(key));
        assertFalse(registry.owned(key));
        assertFalse(registry.claimForHandler(key), "the handler cannot claim a released responder");
    }
}
