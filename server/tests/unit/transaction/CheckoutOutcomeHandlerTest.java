package unit.transaction;

import database.CheckoutAttemptStore.AttemptKey;
import database.CheckoutAttemptStore.ClientOutcome;
import database.CheckoutAttemptStore.RejectionReason;
import database.CheckoutAttemptStore.State;
import database.StoreFailure;
import database.TransactionStore.TransactionStatus;
import events.ParkReason;
import events.PermanentEventFailure;
import events.TransientEventFailure;
import org.junit.jupiter.api.Test;
import support.FakeInventoryReactor.Reply;
import transaction.CheckoutOrchestrator;
import transaction.CheckoutOutcomeHandler;
import transaction.SettlementRegistry;
import transaction.SettlementRegistry.Kind;
import transaction.SettlementRegistry.Settlement;
import transaction.TransactionLocks;
import transaction.TransactionStateCache;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static support.FakeInventoryReactor.outcomeEvent;
import static unit.transaction.CheckoutDb.line;

/**
 * The healthy rules of {@link CheckoutOutcomeHandler} (contracts/checkout-saga.md "Outcome handler rules",
 * D7): T3 or reopen of the PENDING active attempt under the stripe, {@code client_outcome} only for a
 * claimed responder, no-ops for settled attempts, and a state cache equal to the database afterwards.
 */
class CheckoutOutcomeHandlerTest {
    private static final Instant BASE = Instant.parse("2026-10-29T12:00:00Z");
    private static final String ID = "tx-outcome";

    private final CheckoutDb db = new CheckoutDb(BASE);
    private final TransactionLocks locks = new TransactionLocks(64);
    private final TransactionStateCache cache = new TransactionStateCache(locks, 100);
    private final SettlementRegistry registry = new SettlementRegistry(locks);
    private final Clock timing = new Clock();
    private final CheckoutOutcomeHandler handler = new CheckoutOutcomeHandler(db, cache, registry, timing);

    @Test
    void stockDeductedForThePendingActiveAttemptRunsT3AndCompletesTheClaimedFuture() throws Exception {
        AttemptKey key = pending(ID);
        CompletableFuture<Settlement> future = register(key);
        timing.advance(Duration.ofMillis(12));
        handler.handle(outcomeEvent(key, Reply.deduct()));

        assertEquals(TransactionStatus.COMPLETED, db.row(ID).status());
        assertEquals(timing.now(), db.row(ID).completedAt());
        assertNull(db.row(ID).active());
        assertEquals(State.COMPLETED, db.attempt(ID, 1).state());
        assertEquals(ClientOutcome.SUCCESS, db.attempt(ID, 1).clientOutcome(),
                "the claimed responder's outcome is written inside T3");
        assertEquals(0, db.clientOutcomeWrites, "no second commit records it");
        Settlement settlement = future.getNow(null);
        assertNotNull(settlement, "the settlement future completed");
        assertEquals(Kind.COMPLETED, settlement.kind());
        assertEquals(timing.now(), settlement.completedAt(), "the receipt's completed_at comes with the settlement");
        assertTrue(settlement.outcomeRecorded());
        assertNull(registry.responder(key), "the entry is dropped once its future completed");
        try (var held = locks.acquire(ID)) {
            assertFalse(cache.contains(ID), "a COMPLETED transaction leaves the cache");
        }
    }

    @Test
    void withoutAClaimedResponderTheSettlementRecordsNoClientOutcome() {
        AttemptKey key = pending(ID);
        handler.handle(outcomeEvent(key, Reply.deduct()));
        assertEquals(State.COMPLETED, db.attempt(ID, 1).state());
        assertEquals(ClientOutcome.NONE, db.attempt(ID, 1).clientOutcome(), "no live request of this process answered");

        String timedOut = "tx-timed-out";
        AttemptKey late = pending(timedOut);
        register(late);
        assertTrue(registry.claimTimeout(late), "the request's budget ended first");
        handler.handle(outcomeEvent(late, Reply.insufficient("A")));
        assertEquals(State.REJECTED, db.attempt(timedOut, 1).state());
        assertEquals(ClientOutcome.NONE, db.attempt(timedOut, 1).clientOutcome(),
                "the timed-out request writes PENDING_ERROR itself; the handler writes nothing");
    }

    @Test
    void rejectionsAndRefusalsReopenWithTheirReasonAndSku() throws Exception {
        record Case(Reply reply, RejectionReason reason, String sku) { }
        List<Case> cases = List.of(new Case(Reply.insufficient("B"), RejectionReason.INSUFFICIENT_STOCK, "B"),
                new Case(Reply.invalid("GONE"), RejectionReason.INVALID_REQUEST, "GONE"),
                new Case(Reply.refuse(), RejectionReason.STOCK_RESTORED, null));
        for (Case c : cases) {
            String id = "tx-" + c.reason();
            AttemptKey key = pending(id);
            CompletableFuture<Settlement> future = register(key);
            handler.handle(outcomeEvent(key, c.reply()));

            assertEquals(TransactionStatus.OPEN, db.row(id).status(), "the basket is open again");
            assertNull(db.row(id).active());
            assertEquals(2, db.row(id).itemCount(), "with the same basket");
            var attempt = db.attempt(id, 1);
            assertEquals(State.REJECTED, attempt.state());
            assertEquals(c.reason(), attempt.rejectionReason());
            assertEquals(c.sku(), attempt.rejectedSku());
            assertEquals(ClientOutcome.REJECTED, attempt.clientOutcome());
            Settlement settlement = future.getNow(null);
            assertEquals(Kind.REOPENED, settlement.kind());
            assertEquals(c.reason(), settlement.reason());
            assertEquals(c.sku(), settlement.rejectedSku());
            assertTrue(settlement.outcomeRecorded());
            assertCacheEqualsDatabase(id);
        }
    }

    @Test
    void anOutcomeForAnAlreadySettledAttemptWithTheSameResultIsANoOp() {
        AttemptKey completed = pending(ID);
        handler.handle(outcomeEvent(completed, Reply.deduct()));
        Instant completedAt = db.row(ID).completedAt();
        int writes = db.calls.size();
        timing.advance(Duration.ofSeconds(5));
        handler.handle(outcomeEvent(completed, Reply.deduct()));
        assertEquals(writes, db.calls.size(), "a redelivered deduction writes nothing");
        assertEquals(completedAt, db.row(ID).completedAt());

        String id = "tx-rejected-twice";
        AttemptKey rejected = pending(id);
        handler.handle(outcomeEvent(rejected, Reply.insufficient("A")));
        writes = db.calls.size();
        handler.handle(outcomeEvent(rejected, Reply.insufficient("A")));
        handler.handle(outcomeEvent(rejected, Reply.refuse()));
        assertEquals(writes, db.calls.size(), "a repeated or later rejection of a rejected attempt writes nothing");

        // A stale outcome for an earlier attempt never settles the newer active one.
        AttemptKey second = db.beginBehindTheCache(id);
        handler.handle(outcomeEvent(rejected, Reply.insufficient("A")));
        assertEquals(State.PENDING, db.attempt(id, second.attemptNumber()).state(), "attempt 2 stays pending");
        assertEquals(second.attemptNumber(), db.row(id).active());
    }

    @Test
    void restorationNotNeededChangesNothing() {
        AttemptKey key = pending(ID);
        int writes = db.calls.size();
        handler.handle(outcomeEvent(key, Reply.notNeeded()));
        assertEquals(writes, db.calls.size());
        assertEquals(State.PENDING, db.attempt(ID, 1).state());
        assertEquals(0, db.finds, "not even a read");
    }

    @Test
    void everyActionRunsUnderTheStripeAndLeavesTheCacheEqualToTheDatabase() {
        List<Boolean> held = new ArrayList<>();
        db.onSettle = id -> held.add(locks.heldByCurrentThread(id));
        AttemptKey reopened = pending("tx-reopen");
        AttemptKey completed = pending("tx-complete");
        handler.handle(outcomeEvent(reopened, Reply.insufficient("A")));
        handler.handle(outcomeEvent(completed, Reply.deduct()));
        assertEquals(List.of(true, true), held, "T3 and reopen ran while holding the transaction's stripe");
        assertCacheEqualsDatabase("tx-reopen");
        assertFalse(locks.heldByCurrentThread("tx-reopen"), "and released it");
    }

    @Test
    void aTransientWriteFailureIsRetriedUnderTheLockThenAnswers500AndLeavesTheEventToBeRedelivered() {
        AttemptKey key = pending(ID);
        db.completeFailures.add(new CheckoutDb.Failure(new StoreFailure("database down"), false));
        handler.handle(outcomeEvent(key, Reply.deduct()));
        assertEquals(List.of(Duration.ofMillis(50)), timing.sleeps, "one retry after 50 ms");
        assertEquals(State.COMPLETED, db.attempt(ID, 1).state());

        String exhausted = "tx-exhausted";
        AttemptKey failing = pending(exhausted);
        CompletableFuture<Settlement> future = register(failing);
        for (int i = 0; i < 4; i++) db.completeFailures.add(new CheckoutDb.Failure(new StoreFailure("database down"), false));
        assertThrows(TransientEventFailure.class, () -> handler.handle(outcomeEvent(failing, Reply.deduct())),
                "the offset stays uncommitted so the deduction returns");
        assertEquals(Kind.FAILED, future.getNow(null).kind(), "the claimed request answers 500");
        assertEquals(State.PENDING, db.attempt(exhausted, 1).state(), "T3 is retried on redelivery");
        handler.handle(outcomeEvent(failing, Reply.deduct()));
        assertEquals(State.COMPLETED, db.attempt(exhausted, 1).state(), "the redelivered event completes it");
    }

    @Test
    void anAmbiguousT3CommitIsAnsweredFromTheDurableCompletion() {
        AttemptKey key = pending(ID);
        CompletableFuture<Settlement> future = register(key);
        db.completeFailures.add(new CheckoutDb.Failure(new StoreFailure("commit lost", null, true), true));
        Instant firstAt = timing.now();
        handler.handle(outcomeEvent(key, Reply.deduct()));
        Settlement settlement = future.getNow(null);
        assertEquals(Kind.COMPLETED, settlement.kind());
        assertEquals(firstAt, settlement.completedAt(), "the durable completed_at of the first commit");
        assertTrue(settlement.outcomeRecorded(), "the first commit recorded the claimed outcome");
        assertEquals(ClientOutcome.SUCCESS, db.attempt(ID, 1).clientOutcome());
    }

    @Test
    void contradictionsAreParkedAsIntegrityFaultsAndUnknownAttemptsAsUnknown() {
        AttemptKey rejected = pending("tx-integrity");
        handler.handle(outcomeEvent(rejected, Reply.insufficient("A")));
        PermanentEventFailure deduction = assertThrows(PermanentEventFailure.class,
                () -> handler.handle(outcomeEvent(rejected, Reply.deduct())));
        assertEquals(ParkReason.INTEGRITY, deduction.reason());
        assertEquals("INTEGRITY:deduction-after-reject", db.attempt("tx-integrity", 1).lastError());
        assertEquals(State.REJECTED, db.attempt("tx-integrity", 1).state(), "no state change");

        AttemptKey completed = pending("tx-sold");
        handler.handle(outcomeEvent(completed, Reply.deduct()));
        PermanentEventFailure restored = assertThrows(PermanentEventFailure.class,
                () -> handler.handle(outcomeEvent(completed, Reply.restored())));
        assertEquals(ParkReason.INTEGRITY, restored.reason());
        assertEquals("INTEGRITY:restored-after-completion", db.attempt("tx-sold", 1).lastError());
        assertEquals(TransactionStatus.COMPLETED, db.row("tx-sold").status(), "a completed sale is never reversed");

        PermanentEventFailure unknown = assertThrows(PermanentEventFailure.class,
                () -> handler.handle(outcomeEvent(new AttemptKey("tx-never", 1), Reply.deduct())));
        assertEquals(ParkReason.UNKNOWN_ATTEMPT, unknown.reason());

        AttemptKey pendingRestore = pending("tx-restored");
        handler.handle(outcomeEvent(pendingRestore, Reply.restored()));
        assertEquals(RejectionReason.STOCK_RESTORED, db.attempt("tx-restored", 1).rejectionReason(),
                "StockRestored reopens a pending attempt");
    }

    /** A transaction with two units and a PENDING active attempt 1, cached as the request left it after T1. */
    private AttemptKey pending(String id) {
        db.open(id, line("A", "Apple", "1.25", 1), line("B", "Bread", "2.50", 1));
        AttemptKey key = db.beginBehindTheCache(id);
        try (var held = locks.acquire(id)) {
            cache.getOrLoad(id, db::loadStateAsCache);
        }
        return key;
    }

    private CompletableFuture<Settlement> register(AttemptKey key) {
        try (var held = locks.acquire(key.transactionId())) {
            return registry.register(key);
        }
    }

    private void assertCacheEqualsDatabase(String id) {
        try (var held = locks.acquire(id)) {
            assertEquals(db.loadStateAsCache(id).orElseThrow(), cache.peek(id), "the cache matches the database");
        }
    }

    /** Service time that moves only on sleeps and explicit advances. */
    private static final class Clock implements CheckoutOrchestrator.Timing {
        long nanos;
        final List<Duration> sleeps = new ArrayList<>();

        @Override
        public long nanoTime() {
            return nanos;
        }

        @Override
        public Instant now() {
            return BASE.plusNanos(nanos).truncatedTo(ChronoUnit.MILLIS);
        }

        @Override
        public void sleep(Duration duration) {
            sleeps.add(duration);
            nanos += duration.toNanos();
        }

        void advance(Duration duration) {
            nanos += duration.toNanos();
        }
    }
}
