package performance;

import api.ApiErrorMapper;
import database.CheckoutAttemptStore;
import database.ScanStore;
import database.StoreFailure;
import database.TransactionStore;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.FakeInventoryReactor;
import support.FaultingConnections;
import support.FaultingConnections.CommitFault;
import support.InProcessInventory;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.TransactionTestSchema;
import transaction.CheckoutPendingFailure;
import transaction.TransactionFailure;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An ambiguous Scan or Complete commit is never acknowledged, and the cache is reconciled from the
 * database before the next mutation, whichever way the commit went (R19 rule 1). An ambiguous T1 also
 * queues a reconcile entry, which the recovery worker republishes or drops, so the attempt is settled by
 * its outcome event (006 D7, FR-037); an ambiguous T3 is retried by the outcome handler under the lock and
 * answers from the durable completion.
 */
class AmbiguousCommitReconcileTest {

    @Test
    void aCommitThatFailsBeforeCommittingIsReconciledWithoutAGapOrADuplicate() throws Exception {
        reconcile(CommitFault.failBeforeCommit(), 1);
    }

    @Test
    void aCommitThatSucceedsButReportsAFailureIsReconciledWithoutAGapOrADuplicate() throws Exception {
        reconcile(CommitFault.failAfterCommit(), 2);
    }

    private static void reconcile(CommitFault fault, int durableAfterFailure) throws Exception {
        FaultingConnections faults = new FaultingConnections();
        CountingConnections counting = new CountingConnections();
        List<String> events = new CopyOnWriteArrayList<>();
        Probe probe = new Probe();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            schema.seedReplica(10, UUID.randomUUID().toString());
            InProcessTransaction service = InProcessTransaction.over(schema)
                    .decorator(connection -> counting.wrap(faults.wrap(connection)))
                    .wrapTransactions(store -> recordLoads(store, events))
                    .wrapScans(store -> probe.around(recordAppends(store, events)))
                    .build();
            String id = service.start();
            String other = service.start();
            service.scan(id, "SKU-000001");
            service.scan(other, "SKU-000002");

            probe.target = id;
            probe.service = service;
            faults.armForCurrentThread(fault);
            StoreFailure failure = assertThrows(StoreFailure.class, () -> service.scan(id, "SKU-000001"));
            assertTrue(failure.commitOutcomeUnknown());
            ApiErrorMapper.ApiError response = ApiErrorMapper.map(failure);
            assertEquals(500, response.httpStatus(), "the ambiguous Scan answers 500 and is not acknowledged");
            assertEquals("INTERNAL_ERROR", response.error());
            assertEquals(Boolean.FALSE, probe.cachedWhenTheWaiterGotTheLock.get(10, TimeUnit.SECONDS),
                    "the entry was invalidated before the transaction's lock was released");
            probe.target = null;
            assertEquals(durableAfterFailure, schema.scans(id).size());
            assertEquals(durableAfterFailure, schema.transaction(id).orElseThrow().itemCount());

            events.clear();
            try (CountingConnections.Scope scope = counting.scope()) {
                assertEquals(durableAfterFailure + 1, service.scan(id, "SKU-000001").itemCount());
                assertEquals(2, scope.counts().selects(), "exactly one cold load");
            }
            assertEquals(List.of("load " + id, "append " + id), events, "the reload precedes any mutation");
            List<TransactionTestSchema.ScanRow> rows = schema.scans(id);
            for (int i = 0; i < rows.size(); i++) {
                assertEquals(i + 1, rows.get(i).scanSequence(), "no duplicate sequence, no gap, no lost row");
            }
            assertEquals(durableAfterFailure + 1, rows.size());

            try (var held = service.locks.acquire(other)) {
                assertTrue(service.cache.contains(other), "other transactions' entries are unaffected");
            }
            try (CountingConnections.Scope scope = counting.scope()) {
                assertEquals(2, service.scan(other, "SKU-000002").itemCount());
                assertEquals(0, scope.counts().selects(), "and stay warm");
            }
        }
    }

    @Test
    void anAmbiguousT1CommitThatNeverHappenedIsReconciledAndTheNextCompleteBeginsAttemptOneAgain() throws Exception {
        ambiguousT1(CommitFault.failBeforeCommit(), false);
    }

    @Test
    void anAmbiguousT1CommitThatHappenedIsReconciledAndTheNextCompleteFindsThePendingAttempt() throws Exception {
        ambiguousT1(CommitFault.failAfterCommit(), true);
    }

    /**
     * The Complete answers 500 without signalling its request event, the entry is invalidated, a reconcile
     * entry is queued, and the next Complete reloads before anything else; no second PENDING attempt ever
     * exists.
     */
    private static void ambiguousT1(CommitFault fault, boolean durable) throws Exception {
        FaultingConnections faults = new FaultingConnections();
        List<String> events = new CopyOnWriteArrayList<>();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            schema.seedReplica(10, UUID.randomUUID().toString());
            InProcessTransaction service = InProcessTransaction.over(schema).decorator(faults::wrap)
                    .wrapTransactions(store -> recordLoads(store, events)).build();
            FakeInventoryReactor inventory = service.reactor().start();
            String id = service.start();
            service.scan(id, "SKU-000001");

            faults.armForCurrentThread(fault);
            StoreFailure failure = assertThrows(StoreFailure.class, () -> service.complete(id));
            assertTrue(failure.commitOutcomeUnknown());
            assertEquals(500, ApiErrorMapper.map(failure).httpStatus());
            assertEquals(0, inventory.requests(), "no request event is signalled after an ambiguous T1");
            try (var held = service.locks.acquire(id)) {
                assertFalse(service.cache.contains(id), "the entry was invalidated before the lock was released");
            }
            assertTrue(service.schedule.contains(new CheckoutAttemptStore.AttemptKey(id, 1)),
                    "a reconcile entry lets recovery republish or drop the attempt");
            assertEquals(durable ? 1 : 0, schema.pendingAttemptCount());
            assertEquals(durable ? 1 : 0, schema.queryLong("SELECT COUNT(*) FROM outbox_event WHERE aggregate_key = ?",
                    id + ":1"), "the request row exists exactly when the attempt does");

            events.clear();
            if (durable) {
                // The next Complete finds the committed attempt pending and waits; the worker's reconcile
                // entry republishes its recorded request, Inventory's outcome settles it, and the waiter
                // resumes from the settled state.
                CompletableFuture<String> next = CompletableFuture.supplyAsync(() -> {
                    try {
                        return "OK " + service.complete(id).itemCount();
                    } catch (TransactionFailure refused) {
                        return refused.code().name();
                    } catch (CheckoutPendingFailure pending) {
                        return "PENDING";
                    }
                });
                awaitWaiter(service);
                assertTrue(service.recovery.runOnce(Duration.ofSeconds(5)), "the reconcile entry is due at once");
                assertEquals("NOT_OPEN", next.get(10, TimeUnit.SECONDS), "settled COMPLETED: the waiter answers 409");
                assertEquals(List.of(1), inventory.calls().stream().map(call -> call.key().attemptNumber()).distinct()
                        .toList(), "only attempt 1 was ever requested, by the republish or the outbox sweep");
                var attempts = schema.attempts(id);
                assertEquals("COMPLETED", attempts.get(0).state());
                assertEquals("NONE", attempts.get(0).clientOutcome(), "recovery never sets client_outcome");
                assertEquals("COMPLETED", schema.transaction(id).orElseThrow().status());
                assertEquals(0, service.registry.waiting(), "no local future is left behind");
            } else {
                assertTrue(service.recovery.runOnce(Duration.ofSeconds(5)), "the reconcile entry is due at once");
                assertEquals(0, schema.attempts(id).size(), "no attempt: the reconcile entry is dropped");
                var row = schema.transaction(id).orElseThrow();
                assertEquals(0, row.attemptCount(), "and nothing was written");
                assertNull(row.activeAttemptNumber());
                assertEquals(0, inventory.requests());
                assertEquals(0, service.schedule.size());
                assertEquals(1, service.complete(id).itemCount(), "the next Complete begins attempt 1 again");
                assertEquals(List.of(1), inventory.calls().stream().map(call -> call.key().attemptNumber()).toList());
            }
            assertEquals("load " + id, events.get(0), "the next Complete reloaded before any mutation");
            assertEquals(1, schema.attempts(id).size(), "never a second attempt");
            assertTrue(schema.pendingAttemptCount() <= 1);
            inventory.close();
        }
    }

    private static void awaitWaiter(InProcessTransaction service) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (service.registry.waiting() == 0) {
            assertTrue(System.nanoTime() < deadline, "the next Complete never started waiting");
            Thread.sleep(5);
        }
    }

    @Test
    void anAmbiguousT3CommitAnswersFromTheDurableCompletionWithOneDeduction() throws Exception {
        for (CommitFault fault : List.of(CommitFault.failBeforeCommit(), CommitFault.failAfterCommit())) {
            FaultingConnections faults = new FaultingConnections();
            try (TransactionTestSchema schema = TransactionTestSchema.create();
                 InventoryTestSchema stock = InventoryTestSchema.create();
                 InProcessInventory inventory = InProcessInventory.over(stock)) {
                String version = stock.seed(10, 5);
                schema.seedReplica(10, version);
                AtomicBoolean armed = new AtomicBoolean();
                InProcessTransaction service = InProcessTransaction.over(schema).decorator(faults::wrap)
                        .bus(inventory.bus, inventory.names)
                        // T3 runs on the outcome consumer: its first commit there fails.
                        .wrapAttempts(store -> CompleteStatementBudgetTest.tap(store, "complete", () -> {
                            if (armed.compareAndSet(false, true)) faults.armForCurrentThread(fault);
                        }, () -> { }))
                        .build();
                String id = service.start();
                service.scan(id, "SKU-000001");

                var receipt = service.complete(id);
                assertEquals(1, faults.faultsFired(), fault.kind() + ": T3's first commit failed");
                assertEquals(completedAt(schema, id), receipt.completedAt(), fault.kind() + ": the durable completed_at");
                var attempts = schema.attempts(id);
                assertEquals(1, attempts.size());
                assertEquals("COMPLETED", attempts.get(0).state(), "one COMPLETED attempt");
                assertEquals("SUCCESS", attempts.get(0).clientOutcome());
                assertEquals(1, stock.queryLong("SELECT COUNT(*) FROM checkout_operation WHERE transaction_id = ?", id),
                        "T3 is retried locally; Inventory is not asked again");
                assertEquals(4, stock.stock("SKU-000001"), fault.kind() + ": one deduction");
            }
        }
    }

    private static Instant completedAt(TransactionTestSchema schema, String id) throws Exception {
        try (var select = schema.admin().prepareStatement("SELECT completed_at FROM `transaction` WHERE transaction_id = ?")) {
            select.setString(1, id);
            try (var row = select.executeQuery()) {
                assertTrue(row.next());
                return row.getTimestamp(1).toInstant();
            }
        }
    }

    private static TransactionStore recordLoads(TransactionStore store, List<String> events) {
        return new TransactionStore() {
            @Override
            public void insertOpen(String transactionId, String stationId, java.time.Instant startedAt) {
                store.insertOpen(transactionId, stationId, startedAt);
            }

            @Override
            public java.util.Optional<TransactionRecord> findById(String transactionId) {
                return store.findById(transactionId);
            }

            @Override
            public java.util.Optional<TransactionState> loadState(String transactionId) {
                events.add("load " + transactionId);
                return store.loadState(transactionId);
            }
        };
    }

    private static ScanStore recordAppends(ScanStore store, List<String> events) {
        return (id, expected, total, row) -> {
            events.add("append " + id);
            return store.appendScan(id, expected, total, row);
        };
    }

    /**
     * While the target transaction's append runs (and its commit fails), queues a second thread on the
     * same transaction's lock; once that thread gets the lock, it records whether the entry was cached.
     */
    private static final class Probe {
        volatile String target;
        volatile InProcessTransaction service;
        final CompletableFuture<Boolean> cachedWhenTheWaiterGotTheLock = new CompletableFuture<>();

        ScanStore around(ScanStore delegate) {
            return (id, expected, total, row) -> {
                if (id.equals(target)) {
                    Thread waiter = Thread.ofPlatform().start(() -> {
                        try (var held = service.locks.acquire(id)) {
                            cachedWhenTheWaiterGotTheLock.complete(service.cache.contains(id));
                        } catch (Throwable failure) {
                            cachedWhenTheWaiterGotTheLock.completeExceptionally(failure);
                        }
                    });
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (!service.locks.lockFor(id).hasQueuedThread(waiter)) {
                        if (System.nanoTime() > deadline) throw new AssertionError("the waiter never queued");
                        Thread.onSpinWait();
                    }
                }
                return delegate.appendScan(id, expected, total, row);
            };
        }
    }
}
