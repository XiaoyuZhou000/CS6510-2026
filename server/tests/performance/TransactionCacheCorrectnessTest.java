package performance;

import database.CheckoutAttemptStore;
import database.ScanStore;
import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.FakeInventoryGateway;
import support.InProcessTransaction;
import support.TransactionTestSchema;
import transaction.CheckoutPendingFailure;
import transaction.InventoryGateway;
import transaction.TransactionFailure;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A stale cache never decides an outcome: the database's guarded writes and confirmed rejections do,
 * so the cache on and off give the same responses and rows (R19), for Scan and for Complete.
 */
class TransactionCacheCorrectnessTest {
    private static final String MISSES = "transaction.cache.guardMisses";

    @Test
    void aRowChangedBehindAWarmEntryGivesTheUncachedOutcomeThroughAGuardMissAndAReload() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction cached = InProcessTransaction.over(schema).decorator(counting::wrap).build();
            InProcessTransaction uncached = InProcessTransaction.over(schema).pool(cached.pool).cacheEntries(0).build();

            for (Change change : Change.values()) {
                String warm = cached.start();
                cached.scan(warm, "SKU-000001");
                String cold = uncached.start();
                uncached.scan(cold, "SKU-000001");
                change.apply(schema, cached, warm);
                change.apply(schema, cached, cold);

                long misses = PerfMetrics.counterValue(MISSES);
                String withCache;
                try (CountingConnections.Scope scope = counting.scope()) {
                    withCache = outcome(() -> cached.scan(warm, "SKU-000003"));
                    assertEquals(2, scope.counts().selects(), change + ": the entry reloads once after the miss");
                }
                assertEquals(misses + 1, PerfMetrics.counterValue(MISSES), change + ": the guarded write missed");
                String withoutCache = outcome(() -> uncached.scan(cold, "SKU-000003"));
                assertEquals(withoutCache, withCache, change + ": the cache changes no outcome");
                assertEquals(change.expected, withCache, change.toString());
                assertEquals(rows(schema, cold), rows(schema, warm), change + ": the same rows");
            }
        }
    }

    @Test
    void aStaleOpenEntryWithAnUnknownSkuStillAnswersNotOpenWhenTheDatabaseSaysCompleted() throws Exception {
        try (TransactionTestSchema schema = seeded()) {
            InProcessTransaction cached = InProcessTransaction.over(schema).build();
            String id = cached.start();
            cached.scan(id, "SKU-000001");
            Change.COMPLETED.apply(schema, cached, id);
            long misses = PerfMetrics.counterValue(MISSES);
            assertEquals("NOT_OPEN", outcome(() -> cached.scan(id, "NO-SUCH-SKU")),
                    "409 precedes 404 SKU_NOT_FOUND, confirmed against the database");
            assertEquals(misses, PerfMetrics.counterValue(MISSES), "a rejection is confirmed by a reload, not a write");
            assertEquals(1, schema.scans(id).size());
        }
    }

    @Test
    void aScriptedSequenceGivesIdenticalResponsesAndRowsWithTheCacheOnAndOff() throws Exception {
        try (TransactionTestSchema on = seeded(); TransactionTestSchema off = seeded()) {
            List<String> onIds = new ArrayList<>();
            List<String> offIds = new ArrayList<>();
            List<String> withCache = script(on, InProcessTransaction.over(on).build(), onIds);
            List<String> withoutCache = script(off, InProcessTransaction.over(off).cacheEntries(0).build(), offIds);
            assertEquals(anonymous(withoutCache, offIds), anonymous(withCache, onIds));
            assertEquals(dump(off, offIds), dump(on, onIds), "identical accepted_scan rows and transaction rows");

            long acknowledged = withCache.stream().filter(response -> response.startsWith("OK")).count();
            assertEquals(8, acknowledged);
            assertEquals(acknowledged + 2, on.scanCount(), "every 200 has its row, besides the two foreign writes");
            for (String response : withCache) {
                if (!response.startsWith("OK")) continue;
                String[] parts = response.split(" ");
                assertEquals(1, on.queryLong("SELECT COUNT(*) FROM accepted_scan WHERE transaction_id = ? "
                        + "AND scan_sequence = ? AND sku = ?", parts[1], Integer.parseInt(parts[2]), parts[3]),
                        "the acknowledged scan " + response + " is durable");
            }
        }
    }

    /**
     * A row changed behind a warm entry before each of Complete's guarded writes (T1, T3, reopen),
     * including a stale attempt_count before T1: the guard misses, and the outcome and the attempts
     * equal those of the cache-off service, with no duplicate-key failure (R19).
     */
    @Test
    void staleEntriesBeforeEachCompleteWriteGiveTheUncachedOutcome() throws Exception {
        try (TransactionTestSchema schema = seeded()) {
            for (CompleteChange change : CompleteChange.values()) {
                FakeInventoryGateway cachedInventory = FakeInventoryGateway.applying();
                FakeInventoryGateway uncachedInventory = FakeInventoryGateway.applying();
                InProcessTransaction cached = InProcessTransaction.over(schema).inventory(cachedInventory).build();
                InProcessTransaction uncached = InProcessTransaction.over(schema).pool(cached.pool).cacheEntries(0)
                        .inventory(uncachedInventory).build();
                String warm = cached.start();
                cached.scan(warm, "SKU-000001");
                String cold = uncached.start();
                uncached.scan(cold, "SKU-000001");
                change.arrange(schema, cached, warm, cachedInventory);
                change.arrange(schema, uncached, cold, uncachedInventory);

                long misses = PerfMetrics.counterValue(MISSES);
                String withCache = completeOutcome(cached, warm);
                assertTrue(PerfMetrics.counterValue(MISSES) > misses, change + ": the stale entry's guard missed");
                String withoutCache = completeOutcome(uncached, cold);
                assertEquals(withoutCache, withCache, change + ": the cache changes no outcome");
                assertEquals(change.expected, withCache, change.toString());
                assertEquals(attempts(schema, cold), attempts(schema, warm), change + ": the same attempts");
                assertTrue(schema.queryLong("SELECT COUNT(*) FROM checkout_attempt WHERE transaction_id = ? "
                        + "AND state = 'PENDING'", warm) <= 1, change + ": never a second pending attempt");
            }
        }
    }

    private static String completeOutcome(InProcessTransaction service, String id) {
        return outcome(() -> {
            var receipt = service.complete(id);
            return "OK " + receipt.itemCount() + " " + receipt.totalAmount();
        });
    }

    private static String attempts(TransactionTestSchema schema, String id) throws Exception {
        StringBuilder attempts = new StringBuilder();
        for (var attempt : schema.attempts(id)) {
            attempts.append(attempt.attemptNumber()).append(':').append(attempt.state()).append(':')
                    .append(attempt.rejectionReason()).append(':').append(attempt.clientOutcome()).append(' ');
        }
        return attempts.toString();
    }

    private enum CompleteChange {
        ITEM_COUNT_BEFORE_T1("OK 2 2.05") {
            @Override
            void arrange(TransactionTestSchema schema, InProcessTransaction service, String id, FakeInventoryGateway inventory)
                    throws Exception {
                foreignScan(schema, service, id, "SKU-000002", "Item 2", "1.20");
            }
        },
        ATTEMPT_COUNT_BEFORE_T1("OK 1 0.85") {
            @Override
            void arrange(TransactionTestSchema schema, InProcessTransaction service, String id, FakeInventoryGateway inventory)
                    throws Exception {
                // Another instance's attempt 1 was rejected and the basket reopened behind this cache.
                schema.update("UPDATE `transaction` SET attempt_count = 1 WHERE transaction_id = ?", id);
                schema.update("INSERT INTO checkout_attempt (transaction_id, attempt_number, state, rejection_reason, "
                        + "rejected_sku, `lines`, item_count, total_amount, created_at, settled_at, next_retry_at) VALUES "
                        + "(?, 1, 'REJECTED', 'INSUFFICIENT_STOCK', 'SKU-000001', '[]', 1, 0.85, NOW(3), NOW(3), NOW(3))", id);
            }
        },
        COMPLETED_BEFORE_T1("NOT_OPEN") {
            @Override
            void arrange(TransactionTestSchema schema, InProcessTransaction service, String id, FakeInventoryGateway inventory)
                    throws Exception {
                Change.COMPLETED.apply(schema, service, id);
            }
        },
        PENDING_BEFORE_T1("PENDING") {
            @Override
            void arrange(TransactionTestSchema schema, InProcessTransaction service, String id, FakeInventoryGateway inventory)
                    throws Exception {
                schema.update("UPDATE `transaction` SET attempt_count = 1, active_attempt_number = 1 WHERE transaction_id = ?", id);
                schema.update("INSERT INTO checkout_attempt (transaction_id, attempt_number, state, `lines`, item_count, "
                        + "total_amount, created_at, next_retry_at) VALUES (?, 1, 'PENDING', '[]', 1, 0.85, NOW(3), NOW(3))", id);
            }
        },
        SETTLED_BEFORE_T3("OK 1 0.85") {
            @Override
            void arrange(TransactionTestSchema schema, InProcessTransaction service, String id, FakeInventoryGateway inventory) {
                inventory.thenAnswer(call -> {
                    service.attempts.complete(call.key(), Instant.now().truncatedTo(ChronoUnit.MILLIS), false);
                    return new InventoryGateway.Applied(true);
                });
            }
        },
        SETTLED_BEFORE_REOPEN("INSUFFICIENT_STOCK") {
            @Override
            void arrange(TransactionTestSchema schema, InProcessTransaction service, String id, FakeInventoryGateway inventory) {
                inventory.thenAnswer(call -> {
                    service.attempts.reopen(call.key(), CheckoutAttemptStore.RejectionReason.INSUFFICIENT_STOCK,
                            "SKU-000001", Instant.now().truncatedTo(ChronoUnit.MILLIS), false);
                    return new InventoryGateway.Rejected(InventoryGateway.RejectionReason.INSUFFICIENT_STOCK, "SKU-000001");
                });
            }
        };

        final String expected;

        CompleteChange(String expected) {
            this.expected = expected;
        }

        abstract void arrange(TransactionTestSchema schema, InProcessTransaction service, String id,
                              FakeInventoryGateway inventory) throws Exception;
    }

    private static List<String> anonymous(List<String> responses, List<String> ids) {
        return responses.stream().map(response -> response.replace(ids.get(0), "FIRST").replace(ids.get(1), "SECOND"))
                .toList();
    }

    /** Scans, rejections, foreign writes, a pending attempt, and a completion behind the service's back. */
    private static List<String> script(TransactionTestSchema schema, InProcessTransaction service, List<String> ids)
            throws Exception {
        List<String> responses = new ArrayList<>();
        String first = service.start();
        String second = service.start();
        ids.add(first);
        ids.add(second);
        String[] steps = {"1", "2", "1", "X", "3"};
        for (String step : steps) responses.add(scanOutcome(service, first, step));
        foreignScan(schema, service, first, "SKU-000004", "Item 4", "1.90");
        responses.add(scanOutcome(service, first, "1"));
        responses.add(scanOutcome(service, second, "2"));
        foreignScan(schema, service, second, "SKU-000002", "Item 2", "1.20");
        responses.add(scanOutcome(service, second, "2"));
        Change.PENDING.apply(schema, service, second);
        responses.add(scanOutcome(service, second, "5"));
        schema.update("UPDATE `transaction` SET active_attempt_number = NULL WHERE transaction_id = ?", second);
        responses.add(scanOutcome(service, second, "5"));
        Change.COMPLETED.apply(schema, service, first);
        responses.add(scanOutcome(service, first, "X"));
        responses.add(scanOutcome(service, first, "2"));
        responses.add(outcome(() -> service.scan("missing-" + UUID.randomUUID().toString().substring(0, 8), "SKU-000001")));
        return responses;
    }

    private static String scanOutcome(InProcessTransaction service, String id, String step) {
        String sku = step.equals("X") ? "NO-SUCH-SKU" : "SKU-00000" + step;
        return outcome(() -> {
            var view = service.scan(id, sku);
            return "OK " + id + " " + view.itemCount() + " " + view.sku() + " " + view.unitPrice() + " " + view.runningTotal();
        });
    }

    private static String outcome(ThrowingSupplier action) {
        try {
            Object result = action.get();
            return result instanceof String text ? text : "OK";
        } catch (TransactionFailure failure) {
            return switch (failure.code()) {
                case NOT_FOUND -> "NOT_FOUND";
                case NOT_OPEN -> "NOT_OPEN";
                case UNKNOWN_SKU -> "UNKNOWN_SKU";
                default -> failure.code().name();
            };
        } catch (CheckoutPendingFailure pending) {
            return "PENDING";
        } catch (Exception other) {
            return "ERROR " + other.getClass().getSimpleName();
        }
    }

    private static String rows(TransactionTestSchema schema, String id) throws Exception {
        StringBuilder rows = new StringBuilder(schema.transaction(id).orElseThrow().toString());
        for (var scan : schema.scans(id)) {
            rows.append(" | ").append(scan.scanSequence()).append(' ').append(scan.sku()).append(' ')
                    .append(scan.productName()).append(' ').append(scan.unitPrice());
        }
        return rows.toString();
    }

    private static String dump(TransactionTestSchema schema, List<String> ids) throws Exception {
        assertEquals(ids.size(), schema.queryLong("SELECT COUNT(*) FROM `transaction`"));
        StringBuilder dump = new StringBuilder();
        for (String id : ids) dump.append(rows(schema, id).replace(id, "TX")).append('\n');
        return dump.toString();
    }

    /** Another writer's committed Scan through the real store, with the correct expectation. */
    private static void foreignScan(TransactionTestSchema schema, InProcessTransaction service, String id, String sku,
                                    String name, String price) throws Exception {
        var row = schema.transaction(id).orElseThrow();
        assertEquals(ScanStore.AppendResult.OK, service.scans.appendScan(id, row.itemCount(),
                row.totalAmount().add(new BigDecimal(price)), new ScanStore.AcceptedScan(row.itemCount() + 1, sku, name,
                        new BigDecimal(price), Instant.now().truncatedTo(ChronoUnit.MILLIS))));
    }

    private enum Change {
        COMPLETED("NOT_OPEN") {
            @Override
            void apply(TransactionTestSchema schema, InProcessTransaction service, String id) throws Exception {
                schema.update("UPDATE `transaction` SET status = 'COMPLETED', completed_at = NOW(3) WHERE transaction_id = ?", id);
            }
        },
        PENDING("PENDING") {
            @Override
            void apply(TransactionTestSchema schema, InProcessTransaction service, String id) throws Exception {
                schema.update("UPDATE `transaction` SET attempt_count = attempt_count + 1, "
                        + "active_attempt_number = attempt_count WHERE transaction_id = ?", id);
            }
        },
        ITEM_COUNT("OK") {
            @Override
            void apply(TransactionTestSchema schema, InProcessTransaction service, String id) throws Exception {
                foreignScan(schema, service, id, "SKU-000002", "Item 2", "1.20");
            }
        };

        final String expected;

        Change(String expected) {
            this.expected = expected;
        }

        abstract void apply(TransactionTestSchema schema, InProcessTransaction service, String id) throws Exception;
    }

    @FunctionalInterface
    private interface ThrowingSupplier {
        Object get() throws Exception;
    }

    private static TransactionTestSchema seeded() throws Exception {
        TransactionTestSchema schema = TransactionTestSchema.create();
        schema.seedReplica(20, UUID.randomUUID().toString());
        return schema;
    }
}
