package database;

import database.CheckoutAttemptStore.AttemptKey;
import database.CheckoutAttemptStore.AttemptRecord;
import database.CheckoutAttemptStore.Begin;
import database.CheckoutAttemptStore.ClientOutcome;
import database.CheckoutAttemptStore.RejectionReason;
import database.CheckoutAttemptStore.State;
import database.CheckoutAttemptStore.WriteResult;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.TransactionTestSchema;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Transaction side of the checkout Saga over a disposable schema (R5, R9, R19; contracts/checkout-saga.md
 * "Steps"). Migrates the transaction side of JdbcCheckoutCompletionStoreTest.
 */
class JdbcCheckoutAttemptStoreTest {
    private static final String LINES = "[{\"sku\":\"SKU-000001\",\"quantity\":2},{\"sku\":\"SKU-000002\",\"quantity\":1}]";
    private static final Duration BUDGET = Duration.ofMillis(8_000);

    @Test
    void beginIsGuardedAndCommitsThePendingAttemptWithItsSnapshot() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.newPool("test.begin." + suffix(), 2, 4, counting::wrap);
            JdbcCheckoutAttemptStore store = new JdbcCheckoutAttemptStore(pool);
            String id = basket(pool, 3);
            Instant createdAt = now();

            try (CountingConnections.Scope scope = counting.scope()) {
                assertEquals(WriteResult.OK, store.begin(begin(id, 1, 3, createdAt)));
                assertEquals(1, scope.counts().updates(), "one guarded UPDATE");
                assertEquals(1, scope.counts().inserts(), "one attempt INSERT");
                assertEquals(0, scope.counts().selects(), "no read");
                assertEquals(1, scope.counts().commits(), "both in one commit");
            }
            var row = schema.transaction(id).orElseThrow();
            assertEquals("OPEN", row.status(), "lookup still says OPEN while the attempt is pending");
            assertEquals(1, row.attemptCount());
            assertEquals(1, row.activeAttemptNumber());
            AttemptRecord attempt = store.findAttempt(new AttemptKey(id, 1)).orElseThrow();
            assertEquals(State.PENDING, attempt.state());
            assertEquals(LINES, attempt.lines());
            assertEquals(3, attempt.itemCount());
            assertEquals(new BigDecimal("2.55"), attempt.totalAmount());
            assertEquals(ClientOutcome.NONE, attempt.clientOutcome());
            assertEquals(createdAt, attempt.createdAt());
            assertEquals(createdAt.plus(BUDGET), attempt.nextRetryAt(), "recovery leaves a live request's attempt alone");
            assertNull(attempt.settledAt());
            assertEquals(0, attempt.retryCount());

            assertEquals(WriteResult.GUARD_MISS, store.begin(begin(id, 2, 3, now())), "an active attempt freezes the basket");
            String stale = basket(pool, 2);
            assertEquals(WriteResult.GUARD_MISS, store.begin(begin(stale, 1, 1, now())), "a stale item_count");
            String completed = basket(pool, 1);
            schema.update("UPDATE `transaction` SET status = 'COMPLETED', completed_at = NOW(3) WHERE transaction_id = ?",
                    completed);
            assertEquals(WriteResult.GUARD_MISS, store.begin(begin(completed, 1, 1, now())), "not OPEN");
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM checkout_attempt"), "every miss wrote nothing");
            assertEquals(0, schema.transaction(stale).orElseThrow().attemptCount());
        }
    }

    @Test
    void aStaleExpectedAttemptCountMissesItsGuardInsteadOfHittingTheKey() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcCheckoutAttemptStore store = new JdbcCheckoutAttemptStore(schema.pool());
            String id = basket(schema.pool(), 1);
            AttemptKey first = new AttemptKey(id, 1);
            assertEquals(WriteResult.OK, store.begin(begin(id, 1, 1, now())));
            assertEquals(WriteResult.OK, store.reopen(first, RejectionReason.INSUFFICIENT_STOCK, "SKU-000001", now(), true));

            assertEquals(WriteResult.GUARD_MISS, assertDoesNotThrow(() -> store.begin(begin(id, 1, 1, now()))),
                    "a cache that still says attempt_count 0 misses; it never fails on the primary key");
            assertEquals(WriteResult.OK, store.begin(begin(id, 2, 1, now())), "attempt n + 1 after a reload");
            assertEquals(List.of(1, 2), schema.attempts(id).stream().map(TransactionTestSchema.AttemptRow::attemptNumber).toList());
        }
    }

    @Test
    void recordClientOutcomeWritesOnlyWhileNoOutcomeIsRecorded() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcCheckoutAttemptStore store = new JdbcCheckoutAttemptStore(schema.pool());
            String id = basket(schema.pool(), 1);
            AttemptKey key = new AttemptKey(id, 1);
            store.begin(begin(id, 1, 1, now()));
            store.complete(key, now(), false);
            assertEquals(ClientOutcome.NONE, store.findAttempt(key).orElseThrow().clientOutcome(),
                    "a settler that is not answering records nothing");

            assertTrue(store.recordClientOutcome(key, ClientOutcome.SUCCESS));
            assertEquals(ClientOutcome.SUCCESS, store.findAttempt(key).orElseThrow().clientOutcome());
            assertFalse(store.recordClientOutcome(key, ClientOutcome.REJECTED), "an outcome is recorded once");
            assertEquals(ClientOutcome.SUCCESS, store.findAttempt(key).orElseThrow().clientOutcome());
            assertFalse(store.recordClientOutcome(new AttemptKey(id, 9), ClientOutcome.SUCCESS), "no such attempt");
        }
    }

    @Test
    void completeIsGuardedByTheActiveAttemptAndSettlesTheTransactionAndTheAttemptTogether() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.newPool("test.complete." + suffix(), 2, 4, counting::wrap);
            JdbcCheckoutAttemptStore store = new JdbcCheckoutAttemptStore(pool);
            String id = basket(pool, 3);
            AttemptKey key = new AttemptKey(id, 1);
            store.begin(begin(id, 1, 3, now()));

            assertEquals(WriteResult.GUARD_MISS, store.complete(new AttemptKey(id, 2), now(), true), "not the active attempt");
            Instant completedAt = now().plusMillis(5);
            try (CountingConnections.Scope scope = counting.scope()) {
                assertEquals(WriteResult.OK, store.complete(key, completedAt, true));
                assertEquals(2, scope.counts().updates());
                assertEquals(0, scope.counts().selects());
                assertEquals(1, scope.counts().commits());
            }
            var row = schema.transaction(id).orElseThrow();
            assertEquals("COMPLETED", row.status());
            assertNull(row.activeAttemptNumber());
            assertEquals(Timestamp.from(completedAt), timestamp(schema, "SELECT completed_at FROM `transaction` "
                    + "WHERE transaction_id = ?", id), "the service-supplied completed_at");
            AttemptRecord attempt = store.findAttempt(key).orElseThrow();
            assertEquals(State.COMPLETED, attempt.state());
            assertEquals(completedAt, attempt.settledAt());
            assertEquals(ClientOutcome.SUCCESS, attempt.clientOutcome());

            assertEquals(WriteResult.GUARD_MISS, store.complete(key, now(), true), "a duplicate completion misses");
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED'"));
        }
    }

    @Test
    void theUniqueGeneratedColumnsRejectASecondPendingAndASecondCompletedAttempt() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcCheckoutAttemptStore store = new JdbcCheckoutAttemptStore(schema.pool());
            String id = basket(schema.pool(), 1);
            store.begin(begin(id, 1, 1, now()));

            SQLException pending = assertThrows(SQLIntegrityConstraintViolationException.class,
                    () -> insertAttempt(schema, id, 2, "PENDING"));
            assertTrue(pending.getMessage().contains("uq_checkout_attempt_pending"), pending.getMessage());

            store.complete(new AttemptKey(id, 1), now(), true);
            SQLException completed = assertThrows(SQLIntegrityConstraintViolationException.class,
                    () -> insertAttempt(schema, id, 3, "COMPLETED"));
            assertTrue(completed.getMessage().contains("uq_checkout_attempt_completed"), completed.getMessage());
            assertEquals(1, schema.attempts(id).size(), "defense in depth: neither duplicate was stored");
        }
    }

    @Test
    void reopenClearsTheActiveAttemptAndKeepsTheOpenBasket() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            ConnectionPool pool = schema.newPool("test.reopen." + suffix(), 2, 4, counting::wrap);
            JdbcCheckoutAttemptStore store = new JdbcCheckoutAttemptStore(pool);
            String id = basket(pool, 3);
            AttemptKey key = new AttemptKey(id, 1);
            store.begin(begin(id, 1, 3, now()));

            try (CountingConnections.Scope scope = counting.scope()) {
                assertEquals(WriteResult.OK, store.reopen(key, RejectionReason.INSUFFICIENT_STOCK, "SKU-000001", now(), true));
                assertEquals(2, scope.counts().updates());
                assertEquals(0, scope.counts().selects());
                assertEquals(1, scope.counts().commits());
            }
            var row = schema.transaction(id).orElseThrow();
            assertEquals("OPEN", row.status(), "the transaction stays OPEN with its basket");
            assertNull(row.activeAttemptNumber());
            assertEquals(3, row.itemCount());
            assertEquals(3, schema.scans(id).size());
            assertEquals(1, row.attemptCount());
            var attempt = schema.attempts(id).get(0);
            assertEquals("REJECTED", attempt.state());
            assertEquals("INSUFFICIENT_STOCK", attempt.rejectionReason());
            assertEquals("SKU-000001", attempt.rejectedSku());
            assertEquals("REJECTED", attempt.clientOutcome());
            assertEquals(WriteResult.GUARD_MISS, store.reopen(key, RejectionReason.INSUFFICIENT_STOCK, "SKU-000001",
                    now(), true), "settled once");

            String other = basket(pool, 1);
            store.begin(begin(other, 1, 1, now()));
            store.reopen(new AttemptKey(other, 1), RejectionReason.STOCK_RESTORED, null, now(), false);
            var restored = schema.attempts(other).get(0);
            assertEquals("STOCK_RESTORED", restored.rejectionReason());
            assertNull(restored.rejectedSku());
            assertEquals("NONE", restored.clientOutcome(), "recovery never records a client outcome");
        }
    }

    @Test
    void markPendingErrorChangesOnlyNoneOnPendingAttemptsAndMakesThemDueNow() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcCheckoutAttemptStore store = new JdbcCheckoutAttemptStore(schema.pool());
            String id = basket(schema.pool(), 1);
            AttemptKey key = new AttemptKey(id, 1);
            store.begin(begin(id, 1, 1, now()));
            Instant handover = now().plusMillis(10);

            assertTrue(store.markPendingError(key, handover));
            AttemptRecord attempt = store.findAttempt(key).orElseThrow();
            assertEquals(ClientOutcome.PENDING_ERROR, attempt.clientOutcome());
            assertEquals(State.PENDING, attempt.state(), "the attempt stays pending");
            assertEquals(handover, attempt.nextRetryAt(), "next_retry_at moves to now in the same UPDATE");
            assertFalse(store.markPendingError(key, handover.plusSeconds(5)), "only NONE changes");
            assertEquals(handover, store.findAttempt(key).orElseThrow().nextRetryAt());

            String settled = basket(schema.pool(), 1);
            AttemptKey done = new AttemptKey(settled, 1);
            store.begin(begin(settled, 1, 1, now()));
            store.complete(done, now(), false);
            assertFalse(store.markPendingError(done, now()), "only PENDING attempts change");
            assertEquals(ClientOutcome.NONE, store.findAttempt(done).orElseThrow().clientOutcome());
        }
    }

    @Test
    void findPendingPagesDueAttemptsInKeyOrderOverTheDueIndex() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcCheckoutAttemptStore store = new JdbcCheckoutAttemptStore(schema.pool());
            Instant base = now().minusSeconds(60);
            java.util.Map<AttemptKey, Instant> dueTimes = new java.util.HashMap<>();
            for (int i = 0; i < 7; i++) {
                String id = basket(schema.pool(), 1);
                // Pairs share a due time, so the keyset must order by key within a timestamp.
                Instant due = base.plusMillis(1_000L * (i / 2));
                store.begin(new Begin(new AttemptKey(id, 1), 1, new BigDecimal("0.85"), LINES, due.minus(BUDGET), due));
                dueTimes.put(new AttemptKey(id, 1), due);
            }
            List<AttemptKey> expected = new ArrayList<>(dueTimes.keySet());
            expected.sort(java.util.Comparator.comparing((AttemptKey key) -> dueTimes.get(key))
                    .thenComparing(AttemptKey::transactionId));
            String future = basket(schema.pool(), 1);
            store.begin(begin(future, 1, 1, now()));
            String settled = basket(schema.pool(), 1);
            store.begin(new Begin(new AttemptKey(settled, 1), 1, new BigDecimal("0.85"), LINES, base.minus(BUDGET), base));
            store.complete(new AttemptKey(settled, 1), now(), false);

            Instant dueBefore = now();
            List<AttemptKey> paged = new ArrayList<>();
            CheckoutAttemptStore.PageCursor cursor = null;
            int pages = 0;
            while (true) {
                List<AttemptRecord> page = store.findPending(dueBefore, cursor, 3);
                pages++;
                assertTrue(page.size() <= 3, "at most limit rows");
                page.forEach(row -> paged.add(row.key()));
                page.forEach(row -> assertEquals(State.PENDING, row.state()));
                if (page.size() < 3) break;
                cursor = page.get(page.size() - 1).cursor();
            }
            assertEquals(expected, paged, "every due PENDING attempt once, in (next_retry_at, key) order");
            assertEquals(3, pages);
            assertFalse(paged.contains(new AttemptKey(future, 1)), "a live request's attempt is not due yet");
            assertFalse(paged.contains(new AttemptKey(settled, 1)), "settled attempts are never scanned");

            for (boolean after : List.of(false, true)) {
                String plan = explain(schema, JdbcCheckoutAttemptStore.pendingQuery(after), after);
                assertTrue(plan.contains("key=idx_checkout_attempt_due"), plan);
                assertTrue(plan.contains("type=range"), "a range scan, not a full scan: " + plan);
                assertFalse(plan.contains("filesort"), "the index already orders the page: " + plan);
            }
        }
    }

    @Test
    void scheduleRetryCountsTheRetryAndStoresTheDueTimeAndTheLastError() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcCheckoutAttemptStore store = new JdbcCheckoutAttemptStore(schema.pool());
            String id = basket(schema.pool(), 1);
            AttemptKey key = new AttemptKey(id, 1);
            store.begin(begin(id, 1, 1, now()));
            Instant later = now().plusSeconds(5);
            store.scheduleRetry(key, later, "UNAVAILABLE");
            store.scheduleRetry(key, later, "x".repeat(400));
            AttemptRecord attempt = store.findAttempt(key).orElseThrow();
            assertEquals(2, attempt.retryCount());
            assertEquals(later, attempt.nextRetryAt());
            assertEquals(255, attempt.lastError().length(), "the stored error fits its column");
            assertTrue(store.findAttempt(new AttemptKey(id, 2)).isEmpty());
        }
    }

    private static String explain(TransactionTestSchema schema, String sql, boolean after) throws SQLException {
        try (PreparedStatement explain = schema.admin().prepareStatement("EXPLAIN " + sql)) {
            int index = 1;
            Timestamp at = Timestamp.from(now());
            explain.setTimestamp(index++, at);
            if (after) {
                explain.setTimestamp(index++, at);
                explain.setTimestamp(index++, at);
                explain.setTimestamp(index++, at);
                explain.setString(index++, "a");
                explain.setString(index++, "a");
                explain.setInt(index++, 1);
            }
            explain.setInt(index, 100);
            StringBuilder plan = new StringBuilder();
            try (ResultSet rows = explain.executeQuery()) {
                while (rows.next()) {
                    plan.append("type=").append(rows.getString("type")).append(" key=").append(rows.getString("key"))
                            .append(" extra=").append(rows.getString("Extra")).append("; ");
                }
            }
            return plan.toString();
        }
    }

    private static Timestamp timestamp(TransactionTestSchema schema, String sql, String id) throws SQLException {
        try (PreparedStatement select = schema.admin().prepareStatement(sql)) {
            select.setString(1, id);
            try (ResultSet row = select.executeQuery()) {
                assertTrue(row.next());
                return row.getTimestamp(1);
            }
        }
    }

    private static void insertAttempt(TransactionTestSchema schema, String id, int number, String state) throws SQLException {
        schema.update("INSERT INTO checkout_attempt (transaction_id, attempt_number, state, `lines`, item_count, "
                + "total_amount, created_at, settled_at, next_retry_at) VALUES (?, ?, ?, ?, 1, 0.85, NOW(3), "
                + (state.equals("PENDING") ? "NULL" : "NOW(3)") + ", NOW(3))", id, number, state, LINES);
    }

    private static Begin begin(String id, int attemptNumber, int itemCount, Instant createdAt) {
        return new Begin(new AttemptKey(id, attemptNumber), itemCount, new BigDecimal("2.55"), LINES, createdAt,
                createdAt.plus(BUDGET));
    }

    /** An OPEN transaction with {@code units} accepted scans of SKU-000001 at 0.85. */
    private static String basket(ConnectionPool pool, int units) {
        String id = UUID.randomUUID().toString();
        new JdbcTransactionStore(pool).insertOpen(id, "attempt-store-test", now());
        JdbcScanStore scans = new JdbcScanStore(pool);
        BigDecimal total = new BigDecimal("0.00");
        for (int n = 1; n <= units; n++) {
            BigDecimal price = new BigDecimal("0.85");
            total = total.add(price);
            assertEquals(ScanStore.AppendResult.OK, scans.appendScan(id, n - 1, total,
                    new ScanStore.AcceptedScan(n, "SKU-000001", "Item 1", price, now())));
        }
        return id;
    }

    private static Instant now() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
