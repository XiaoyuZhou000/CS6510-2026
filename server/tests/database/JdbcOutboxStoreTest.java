package database;

import events.OutboxRow;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.InventoryTestSchema;
import support.TestSchema;
import support.TransactionTestSchema;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/** {@code outbox_event} through the shared adapter, in the Transaction and the Inventory schemas (D5). */
class JdbcOutboxStoreTest {

    @Test
    void anInsertJoinsTheCallersUnitOfWorkInTheTransactionSchema() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            assertInsertFollowsTheUnit(schema);
        }
    }

    @Test
    void anInsertJoinsTheCallersUnitOfWorkInTheInventorySchema() throws Exception {
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            assertInsertFollowsTheUnit(schema);
        }
    }

    private static void assertInsertFollowsTheUnit(TestSchema schema) throws Exception {
        JdbcOutboxStore store = new JdbcOutboxStore(schema.pool(), "test.outbox");
        OutboxRow committed = row("e-commit", "tx-1:1", Instant.now());
        JdbcUnitOfWork.inTransaction(schema.pool(), "test.unit", connection -> {
            store.insert(connection, committed);
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM outbox_event"), "invisible before the commit");
            return null;
        });
        assertTrue(store.findByEventId("e-commit").isPresent(), "visible after the commit");

        OutboxRow rolledBack = row("e-rollback", "tx-2:1", Instant.now());
        assertThrows(IllegalStateException.class, () -> JdbcUnitOfWork.inTransaction(schema.pool(), "test.unit", connection -> {
            store.insert(connection, rolledBack);
            throw new IllegalStateException("the caller's unit fails after the insert");
        }));
        assertTrue(store.findByEventId("e-rollback").isEmpty(), "rolled back with the caller's unit");
        assertEquals(1, store.countUnpublished());

        StoreFailure duplicate = assertThrows(StoreFailure.class, () -> JdbcUnitOfWork.inTransaction(schema.pool(),
                "test.unit", connection -> {
                    store.insert(connection, row("e-commit", "tx-3:1", Instant.now()));
                    return null;
                }));
        assertNotNull(duplicate.getCause(), "event_id is unique");
        assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM outbox_event"));
    }

    @Test
    void markPublishedIsOneUpdateAndOneCommitPerBatchAndKeepsTheFirstTime() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create(
                TransactionTestSchema.Options.defaults().decorator(counting::wrap))) {
            JdbcOutboxStore store = new JdbcOutboxStore(schema.pool(), "test.outbox");
            insertAll(schema, store, List.of(row("a", "tx:1", Instant.now()), row("b", "tx:2", Instant.now()),
                    row("c", "tx:3", Instant.now())));
            Instant first = Instant.parse("2026-10-29T12:00:00.123Z");
            try (CountingConnections.Scope scope = counting.scope()) {
                store.markPublished(List.of("a", "c"), first);
                assertEquals(1, scope.counts().updates());
                assertEquals(1, scope.counts().statements());
                assertEquals(1, scope.counts().commits());
            }
            assertEquals(1, store.countUnpublished());
            store.markPublished(List.of("a", "b"), first.plusSeconds(60));
            assertEquals(first, published(schema, "a"), "a later confirmation keeps the first publication time");
            assertEquals(first.plusSeconds(60), published(schema, "b"));

            store.incrementSendCount(List.of("a", "a2-missing"));
            store.incrementSendCount(List.of("a"));
            assertEquals(2, schema.queryLong("SELECT send_count FROM outbox_event WHERE event_id = 'a'"));
            assertEquals(0, schema.queryLong("SELECT send_count FROM outbox_event WHERE event_id = 'b'"));
        }
    }

    @Test
    void theSweepUsesTheUnpublishedIndexAndHonoursAgeAndLimit() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcOutboxStore store = new JdbcOutboxStore(schema.pool(), "test.outbox");
            Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            List<OutboxRow> rows = new ArrayList<>();
            for (int i = 0; i < 1_500; i++) rows.add(row("p" + i, "tx-p" + i + ":1", now.minusSeconds(30)));
            for (int i = 0; i < 510; i++) rows.add(row("u" + i, "tx-u" + i + ":1", now.minusSeconds(5)));
            rows.add(row("young", "tx-y:1", now));
            insertAll(schema, store, rows);
            List<String> published = new ArrayList<>();
            for (int i = 0; i < 1_500; i++) published.add("p" + i);
            for (int from = 0; from < published.size(); from += 200) {
                store.markPublished(published.subList(from, Math.min(published.size(), from + 200)), now.minusSeconds(29));
            }

            List<OutboxRow> due = store.selectUnpublished(now.minusSeconds(1), 500);
            assertEquals(500, due.size(), "at most the limit");
            for (int i = 0; i < 500; i++) assertEquals("u" + i, due.get(i).eventId(), "outbox_seq order");
            assertTrue(due.stream().noneMatch(r -> r.eventId().equals("young")), "only rows older than the cutoff");
            store.markPublished(due.stream().map(OutboxRow::eventId).toList(), now);
            assertEquals(List.of("u500", "u501", "u502", "u503", "u504", "u505", "u506", "u507", "u508", "u509"),
                    store.selectUnpublished(now.minusSeconds(1), 500).stream().map(OutboxRow::eventId).toList(),
                    "the next sweep continues after the published rows");
            assertEquals(11, store.selectUnpublished(now.plusSeconds(1), 1_000).size(), "the young row once old enough");

            try (Statement explain = schema.admin().createStatement();
                 ResultSet plan = explain.executeQuery("EXPLAIN SELECT event_id FROM outbox_event WHERE published_at IS NULL "
                         + "AND created_at < '" + Timestamp.from(now.minusSeconds(1)) + "' ORDER BY outbox_seq LIMIT 500")) {
                assertTrue(plan.next());
                assertEquals("idx_outbox_event_unpublished", plan.getString("key"), "the sweep reads (published_at, outbox_seq)");
                String extra = plan.getString("Extra");
                assertTrue(extra == null || !extra.contains("filesort"), "the index already gives outbox_seq order: " + extra);
            }
        }
    }

    @Test
    void lookupsReturnTheStoredEnvelopeBytesUnchanged() throws Exception {
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            JdbcOutboxStore store = new JdbcOutboxStore(schema.pool(), "test.outbox");
            byte[] envelope = new byte[300];
            new Random(5).nextBytes(envelope);
            envelope[0] = 0;
            envelope[1] = (byte) 0xff;
            Instant created = Instant.parse("2026-10-29T08:15:30.456Z");
            OutboxRow first = new OutboxRow("o-1", "selfcheckout.checkout-outcome.v1", "tx-9", "StockDeducted", "tx-9:2",
                    envelope, created);
            OutboxRow second = new OutboxRow("o-2", "selfcheckout.checkout-outcome.v1", "tx-9", "StockRestored", "tx-9:2",
                    new byte[] {1, 2, 3}, created.plusMillis(1));
            OutboxRow catalog = new OutboxRow("o-3", "selfcheckout.catalog.v1", "SKU-1", "CatalogItemPublished", null,
                    new byte[] {9}, created);
            insertAll(schema, store, List.of(first, second, catalog));

            OutboxRow loaded = store.findByEventId("o-1").orElseThrow();
            assertArrayEquals(envelope, loaded.envelope());
            assertEquals(first.topic(), loaded.topic());
            assertEquals("tx-9", loaded.routingKey());
            assertEquals("StockDeducted", loaded.eventType());
            assertEquals("tx-9:2", loaded.aggregateKey());
            assertEquals(created, loaded.createdAt());
            List<OutboxRow> attempt = store.findByAggregateKey("tx-9:2");
            assertEquals(List.of("o-1", "o-2"), attempt.stream().map(OutboxRow::eventId).toList(), "outbox_seq order");
            assertArrayEquals(envelope, attempt.get(0).envelope());
            assertNull(store.findByEventId("o-3").orElseThrow().aggregateKey(), "catalog events have no aggregate");
            assertEquals(3, store.findByEventIds(List.of("o-3", "o-1", "o-2", "missing")).size());
            assertTrue(store.findByEventId("missing").isEmpty());
        }
    }

    private static void insertAll(TestSchema schema, JdbcOutboxStore store, List<OutboxRow> rows) throws Exception {
        JdbcUnitOfWork.inTransaction(schema.pool(), "test.unit", connection -> {
            for (OutboxRow row : rows) JdbcOutboxStore.insertRow(connection, row);
            return null;
        });
    }

    private static Instant published(TestSchema schema, String eventId) throws Exception {
        try (java.sql.PreparedStatement select = schema.admin().prepareStatement(
                "SELECT published_at FROM outbox_event WHERE event_id = ?")) {
            select.setString(1, eventId);
            try (ResultSet row = select.executeQuery()) {
                assertTrue(row.next());
                return row.getTimestamp(1).toInstant();
            }
        }
    }

    private static OutboxRow row(String eventId, String aggregateKey, Instant createdAt) {
        String transactionId = aggregateKey.substring(0, aggregateKey.indexOf(':'));
        return new OutboxRow(eventId, "selfcheckout.checkout.v1", transactionId, "CheckoutRequested", aggregateKey,
                ("envelope-" + eventId).getBytes(java.nio.charset.StandardCharsets.UTF_8), createdAt);
    }
}
