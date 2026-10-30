package performance;

import database.JdbcCatalogMetaStore;
import database.JdbcCatalogStore;
import database.JdbcCheckoutOperationStore;
import events.TopicNames;
import inventory.CatalogCache;
import inventory.CatalogPublisher;
import inventory.CheckoutStockService;
import inventory.StockOutcomeEvents;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.InventoryTestSchema;
import support.RecordingOutboxSignal;
import support.StatementBudgets;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Inventory's apply of a {@code CheckoutRequested} in a fixed number of round trips, whatever the basket size
 * (R8, R22, 006 D8): the locking SELECT, the operation INSERT, the CASE UPDATE, and the outcome outbox INSERT
 * share one commit; a replay writes nothing and re-sends the recorded outcome; a rejection issues no stock
 * UPDATE. The startup catalog publication is one batched outbox INSERT and one commit, whatever the catalog size.
 */
class InventoryApplyStatementBudgetTest {

    @Test
    void appliedAndRejectedAppliesStayWithinTheirBudgetsForEveryBasketSize() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            schema.seed(40, 100);
            RecordingOutboxSignal signal = new RecordingOutboxSignal();
            CheckoutStockService stock = new CheckoutStockService(new JdbcCheckoutOperationStore(
                    schema.newPool("test.apply.budget." + UUID.randomUUID().toString().substring(0, 8), 2, 4, counting::wrap)),
                    new StockOutcomeEvents(TopicNames.unprefixed(), Clock.systemUTC()), signal);
            for (int size : List.of(1, 5, 20)) {
                List<CheckoutStockService.Line> lines = lines(size, 1);
                String tx = UUID.randomUUID().toString();
                try (CountingConnections.Scope scope = counting.scope()) {
                    CheckoutStockService.ApplyResult applied = stock.apply(tx, 1, lines);
                    assertEquals(CheckoutStockService.Outcome.APPLIED, applied.outcome());
                    CountingConnections.Counts counts = scope.counts();
                    assertEquals(1, counts.selects(), size + " lines: one locking SELECT");
                    assertEquals(2, counts.inserts(), size + " lines: the operation INSERT and the outcome outbox INSERT");
                    assertEquals(1, counts.updates(), size + " lines: one CASE UPDATE");
                    assertEquals(4, counts.statements());
                    assertEquals(1, counts.commits(), size + " lines: the outcome event and the deduction share one commit");
                    StatementBudgets.assertWithin("inventory.apply.statements", counts.statements());
                    StatementBudgets.assertWithin("inventory.apply.commits", counts.commits());
                    assertEquals(applied.eventId(), signal.signalled.get(signal.signalled.size() - 1),
                            "the dispatcher is signalled after the commit");
                }
                try (CountingConnections.Scope scope = counting.scope()) {
                    CheckoutStockService.ApplyResult replay = stock.apply(tx, 1, lines);
                    assertTrue(replay.replayed());
                    assertEquals(0, scope.counts().updates(), "a replay issues no stock UPDATE");
                    assertEquals(0, scope.counts().commits(), "and commits nothing");
                    assertEquals(replay.eventId(), signal.resent.get(signal.resent.size() - 1),
                            "it re-sends the recorded outcome instead");
                }

                String rejectedTx = UUID.randomUUID().toString();
                List<CheckoutStockService.Line> tooMany = lines(size, 1_000);
                try (CountingConnections.Scope scope = counting.scope()) {
                    assertEquals(CheckoutStockService.Outcome.INSUFFICIENT_STOCK,
                            stock.apply(rejectedTx, 1, tooMany).outcome());
                    CountingConnections.Counts counts = scope.counts();
                    assertEquals(0, counts.updates(), size + " lines: a rejection issues no stock UPDATE");
                    assertEquals(1, counts.selects());
                    assertEquals(2, counts.inserts(), "the rejection and its outcome event are recorded");
                    StatementBudgets.assertWithin("inventory.apply.rejected.statements", counts.statements());
                    StatementBudgets.assertWithin("inventory.apply.rejected.commits", counts.commits());
                }
            }
            assertEquals(97, schema.stock(InventoryTestSchema.sku(1)), "in all three applied baskets, deducted once each");
            assertEquals(98, schema.stock(InventoryTestSchema.sku(2)), "in the 5- and 20-line baskets");
            assertEquals(99, schema.stock(InventoryTestSchema.sku(20)), "in the 20-line basket only");
            assertEquals(100, schema.stock(InventoryTestSchema.sku(21)), "never referenced");

            // The startup catalog publication (006 D9): every item and the marker in one batched INSERT and one commit.
            var pool = schema.newPool("test.catalog.budget." + UUID.randomUUID().toString().substring(0, 8), 1, 2,
                    counting::wrap);
            CatalogPublisher publisher = new CatalogPublisher(new CatalogCache(new JdbcCatalogStore(pool),
                    new JdbcCatalogMetaStore(pool)), new JdbcCatalogMetaStore(pool), TopicNames.unprefixed(), signal,
                    Clock.systemUTC());
            try (CountingConnections.Scope scope = counting.scope()) {
                CatalogPublisher.Publication published = publisher.publish().orElseThrow();
                CountingConnections.Counts counts = scope.counts();
                assertEquals(41, published.eventIds().size(), "40 items and one snapshot marker");
                assertEquals(1, counts.inserts(), "one batched INSERT of all 41 outbox rows");
                assertEquals(1, counts.updates(), "the catalog_meta published_at UPDATE");
                StatementBudgets.assertWithin("inventory.catalog.publish.statements", counts.statements());
                StatementBudgets.assertWithin("inventory.catalog.publish.commits", counts.commits());
                assertEquals(published.eventIds(), signal.signalled.subList(signal.signalled.size() - 41,
                        signal.signalled.size()), "the dispatcher is signalled after the commit, in outbox_seq order");
            }
        }
    }

    private static List<CheckoutStockService.Line> lines(int size, int quantity) {
        List<CheckoutStockService.Line> lines = new ArrayList<>(size);
        for (int n = 1; n <= size; n++) lines.add(new CheckoutStockService.Line(InventoryTestSchema.sku(n), quantity));
        return lines;
    }
}
