package performance;

import database.JdbcSelectionStore;
import database.JdbcSubscriptionStore;
import database.NotificationStore;
import events.DecodedEvent;
import events.EnvelopeCodec;
import events.EventIds;
import events.EventTypes;
import notification.SaleSelectionHandler;
import notification.SubscriptionIndex;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.SoldLine;
import support.CountingConnections;
import support.NotificationTestSchema;
import support.StatementBudgets;
import support.TestSchema;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The selection transaction's statement budget (007 T046, contracts/notification-delivery.md "Budget"): on the warm
 * path, with the index at the clock's version, a sale costs the clock {@code FOR SHARE}, the {@code sale_selection}
 * INSERT, and one notification INSERT per 500 rows, in one commit: 3 statements for 1 ≤ n ≤ 500
 * ({@code notification.select.statements}), plus 1 per further 500. A catch-up after a subscription change adds one
 * history read, off the hot path.
 */
class SelectionStatementBudgetTest {

    @Test
    void aWarmSelectionStaysWithinItsStatementBudget() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (NotificationTestSchema schema = NotificationTestSchema.create(TestSchema.Options.defaults().decorator(counting::wrap))) {
            JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(schema.pool());
            for (int i = 0; i < 1_001; i++) {
                subscriptions.register(String.format("b%04d", i), new TreeSet<>(List.of(i < 3 ? "A" : "B")), "t" + i, null,
                        Instant.now());
            }
            SubscriptionIndex index = new SubscriptionIndex();
            SubscriptionStoreLoader.load(subscriptions, index);
            List<List<NotificationStore.Pending>> handed = new ArrayList<>();
            SaleSelectionHandler handler = new SaleSelectionHandler(new JdbcSelectionStore(schema.pool(), Clock.systemUTC()),
                    index, handed::add);

            measure(counting, handler, sale("warm-1", "A"), 3);
            measure(counting, handler, sale("warm-2", "C"), 2);
            measure(counting, handler, sale("warm-3", "A", "B"), 2 + 3, "1,001 notifications in three INSERTs");
            assertEquals(List.of(3, 1_001), handed.stream().map(List::size).toList(), "a zero-recipient sale hands nothing over");

            try (CountingConnections.Scope scope = counting.scope()) {
                handler.handle(sale("warm-1", "A"));
                assertEquals(1, scope.counts().inserts(), "a duplicate tries its selection INSERT and rolls back");
                assertEquals(0, scope.counts().commits());
            }

            subscriptions.register("late", new TreeSet<>(List.of("A")), "t-late", null, Instant.now());
            try (CountingConnections.Scope scope = counting.scope()) {
                handler.handle(sale("cold-1", "A"));
                assertEquals(4, scope.counts().statements(), "a catch-up adds one history read");
                assertEquals(1, scope.counts().commits());
            }
            assertEquals(4, schema.queryLong("SELECT recipient_count FROM sale_selection WHERE sale_id = 'cold-1'"));
        }
    }

    private static void measure(CountingConnections counting, SaleSelectionHandler handler, DecodedEvent event, long expected) {
        measure(counting, handler, event, expected, "");
    }

    private static void measure(CountingConnections counting, SaleSelectionHandler handler, DecodedEvent event, long expected,
                                String note) {
        try (CountingConnections.Scope scope = counting.scope()) {
            handler.handle(event);
            CountingConnections.Counts counts = scope.counts();
            assertEquals(expected, counts.statements(), note + " " + counts);
            long further = Math.max(0, expected - StatementBudgets.load().ceiling("notification.select.statements"));
            StatementBudgets.assertWithin("notification.select.statements", counts.statements() - further);
            StatementBudgets.assertWithin("notification.select.commits", counts.commits());
        }
    }

    private static DecodedEvent sale(String id, String... skus) {
        SaleCompleted.Builder sale = SaleCompleted.newBuilder().setSaleId(id).setTransactionId(id).setAttemptNumber(1)
                .setCompletedAtMs(System.currentTimeMillis()).setStationId("budget");
        for (String sku : skus) sale.addLines(SoldLine.newBuilder().setSku(sku).setQuantity(2));
        return EnvelopeCodec.decode(EnvelopeCodec.encode(EventIds.saleEventId(id), EventTypes.SALE_COMPLETED, id, id,
                EventTypes.TRANSACTION, System.currentTimeMillis(), sale.build()), id);
    }

    /** Builds the index as {@code SubscriptionService.loadIndex} does. */
    private static final class SubscriptionStoreLoader {
        static void load(JdbcSubscriptionStore store, SubscriptionIndex index) {
            var rows = store.loadIndex();
            index.rebuild(rows.skusBySubscriber(), rows.version());
        }
    }
}
