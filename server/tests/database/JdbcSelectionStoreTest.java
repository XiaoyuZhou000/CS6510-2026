package database;

import database.NotificationJson.Line;
import database.SelectionStore.AlreadySelected;
import database.SelectionStore.Plan;
import database.SelectionStore.PlannedNotification;
import database.SelectionStore.Sale;
import database.SelectionStore.Selected;
import events.EventIds;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.NotificationTestSchema;
import support.TestSchema;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The selection transaction (007 T040, contracts/notification-delivery.md "Selection transaction"): the clock is held
 * {@code FOR SHARE} and its version recorded, a duplicate sale inserts nothing, notifications go in chunks of at most
 * 500 rows, a zero-recipient sale inserts only its selection, and catch-up reads the history in version order.
 */
class JdbcSelectionStoreTest {
    private static final Instant NOW = Instant.parse("2026-11-11T12:00:00.123Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void theSelectionRecordsTheLockedVersionAndItsRows() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(schema.pool());
            subscriptions.register("s1", new TreeSet<>(List.of("A", "B")), "t1", null, NOW);
            subscriptions.register("s2", new TreeSet<>(List.of("B")), "t2", null, NOW);
            JdbcSelectionStore store = new JdbcSelectionStore(schema.pool(), CLOCK);
            AtomicLong seen = new AtomicLong(-1);
            Selected selected = assertInstanceOf(Selected.class, store.select(sale("tx-1", new Line("A", 3), new Line("B", 1)),
                    (version, changes) -> {
                        seen.set(version);
                        return plan("tx-1", List.of(new PlannedNotification(EventIds.notificationId("tx-1", "A", "s1"), "A", "s1", 3),
                                new PlannedNotification(EventIds.notificationId("tx-1", "B", "s1"), "B", "s1", 1),
                                new PlannedNotification(EventIds.notificationId("tx-1", "B", "s2"), "B", "s2", 1)), 2);
                    }));
            assertEquals(2, seen.get(), "the planner sees the clock version");
            assertEquals(2, selected.subscriptionVersion());
            assertEquals(NOW, selected.selectedAt());
            assertEquals(3, selected.notifications().size());
            assertEquals(2, schema.queryLong("SELECT subscription_version FROM sale_selection WHERE sale_id = 'tx-1'"));
            assertEquals(2, schema.queryLong("SELECT recipient_count FROM sale_selection WHERE sale_id = 'tx-1'"));
            assertEquals(3, schema.queryLong("SELECT notification_count FROM sale_selection WHERE sale_id = 'tx-1'"));
            assertEquals(EventIds.saleEventId("tx-1"), schema.queryString("SELECT sale_event_id FROM sale_selection"));
            assertEquals("[{\"sku\":\"A\",\"quantity\":3},{\"sku\":\"B\",\"quantity\":1}]",
                    schema.queryString("SELECT `lines` FROM sale_selection"));
            assertEquals(3, schema.notifications("PENDING"));
            assertEquals(3, schema.queryLong("SELECT quantity FROM notification WHERE sku = 'A'"));
            assertEquals(3, schema.queryLong("SELECT COUNT(*) FROM notification WHERE next_attempt_at = created_at "
                    + "AND state = 'PENDING' AND attempt_count = 0"), "every row is PENDING and due at once");
            assertEquals(NOW, selected.notifications().get(0).createdAt());
        }
    }

    @Test
    void aDuplicateSaleReturnsAlreadySelectedAndInsertsNothing() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSelectionStore store = new JdbcSelectionStore(schema.pool(), CLOCK);
            List<PlannedNotification> one = List.of(new PlannedNotification(EventIds.notificationId("tx-1", "A", "s1"), "A", "s1", 1));
            assertInstanceOf(Selected.class, store.select(sale("tx-1", new Line("A", 1)), (v, c) -> plan("tx-1", one, 1)));
            assertInstanceOf(AlreadySelected.class, store.select(sale("tx-1", new Line("A", 1)), (v, c) -> plan("tx-1", one, 1)));
            assertEquals(1, schema.saleSelections());
            assertEquals(1, schema.notifications());
        }
    }

    @Test
    void notificationsAreInsertedInChunksOfAtMost500Rows() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (NotificationTestSchema schema = NotificationTestSchema.create(TestSchema.Options.defaults().decorator(counting::wrap))) {
            JdbcSelectionStore store = new JdbcSelectionStore(schema.pool(), CLOCK);
            List<PlannedNotification> many = new ArrayList<>();
            for (int i = 0; i < 1_201; i++) {
                String subscriber = String.format("s%04d", i);
                many.add(new PlannedNotification(EventIds.notificationId("tx-big", "A", subscriber), "A", subscriber, 2));
            }
            try (CountingConnections.Scope scope = counting.scope()) {
                assertInstanceOf(Selected.class, store.select(sale("tx-big", new Line("A", 2)), (v, c) -> plan("tx-big", many, many.size())));
                CountingConnections.Counts counts = scope.counts();
                assertEquals(3, counts.inserts() - 1, "1,201 notifications in three INSERTs, besides the selection's");
                assertEquals(1, counts.selects(), "the clock, held FOR SHARE");
                assertEquals(1, counts.commits());
            }
            assertEquals(1_201, schema.notifications("PENDING"));
            assertEquals(1_201, schema.queryLong("SELECT notification_count FROM sale_selection"));
        }
    }

    @Test
    void aZeroRecipientSaleInsertsOnlyItsSelection() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (NotificationTestSchema schema = NotificationTestSchema.create(TestSchema.Options.defaults().decorator(counting::wrap))) {
            JdbcSelectionStore store = new JdbcSelectionStore(schema.pool(), CLOCK);
            try (CountingConnections.Scope scope = counting.scope()) {
                Selected selected = assertInstanceOf(Selected.class,
                        store.select(sale("tx-0", new Line("C", 2)), (v, c) -> plan("tx-0", List.of(), 0)));
                assertTrue(selected.notifications().isEmpty());
                assertEquals(1, scope.counts().inserts());
            }
            assertEquals(1, schema.saleSelections());
            assertEquals(0, schema.queryLong("SELECT recipient_count FROM sale_selection"));
            assertEquals(0, schema.notifications());
        }
    }

    @Test
    void catchUpReadsTheHistoryInVersionOrderOnTheSelectionsConnection() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(schema.pool());
            for (String id : List.of("s1", "s2", "s3")) subscriptions.register(id, new TreeSet<>(List.of("A")), "t-" + id, null, NOW);
            JdbcSelectionStore store = new JdbcSelectionStore(schema.pool(), CLOCK);
            List<Long> versions = new ArrayList<>();
            store.select(sale("tx-1", new Line("A", 1)), (version, changes) -> {
                changes.changesAfter(1, version).forEach(row -> versions.add(row.version()));
                return plan("tx-1", List.of(), 0);
            });
            assertEquals(List.of(2L, 3L), versions);
        }
    }

    @Test
    void theClockIsHeldForShareSoAChangeCannotCommitDuringASelection() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(schema.pool());
            JdbcSelectionStore store = new JdbcSelectionStore(schema.pool(), CLOCK);
            CompletableFuture<Void> planning = new CompletableFuture<>();
            CompletableFuture<Void> release = new CompletableFuture<>();
            CompletableFuture<SelectionStore.Result> selection = CompletableFuture.supplyAsync(() ->
                    store.select(sale("tx-1", new Line("A", 1)), (version, changes) -> {
                        planning.complete(null);
                        release.join();
                        return plan("tx-1", List.of(), 0);
                    }));
            planning.get(10, TimeUnit.SECONDS);
            CompletableFuture<SubscriptionStore.ChangeResult> change =
                    CompletableFuture.supplyAsync(() -> subscriptions.register("s1", new TreeSet<>(List.of("A")), "t1", null, NOW));
            assertThrows(TimeoutException.class, () -> change.get(500, TimeUnit.MILLISECONDS));
            release.complete(null);
            assertEquals(0, assertInstanceOf(Selected.class, selection.get(10, TimeUnit.SECONDS)).subscriptionVersion());
            assertInstanceOf(SubscriptionStore.Applied.class, change.get(10, TimeUnit.SECONDS));
            assertEquals(1, schema.subscriptionVersion());
        }
    }

    private static Sale sale(String id, Line... lines) {
        return new Sale(id, EventIds.saleEventId(id), NOW.minusSeconds(1), List.of(lines));
    }

    private static Plan plan(String saleId, List<PlannedNotification> notifications, int recipients) {
        return new Plan(notifications, recipients);
    }
}
