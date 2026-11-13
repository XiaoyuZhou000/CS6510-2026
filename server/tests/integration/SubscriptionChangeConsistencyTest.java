package integration;

import database.ConnectionPool;
import database.JdbcSubscriptionStore;
import faults.FaultHooks;
import notification.SubscriptionFailure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import support.InProcessCheckout;
import support.InProcessNotification;
import support.InventoryTestSchema;
import support.NotificationTestSchema;
import transaction.TransactionOperations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 007 US4 acceptance scenarios 1–8 (T084) on the real Transaction, Inventory, and Notification components over one
 * in-memory bus: each confirmed change is in the next sale's selection, an already-selected notification survives a
 * cancellation, a failed change has no effect anywhere, concurrent changes during sales leave every selection equal to
 * the history replayed to its version, a restart rebuilds the index from committed data, and two Notification
 * instances sharing one schema both catch up to the version their selection locks.
 */
class SubscriptionChangeConsistencyTest {
    private static final List<String> SKUS = List.of(InventoryTestSchema.sku(1), InventoryTestSchema.sku(2),
            InventoryTestSchema.sku(3), InventoryTestSchema.sku(4), InventoryTestSchema.sku(5));
    private static final String A = SKUS.get(0);
    private static final String B = SKUS.get(1);
    private static final Duration WAIT = Duration.ofSeconds(30);

    @AfterEach
    void clearFaults() {
        System.clearProperty(FaultHooks.PROPERTY);
    }

    /** Scenarios 1–3: a confirmed register, add, remove, and cancel are each reflected in the next sale. */
    @Test
    void eachConfirmedChangeIsReflectedInTheNextSale() throws Exception {
        try (Flow flow = Flow.start()) {
            long registered = flow.service().register("s1", List.of(A)).state().updatedVersion();
            String saleA = flow.sell(A);
            flow.await(() -> flow.selected(saleA), "the sale of A is selected");
            assertEquals(Set.of(A + "/s1"), flow.recipients(saleA));
            assertTrue(flow.selectionVersion(saleA) >= registered);
            flow.await(() -> flow.delivered("s1") == 1, "s1 receives its notification for A");

            long added = flow.service().update("s1", List.of(B), List.of()).updatedVersion();
            String saleB = flow.sell(B);
            flow.await(() -> flow.selected(saleB), "the sale of B is selected");
            assertEquals(Set.of(B + "/s1"), flow.recipients(saleB), "the added SKU is selected");
            assertTrue(flow.selectionVersion(saleB) >= added);

            long removed = flow.service().update("s1", List.of(), List.of(B)).updatedVersion();
            String saleB2 = flow.sell(B);
            flow.await(() -> flow.selected(saleB2), "the second sale of B is selected");
            assertEquals(Set.of(), flow.recipients(saleB2), "the removed SKU is not selected");
            assertTrue(flow.selectionVersion(saleB2) >= removed);

            long cancelled = flow.service().cancel("s1").updatedVersion();
            String saleA2 = flow.sell(A, B);
            flow.await(() -> flow.selected(saleA2), "the sale after the cancellation is selected");
            assertEquals(Set.of(), flow.recipients(saleA2), "a cancelled subscriber is selected for nothing");
            assertTrue(flow.selectionVersion(saleA2) >= cancelled);
            assertEquals(List.of(1L, 2L, 3L, 4L), List.of(registered, added, removed, cancelled));
            flow.assertEverySelectionFollowsTheHistory();
        }
    }

    /** Scenario 4: a notification selected before a cancellation is still delivered. */
    @Test
    void anAlreadySelectedNotificationSurvivesACancellationAndARemoval() throws Exception {
        try (Flow flow = Flow.start()) {
            flow.service().register("s1", List.of(A));
            flow.service().register("s2", List.of(A, B));
            // Hold every send to s1 and s2 for 2 s, so both changes land between the selection and the delivery.
            System.setProperty(FaultHooks.PROPERTY, "notification.channel.stall:s1,notification.channel.stall:s2");
            String sale = flow.sell(A, B);
            flow.await(() -> flow.selected(sale), "the sale is selected");
            assertEquals(Set.of(A + "/s1", A + "/s2", B + "/s2"), flow.recipients(sale));
            flow.service().cancel("s1");
            flow.service().update("s2", List.of(), List.of(B));
            assertEquals(0, flow.delivered("s1"), "the send is still held");
            System.clearProperty(FaultHooks.PROPERTY);
            flow.await(() -> flow.delivered("s1") == 1 && flow.delivered("s2") == 2,
                    "the selected notifications are delivered after the changes");
            assertEquals(0, flow.schema.notifications("PENDING"));
        }
    }

    /** Scenario 5: a change that fails before its commit has no effect on selections, rows, or the index. */
    @Test
    void aFailedChangeHasNoEffect() throws Exception {
        try (Flow flow = Flow.start()) {
            flow.service().register("s1", List.of(A));
            var before = flow.notification.index().snapshot();
            System.setProperty(FaultHooks.PROPERTY, "notification.subscription.beforeCommit:throw");
            for (Runnable change : List.<Runnable>of(() -> flow.service().update("s1", List.of(B), List.of()),
                    () -> flow.service().cancel("s1"), () -> flow.service().register("s2", List.of(B)))) {
                SubscriptionFailure failure = assertThrows(SubscriptionFailure.class, change::run);
                assertEquals(SubscriptionFailure.Code.INTERNAL_ERROR, failure.code(), "the requester is told it was not applied");
            }
            System.clearProperty(FaultHooks.PROPERTY);
            assertEquals(before, flow.notification.index().snapshot(), "the index reflects the state before the failures");
            assertEquals(1, flow.schema.subscriptionVersion());
            assertEquals(1, flow.schema.subscriptionChanges());

            String sale = flow.sell(A, B);
            flow.await(() -> flow.selected(sale), "the sale is selected");
            assertEquals(Set.of(A + "/s1"), flow.recipients(sale), "only the committed subscription is selected");
            assertEquals(1, flow.selectionVersion(sale));
        }
    }

    /** Scenario 6: changes committed while sales complete; every selection equals the history at its version. */
    @Test
    void concurrentChangesDuringSalesLeaveEverySelectionEqualToTheReplayedHistory() throws Exception {
        try (Flow flow = Flow.start()) {
            for (int i = 1; i <= 4; i++) flow.service().register("s" + i, List.of(SKUS.get(i % SKUS.size())));
            AtomicLong confirmed = new AtomicLong(flow.schema.subscriptionVersion());
            AtomicBoolean stop = new AtomicBoolean();
            Thread changes = new Thread(() -> randomChanges(flow, new Random(4), confirmed, stop), "changes");
            changes.start();
            Random random = new Random(6);
            Map<String, Long> confirmedBefore = new HashMap<>();
            try {
                for (int i = 0; i < 40; i++) {
                    String[] basket = new String[1 + random.nextInt(3)];
                    for (int j = 0; j < basket.length; j++) basket[j] = SKUS.get(random.nextInt(SKUS.size()));
                    long before = confirmed.get();
                    confirmedBefore.put(flow.sell(basket), before);
                }
            } finally {
                stop.set(true);
                changes.join(WAIT.toMillis());
            }
            assertFalse(changes.isAlive());
            flow.await(() -> confirmedBefore.keySet().stream().allMatch(flow::selected), "every sale is selected");
            assertTrue(flow.schema.subscriptionVersion() > 10, "changes ran during the sales");
            for (Map.Entry<String, Long> sale : confirmedBefore.entrySet()) {
                assertTrue(flow.selectionVersion(sale.getKey()) >= sale.getValue(),
                        "sale " + sale.getKey() + " includes every change confirmed before its Complete");
            }
            flow.assertEverySelectionFollowsTheHistory();
        }
    }

    /** Scenario 7: after a restart the index is rebuilt from committed data, and selections follow the rule. */
    @Test
    void afterARestartTheRebuiltIndexEqualsTheCommittedSubscriptions() throws Exception {
        try (Flow flow = Flow.start()) {
            flow.service().register("s1", List.of(A, B));
            flow.service().register("s2", List.of(B));
            flow.service().update("s1", List.of(SKUS.get(2)), List.of(A));
            flow.service().cancel("s2");
            flow.notification.restart();
            flow.notification.awaitCatalog(WAIT);
            var rows = new JdbcSubscriptionStore(flow.schema.pool()).loadIndex();
            assertEquals(rows.version(), flow.notification.index().version());
            Map<String, Set<String>> expected = new TreeMap<>();
            rows.skusBySubscriber().forEach((subscriber, skus) -> expected.put(subscriber, new TreeSet<>(skus)));
            assertEquals(expected, new TreeMap<>(flow.notification.index().snapshot().subscriptions()));
            assertEquals(Map.of("s1", Set.of(B, SKUS.get(2))), expected);

            String sale = flow.sell(A, B, SKUS.get(2));
            flow.await(() -> flow.selected(sale), "the sale after the restart is selected");
            assertEquals(Set.of(B + "/s1", SKUS.get(2) + "/s1"), flow.recipients(sale));
            flow.assertEverySelectionFollowsTheHistory();
        }
    }

    /** Scenario 8: rejected requests change nothing. */
    @Test
    void rejectedRequestsChangeNothing() throws Exception {
        try (Flow flow = Flow.start()) {
            flow.service().register("s1", List.of(A));
            for (Runnable request : List.<Runnable>of(() -> flow.service().register("s2", List.of()),
                    () -> flow.service().register("s2", List.of("SKU-999999")),
                    () -> flow.service().register("s1", List.of(A, B)),
                    () -> flow.service().update("nobody", List.of(A), List.of()),
                    () -> flow.service().cancel("nobody"),
                    () -> flow.service().update("s1", List.of(), List.of(A)))) {
                assertThrows(SubscriptionFailure.class, request::run);
            }
            assertEquals(1, flow.schema.subscriptionVersion());
            assertEquals(1, flow.schema.subscriptionChanges());
            assertEquals(List.of(A), flow.service().find("s1").skus());
        }
    }

    /** FR-008: two Notification processes on one schema; each selection catches up to the version it locks. */
    @Test
    void twoInstancesSharingOneSchemaEachCatchUpToTheLockedVersion() throws Exception {
        try (Flow flow = Flow.start();
             InProcessNotification second = InProcessNotification.over(flow.schema.pool(), flow.saga.bus, flow.saga.names)) {
            second.awaitCatalog(WAIT);
            Random random = new Random(8);
            Map<String, Long> confirmedBefore = new HashMap<>();
            for (int i = 0; i < 24; i++) {
                // Changes alternate between the two instances, so each one's index misses the other's changes.
                InProcessNotification changer = i % 2 == 0 ? flow.notification : second;
                String subscriber = "s" + (1 + random.nextInt(4));
                String sku = SKUS.get(random.nextInt(SKUS.size()));
                long version;
                try {
                    version = changer.service().register(subscriber, List.of(sku)).state().updatedVersion();
                } catch (SubscriptionFailure exists) {
                    version = changer.service().cancel(subscriber).updatedVersion();
                }
                confirmedBefore.put(flow.sell(sku, SKUS.get(random.nextInt(SKUS.size()))), version);
            }
            flow.await(() -> confirmedBefore.keySet().stream().allMatch(flow::selected), "every sale is selected");
            for (Map.Entry<String, Long> sale : confirmedBefore.entrySet()) {
                assertTrue(flow.selectionVersion(sale.getKey()) >= sale.getValue(), "sale " + sale.getKey());
            }
            flow.assertEverySelectionFollowsTheHistory();
        }
    }

    private static void randomChanges(Flow flow, Random random, AtomicLong confirmed, AtomicBoolean stop) {
        while (!stop.get()) {
            String subscriber = "s" + (1 + random.nextInt(6));
            String sku = SKUS.get(random.nextInt(SKUS.size()));
            try {
                long version = switch (random.nextInt(4)) {
                    case 0 -> flow.service().register(subscriber, List.of(sku)).state().updatedVersion();
                    case 1 -> flow.service().update(subscriber, List.of(sku), List.of()).updatedVersion();
                    case 2 -> flow.service().update(subscriber, List.of(SKUS.get(random.nextInt(SKUS.size()))), List.of(sku))
                            .updatedVersion();
                    default -> flow.service().cancel(subscriber).updatedVersion();
                };
                confirmed.accumulateAndGet(version, Math::max);
            } catch (SubscriptionFailure rejected) {
                // Not found, cancelled, last SKU, or exists: a defined rejection, nothing changed.
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException interrupted) {
                return;
            }
        }
    }

    /** Checkout, Inventory, and Notification on one bus, with the catalog loaded; remembers what each sale sold. */
    private static final class Flow implements AutoCloseable {
        final InProcessCheckout saga;
        final NotificationTestSchema schema;
        final InProcessNotification notification;
        final Map<String, Set<String>> sold = new ConcurrentHashMap<>();

        private Flow(InProcessCheckout saga, NotificationTestSchema schema, InProcessNotification notification) {
            this.saga = saga;
            this.schema = schema;
            this.notification = notification;
        }

        static Flow start() throws Exception {
            InProcessCheckout saga = InProcessCheckout.start(SKUS.size(), 1000);
            NotificationTestSchema schema = NotificationTestSchema.create();
            InProcessNotification notification = InProcessNotification.over(schema.pool(), saga.bus, saga.names);
            saga.inventory.publishCatalog();
            notification.awaitCatalog(WAIT);
            return new Flow(saga, schema, notification);
        }

        notification.SubscriptionService service() {
            return notification.service();
        }

        /** Completes a sale of {@code skus}; Complete returns only after the sale committed. */
        String sell(String... skus) {
            TransactionOperations service = saga.checkout();
            String id = service.start(new TransactionOperations.StartCommand("change-station")).transactionId();
            for (String sku : skus) service.scan(new TransactionOperations.ScanCommand(id, sku));
            sold.put(id, new TreeSet<>(List.of(skus)));
            service.complete(id);
            return id;
        }

        boolean selected(String sale) {
            return query("SELECT COUNT(*) FROM sale_selection WHERE sale_id = ?", sale).get(0).get(0).equals("1");
        }

        long selectionVersion(String sale) {
            return Long.parseLong(query("SELECT subscription_version FROM sale_selection WHERE sale_id = ?", sale).get(0).get(0));
        }

        /** The sale's notification keys, as {@code sku/subscriber}. */
        Set<String> recipients(String sale) {
            Set<String> keys = new TreeSet<>();
            for (List<String> row : query("SELECT sku, subscriber_id FROM notification WHERE sale_id = ?", sale)) {
                keys.add(row.get(0) + "/" + row.get(1));
            }
            return keys;
        }

        long delivered(String subscriber) {
            return Long.parseLong(query("SELECT COUNT(*) FROM notification WHERE subscriber_id = ? AND state = 'DELIVERED'",
                    subscriber).get(0).get(0));
        }

        /**
         * FR-010 for every recorded selection: replaying {@code subscription_change} to the selection's version gives
         * exactly the recipients of the sale's notification rows, and recipient_count matches.
         */
        void assertEverySelectionFollowsTheHistory() {
            List<List<String>> history = query("SELECT version, subscriber_id, change_type, resulting_skus "
                    + "FROM subscription_change ORDER BY version");
            for (int i = 0; i < history.size(); i++) {
                assertEquals(i + 1, Long.parseLong(history.get(i).get(0)), "the history versions are contiguous");
            }
            for (List<String> selection : query("SELECT sale_id, subscription_version, recipient_count FROM sale_selection")) {
                String sale = selection.get(0);
                long version = Long.parseLong(selection.get(1));
                Map<String, Set<String>> state = new HashMap<>();
                for (List<String> change : history) {
                    if (Long.parseLong(change.get(0)) > version) break;
                    Set<String> skus = new TreeSet<>(database.NotificationJson.parseSkuArray(change.get(3)));
                    if (change.get(2).equals("CANCEL") || skus.isEmpty()) state.remove(change.get(1));
                    else state.put(change.get(1), skus);
                }
                Set<String> expected = new TreeSet<>();
                Set<String> recipients = new TreeSet<>();
                for (String sku : sold.getOrDefault(sale, Set.of())) {
                    state.forEach((subscriber, skus) -> {
                        if (skus.contains(sku)) {
                            expected.add(sku + "/" + subscriber);
                            recipients.add(subscriber);
                        }
                    });
                }
                assertEquals(expected, recipients(sale), "sale " + sale + " at version " + version);
                assertEquals(recipients.size(), Integer.parseInt(selection.get(2)), "recipient_count of " + sale);
            }
        }

        List<List<String>> query(String sql, Object... parameters) {
            try {
                ConnectionPool pool = schema.pool();
                Connection connection = pool.borrow();
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (int i = 0; i < parameters.length; i++) statement.setObject(i + 1, parameters[i]);
                    List<List<String>> rows = new ArrayList<>();
                    try (ResultSet result = statement.executeQuery()) {
                        int columns = result.getMetaData().getColumnCount();
                        while (result.next()) {
                            List<String> row = new ArrayList<>(columns);
                            for (int c = 1; c <= columns; c++) row.add(result.getString(c));
                            rows.add(row);
                        }
                    }
                    connection.commit();
                    return rows;
                } finally {
                    pool.release(connection);
                }
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        }

        void await(BooleanSupplier condition, String what) throws InterruptedException {
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (!condition.getAsBoolean()) {
                assertTrue(System.nanoTime() < deadline, "timed out waiting: " + what);
                Thread.sleep(20);
            }
        }

        @Override
        public void close() throws Exception {
            try (saga; schema) {
                notification.close();
            }
        }
    }
}
