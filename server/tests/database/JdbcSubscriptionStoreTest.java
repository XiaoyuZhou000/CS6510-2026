package database;

import database.SubscriptionStore.Applied;
import database.SubscriptionStore.ChangeRow;
import database.SubscriptionStore.ChangeType;
import database.SubscriptionStore.Rejected;
import database.SubscriptionStore.Rejection;
import database.SubscriptionStore.Status;
import database.SubscriptionStore.SubscriberState;
import database.SubscriptionStore.Unchanged;
import faults.FaultHooks;
import notification.SubscriptionFailure;
import org.junit.jupiter.api.Test;
import support.NotificationTestSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The registration change transaction (007 T039, contracts/notification-delivery.md "Subscription change
 * transaction"): the clock is locked {@code FOR UPDATE}, an applied change advances it by one with one history row,
 * repeats and conflicts write nothing, and a failure before the commit leaves no trace.
 */
class JdbcSubscriptionStoreTest {
    private static final Instant AT = Instant.parse("2026-11-11T12:00:00Z");

    @Test
    void aNewSubscriberAdvancesTheClockWithOneRegisterChangeAndItsSubscriptions() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore store = new JdbcSubscriptionStore(schema.pool());
            Applied applied = assertInstanceOf(Applied.class, store.register("s1", skus("B", "A"), "selfcheckout.notify.s1.v1", null, AT));
            assertTrue(applied.created());
            assertEquals(new ChangeRow(1, "s1", ChangeType.REGISTER, List.of("A", "B"), List.of(), List.of("A", "B")),
                    applied.change());
            assertEquals(new SubscriberState("s1", Status.ACTIVE, List.of("A", "B"), "selfcheckout.notify.s1.v1", false, 1, 1),
                    applied.state());
            assertEquals(1, schema.subscriptionVersion());
            assertEquals(1, schema.subscriptionChanges());
            assertEquals("[\"A\",\"B\"]", schema.queryString("SELECT resulting_skus FROM subscription_change WHERE version = 1"));
            assertEquals("[]", schema.queryString("SELECT removed_skus FROM subscription_change WHERE version = 1"));
            assertEquals(2, schema.queryLong("SELECT COUNT(*) FROM subscription WHERE subscriber_id = 's1' AND added_version = 1"));

            assertInstanceOf(Applied.class, store.register("s2", skus("B"), "selfcheckout.notify.s2.v1", null, AT));
            assertEquals(2, schema.subscriptionVersion());
            assertEquals(List.of(1L, 2L), List.of(schema.queryLong("SELECT MIN(version) FROM subscription_change"),
                    schema.queryLong("SELECT MAX(version) FROM subscription_change")));
            assertEquals(Optional.of(applied.state()), store.find("s1"));
            assertEquals(Optional.empty(), store.find("nobody"));
        }
    }

    @Test
    void anIdenticalRepeatWritesNothingAndReturnsTheVersionThatProducedIt() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore store = new JdbcSubscriptionStore(schema.pool());
            store.register("s1", skus("A", "B"), "t1", null, AT);
            store.register("s2", skus("C"), "t2", null, AT);
            Unchanged repeat = assertInstanceOf(Unchanged.class, store.register("s1", skus("B", "A"), "t1", null, AT));
            assertEquals(1, repeat.state().updatedVersion());
            assertEquals(List.of("A", "B"), repeat.state().skus());
            assertEquals(2, schema.subscriptionVersion());
            assertEquals(2, schema.subscriptionChanges());
        }
    }

    @Test
    void aDifferentSetForAnActiveSubscriberIsRejectedBeforeUnknownSkusAndChangesNothing() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore store = new JdbcSubscriptionStore(schema.pool());
            store.register("s1", skus("A"), "t1", null, AT);
            Rejected exists = assertInstanceOf(Rejected.class, store.register("s1", skus("A", "B"), "t1", "B", AT));
            assertEquals(Rejection.SUBSCRIBER_EXISTS, exists.reason(), "the state conflict comes before the unknown SKU");
            Rejected unknown = assertInstanceOf(Rejected.class, store.register("s2", skus("A", "Z"), "t2", "Z", AT));
            assertEquals(Rejection.UNKNOWN_SKU, unknown.reason());
            assertEquals("Z", unknown.detail());
            assertEquals(1, schema.subscriptionVersion());
            assertEquals(1, schema.subscriptionChanges());
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM subscriber"));
            assertEquals(List.of("A"), store.find("s1").orElseThrow().skus());
        }
    }

    @Test
    void aCancelledSubscriberRegistersAgainKeepingItsIdentityAndChannel() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore store = new JdbcSubscriptionStore(schema.pool());
            store.register("s1", skus("A"), "t1", null, AT);
            store.markChannelReady("s1");
            // A cancellation as US4 records it: no subscriptions, CANCELLED, at version 2.
            schema.update("DELETE FROM subscription WHERE subscriber_id = 's1'");
            schema.update("UPDATE subscriber SET status = 'CANCELLED', updated_version = 2 WHERE subscriber_id = 's1'");
            schema.update("INSERT INTO subscription_change VALUES (2, 's1', 'CANCEL', '[]', '[\"A\"]', '[]', NOW(3))");
            schema.update("UPDATE subscription_clock SET version = 2 WHERE id = 1");

            Applied again = assertInstanceOf(Applied.class, store.register("s1", skus("C"), "ignored", null, AT));
            assertTrue(again.created());
            assertEquals(new SubscriberState("s1", Status.ACTIVE, List.of("C"), "t1", true, 1, 3), again.state());
            assertEquals(ChangeType.REGISTER, again.change().type());
            assertEquals(3, schema.subscriptionVersion());
        }
    }

    @Test
    void anInjectedFailureBeforeTheCommitLeavesNoRowAndTheClockUnchanged() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore store = new JdbcSubscriptionStore(schema.pool());
            String previous = System.getProperty(FaultHooks.PROPERTY);
            System.setProperty(FaultHooks.PROPERTY, "notification.subscription.beforeCommit:throw");
            try {
                assertThrows(FaultHooks.InjectedFault.class, () -> store.register("s1", skus("A"), "t1", null, AT));
            } finally {
                restore(previous);
            }
            assertEquals(0, schema.subscriptionVersion());
            assertEquals(0, schema.subscriptionChanges());
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM subscriber"));
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM subscription"));
            assertInstanceOf(Applied.class, store.register("s1", skus("A"), "t1", null, AT));
            assertEquals(1, schema.subscriptionVersion(), "the next change takes version 1");
        }
    }

    @Test
    void theClockIsLockedForUpdateSoAChangeWaitsForASelectionsSharedLock() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore store = new JdbcSubscriptionStore(schema.pool());
            ConnectionPool other = schema.newPool("ntf-share", 1, 1, null);
            Connection selection = other.borrow();
            try {
                try (PreparedStatement share = selection.prepareStatement(
                        "SELECT version FROM subscription_clock WHERE id = 1 FOR SHARE")) {
                    share.executeQuery().close();
                }
                CompletableFuture<SubscriptionStore.ChangeResult> change =
                        CompletableFuture.supplyAsync(() -> store.register("s1", skus("A"), "t1", null, AT));
                assertThrows(TimeoutException.class, () -> change.get(500, TimeUnit.MILLISECONDS),
                        "the change waits for the clock while the shared lock is held");
                selection.rollback();
                assertInstanceOf(Applied.class, change.get(10, TimeUnit.SECONDS));
            } finally {
                selection.rollback();
                other.release(selection);
            }
            assertEquals(1, schema.subscriptionVersion());
        }
    }

    @Test
    void theIndexAndTheHistoryAreReadAtOneVersion() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore store = new JdbcSubscriptionStore(schema.pool());
            store.register("s1", skus("A", "B"), "t1", null, AT);
            store.register("s2", skus("B"), "t2", null, AT);
            store.register("s3", skus("C"), "t3", null, AT);
            SubscriptionStore.IndexRows rows = store.loadIndex();
            assertEquals(3, rows.version());
            assertEquals(Map.of("s1", List.of("A", "B"), "s2", List.of("B"), "s3", List.of("C")), rows.skusBySubscriber());
            assertEquals(List.of(2L, 3L), store.changesAfter(1, 3).stream().map(ChangeRow::version).toList());
            assertEquals(List.of(), store.changesAfter(3, 3));
            assertEquals(3, store.currentVersion());
            assertEquals(Optional.of(new SubscriptionStore.Channel("s2", "t2", false)), store.channel("s2"));
            store.markChannelReady("s2");
            assertTrue(store.channel("s2").orElseThrow().ready());
        }
    }

    /** T082: UPDATE and CANCEL rows take contiguous versions, and the subscription rows follow resulting_skus. */
    @Test
    void updatesAndCancelsTakeContiguousVersionsAndTheSubscriptionRowsFollowTheResultingSkus() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore store = new JdbcSubscriptionStore(schema.pool());
            store.register("s1", skus("A", "B"), "t1", null, AT);
            store.register("s2", skus("B"), "t2", null, AT);

            Applied added = assertInstanceOf(Applied.class, store.update("s1", skus("C"), skus("A"), null, AT));
            assertFalse(added.created());
            assertEquals(new ChangeRow(3, "s1", ChangeType.UPDATE, List.of("C"), List.of("A"), List.of("B", "C")), added.change());
            assertEquals(new SubscriberState("s1", Status.ACTIVE, List.of("B", "C"), "t1", false, 1, 3), added.state());
            assertEquals(3, schema.queryLong("SELECT added_version FROM subscription WHERE subscriber_id = 's1' AND sku = 'C'"));
            assertEquals(1, schema.queryLong("SELECT added_version FROM subscription WHERE subscriber_id = 's1' AND sku = 'B'"),
                    "a kept SKU keeps the version that added it");

            Applied cancelled = assertInstanceOf(Applied.class, store.cancel("s2", AT));
            assertEquals(new ChangeRow(4, "s2", ChangeType.CANCEL, List.of(), List.of("B"), List.of()), cancelled.change());
            assertEquals(new SubscriberState("s2", Status.CANCELLED, List.of(), "t2", false, 2, 4), cancelled.state());
            assertEquals("CANCELLED", schema.queryString("SELECT status FROM subscriber WHERE subscriber_id = 's2'"));

            Applied again = assertInstanceOf(Applied.class, store.register("s2", skus("D"), "t2", null, AT));
            assertEquals(5, again.change().version());
            assertInstanceOf(Applied.class, store.update("s1", skus(), skus("B"), null, AT));

            assertEquals(6, schema.subscriptionVersion());
            assertEquals(6, schema.subscriptionChanges());
            assertEquals(1, schema.queryLong("SELECT MIN(version) FROM subscription_change"));
            assertEquals(6, schema.queryLong("SELECT MAX(version) FROM subscription_change"), "versions are contiguous");
            assertEquals("[\"C\"]", schema.queryString("SELECT resulting_skus FROM subscription_change WHERE version = 6"));
            // Every subscriber's subscription rows equal the resulting_skus of its latest change.
            for (String subscriber : List.of("s1", "s2")) {
                String latest = schema.queryString("SELECT resulting_skus FROM subscription_change WHERE subscriber_id = ? "
                        + "ORDER BY version DESC LIMIT 1", subscriber);
                assertEquals(latest, NotificationJson.skuArray(store.find(subscriber).orElseThrow().skus()), subscriber);
            }
            assertEquals(Map.of("s1", List.of("C"), "s2", List.of("D")), store.loadIndex().skusBySubscriber());
            assertEquals(6, store.find("s1").orElseThrow().updatedVersion());
        }
    }

    @Test
    void aNoOpOrARejectedUpdateOrCancelWritesNoRow() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore store = new JdbcSubscriptionStore(schema.pool());
            store.register("s1", skus("A", "B"), "t1", null, AT);
            store.register("s2", skus("C"), "t2", null, AT);
            store.cancel("s2", AT);

            Unchanged noOp = assertInstanceOf(Unchanged.class, store.update("s1", skus("A"), skus("Z"), null, AT));
            assertEquals(1, noOp.state().updatedVersion(), "a no-op answers the current version");
            Unchanged repeat = assertInstanceOf(Unchanged.class, store.cancel("s2", AT));
            assertEquals(3, repeat.state().updatedVersion(), "a repeated cancel answers the version of the first");
            assertEquals(Rejection.LAST_SKU,
                    assertInstanceOf(Rejected.class, store.update("s1", skus(), skus("A", "B"), null, AT)).reason());
            assertEquals(Rejection.SUBSCRIBER_CANCELLED,
                    assertInstanceOf(Rejected.class, store.update("s2", skus("A"), skus(), null, AT)).reason());
            assertEquals(Rejection.SUBSCRIBER_NOT_FOUND,
                    assertInstanceOf(Rejected.class, store.update("nobody", skus("A"), skus(), null, AT)).reason());
            assertEquals(Rejection.SUBSCRIBER_NOT_FOUND, assertInstanceOf(Rejected.class, store.cancel("nobody", AT)).reason());
            assertEquals(Rejection.UNKNOWN_SKU,
                    assertInstanceOf(Rejected.class, store.update("s1", skus("Z"), skus(), "Z", AT)).reason());

            assertEquals(3, schema.subscriptionVersion());
            assertEquals(3, schema.subscriptionChanges());
            assertEquals(List.of("A", "B"), store.find("s1").orElseThrow().skus());
        }
    }

    @Test
    void anInjectedFailureBeforeAnUpdateOrCancelCommitsLeavesNoRowTheClockAndTheIndexUnchanged() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create();
             support.InProcessNotification notification = support.InProcessNotification.over(schema)) {
            notification.publishCatalog("v1", List.of("A", "B", "C"));
            notification.awaitCatalog(java.time.Duration.ofSeconds(10));
            notification.service().register("s1", List.of("A", "B"));
            var before = notification.index().snapshot();
            String previous = System.getProperty(FaultHooks.PROPERTY);
            System.setProperty(FaultHooks.PROPERTY, "notification.subscription.beforeCommit:throw");
            try {
                for (Runnable change : List.<Runnable>of(
                        () -> notification.service().update("s1", List.of("C"), List.of("A")),
                        () -> notification.service().cancel("s1"))) {
                    SubscriptionFailure failure = assertThrows(SubscriptionFailure.class, change::run);
                    assertEquals(SubscriptionFailure.Code.INTERNAL_ERROR, failure.code());
                }
            } finally {
                restore(previous);
            }
            assertEquals(1, schema.subscriptionVersion());
            assertEquals(1, schema.subscriptionChanges());
            assertEquals(2, schema.queryLong("SELECT COUNT(*) FROM subscription WHERE subscriber_id = 's1'"));
            assertEquals("ACTIVE", schema.queryString("SELECT status FROM subscriber WHERE subscriber_id = 's1'"));
            assertEquals(before, notification.index().snapshot(), "the index is untouched");
        }
    }

    private static TreeSet<String> skus(String... skus) {
        return new TreeSet<>(List.of(skus));
    }

    private static void restore(String previous) {
        if (previous == null) System.clearProperty(FaultHooks.PROPERTY);
        else System.setProperty(FaultHooks.PROPERTY, previous);
    }
}
