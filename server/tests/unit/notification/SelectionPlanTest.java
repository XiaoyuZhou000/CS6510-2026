package unit.notification;

import database.NotificationJson.Line;
import database.SelectionStore;
import events.EventIds;
import notification.SelectionPlan;
import notification.SubscriptionIndex;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Recipient Selection Rule on one sale (007 T037): exactly one planned notification per (SKU, subscriber) for
 * subscribed SKUs only, with the line's quantity and the deterministic notification ID, and {@code recipient_count}
 * counting distinct subscribers.
 */
class SelectionPlanTest {

    @Test
    void eachSubscribedSkuGivesOneNotificationPerSubscriberWithTheLineQuantity() {
        SubscriptionIndex index = index(Map.of("s1", List.of("A", "B"), "s2", List.of("B")));
        SelectionStore.Plan plan = SelectionPlan.plan("tx-1", List.of(new Line("A", 3), new Line("B", 1), new Line("C", 2)),
                index.snapshot());
        assertEquals(List.of(
                new SelectionStore.PlannedNotification(EventIds.notificationId("tx-1", "A", "s1"), "A", "s1", 3),
                new SelectionStore.PlannedNotification(EventIds.notificationId("tx-1", "B", "s1"), "B", "s1", 1),
                new SelectionStore.PlannedNotification(EventIds.notificationId("tx-1", "B", "s2"), "B", "s2", 1)),
                plan.notifications());
        assertEquals(2, plan.recipientCount(), "s1 is one recipient for two SKUs");
        assertEquals(3, plan.notifications().stream().map(SelectionStore.PlannedNotification::notificationId).distinct().count());
    }

    @Test
    void aSaleWithNoSubscribedSkuGivesZeroNotificationsAndZeroRecipients() {
        SubscriptionIndex index = index(Map.of("s1", List.of("A")));
        SelectionStore.Plan plan = SelectionPlan.plan("tx-2", List.of(new Line("C", 2), new Line("D", 1)), index.snapshot());
        assertTrue(plan.notifications().isEmpty());
        assertEquals(0, plan.recipientCount());
    }

    @Test
    void theIdentityIsStableAndDiffersPerSaleSkuAndSubscriber() {
        SubscriptionIndex index = index(Map.of("s1", List.of("A")));
        String first = SelectionPlan.plan("tx-3", List.of(new Line("A", 1)), index.snapshot()).notifications().get(0).notificationId();
        String again = SelectionPlan.plan("tx-3", List.of(new Line("A", 5)), index.snapshot()).notifications().get(0).notificationId();
        String other = SelectionPlan.plan("tx-4", List.of(new Line("A", 1)), index.snapshot()).notifications().get(0).notificationId();
        assertEquals(first, again, "the same (sale, SKU, subscriber) always has the same identity");
        assertNotEquals(first, other);
        assertThrows(IllegalArgumentException.class,
                () -> SelectionPlan.plan("tx-5", List.of(new Line("A", 0)), index.snapshot()));
    }

    private static SubscriptionIndex index(Map<String, List<String>> subscriptions) {
        SubscriptionIndex index = new SubscriptionIndex();
        index.rebuild(subscriptions, 1);
        return index;
    }
}
