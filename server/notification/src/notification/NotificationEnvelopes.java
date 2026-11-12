package notification;

import database.NotificationStore;
import events.EnvelopeCodec;
import events.EventTypes;
import selfcheckout.events.v1.SubscriberNotified;

/**
 * The {@code SubscriberNotified} record of one stored {@code notification} row (007 contracts/sale-completed-event.md,
 * research N7): {@code event_id} = {@code notification_id}, {@code routing_key} = {@code subscriber_id},
 * {@code correlation_id} = {@code sale_id}, {@code producer} = {@code notification}, and {@code occurred_at_ms} =
 * the selection time. Every field comes from the row, so a re-send carries exactly the same record (FR-014).
 */
public final class NotificationEnvelopes {
    private NotificationEnvelopes() { }

    public static SubscriberNotified payload(NotificationStore.Pending row) {
        return SubscriberNotified.newBuilder()
                .setNotificationId(row.notificationId())
                .setSubscriberId(row.subscriberId())
                .setSaleId(row.saleId())
                .setSku(row.sku())
                .setQuantity(row.quantity())
                .setCompletedAtMs(row.completedAt().toEpochMilli())
                .setSubscriptionVersion(row.subscriptionVersion())
                .build();
    }

    public static byte[] envelope(NotificationStore.Pending row) {
        return EnvelopeCodec.encode(row.notificationId(), EventTypes.SUBSCRIBER_NOTIFIED, row.subscriberId(), row.saleId(),
                EventTypes.NOTIFICATION, row.createdAt().toEpochMilli(), payload(row));
    }
}
