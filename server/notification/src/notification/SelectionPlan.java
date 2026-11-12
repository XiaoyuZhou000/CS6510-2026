package notification;

import database.NotificationJson;
import database.SelectionStore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import events.EventIds;

/**
 * The Recipient Selection Rule applied to one sale (007 spec, FR-010–FR-013): for each line, every subscriber
 * of its SKU in the snapshot gets exactly one notification with the line's quantity and the deterministic
 * {@link EventIds#notificationId}. SKUs nobody subscribes to give nothing; a sale with none gives a selection with
 * zero notifications and {@code recipient_count = 0}. {@code recipient_count} counts distinct subscribers.
 */
public final class SelectionPlan {
    private SelectionPlan() { }

    /** The plan for {@code lines}, unique and ascending by SKU, against {@code snapshot}. */
    public static SelectionStore.Plan plan(String saleId, List<NotificationJson.Line> lines,
                                           SubscriptionIndex.Snapshot snapshot) {
        List<SelectionStore.PlannedNotification> notifications = new ArrayList<>();
        Set<String> recipients = new HashSet<>();
        for (NotificationJson.Line line : lines) {
            if (line.quantity() < 1) throw new IllegalArgumentException("SKU " + line.sku() + " has quantity " + line.quantity());
            for (String subscriber : snapshot.lookup(line.sku())) {
                notifications.add(new SelectionStore.PlannedNotification(
                        EventIds.notificationId(saleId, line.sku(), subscriber), line.sku(), subscriber, line.quantity()));
                recipients.add(subscriber);
            }
        }
        return new SelectionStore.Plan(notifications, recipients.size());
    }
}
