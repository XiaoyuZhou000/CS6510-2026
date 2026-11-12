package database;

import java.time.Instant;
import java.util.List;

/**
 * Notification's recipient selection, one local transaction per {@code SaleCompleted} (007 research N5,
 * contracts/notification-delivery.md "Selection transaction"). The subscription clock is held {@code FOR SHARE},
 * so no change can commit while a sale is selected and the selection names one exact history version.
 */
public interface SelectionStore {
    /** The selected sale: its identity, its {@code SaleCompleted} event, and its lines in SKU order. */
    record Sale(String saleId, String saleEventId, Instant completedAt, List<NotificationJson.Line> lines) {
        public Sale {
            lines = List.copyOf(lines);
        }
    }

    /** One notification to create: the selection result for one (SKU, subscriber) of the sale. */
    record PlannedNotification(String notificationId, String sku, String subscriberId, int quantity) { }

    /** The planner's result: the notifications and the number of distinct subscribers among them. */
    record Plan(List<PlannedNotification> notifications, int recipientCount) {
        public Plan {
            notifications = List.copyOf(notifications);
        }
    }

    /** Reads committed history rows in {@code (after, upTo]} inside the selection transaction. */
    @FunctionalInterface
    interface ChangeSource {
        List<SubscriptionStore.ChangeRow> changesAfter(long after, long upTo);
    }

    /**
     * Plans the selection against the history version the transaction locked; {@code changes} lets an index that
     * is behind catch up first, on the transaction's own connection.
     */
    @FunctionalInterface
    interface Planner {
        Plan plan(long lockedVersion, ChangeSource changes);
    }

    sealed interface Result permits Selected, AlreadySelected { }

    /** The selection committed at {@code subscriptionVersion}; its new rows, ready for the dispatcher. */
    record Selected(long subscriptionVersion, Instant selectedAt, List<NotificationStore.Pending> notifications)
            implements Result {
        public Selected {
            notifications = List.copyOf(notifications);
        }
    }

    /** The sale was selected before: nothing was written (FR-012). */
    record AlreadySelected() implements Result { }

    Result select(Sale sale, Planner planner);
}
