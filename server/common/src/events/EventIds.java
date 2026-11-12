package events;

import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The deterministic event IDs of the notification feature (007 contracts/sale-completed-event.md "Envelope"):
 * UUID version 3 of a name, so a retried build of the same fact always carries the same ID.
 */
public final class EventIds {
    private EventIds() { }

    /** The {@code event_id} of a sale's {@code SaleCompleted}: UUID v3 of {@code sale:<transaction_id>}. */
    public static String saleEventId(String transactionId) {
        require(transactionId, "transactionId");
        return UUID.nameUUIDFromBytes(("sale:" + transactionId).getBytes(UTF_8)).toString();
    }

    /**
     * The {@code notification_id} (and {@code event_id}) of one subscriber's notification for one SKU of one
     * sale: UUID v3 of {@code notification:<sale_id>:<sku>:<subscriber_id>}.
     */
    public static String notificationId(String saleId, String sku, String subscriberId) {
        require(saleId, "saleId");
        require(sku, "sku");
        require(subscriberId, "subscriberId");
        return UUID.nameUUIDFromBytes(("notification:" + saleId + ":" + sku + ":" + subscriberId).getBytes(UTF_8))
                .toString();
    }

    private static void require(String value, String name) {
        if (value == null || value.isEmpty()) throw new IllegalArgumentException(name + " is required");
    }
}
