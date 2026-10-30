package messaging;

import events.ConsumerLoop;
import events.DeadLetterStore;
import events.EventHandler;
import events.EventTypes;
import events.ParkReason;
import events.PermanentEventFailure;
import events.RecordStream;
import events.TopicNames;
import inventory.CheckoutRequestHandler;
import inventory.StockRestorationHandler;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Inventory's consumer of {@code selfcheckout.checkout.v1} (contracts/event-catalog.md "Topics"): group
 * {@code inventory-checkout}, {@code CHECKOUT_CONSUMER_THREADS} (8) threads, parking into Inventory's
 * {@code dead_letter_event}. Records are dispatched by {@code event_type}: {@code CheckoutRequested} to
 * {@link CheckoutRequestHandler}, the reserved {@code StockRestorationRequested} to
 * {@link StockRestorationHandler}.
 */
public final class CheckoutEventConsumer {
    public static final String THREADS_VARIABLE = "CHECKOUT_CONSUMER_THREADS";
    public static final int DEFAULT_THREADS = 8;

    private CheckoutEventConsumer() { }

    public static ConsumerLoop create(TopicNames names, Supplier<RecordStream> streams, CheckoutRequestHandler checkouts,
                                      StockRestorationHandler restorations, DeadLetterStore deadLetters,
                                      ConsumerLoop.Settings settings) {
        EventHandler dispatch = event -> {
            if (event.type() == EventTypes.CHECKOUT_REQUESTED) {
                checkouts.handle(event);
            } else if (event.type() == EventTypes.STOCK_RESTORATION_REQUESTED) {
                restorations.handle(event);
            } else {
                throw new PermanentEventFailure(ParkReason.UNKNOWN_TYPE,
                        event.type().typeName() + " does not belong on the checkout topic");
            }
        };
        String group = names.group(TopicNames.INVENTORY_CHECKOUT_GROUP);
        return ConsumerLoop.group("inventory", group, names.checkout(), streams, dispatch, deadLetters, settings);
    }

    /** Kafka streams for the group, from {@code KAFKA_BOOTSTRAP} and {@code KAFKA_TOPIC_PREFIX}. */
    public static Supplier<RecordStream> kafkaStreams(Map<String, String> environment, TopicNames names) {
        return KafkaClients.streams(environment, "inventory-checkout", names.group(TopicNames.INVENTORY_CHECKOUT_GROUP));
    }

    public static int threads(Map<String, String> environment) {
        String value = environment.get(THREADS_VARIABLE);
        return value == null || value.isBlank() ? DEFAULT_THREADS : Integer.parseInt(value.trim());
    }
}
