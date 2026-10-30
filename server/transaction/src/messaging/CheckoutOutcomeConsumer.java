package messaging;

import events.ConsumerLoop;
import events.DeadLetterStore;
import events.RecordStream;
import events.TopicNames;
import transaction.CheckoutOutcomeHandler;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Transaction's consumer of {@code selfcheckout.checkout-outcome.v1} (contracts/event-catalog.md "Topics"):
 * group {@code transaction-checkout-outcome}, {@code CHECKOUT_CONSUMER_THREADS} (8) threads, every record
 * handed to {@link CheckoutOutcomeHandler}, parking into Transaction's {@code dead_letter_event}. Several
 * Transaction processes share the group, so an outcome may be settled by a process that does not hold the
 * waiting request (D11).
 */
public final class CheckoutOutcomeConsumer {
    public static final String THREADS_VARIABLE = "CHECKOUT_CONSUMER_THREADS";
    public static final int DEFAULT_THREADS = 8;

    private CheckoutOutcomeConsumer() { }

    public static ConsumerLoop create(TopicNames names, Supplier<RecordStream> streams, CheckoutOutcomeHandler handler,
                                      DeadLetterStore deadLetters, ConsumerLoop.Settings settings) {
        String group = names.group(TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP);
        return ConsumerLoop.group("transaction", group, names.checkoutOutcome(), streams, handler, deadLetters, settings);
    }

    /** Kafka streams for the group, from {@code KAFKA_BOOTSTRAP} and {@code KAFKA_TOPIC_PREFIX}. */
    public static Supplier<RecordStream> kafkaStreams(Map<String, String> environment, TopicNames names) {
        return KafkaClients.streams(environment, "transaction-checkout-outcome",
                names.group(TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP));
    }

    public static int threads(Map<String, String> environment) {
        String value = environment.get(THREADS_VARIABLE);
        return value == null || value.isBlank() ? DEFAULT_THREADS : Integer.parseInt(value.trim());
    }
}
