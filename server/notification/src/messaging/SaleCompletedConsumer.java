package messaging;

import events.ConsumerLoop;
import events.DeadLetterStore;
import events.RecordStream;
import events.TopicNames;
import notification.SaleSelectionHandler;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Notification's consumer of {@code selfcheckout.sale-completed.v1} (007 research N3): group
 * {@code notification-sale-completed}, {@code NOTIFICATION_SALE_CONSUMER_THREADS} (4) threads, the consumers'
 * {@code fetch.max.wait.ms=50}, every record handed to {@link SaleSelectionHandler}, and parking into Notification's
 * {@code dead_letter_event}. The offset is committed only after the selection committed (006 D6). Notification's own
 * hooks ({@code notification.selection.*}) replace the Saga's {@code consumer.afterHandlerCommit}, which is off.
 */
public final class SaleCompletedConsumer {
    private SaleCompletedConsumer() { }

    public static ConsumerLoop create(TopicNames names, Supplier<RecordStream> streams, SaleSelectionHandler handler,
                                      DeadLetterStore deadLetters, ConsumerLoop.Settings settings) {
        String group = names.group(TopicNames.NOTIFICATION_SALE_COMPLETED_GROUP);
        return ConsumerLoop.group("notification", group, names.saleCompleted(), streams, handler, deadLetters,
                settings.withoutFaultHooks());
    }

    /** Kafka streams for the group, from {@code KAFKA_BOOTSTRAP} and {@code KAFKA_TOPIC_PREFIX}. */
    public static Supplier<RecordStream> kafkaStreams(Map<String, String> environment, TopicNames names) {
        return KafkaClients.streams(environment, "notification-sale-completed",
                names.group(TopicNames.NOTIFICATION_SALE_COMPLETED_GROUP));
    }
}
