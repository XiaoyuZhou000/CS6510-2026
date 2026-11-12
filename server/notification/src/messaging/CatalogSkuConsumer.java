package messaging;

import events.ConsumerLoop;
import events.DeadLetterStore;
import events.RecordStream;
import events.TopicNames;
import notification.KnownSkus;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Notification's reader of {@code selfcheckout.catalog.v1} (007 research N6), modeled on Transaction's
 * {@code CatalogConsumer}: one thread manually assigned partition 0, seeking to offset 0 at every process start and
 * then tailing, every record handed to {@link KnownSkus}, parking into Notification's {@code dead_letter_event}. It
 * belongs to no group and commits nothing, and the Saga's {@code consumer.afterHandlerCommit} hook is off for it.
 * A stream reopened after a failure resumes after the last record handled.
 */
public final class CatalogSkuConsumer {
    private CatalogSkuConsumer() { }

    public static ConsumerLoop create(TopicNames names, Supplier<RecordStream> streams, KnownSkus handler,
                                      DeadLetterStore deadLetters, ConsumerLoop.Settings settings) {
        RecordStream.Partition partition = new RecordStream.Partition(names.catalog(), 0);
        return ConsumerLoop.manual("notification", "notification-catalog", List.of(partition),
                ignored -> handler.nextOffset(), streams, handler, deadLetters, settings.withoutFaultHooks());
    }

    /** Kafka streams without a group, from {@code KAFKA_BOOTSTRAP}. */
    public static Supplier<RecordStream> kafkaStreams(Map<String, String> environment) {
        return KafkaClients.streams(environment, "notification-catalog", null);
    }
}
