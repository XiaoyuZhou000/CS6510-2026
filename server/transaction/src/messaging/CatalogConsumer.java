package messaging;

import events.ConsumerLoop;
import events.DeadLetterStore;
import events.RecordStream;
import events.TopicNames;
import transaction.CatalogEventHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Transaction's reader of {@code selfcheckout.catalog.v1} (contracts/event-catalog.md "Topics", D9): one thread
 * manually assigned partition 0, seeking to offset 0 at every process start and then tailing, every record
 * handed to {@link CatalogEventHandler}, parking into Transaction's {@code dead_letter_event}. It belongs to no
 * consumer group and commits no offsets: replaying the 2,001 small records of a publication is cheaper than
 * tracking them, and the version buffer it rebuilds is discarded at every restart. The Saga's
 * {@code consumer.afterHandlerCommit} hook is off for it.
 *
 * <p>A stream reopened after a failure, such as a broker restart, resumes after the last record the handler
 * processed, because the handler's buffer survives in memory; a log that no longer reaches that offset, for
 * example a recreated topic, is read again from its earliest offset ({@code auto.offset.reset=earliest}).</p>
 */
public final class CatalogConsumer {
    private CatalogConsumer() { }

    public static ConsumerLoop create(TopicNames names, Supplier<RecordStream> streams, CatalogEventHandler handler,
                                      DeadLetterStore deadLetters, ConsumerLoop.Settings settings) {
        RecordStream.Partition partition = new RecordStream.Partition(names.catalog(), 0);
        return ConsumerLoop.manual("transaction", "transaction-catalog", List.of(partition),
                ignored -> handler.nextOffset(), streams, handler, deadLetters, settings.withoutFaultHooks());
    }

    /** Kafka streams without a group, from {@code KAFKA_BOOTSTRAP}. */
    public static Supplier<RecordStream> kafkaStreams(Map<String, String> environment) {
        return KafkaClients.streams(environment, "transaction-catalog", null);
    }
}
