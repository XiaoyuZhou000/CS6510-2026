package messaging;

import analytics.ScanIngestionService;
import events.BatchEventHandler;
import events.ConsumerLoop;
import events.DeadLetterStore;
import events.RecordStream;
import events.TopicNames;

import java.util.Map;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * Analytics' consumer of {@code selfcheckout.scan-accepted.v1} (contracts/scan-event-delivery.md section 3, D10):
 * group {@code analytics-scan-ingest}, one thread, each poll of up to 500 records handed to
 * {@link ScanIngestionService} as one batch, parking into Analytics' {@code dead_letter_event}.
 *
 * <p>Analytics' {@code consumer_offset} is the authority for its progress (FR-049): on every assignment the
 * partition seeks to the stored next offset, or to 0 before the first batch, so lost or reset broker offsets
 * cause neither a gap nor a double count; a stored offset beyond the log end raises an alarm and reads from the
 * earliest offset, and deduplication absorbs the overlap. The Saga's {@code consumer.afterHandlerCommit} hook is
 * off; ingestion has its own hooks.</p>
 */
public final class ScanAcceptedConsumer {
    private ScanAcceptedConsumer() { }

    public static ConsumerLoop create(TopicNames names, Supplier<RecordStream> streams, ScanIngestionService ingestion,
                                      DeadLetterStore deadLetters, ConsumerLoop.Settings settings) {
        return create(names, streams, ingestion, ingestion::nextOffset, deadLetters, settings);
    }

    /** As above with the batch handler and the stored offsets given separately, for a test's decorator. */
    public static ConsumerLoop create(TopicNames names, Supplier<RecordStream> streams, BatchEventHandler handler,
                                      ToLongFunction<RecordStream.Partition> storedOffset, DeadLetterStore deadLetters,
                                      ConsumerLoop.Settings settings) {
        return ConsumerLoop.batchGroup("analytics", names.group(TopicNames.ANALYTICS_SCAN_INGEST_GROUP),
                names.scanAccepted(), storedOffset, streams, handler, deadLetters, settings.withoutFaultHooks());
    }

    /** Kafka streams for the group, from {@code KAFKA_BOOTSTRAP} and {@code KAFKA_TOPIC_PREFIX}. */
    public static Supplier<RecordStream> kafkaStreams(Map<String, String> environment, TopicNames names) {
        return KafkaClients.streams(environment, "analytics-scan-ingest",
                names.group(TopicNames.ANALYTICS_SCAN_INGEST_GROUP));
    }
}
