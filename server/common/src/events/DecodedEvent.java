package events;

import com.google.protobuf.Message;

/**
 * One consumed record after every envelope check passed: the envelope's fields, the decoded payload, and
 * the record's position. {@link EnvelopeCodec#decode} leaves the position unset ({@code topic} null,
 * {@code partition} and {@code offset} -1); {@link ConsumerLoop} fills it with {@link #at}.
 */
public record DecodedEvent(String eventId, EventTypes type, int schemaVersion, String routingKey,
                           long occurredAtMs, String correlationId, String producer, Message payload,
                           String topic, int partition, long offset) {

    public DecodedEvent at(String recordTopic, int recordPartition, long recordOffset) {
        return new DecodedEvent(eventId, type, schemaVersion, routingKey, occurredAtMs, correlationId, producer,
                payload, recordTopic, recordPartition, recordOffset);
    }

    /** The payload as its generated class, for example {@code event.payload(CheckoutRequested.class)}. */
    public <T extends Message> T payload(Class<T> payloadClass) {
        return payloadClass.cast(payload);
    }
}
