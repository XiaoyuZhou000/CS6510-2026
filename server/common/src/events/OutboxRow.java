package events;

import java.time.Instant;
import java.util.Objects;

/**
 * One recorded event of a service's {@code outbox_event} table (data-model.md, D5). {@code envelope} holds
 * the serialized {@code EventEnvelope} exactly as it is sent, on the first send and on every re-send.
 * {@code topic} is the full, prefixed topic name; {@code aggregateKey} is
 * {@code <transaction_id>:<attempt_number>} for checkout events and null for catalog events.
 */
public record OutboxRow(String eventId, String topic, String routingKey, String eventType, String aggregateKey,
                        byte[] envelope, Instant createdAt) {
    public OutboxRow {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(routingKey, "routingKey");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(envelope, "envelope");
        Objects.requireNonNull(createdAt, "createdAt");
    }
}
