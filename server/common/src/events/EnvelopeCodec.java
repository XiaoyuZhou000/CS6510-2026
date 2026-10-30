package events;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.ScanAccepted;

import java.util.Optional;

/**
 * Encodes and checks {@code EventEnvelope} record values (contracts/event-catalog.md "Envelope", D3).
 *
 * <p>{@link #decode} runs every check a consumer needs before calling a handler, in this order, and throws
 * {@link EventDecodeException} with the {@code dead_letter_event.reason} of the first that fails:</p>
 * <ol>
 *   <li>The value parses as an envelope: else {@link ParkReason#UNDECODABLE}.</li>
 *   <li>{@code event_id}, {@code event_type}, {@code routing_key}, and {@code producer} are present: else
 *       {@link ParkReason#INVALID_PAYLOAD}.</li>
 *   <li>{@code event_type} is one of the catalog's types: else {@link ParkReason#UNKNOWN_TYPE}.</li>
 *   <li>{@code schema_version} is supported: else {@link ParkReason#UNSUPPORTED_VERSION}.</li>
 *   <li>{@code producer} is the type's owner, and the payload parses: else {@link ParkReason#INVALID_PAYLOAD}.</li>
 *   <li>The record key equals {@code routing_key}, the payload's identity field equals {@code routing_key},
 *       and a scan's {@code event_id} is {@code scan:<transaction_id>:<scan_sequence>}: else
 *       {@link ParkReason#PAYLOAD_MISMATCH}.</li>
 * </ol>
 */
public final class EnvelopeCodec {
    private EnvelopeCodec() { }

    /** A serialized envelope with {@code schema_version} 1 around {@code payload}. */
    public static byte[] encode(String eventId, EventTypes type, String routingKey, String correlationId,
                                String producer, long occurredAtMs, Message payload) {
        if (eventId == null || eventId.isEmpty()) throw new IllegalArgumentException("eventId is required");
        if (routingKey == null || routingKey.isEmpty()) throw new IllegalArgumentException("routingKey is required");
        if (!type.producer().equals(producer)) {
            throw new IllegalArgumentException(type.typeName() + " is produced only by " + type.producer());
        }
        if (payload.getParserForType() != type.parser()) {
            throw new IllegalArgumentException(type.typeName() + " cannot carry " + payload.getClass().getSimpleName());
        }
        return EventEnvelope.newBuilder()
                .setEventId(eventId)
                .setEventType(type.typeName())
                .setSchemaVersion(EventTypes.CURRENT_VERSION)
                .setRoutingKey(routingKey)
                .setOccurredAtMs(occurredAtMs)
                .setCorrelationId(correlationId == null ? "" : correlationId)
                .setProducer(producer)
                .setPayload(payload.toByteString())
                .build()
                .toByteArray();
    }

    /** Decodes a record value whose record key is {@code recordKey}; see the class comment for the checks. */
    public static DecodedEvent decode(byte[] value, String recordKey) {
        if (value == null) {
            throw new EventDecodeException(ParkReason.UNDECODABLE, "record has no value", null, null, null, null);
        }
        EventEnvelope envelope;
        try {
            envelope = EventEnvelope.parseFrom(value);
        } catch (InvalidProtocolBufferException failure) {
            throw new EventDecodeException(ParkReason.UNDECODABLE, "record value is not an EventEnvelope: "
                    + failure.getMessage(), null, null, null, failure);
        }
        String eventId = envelope.getEventId();
        String typeName = envelope.getEventType();
        Integer version = envelope.getSchemaVersion();
        for (String[] field : new String[][] {{"event_id", eventId}, {"event_type", typeName},
                {"routing_key", envelope.getRoutingKey()}, {"producer", envelope.getProducer()}}) {
            if (field[1].isEmpty()) {
                throw failure(ParkReason.INVALID_PAYLOAD, "envelope has no " + field[0], envelope, null);
            }
        }
        Optional<EventTypes> known = EventTypes.byName(typeName);
        if (known.isEmpty()) throw failure(ParkReason.UNKNOWN_TYPE, "unknown event_type " + typeName, envelope, null);
        EventTypes type = known.get();
        if (!type.supportedVersions().contains(version)) {
            throw failure(ParkReason.UNSUPPORTED_VERSION, typeName + " schema_version " + version + " is not supported",
                    envelope, null);
        }
        if (!type.producer().equals(envelope.getProducer())) {
            throw failure(ParkReason.INVALID_PAYLOAD, typeName + " is produced only by " + type.producer()
                    + ", not " + envelope.getProducer(), envelope, null);
        }
        Message payload;
        try {
            payload = type.parser().parseFrom(envelope.getPayload());
        } catch (InvalidProtocolBufferException failure) {
            throw failure(ParkReason.INVALID_PAYLOAD, typeName + " payload does not decode: " + failure.getMessage(),
                    envelope, failure);
        }
        String routingKey = envelope.getRoutingKey();
        if (!routingKey.equals(recordKey)) {
            throw failure(ParkReason.PAYLOAD_MISMATCH, "record key " + recordKey + " differs from routing_key "
                    + routingKey, envelope, null);
        }
        String identity = type.routingKeyOf(payload);
        if (!routingKey.equals(identity)) {
            throw failure(ParkReason.PAYLOAD_MISMATCH, typeName + " payload identity " + identity
                    + " differs from routing_key " + routingKey, envelope, null);
        }
        if (type == EventTypes.SCAN_ACCEPTED) {
            ScanAccepted scan = (ScanAccepted) payload;
            String expected = EventTypes.scanEventId(scan.getTransactionId(), scan.getScanSequence());
            if (!expected.equals(eventId)) {
                throw failure(ParkReason.PAYLOAD_MISMATCH, "scan event_id " + eventId + " is not " + expected,
                        envelope, null);
            }
        }
        return new DecodedEvent(eventId, type, version, routingKey, envelope.getOccurredAtMs(),
                envelope.getCorrelationId(), envelope.getProducer(), payload, null, -1, -1L);
    }

    private static EventDecodeException failure(ParkReason reason, String message, EventEnvelope envelope,
                                                Throwable cause) {
        return new EventDecodeException(reason, message, envelope.getEventId(), envelope.getEventType(),
                envelope.getSchemaVersion(), cause);
    }
}
