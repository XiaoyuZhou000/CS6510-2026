package unit.common;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import events.DecodedEvent;
import events.EnvelopeCodec;
import events.EventDecodeException;
import events.EventTypes;
import events.ParkReason;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.AttemptId;
import selfcheckout.events.v1.CatalogItemPublished;
import selfcheckout.events.v1.CatalogSnapshotPublished;
import selfcheckout.events.v1.CheckoutLine;
import selfcheckout.events.v1.CheckoutRequested;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.RejectionReason;
import selfcheckout.events.v1.RestorationNotNeeded;
import selfcheckout.events.v1.RestorationResult;
import selfcheckout.events.v1.ScanAccepted;
import selfcheckout.events.v1.StockDeducted;
import selfcheckout.events.v1.StockDeductionRefused;
import selfcheckout.events.v1.StockRejected;
import selfcheckout.events.v1.StockRestorationRequested;
import selfcheckout.events.v1.StockRestored;

import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** The envelope checks every consumer runs before a handler (contracts/event-catalog.md "Envelope", D3). */
class EnvelopeCodecTest {
    private static final String TX = "4b1f7c2e-0000-4000-8000-000000000001";
    private static final AttemptId ATTEMPT = AttemptId.newBuilder().setTransactionId(TX).setAttemptNumber(2).build();

    /** One payload of every type, keyed by the routing key that payload requires. */
    private static Map<EventTypes, Message> samples() {
        Map<EventTypes, Message> samples = new EnumMap<>(EventTypes.class);
        samples.put(EventTypes.CATALOG_ITEM_PUBLISHED, CatalogItemPublished.newBuilder().setSku("SKU-0001")
                .setName("Apples").setPrice("12.99").setCatalogVersion("v-1").build());
        samples.put(EventTypes.CATALOG_SNAPSHOT_PUBLISHED, CatalogSnapshotPublished.newBuilder()
                .setCatalogVersion("v-1").setProductCount(2000).build());
        samples.put(EventTypes.CHECKOUT_REQUESTED, CheckoutRequested.newBuilder().setAttempt(ATTEMPT)
                .addLines(CheckoutLine.newBuilder().setSku("SKU-0001").setQuantity(2))
                .addLines(CheckoutLine.newBuilder().setSku("SKU-0002").setQuantity(1)).build());
        samples.put(EventTypes.STOCK_RESTORATION_REQUESTED, StockRestorationRequested.newBuilder().setAttempt(ATTEMPT).build());
        samples.put(EventTypes.STOCK_DEDUCTED, StockDeducted.newBuilder().setAttempt(ATTEMPT).build());
        samples.put(EventTypes.STOCK_REJECTED, StockRejected.newBuilder().setAttempt(ATTEMPT)
                .setReason(RejectionReason.INSUFFICIENT_STOCK).setSku("SKU-0002").build());
        samples.put(EventTypes.STOCK_DEDUCTION_REFUSED, StockDeductionRefused.newBuilder().setAttempt(ATTEMPT).build());
        samples.put(EventTypes.STOCK_RESTORED, StockRestored.newBuilder().setAttempt(ATTEMPT).build());
        samples.put(EventTypes.RESTORATION_NOT_NEEDED, RestorationNotNeeded.newBuilder().setAttempt(ATTEMPT)
                .setResult(RestorationResult.ALREADY_RESTORED).build());
        samples.put(EventTypes.SCAN_ACCEPTED, ScanAccepted.newBuilder().setTransactionId(TX).setScanSequence(7)
                .setSku("SKU-0001").setProductName("Apples").setUnitPrice("12.99").setAcceptedAtMs(1_000).build());
        return samples;
    }

    private static String keyOf(EventTypes type) {
        return switch (type) {
            case CATALOG_ITEM_PUBLISHED -> "SKU-0001";
            case CATALOG_SNAPSHOT_PUBLISHED -> EventTypes.SNAPSHOT_KEY;
            default -> TX;
        };
    }

    private static String idOf(EventTypes type) {
        return type == EventTypes.SCAN_ACCEPTED ? EventTypes.scanEventId(TX, 7) : "e-" + type.typeName();
    }

    @Test
    void everyTypeRoundTripsWithItsEnvelopeFields() {
        Map<EventTypes, Message> samples = samples();
        assertEquals(10, samples.size(), "one sample per catalog type");
        for (Map.Entry<EventTypes, Message> sample : samples.entrySet()) {
            EventTypes type = sample.getKey();
            byte[] bytes = EnvelopeCodec.encode(idOf(type), type, keyOf(type), "corr-" + type, type.producer(),
                    123_456L, sample.getValue());
            DecodedEvent decoded = EnvelopeCodec.decode(bytes, keyOf(type));
            assertEquals(idOf(type), decoded.eventId());
            assertEquals(type, decoded.type());
            assertEquals(1, decoded.schemaVersion());
            assertEquals(keyOf(type), decoded.routingKey());
            assertEquals(123_456L, decoded.occurredAtMs());
            assertEquals("corr-" + type, decoded.correlationId());
            assertEquals(type.producer(), decoded.producer());
            assertEquals(sample.getValue(), decoded.payload(), type.typeName());
            assertNull(decoded.topic());
            assertEquals(-1, decoded.partition());
            assertEquals(-1L, decoded.offset());
            DecodedEvent placed = decoded.at("t", 3, 42);
            assertEquals("t", placed.topic());
            assertEquals(3, placed.partition());
            assertEquals(42, placed.offset());
        }
    }

    @Test
    void ownersFollowTheEventCatalog() {
        assertEquals(EventTypes.TRANSACTION, EventTypes.CHECKOUT_REQUESTED.producer());
        assertEquals(EventTypes.TRANSACTION, EventTypes.STOCK_RESTORATION_REQUESTED.producer());
        assertEquals(EventTypes.TRANSACTION, EventTypes.SCAN_ACCEPTED.producer());
        for (EventTypes type : new EventTypes[] {EventTypes.CATALOG_ITEM_PUBLISHED, EventTypes.CATALOG_SNAPSHOT_PUBLISHED,
                EventTypes.STOCK_DEDUCTED, EventTypes.STOCK_REJECTED, EventTypes.STOCK_DEDUCTION_REFUSED,
                EventTypes.STOCK_RESTORED, EventTypes.RESTORATION_NOT_NEEDED}) {
            assertEquals(EventTypes.INVENTORY, type.producer(), type.typeName());
        }
        assertTrue(EventTypes.byName("StockDeducted").isPresent());
        assertTrue(EventTypes.byName("Nope").isEmpty());
    }

    @Test
    void missingRequiredEnvelopeFieldsArePermanent() {
        EventEnvelope valid = envelope(EventTypes.STOCK_DEDUCTED, TX);
        assertRejected(valid.toBuilder().clearEventId().build(), TX, ParkReason.INVALID_PAYLOAD);
        assertRejected(valid.toBuilder().clearEventType().build(), TX, ParkReason.INVALID_PAYLOAD);
        assertRejected(valid.toBuilder().clearRoutingKey().build(), TX, ParkReason.INVALID_PAYLOAD);
        assertRejected(valid.toBuilder().clearProducer().build(), TX, ParkReason.INVALID_PAYLOAD);
    }

    @Test
    void aProducerThatDoesNotOwnTheTypeIsRejected() {
        EventEnvelope forged = envelope(EventTypes.STOCK_DEDUCTED, TX).toBuilder().setProducer(EventTypes.TRANSACTION).build();
        EventDecodeException failure = assertRejected(forged, TX, ParkReason.INVALID_PAYLOAD);
        assertEquals("e-StockDeducted", failure.eventId(), "the readable event_id is kept for parking");
        assertEquals("StockDeducted", failure.eventType());
        assertThrows(IllegalArgumentException.class, () -> EnvelopeCodec.encode("x", EventTypes.STOCK_DEDUCTED, TX, TX,
                EventTypes.TRANSACTION, 0, samples().get(EventTypes.STOCK_DEDUCTED)), "the encoder refuses a wrong owner");
    }

    @Test
    void unsupportedVersionsAndUnknownTypesAreRejected() {
        EventEnvelope valid = envelope(EventTypes.STOCK_DEDUCTED, TX);
        EventDecodeException version = assertRejected(valid.toBuilder().setSchemaVersion(2).build(), TX,
                ParkReason.UNSUPPORTED_VERSION);
        assertEquals(2, version.schemaVersion());
        assertRejected(valid.toBuilder().setSchemaVersion(0).build(), TX, ParkReason.UNSUPPORTED_VERSION);
        assertRejected(valid.toBuilder().setEventType("StockTeleported").build(), TX, ParkReason.UNKNOWN_TYPE);
    }

    @Test
    void undecodableBytesAndPayloadsAreRejected() {
        EventDecodeException garbage = assertThrows(EventDecodeException.class,
                () -> EnvelopeCodec.decode("not a protobuf \u0000ÿ".getBytes(StandardCharsets.ISO_8859_1), TX));
        assertEquals(ParkReason.UNDECODABLE, garbage.reason());
        assertNull(garbage.eventId(), "an undecodable record has no event_id");
        assertEquals(ParkReason.UNDECODABLE,
                assertThrows(EventDecodeException.class, () -> EnvelopeCodec.decode(null, TX)).reason());

        EventEnvelope badPayload = envelope(EventTypes.STOCK_DEDUCTED, TX).toBuilder()
                .setPayload(ByteString.copyFrom(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff})).build();
        assertRejected(badPayload, TX, ParkReason.INVALID_PAYLOAD);
    }

    @Test
    void payloadIdentityMustMatchTheRoutingKeyAndTheRecordKey() {
        StockDeducted other = StockDeducted.newBuilder().setAttempt(ATTEMPT.toBuilder().setTransactionId("other")).build();
        EventEnvelope mismatched = envelope(EventTypes.STOCK_DEDUCTED, TX).toBuilder().setPayload(other.toByteString()).build();
        assertRejected(mismatched, TX, ParkReason.PAYLOAD_MISMATCH);

        CheckoutRequested request = (CheckoutRequested) samples().get(EventTypes.CHECKOUT_REQUESTED);
        EventEnvelope requestUnderOtherKey = EventEnvelope.newBuilder().setEventId("e-1")
                .setEventType("CheckoutRequested").setSchemaVersion(1).setRoutingKey("tx-elsewhere")
                .setProducer(EventTypes.TRANSACTION).setPayload(request.toByteString()).build();
        assertRejected(requestUnderOtherKey, "tx-elsewhere", ParkReason.PAYLOAD_MISMATCH);

        CatalogItemPublished item = (CatalogItemPublished) samples().get(EventTypes.CATALOG_ITEM_PUBLISHED);
        EventEnvelope itemUnderOtherSku = envelope(EventTypes.CATALOG_ITEM_PUBLISHED, "SKU-0009").toBuilder()
                .setPayload(item.toByteString()).build();
        assertRejected(itemUnderOtherSku, "SKU-0009", ParkReason.PAYLOAD_MISMATCH);

        assertRejected(envelope(EventTypes.STOCK_DEDUCTED, TX), "different-record-key", ParkReason.PAYLOAD_MISMATCH);
    }

    @Test
    void scanEventIdsAreDerivedFromTheScanIdentity() {
        assertEquals("scan:" + TX + ":7", EventTypes.scanEventId(TX, 7));
        ScanAccepted scan = (ScanAccepted) samples().get(EventTypes.SCAN_ACCEPTED);
        byte[] bytes = EnvelopeCodec.encode(EventTypes.scanEventId(TX, 7), EventTypes.SCAN_ACCEPTED, TX, TX,
                EventTypes.TRANSACTION, 1, scan);
        assertEquals("scan:" + TX + ":7", EnvelopeCodec.decode(bytes, TX).eventId());

        EventEnvelope wrongId = EventEnvelope.newBuilder().setEventId("scan:" + TX + ":8")
                .setEventType("ScanAccepted").setSchemaVersion(1).setRoutingKey(TX)
                .setProducer(EventTypes.TRANSACTION).setPayload(scan.toByteString()).build();
        assertRejected(wrongId, TX, ParkReason.PAYLOAD_MISMATCH);
        EventEnvelope uuidId = wrongId.toBuilder().setEventId("7d3c8a10-1111-4222-8333-944455556666").build();
        assertRejected(uuidId, TX, ParkReason.PAYLOAD_MISMATCH);
    }

    private static EventEnvelope envelope(EventTypes type, String routingKey) {
        return EventEnvelope.newBuilder().setEventId(idOf(type)).setEventType(type.typeName()).setSchemaVersion(1)
                .setRoutingKey(routingKey).setCorrelationId(routingKey).setProducer(type.producer())
                .setOccurredAtMs(5).setPayload(samples().get(type).toByteString()).build();
    }

    private static EventDecodeException assertRejected(EventEnvelope envelope, String recordKey, ParkReason reason) {
        EventDecodeException failure = assertThrows(EventDecodeException.class,
                () -> EnvelopeCodec.decode(envelope.toByteArray(), recordKey));
        assertEquals(reason, failure.reason(), failure.getMessage());
        return failure;
    }
}
