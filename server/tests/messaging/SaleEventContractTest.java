package messaging;

import com.google.protobuf.ByteString;
import events.DecodedEvent;
import events.EnvelopeCodec;
import events.EventDecodeException;
import events.EventIds;
import events.EventTypes;
import events.OutboxRow;
import events.ParkReason;
import events.TopicNames;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.SoldLine;
import transaction.CheckoutEvents.Line;
import transaction.SaleEvents;

import java.time.Instant;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The {@code SaleCompleted} contract between Transaction and Notification (007 T025,
 * contracts/sale-completed-event.md "Consumption rules"): a record built by Transaction's {@link SaleEvents} passes
 * every check {@link EnvelopeCodec#decode} applies for a consumer, and records with a wrong producer, a wrong routing
 * key, an unsupported version, unsorted or duplicate lines, a non-positive quantity, or an identity that is not the
 * sale's are rejected with a parking reason, so a consumer parks them instead of retrying forever.
 */
class SaleEventContractTest {
    private static final Instant COMPLETED_AT = Instant.parse("2026-11-11T12:00:00Z");
    private static final OutboxRow ROW = new SaleEvents(TopicNames.unprefixed()).saleCompleted("tx-1", 1, COMPLETED_AT,
            "station-1", List.of(new Line("SKU-B", 1), new Line("SKU-A", 3)));

    @Test
    void transactionsRecordPassesTheConsumerChecks() throws Exception {
        DecodedEvent event = EnvelopeCodec.decode(ROW.envelope(), ROW.routingKey());
        assertEquals(EventTypes.SALE_COMPLETED, event.type());
        assertEquals("tx-1", event.routingKey());
        assertEquals(EventTypes.TRANSACTION, event.producer());
        SaleCompleted sale = event.payload(SaleCompleted.class);
        assertEquals("tx-1", sale.getSaleId());
        assertEquals(List.of("SKU-A", "SKU-B"), sale.getLinesList().stream().map(SoldLine::getSku).toList());
    }

    @Test
    void aWrongProducerRoutingKeyOrVersionIsRejected() throws Exception {
        assertRejected(ParkReason.INVALID_PAYLOAD, envelope -> envelope.setProducer(EventTypes.INVENTORY), "tx-1",
                "only Transaction produces SaleCompleted");
        assertRejected(ParkReason.INVALID_PAYLOAD, envelope -> envelope.setProducer(EventTypes.NOTIFICATION), "tx-1",
                "not Notification either");
        assertRejected(ParkReason.PAYLOAD_MISMATCH, envelope -> envelope.setRoutingKey("tx-2"), "tx-2",
                "the routing key is the transaction ID");
        assertRejected(ParkReason.PAYLOAD_MISMATCH, envelope -> envelope, "tx-2", "the record key is the routing key");
        assertRejected(ParkReason.UNSUPPORTED_VERSION, envelope -> envelope.setSchemaVersion(2), "tx-1", "version 2");
        assertRejected(ParkReason.PAYLOAD_MISMATCH, envelope -> envelope.setEventId("another-id"), "tx-1",
                "the event ID is the sale's");
    }

    @Test
    void unsortedDuplicateEmptyOrNonPositiveLinesAndAForeignSaleIdAreRejected() throws Exception {
        assertRejected(ParkReason.INVALID_PAYLOAD, withSale(sale -> sale.clearLines()
                .addLines(line("SKU-B", 1)).addLines(line("SKU-A", 3))), "tx-1", "unsorted lines");
        assertRejected(ParkReason.INVALID_PAYLOAD, withSale(sale -> sale.clearLines()
                .addLines(line("SKU-A", 1)).addLines(line("SKU-A", 2))), "tx-1", "a duplicate SKU");
        assertRejected(ParkReason.INVALID_PAYLOAD, withSale(SaleCompleted.Builder::clearLines), "tx-1", "no lines");
        assertRejected(ParkReason.INVALID_PAYLOAD, withSale(sale -> sale.clearLines().addLines(line("SKU-A", 0))), "tx-1",
                "a zero quantity");
        assertRejected(ParkReason.INVALID_PAYLOAD, withSale(sale -> sale.clearLines().addLines(line("SKU-A", -2))), "tx-1",
                "a negative quantity");
        assertRejected(ParkReason.PAYLOAD_MISMATCH, withSale(sale -> sale.setSaleId("other-sale")), "tx-1",
                "sale_id = transaction_id");
    }

    /** {@code change} applied to Transaction's envelope, read with record key {@code key}, fails with {@code reason}. */
    private static void assertRejected(ParkReason reason, UnaryOperator<EventEnvelope.Builder> change, String key,
                                       String why) throws Exception {
        byte[] value = change.apply(EventEnvelope.parseFrom(ROW.envelope()).toBuilder()).build().toByteArray();
        EventDecodeException rejected = assertThrows(EventDecodeException.class, () -> EnvelopeCodec.decode(value, key), why);
        assertEquals(reason, rejected.reason(), why + ": " + rejected.getMessage());
    }

    private static UnaryOperator<EventEnvelope.Builder> withSale(UnaryOperator<SaleCompleted.Builder> change) {
        return envelope -> {
            try {
                SaleCompleted.Builder sale = SaleCompleted.parseFrom(envelope.getPayload()).toBuilder();
                ByteString payload = change.apply(sale).build().toByteString();
                return envelope.setPayload(payload);
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        };
    }

    private static SoldLine line(String sku, int quantity) {
        return SoldLine.newBuilder().setSku(sku).setQuantity(quantity).build();
    }

    @Test
    void theEventIdIsDerivedFromTheTransaction() {
        assertEquals(EventIds.saleEventId("tx-1"), ROW.eventId());
    }
}
