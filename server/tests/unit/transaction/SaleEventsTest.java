package unit.transaction;

import events.DecodedEvent;
import events.EnvelopeCodec;
import events.EventIds;
import events.EventTypes;
import events.OutboxRow;
import events.TopicNames;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.SoldLine;
import transaction.CheckoutEvents.Line;
import transaction.SaleEvents;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The {@code SaleCompleted} row T3 records (007 T020, FR-001 to FR-005, contracts/sale-completed-event.md): its
 * identity, envelope, and lines.
 */
class SaleEventsTest {
    private static final Instant COMPLETED_AT = Instant.parse("2026-11-11T12:00:00.123Z");
    private final SaleEvents sales = new SaleEvents(TopicNames.unprefixed());

    @Test
    void linesAreAscendingAndUniqueAndTheSaleIsTheTransaction() throws Exception {
        OutboxRow row = sales.saleCompleted("tx-1", 2, COMPLETED_AT, "station-9", List.of(new Line("B", 1), new Line("A", 3)));
        SaleCompleted sale = SaleCompleted.parseFrom(EventEnvelope.parseFrom(row.envelope()).getPayload());
        assertEquals(List.of(SoldLine.newBuilder().setSku("A").setQuantity(3).build(),
                SoldLine.newBuilder().setSku("B").setQuantity(1).build()), sale.getLinesList(), "[A x3, B x1]");
        assertEquals("tx-1", sale.getSaleId(), "sale_id = transaction_id");
        assertEquals("tx-1", sale.getTransactionId());
        assertEquals(2, sale.getAttemptNumber());
        assertEquals(COMPLETED_AT.toEpochMilli(), sale.getCompletedAtMs());
        assertEquals("station-9", sale.getStationId());
    }

    @Test
    void theRowAndEnvelopeCarryTheDeterministicIdentity() {
        OutboxRow row = sales.saleCompleted("tx-1", 1, COMPLETED_AT, "station-9", List.of(new Line("A", 1)));
        assertEquals(EventIds.saleEventId("tx-1"), row.eventId(), "event_id = EventIds.saleEventId(transactionId)");
        assertEquals("selfcheckout.sale-completed.v1", row.topic());
        assertEquals("tx-1", row.routingKey());
        assertEquals("SaleCompleted", row.eventType());
        assertEquals("sale:tx-1", row.aggregateKey());
        assertEquals(COMPLETED_AT, row.createdAt());
        assertEquals(row.eventId(), sales.saleCompleted("tx-1", 1, COMPLETED_AT.plusSeconds(5), "other", List.of(new Line("B", 2)))
                .eventId(), "a sale has one identity whatever is rebuilt");

        DecodedEvent decoded = EnvelopeCodec.decode(row.envelope(), "tx-1");
        assertEquals(EventTypes.SALE_COMPLETED, decoded.type());
        assertEquals("tx-1", decoded.routingKey(), "routing_key");
        assertEquals("tx-1", decoded.correlationId(), "correlation_id");
        assertEquals(EventTypes.TRANSACTION, decoded.producer());
        assertEquals(1, decoded.schemaVersion());
        assertEquals(COMPLETED_AT.toEpochMilli(), decoded.occurredAtMs());
        assertEquals(row.eventId(), decoded.eventId());
    }

    @Test
    void aPrefixedTopicNameIsUsed() {
        OutboxRow row = new SaleEvents(new TopicNames("t-1.")).saleCompleted("tx-1", 1, COMPLETED_AT, "s",
                List.of(new Line("A", 1)));
        assertEquals("t-1.selfcheckout.sale-completed.v1", row.topic());
    }

    @Test
    void aZeroQuantityADuplicateSkuOrAnEmptyBasketThrows() {
        assertThrows(IllegalArgumentException.class, () -> sales.saleCompleted("tx-1", 1, COMPLETED_AT, "s",
                List.of(new Line("A", 0))), "a zero quantity");
        assertThrows(IllegalArgumentException.class, () -> sales.saleCompleted("tx-1", 1, COMPLETED_AT, "s",
                List.of(new Line("A", 1), new Line("A", 2))), "a duplicate SKU");
        assertThrows(IllegalArgumentException.class, () -> sales.saleCompleted("tx-1", 1, COMPLETED_AT, "s", List.of()),
                "an empty basket");
        assertThrows(IllegalArgumentException.class, () -> sales.saleCompleted("", 1, COMPLETED_AT, "s",
                List.of(new Line("A", 1))), "no transaction");
        assertThrows(IllegalArgumentException.class, () -> sales.saleCompleted("tx-1", 0, COMPLETED_AT, "s",
                List.of(new Line("A", 1))), "no attempt");
    }

    @Test
    void theEventCarriesNoRecipientData() {
        for (var field : SaleCompleted.getDescriptor().getFields()) {
            String name = field.getName().toLowerCase(java.util.Locale.ROOT);
            assertFalse(name.contains("subscri") || name.contains("recipient"), "FR-004: " + name);
        }
    }
}
