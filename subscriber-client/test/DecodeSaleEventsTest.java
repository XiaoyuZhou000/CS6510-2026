import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.SoldLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** T076: exported SaleCompleted outbox rows decode into sale-events.tsv for the SC-001 check. */
class DecodeSaleEventsTest {

    @Test
    void decodesEnvelopesAndMarksUndecodableRows(@TempDir Path dir) throws Exception {
        SaleCompleted sale = SaleCompleted.newBuilder().setSaleId("txn-1").setTransactionId("txn-1").setAttemptNumber(1)
                .setCompletedAtMs(1_800_000_000_123L).setStationId("station-1")
                .addLines(SoldLine.newBuilder().setSku("SKU-000001").setQuantity(3))
                .addLines(SoldLine.newBuilder().setSku("SKU-000002").setQuantity(1)).build();
        byte[] envelope = EventEnvelope.newBuilder().setEventId("e-1").setEventType("SaleCompleted").setSchemaVersion(1)
                .setRoutingKey("txn-1").setProducer("transaction").setPayload(sale.toByteString()).build().toByteArray();
        Path raw = dir.resolve("raw.tsv");
        Files.writeString(raw, "event_id\taggregate_key\tpublished_at_ms\tenvelope_hex\n"
                + "e-1\tsale:txn-1\t1800000000200\t" + HexFormat.of().withUpperCase().formatHex(envelope) + "\n"
                + "e-2\tsale:txn-2\tNULL\t00FF\n");
        Path out = dir.resolve("sale-events.tsv");
        assertArrayEquals(new int[] {2, 1}, DecodeSaleEvents.decode(raw, out));

        List<Map<String, String>> rows = RunData.readTsv(out);
        assertEquals(2, rows.size());
        assertEquals("txn-1", rows.get(0).get("sale_id"));
        assertEquals("1800000000123", rows.get(0).get("completed_at_ms"));
        assertEquals("1800000000200", rows.get(0).get("published_at_ms"));
        assertEquals(Map.of("SKU-000001", 3, "SKU-000002", 1), RunData.parseLines(rows.get(0).get("lines")));
        assertEquals("", rows.get(1).get("sale_id"));
        assertEquals(null, rows.get(1).get("published_at_ms"));
        assertTrue(rows.get(1).get("lines").startsWith("UNDECODABLE"));
    }
}
