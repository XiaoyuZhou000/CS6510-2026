import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** T065: durable, restartable receipts (FR-021, data-model.md section 5, research N10). */
class ReceiptWriterTest {

    private static List<String> lines(Path file) throws Exception {
        if (!Files.exists(file)) return List.of();
        return Files.readAllLines(file, StandardCharsets.UTF_8).stream().filter(l -> !l.isBlank()).toList();
    }

    @Test
    void writesOneJsonLinePerRecordWithEveryField(@TempDir Path dir) throws Exception {
        try (ReceiptWriter writer = new ReceiptWriter(dir, "sub-0001")) {
            assertFalse(writer.record("n-1", "txn-1", "SKU-000001", 3, 1_000L, 1_250L, 0L));
            writer.flush();
        }
        List<String> written = lines(dir.resolve("sub-0001.jsonl"));
        assertEquals(1, written.size());
        Map<String, Object> receipt = Json.parseObject(written.get(0));
        assertEquals(List.of("subscriberId", "notificationId", "saleId", "sku", "quantity", "completedAtMs",
                "receivedAtMs", "channelOffset", "redelivery"), List.copyOf(receipt.keySet()));
        assertEquals("sub-0001", receipt.get("subscriberId"));
        assertEquals("n-1", receipt.get("notificationId"));
        assertEquals("txn-1", receipt.get("saleId"));
        assertEquals("SKU-000001", receipt.get("sku"));
        assertEquals(3L, receipt.get("quantity"));
        assertEquals(1_000L, receipt.get("completedAtMs"));
        assertEquals(1_250L, receipt.get("receivedAtMs"));
        assertEquals(0L, receipt.get("channelOffset"));
        assertEquals(Boolean.FALSE, receipt.get("redelivery"));
    }

    @Test
    void aRepeatedNotificationIdIsARedelivery(@TempDir Path dir) throws Exception {
        try (ReceiptWriter writer = new ReceiptWriter(dir, "sub-0001")) {
            assertFalse(writer.record("n-1", "txn-1", "SKU-000001", 3, 1_000L, 1_250L, 0L));
            assertFalse(writer.record("n-2", "txn-1", "SKU-000002", 1, 1_000L, 1_260L, 1L));
            assertTrue(writer.record("n-1", "txn-1", "SKU-000001", 3, 1_000L, 1_900L, 2L));
            writer.flush();
            assertEquals(2, writer.received());
            assertEquals(1, writer.redeliveries());
        }
        List<String> written = lines(dir.resolve("sub-0001.jsonl"));
        assertEquals(3, written.size());
        assertEquals(Boolean.TRUE, Json.parseObject(written.get(2)).get("redelivery"));
    }

    @Test
    void reopeningResumesAfterTheHighestOffsetAndKeepsKnownIds(@TempDir Path dir) throws Exception {
        try (ReceiptWriter writer = new ReceiptWriter(dir, "sub-0007")) {
            assertEquals(0L, writer.resumeOffset(), "an empty channel starts at offset 0");
            writer.record("n-1", "txn-1", "SKU-000001", 1, 1L, 2L, 0L);
            writer.record("n-2", "txn-2", "SKU-000001", 1, 1L, 2L, 1L);
            writer.recordUndecodable(2L, 3L, "not an EventEnvelope");
            writer.flush();
            assertEquals(3L, writer.resumeOffset());
        }
        try (ReceiptWriter reopened = new ReceiptWriter(dir, "sub-0007")) {
            assertEquals(3L, reopened.resumeOffset(), "highest recorded channelOffset + 1, undecodable lines included");
            assertEquals(2, reopened.received());
            assertEquals(1, reopened.undecodable());
            assertTrue(reopened.knows("n-2"));
            // The unflushed tail received again after a restart is recorded as a redelivery.
            assertTrue(reopened.record("n-2", "txn-2", "SKU-000001", 1, 1L, 9L, 3L));
            assertFalse(reopened.record("n-3", "txn-3", "SKU-000001", 1, 1L, 9L, 4L));
            reopened.flush();
            assertEquals(5L, reopened.resumeOffset());
        }
        List<String> written = lines(dir.resolve("sub-0007.jsonl"));
        assertEquals(5, written.size());
        Map<String, Object> undecodable = Json.parseObject(written.get(2));
        assertEquals(Boolean.TRUE, undecodable.get("undecodable"));
        assertEquals(2L, undecodable.get("channelOffset"));
    }

    @Test
    void recordsReachTheFileWhenTheBatchIsFlushed(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("sub-0002.jsonl");
        try (ReceiptWriter writer = new ReceiptWriter(dir, "sub-0002")) {
            writer.record("n-1", "txn-1", "SKU-000001", 1, 1L, 2L, 0L);
            writer.record("n-2", "txn-1", "SKU-000002", 1, 1L, 2L, 1L);
            assertEquals(0, lines(file).size(), "a batch is buffered until it is flushed");
            writer.flush();
            assertEquals(2, lines(file).size(), "flushed after the batch");
            writer.record("n-3", "txn-2", "SKU-000001", 1, 1L, 2L, 2L);
            writer.flush();
            assertEquals(3, lines(file).size());
        }
    }
}
