package database;

import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.junit.jupiter.api.Assertions.*;

/** Fresh committed reads of positioned rows, over an {@link AnalyticsTestSchema} with rows inserted directly. */
class JdbcAcceptedScanReaderTest {
    @Test
    void pollsAdjacentCommittedPositionsInBatchesAndRetainsBacklog() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            schema.insertPositioned(1, 1200, "sku1");
            JdbcAcceptedScanReader reader = new JdbcAcceptedScanReader(schema.pool());
            var first = reader.readAfter(0, 256);
            assertEquals(256, first.size());
            assertEquals(1, first.getFirst().position());
            assertEquals(256, first.getLast().position());
            var next = reader.readAfter(256, 256);
            assertEquals(257, next.getFirst().position());
            assertEquals(512, next.getLast().position());
            assertEquals(176, reader.readAfter(1024, 256).size());
            assertTrue(reader.readAfter(1200, 256).isEmpty());
            var overlap = reader.prewarm(1000);
            assertEquals(1000, overlap.size());
            assertEquals(1, overlap.getFirst().position());
            assertEquals(1000, overlap.getLast().position());
        }
    }

    @Test
    void freshPollDoesNotSeeConcurrentUncommittedInsert() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            schema.insertPositioned(1, 1, "sku1");
            JdbcAcceptedScanReader reader = new JdbcAcceptedScanReader(schema.pool());
            // The managed pool hands out manual-commit sessions, so this insert stays uncommitted until commit().
            Connection writer = schema.pool().borrow();
            try {
                try (PreparedStatement insert = writer.prepareStatement("INSERT INTO accepted_scan "
                        + "(position, source_transaction_id, source_scan_sequence, sku) VALUES (2, 'tx', 1, 'sku2')")) {
                    insert.executeUpdate();
                }
                assertTrue(reader.readAfter(1, 10).isEmpty());
                writer.commit();
                assertEquals(2, reader.readAfter(1, 10).getFirst().position());
            } finally {
                writer.rollback();
                schema.pool().release(writer);
            }
        }
    }

    @Test
    void refusesMissingPositionDuringPollOrPrewarm() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            JdbcAcceptedScanReader reader = new JdbcAcceptedScanReader(schema.pool());
            schema.insertPositioned(2, 2, "sku1");
            assertThrows(IllegalStateException.class, () -> reader.readAfter(0, 10));
            assertThrows(IllegalStateException.class, () -> reader.prewarm(1000));
        }
    }
}
