package database;

import events.ParkReason;
import events.ParkedRecord;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.InventoryTestSchema;
import support.TestSchema;
import support.TransactionTestSchema;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/** {@code dead_letter_event} through the shared adapter, in all three schemas (D6, FR-013). */
class JdbcDeadLetterStoreTest {

    @Test
    void parkingInTheTransactionSchema() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            assertParkingSemantics(schema);
        }
    }

    @Test
    void parkingInTheInventorySchema() throws Exception {
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            assertParkingSemantics(schema);
        }
    }

    @Test
    void parkingInTheAnalyticsSchema() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            assertParkingSemantics(schema);
        }
    }

    private static void assertParkingSemantics(TestSchema schema) throws Exception {
        JdbcDeadLetterStore store = new JdbcDeadLetterStore(schema.pool(), "test.deadLetter");
        Instant first = Instant.parse("2026-10-29T10:00:00.250Z");
        byte[] bytes = {0, 1, 2, (byte) 0xfe, (byte) 0xff};
        String longDetail = "x".repeat(800);
        store.park(new ParkedRecord("e-1", "selfcheckout.checkout.v1", 7, 42L, "CheckoutRequested", 1,
                ParkReason.PAYLOAD_MISMATCH, longDetail, bytes, first));

        try (PreparedStatement select = schema.admin().prepareStatement("SELECT event_id, topic, partition_no, record_offset, "
                + "event_type, schema_version, reason, detail, record_bytes, first_parked_at, last_parked_at, park_count "
                + "FROM dead_letter_event WHERE event_id = 'e-1'");
             ResultSet row = select.executeQuery()) {
            assertTrue(row.next());
            assertEquals("selfcheckout.checkout.v1", row.getString("topic"));
            assertEquals(7, row.getInt("partition_no"));
            assertEquals(42L, row.getLong("record_offset"));
            assertEquals("CheckoutRequested", row.getString("event_type"));
            assertEquals(1, row.getInt("schema_version"));
            assertEquals("PAYLOAD_MISMATCH", row.getString("reason"));
            assertEquals(500, row.getString("detail").length(), "detail is truncated to 500 characters");
            assertArrayEquals(bytes, row.getBytes("record_bytes"));
            assertEquals(first, row.getTimestamp("first_parked_at").toInstant());
            assertEquals(first, row.getTimestamp("last_parked_at").toInstant());
            assertEquals(1, row.getInt("park_count"));
        }

        Instant again = first.plusSeconds(90);
        store.park(new ParkedRecord("e-1", "selfcheckout.checkout.v1", 7, 77L, "CheckoutRequested", 1,
                ParkReason.PAYLOAD_MISMATCH, "again", bytes, again));
        assertEquals(1, store.countParked(), "the same event_id is one row");
        assertEquals(2, schema.queryLong("SELECT park_count FROM dead_letter_event WHERE event_id = 'e-1'"));
        assertEquals(again, timestamp(schema, "last_parked_at"));
        assertEquals(first, timestamp(schema, "first_parked_at"), "the first park time is kept");

        for (int i = 0; i < 2; i++) {
            store.park(new ParkedRecord(null, "selfcheckout.checkout.v1", 3, 100L + i, null, null,
                    ParkReason.UNDECODABLE, "not an envelope", new byte[] {9}, first));
        }
        assertEquals(3, store.countParked(), "rows without an event_id never collide");
        assertEquals(2, schema.queryLong("SELECT COUNT(*) FROM dead_letter_event WHERE event_id IS NULL AND schema_version IS NULL"));
        assertEquals(3, schema.parkedCount());

        SQLException invalidReason = assertThrows(SQLException.class, () -> schema.update(
                "INSERT INTO dead_letter_event (topic, partition_no, record_offset, reason, record_bytes, first_parked_at, "
                        + "last_parked_at) VALUES ('t', 0, 0, 'GUESSED', x'00', NOW(3), NOW(3))"));
        assertTrue(invalidReason.getMessage().contains("chk_dead_letter_reason"), invalidReason.getMessage());

        schema.resetEventTables();
        assertEquals(0, store.countParked(), "the test schema reset empties the event tables");
    }

    private static Instant timestamp(TestSchema schema, String column) throws Exception {
        try (PreparedStatement select = schema.admin().prepareStatement(
                "SELECT " + column + " FROM dead_letter_event WHERE event_id = 'e-1'");
             ResultSet row = select.executeQuery()) {
            assertTrue(row.next());
            return row.getTimestamp(1).toInstant();
        }
    }
}
