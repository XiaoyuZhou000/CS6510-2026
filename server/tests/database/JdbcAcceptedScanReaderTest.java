package database;

import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.AnalyticsDatabase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JdbcAcceptedScanReaderTest {
    @Test
    void pollsAdjacentCommittedPositionsInBatchesAndRetainsBacklog() throws Exception {
        try (AnalyticsDatabase db = open()) {
            db.appendScans(Collections.nCopies(1200, "sku1"));
            JdbcAcceptedScanReader reader = new JdbcAcceptedScanReader(db.pool);
            var first = reader.readAfter(0, 256);
            assertEquals(256, first.size());
            assertEquals(1, first.getFirst().position());
            assertEquals(256, first.getLast().position());
            var next = reader.readAfter(256, 256);
            assertEquals(257, next.getFirst().position());
            assertEquals(512, next.getLast().position());
            assertEquals(176, reader.readAfter(1024, 256).size());
            assertTrue(reader.readAfter(1200, 256).isEmpty());
            assertEquals(1000, reader.prewarm(1000).size());
        }
    }

    @Test
    void freshPollDoesNotSeeConcurrentUncommittedInsert() throws Exception {
        try (AnalyticsDatabase db = open()) {
            db.appendScans(List.of("sku1"));
            JdbcAcceptedScanReader reader = new JdbcAcceptedScanReader(db.pool);
            Connection writer = db.pool.borrow();
            try {
                writer.setAutoCommit(false);
                try (PreparedStatement insert = writer.prepareStatement(
                        "INSERT INTO accepted_scan(position,transaction_id,sku,unit_price) VALUES (2,'tx','sku2',1.00)")) {
                    insert.executeUpdate();
                }
                assertTrue(reader.readAfter(1, 10).isEmpty());
                writer.commit();
                assertEquals(2, reader.readAfter(1, 10).getFirst().position());
            } finally {
                writer.rollback();
                writer.setAutoCommit(true);
                db.pool.release(writer);
            }
        }
    }

    @Test
    void refusesMissingPositionDuringPollOrPrewarm() throws Exception {
        try (AnalyticsDatabase db = open()) {
            JdbcAcceptedScanReader reader = new JdbcAcceptedScanReader(db.pool);
            Connection writer = db.pool.borrow();
            try (PreparedStatement insert = writer.prepareStatement(
                    "INSERT INTO accepted_scan(position,transaction_id,sku,unit_price) VALUES (2,'tx','sku1',1.00)")) {
                insert.executeUpdate();
            } finally {
                db.pool.release(writer);
            }
            assertThrows(IllegalStateException.class, () -> reader.readAfter(0, 10));
            assertThrows(IllegalStateException.class, () -> reader.prewarm(1000));
        }
    }

    private static AnalyticsDatabase open() throws Exception {
        try { return new AnalyticsDatabase(); }
        catch (SQLException unavailable) { throw new TestAbortedException("MySQL unavailable", unavailable); }
    }
}
