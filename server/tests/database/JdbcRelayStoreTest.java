package database;

import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.TransactionTestSchema;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The outbox relay's reads and writes of {@code accepted_scan} (contracts/scan-event-delivery.md section 2). */
class JdbcRelayStoreTest {

    @Test
    void selectUndeliveredOrdersByScanIdSkipsDeliveredRowsAndHonoursTheLimit() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            schema.insertOpenTransaction("tx-1");
            schema.insertOpenTransaction("tx-2");
            long a = schema.insertScan("tx-2", 1, "SKU-000002");
            long b = schema.insertScan("tx-1", 1, "SKU-000001");
            long c = schema.insertScan("tx-1", 2, "SKU-000003");
            long d = schema.insertScan("tx-2", 2, "SKU-000004");
            schema.update("UPDATE accepted_scan SET delivered_at = NOW(3) WHERE scan_id = ?", b);
            JdbcRelayStore store = new JdbcRelayStore(schema.pool());

            List<RelayStore.UndeliveredScan> undelivered = store.selectUndelivered(10);
            assertEquals(List.of(a, c, d), undelivered.stream().map(RelayStore.UndeliveredScan::scanId).toList());
            RelayStore.UndeliveredScan first = undelivered.getFirst();
            assertEquals("tx-2", first.transactionId());
            assertEquals(1, first.scanSequence());
            assertEquals("SKU-000002", first.sku());
            assertEquals("Name of SKU-000002", first.productName());
            assertNotNull(first.acceptedAt());
            assertEquals(List.of(a, c), store.selectUndelivered(2).stream().map(RelayStore.UndeliveredScan::scanId).toList());
            assertEquals(3, store.countUndelivered());
        }
    }

    @Test
    void markDeliveredSetsExactlyTheGivenRowsInOneStatementAndOneCommit() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create(TransactionTestSchema.Options.defaults().decorator(counting::wrap))) {
            schema.insertOpenTransaction("tx-1");
            long[] ids = new long[6];
            for (int i = 0; i < ids.length; i++) ids[i] = schema.insertScan("tx-1", i + 1, "SKU-000001");
            JdbcRelayStore store = new JdbcRelayStore(schema.pool());
            Instant delivered = Instant.parse("2026-10-21T12:00:00.123Z");
            try (CountingConnections.Scope scope = counting.scope()) {
                assertEquals(3, store.markDelivered(List.of(ids[0], ids[2], ids[4]), delivered));
                assertEquals(1, scope.counts().updates());
                assertEquals(1, scope.counts().statements());
                assertEquals(1, scope.counts().commits());
            }
            assertEquals(List.of(ids[0], ids[2], ids[4]), schema.deliveredIds());
            assertEquals(3, store.countUndelivered());
            // A second relay acknowledging the same rows later keeps the first delivery time.
            assertEquals(1, store.markDelivered(List.of(ids[0], ids[1]), delivered.plusSeconds(60)));
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM accepted_scan WHERE scan_id = ? AND delivered_at = ?",
                    ids[0], java.sql.Timestamp.from(delivered)));
        }
    }

    /** Relay half of {@code JdbcScanMaterializerTest#lowerEventIdCommittingLateIsStillDiscovered}. */
    @Test
    void rowCommittedLateWithALowerScanIdIsSelectedByTheNextRound() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            schema.insertOpenTransaction("tx-low");
            schema.insertOpenTransaction("tx-high");
            JdbcRelayStore store = new JdbcRelayStore(schema.pool());
            Connection low = schema.pool().borrow();
            try {
                long lowId = TransactionTestSchema.insertScan(low, "tx-low", 1, "SKU-000001");
                long highId = schema.insertScan("tx-high", 1, "SKU-000002");
                assertTrue(lowId < highId);

                List<RelayStore.UndeliveredScan> round = store.selectUndelivered(10);
                assertEquals(List.of(highId), round.stream().map(RelayStore.UndeliveredScan::scanId).toList(),
                        "an uncommitted row is invisible to the relay");
                store.markDelivered(List.of(highId), Instant.now());
                low.commit();

                List<RelayStore.UndeliveredScan> next = store.selectUndelivered(10);
                assertEquals(List.of(lowId), next.stream().map(RelayStore.UndeliveredScan::scanId).toList(),
                        "no cursor is kept, so the late row with the lower scan_id is not skipped");
            } finally {
                low.rollback();
                schema.pool().release(low);
            }
        }
    }
}
