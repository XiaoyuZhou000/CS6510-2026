package database;

import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.InventoryTestSchema;
import support.TransactionTestSchema;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** The FR-039 operational status summaries, each read with its own service's pool (contracts/service-deployment.md). */
class ServiceStatusQueriesTest {
    private static final Instant NOW = Instant.parse("2026-10-21T12:00:00Z");

    @Test
    void transactionReportsPendingAlarmedUndeliveredBackgroundAndReplicaState() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            TransactionStatusQueries queries = new TransactionStatusQueries(schema.pool(), Clock.fixed(NOW, ZoneOffset.UTC));
            TransactionStatusQueries.Status empty = queries.read();
            assertEquals(new TransactionStatusQueries.Status(0, 0, 0, 0, 0, null, 0, null), empty);
            assertFalse(empty.pendingWork());
            assertEquals("status service=transaction pending_work=no pending_attempts=0 alarmed_attempts=0 "
                    + "undelivered_scans=0 background_completions=0 completions_without_response=0 "
                    + "replica_version=none replica_products=0 replica_age_seconds=none", empty.line());

            attempt(schema, "pending", "PENDING", "NONE", null);
            attempt(schema, "retrying", "PENDING", "PENDING_ERROR", "UNAVAILABLE: inventory is down");
            attempt(schema, "mismatch", "PENDING", "NONE", "FAILED_PRECONDITION: payload differs");
            attempt(schema, "invalid", "PENDING", "NONE", "INVALID_ARGUMENT: unknown sku");
            attempt(schema, "integrity", "PENDING", "NONE", "INTEGRITY: the attempt is pending but no longer active");
            attempt(schema, "background", "COMPLETED", "PENDING_ERROR", null);
            attempt(schema, "unanswered", "COMPLETED", "NONE", null);
            attempt(schema, "answered", "COMPLETED", "SUCCESS", null);
            schema.insertScan("pending", 1, "SKU-000001");
            schema.insertScan("pending", 2, "SKU-000002");
            long delivered = schema.insertScan("answered", 1, "SKU-000003");
            schema.update("UPDATE accepted_scan SET delivered_at = NOW(3) WHERE scan_id = ?", delivered);
            schema.seedReplica(5, "catalog-v1");
            schema.update("UPDATE product_replica_meta SET refreshed_at = ? WHERE meta_key = 1",
                    Timestamp.from(NOW.minusSeconds(42)));

            TransactionStatusQueries.Status status = queries.read();
            assertEquals(new TransactionStatusQueries.Status(5, 3, 2, 1, 1, "catalog-v1", 5, Duration.ofSeconds(42)), status);
            assertTrue(status.pendingWork());
            assertEquals("status service=transaction pending_work=yes pending_attempts=5 alarmed_attempts=3 "
                    + "undelivered_scans=2 background_completions=1 completions_without_response=1 "
                    + "replica_version=catalog-v1 replica_products=5 replica_age_seconds=42", status.line());
        }
    }

    @Test
    void inventoryReportsCheckoutOperationsByState() throws Exception {
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            InventoryStatusQueries queries = new InventoryStatusQueries(schema.pool());
            InventoryStatusQueries.Status empty = queries.read();
            assertEquals(Map.of("APPLIED", 0L, "REJECTED", 0L, "RESTORED", 0L, "BLOCKED", 0L), empty.operations());
            assertEquals("status service=inventory applied=0 rejected=0 restored=0 blocked=0", empty.line());

            operation(schema, "a", "APPLIED");
            operation(schema, "b", "APPLIED");
            operation(schema, "c", "REJECTED");
            operation(schema, "d", "RESTORED");
            operation(schema, "e", "BLOCKED");
            operation(schema, "f", "BLOCKED");
            InventoryStatusQueries.Status status = queries.read();
            assertEquals(Map.of("APPLIED", 2L, "REJECTED", 1L, "RESTORED", 1L, "BLOCKED", 2L), status.operations());
            assertEquals("status service=inventory applied=2 rejected=1 restored=1 blocked=2", status.line());
        }
    }

    @Test
    void analyticsReportsPositionsCheckpointAndTheHighestEligibleBoundary() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            AnalyticsStatusQueries queries = new AnalyticsStatusQueries(schema.pool());
            AnalyticsStatusQueries.Status empty = queries.read();
            assertEquals(new AnalyticsStatusQueries.Status(0, 0), empty);
            assertEquals(0, empty.highestEligibleBoundary());
            assertFalse(empty.pendingWork());
            assertEquals("status service=analytics pending_work=no last_position=0 last_window_end=0 "
                    + "highest_eligible_boundary=0 unpublished_windows=0", empty.line());

            schema.insertPositioned(1, 999, "SKU-000001");
            assertEquals(0, queries.read().highestEligibleBoundary(), "999 positions complete no window");
            schema.insertPositioned(1000, 1600, "SKU-000001");
            AnalyticsStatusQueries.Status behind = queries.read();
            assertEquals(new AnalyticsStatusQueries.Status(1600, 0), behind);
            assertEquals(1500, behind.highestEligibleBoundary());
            assertEquals(2, behind.unpublishedWindows());
            assertTrue(behind.pendingWork());

            schema.update("UPDATE analytics_checkpoint SET last_window_end = 1000 WHERE checkpoint_key = 1");
            assertEquals("status service=analytics pending_work=yes last_position=1600 last_window_end=1000 "
                    + "highest_eligible_boundary=1500 unpublished_windows=1", queries.read().line());
            schema.update("UPDATE analytics_checkpoint SET last_window_end = 1500 WHERE checkpoint_key = 1");
            AnalyticsStatusQueries.Status caughtUp = queries.read();
            assertFalse(caughtUp.pendingWork());
            assertEquals(0, caughtUp.unpublishedWindows());
        }
    }

    private static void attempt(TransactionTestSchema schema, String transactionId, String state, String clientOutcome,
                                String lastError) throws Exception {
        schema.insertOpenTransaction(transactionId);
        schema.update("INSERT INTO checkout_attempt (transaction_id, attempt_number, state, `lines`, item_count, "
                + "total_amount, client_outcome, created_at, settled_at, last_error) "
                + "VALUES (?, 1, ?, '[]', 0, 0.00, ?, NOW(3), " + (state.equals("PENDING") ? "NULL" : "NOW(3)") + ", ?)",
                transactionId, state, clientOutcome, lastError);
    }

    private static void operation(InventoryTestSchema schema, String transactionId, String state) throws Exception {
        schema.update("INSERT INTO checkout_operation (transaction_id, attempt_number, state, rejection_reason, `lines`, "
                + "restored_at) VALUES (?, 1, ?, ?, ?, " + (state.equals("RESTORED") ? "NOW(3)" : "NULL") + ")",
                transactionId, state, state.equals("REJECTED") ? "INSUFFICIENT_STOCK" : null,
                state.equals("BLOCKED") ? null : "[]");
    }
}
