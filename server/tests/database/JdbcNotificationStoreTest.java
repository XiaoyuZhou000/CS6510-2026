package database;

import database.NotificationJson.Line;
import database.NotificationStore.Failure;
import database.NotificationStore.Pending;
import database.SelectionStore.PlannedNotification;
import events.EventIds;
import org.junit.jupiter.api.Test;
import support.NotificationTestSchema;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The delivery record (007 T041, research N8): marking touches only {@code PENDING} rows, failures count attempts
 * and back off, parking and the sweep follow the state machine, and the table's CHECKs reject inconsistent rows.
 */
class JdbcNotificationStoreTest {
    private static final Instant T0 = Instant.parse("2026-11-11T12:00:00Z");

    @Test
    void markDeliveredUpdatesOnlyPendingRowsAndSetsDeliveredAt() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcNotificationStore store = new JdbcNotificationStore(schema.pool());
            List<String> ids = select(schema, "tx-1", T0, "s1", "s2", "s3");
            assertEquals(1, store.park(List.of(ids.get(2)), T0));
            assertEquals(2, store.markDelivered(ids, T0.plusMillis(5)), "the parked row is not marked");
            assertEquals(0, store.markDelivered(ids, T0.plusMillis(9)), "a delivered row never changes again");
            assertEquals(2, schema.notifications("DELIVERED"));
            assertEquals(1, schema.notifications("PARKED"));
            assertEquals(2, schema.queryLong("SELECT COUNT(*) FROM notification WHERE delivered_at IS NOT NULL AND attempt_count = 1"));
            assertEquals(new NotificationStore.Counts(0, 2, 1, 0, 0), store.counts());
        }
    }

    @Test
    void recordFailuresCountsAttemptsKeepsTheErrorAndBacksOff() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcNotificationStore store = new JdbcNotificationStore(schema.pool());
            List<String> ids = select(schema, "tx-1", T0, "s1", "s2");
            assertEquals(0, store.recordFailures(List.of(new Failure(ids.get(0), true, "rejected"),
                    new Failure(ids.get(1), false, "broker down")), T0, 8));
            assertEquals(1, schema.queryLong("SELECT charged_failures FROM notification WHERE subscriber_id = 's1'"));
            assertEquals(0, schema.queryLong("SELECT uncharged_failures FROM notification WHERE subscriber_id = 's1'"));
            assertEquals(1, schema.queryLong("SELECT uncharged_failures FROM notification WHERE subscriber_id = 's2'"));
            assertEquals(2, schema.queryLong("SELECT SUM(attempt_count) FROM notification"));
            assertEquals("rejected", schema.queryString("SELECT last_error FROM notification WHERE subscriber_id = 's1'"));
            assertEquals("broker down", schema.queryString("SELECT last_error FROM notification WHERE subscriber_id = 's2'"));

            // One failure: due after 200 ms x 2^1.
            assertTrue(store.selectDue(T0.plusMillis(399), 10, List.of()).isEmpty());
            assertEquals(2, store.selectDue(T0.plusMillis(400), 10, List.of()).size());
            // Two more: 200 ms x 2^3 = 1.6 s after the last.
            for (int i = 0; i < 2; i++) store.recordFailures(List.of(new Failure(ids.get(0), false, "x")), T0, 8);
            assertFalse(store.selectDue(T0.plusMillis(1_599), 10, List.of()).stream().anyMatch(p -> p.subscriberId().equals("s1")));
            assertTrue(store.selectDue(T0.plusMillis(1_600), 10, List.of()).stream().anyMatch(p -> p.subscriberId().equals("s1")));
            // The backoff is capped at 5 s.
            for (int i = 0; i < 6; i++) store.recordFailures(List.of(new Failure(ids.get(1), false, "x")), T0, 8);
            assertTrue(store.selectDue(T0.plusMillis(5_000), 10, List.of()).stream().anyMatch(p -> p.subscriberId().equals("s2")));
            assertEquals(new NotificationStore.Counts(2, 0, 0, 1, 9), store.counts());
        }
    }

    @Test
    void theEighthChargedFailureParksTheRowAndARetryRestoresIt() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcNotificationStore store = new JdbcNotificationStore(schema.pool());
            List<String> ids = select(schema, "tx-1", T0, "s1", "s2");
            for (int i = 1; i < 8; i++) {
                assertEquals(0, store.recordFailures(List.of(new Failure(ids.get(0), true, "rejected " + i)), T0, 8));
            }
            assertEquals(1, store.recordFailures(List.of(new Failure(ids.get(0), true, "rejected 8"),
                    new Failure(ids.get(1), false, "broker down")), T0, 8));
            assertEquals(1, schema.notifications("PARKED"));
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM notification WHERE state = 'PARKED' AND parked_at IS NOT NULL "
                    + "AND charged_failures = 8 AND subscriber_id = 's1'"));
            assertEquals(0, store.markDelivered(List.of(ids.get(0)), T0), "a parked row is not delivered");

            assertEquals(0, store.retryParked(Optional.of("s2"), T0.plusSeconds(30)));
            assertEquals(1, store.retryParked(Optional.empty(), T0.plusSeconds(30)));
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM notification WHERE subscriber_id = 's1' AND state = 'PENDING' "
                    + "AND parked_at IS NULL AND charged_failures = 0 AND operator_retries = 1"));
            assertEquals(ids.get(0), schema.queryString("SELECT notification_id FROM notification WHERE subscriber_id = 's1'"),
                    "the identity is kept");
        }
    }

    @Test
    void selectDueOrdersByNextAttemptAndLeavesOutDeliveredParkedAndSkippedSubscribers() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcNotificationStore store = new JdbcNotificationStore(schema.pool());
            List<String> late = select(schema, "tx-late", T0.plusMillis(30), "s1");
            List<String> early = select(schema, "tx-early", T0.plusMillis(10), "s1", "s2");
            List<String> middle = select(schema, "tx-mid", T0.plusMillis(20), "s3", "s4");
            store.markDelivered(List.of(middle.get(0)), T0.plusMillis(25));
            store.park(List.of(middle.get(1)), T0.plusMillis(25));

            List<Pending> due = store.selectDue(T0.plusSeconds(1), 1_000, List.of());
            assertEquals(List.of("tx-early", "tx-early", "tx-late"), due.stream().map(Pending::saleId).toList());
            assertEquals(java.util.Set.copyOf(early), java.util.Set.of(due.get(0).notificationId(), due.get(1).notificationId()));
            assertEquals(late.get(0), due.get(2).notificationId());
            Pending first = due.get(0);
            assertEquals("A", first.sku());
            assertEquals(2, first.quantity());
            assertEquals(0, first.subscriptionVersion());
            assertEquals(T0.plusMillis(10), first.createdAt());
            assertEquals(T0.minusSeconds(5), first.completedAt());
            assertEquals(List.of("s2"), store.selectDue(T0.plusSeconds(1), 1_000, List.of("s1")).stream()
                    .map(Pending::subscriberId).toList());
            assertEquals(1, store.selectDue(T0.plusSeconds(1), 1, List.of()).size());
            assertEquals(2, store.selectDue(T0.plusMillis(15), 1_000, List.of()).size(), "only rows already due: tx-early's");
        }
    }

    @Test
    void theCheckConstraintsRejectInconsistentRows() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            List<String> ids = select(schema, "tx-1", T0, "s1");
            assertThrows(SQLException.class, () -> schema.update("UPDATE notification SET state = 'DELIVERED' WHERE notification_id = ?",
                    ids.get(0)), "DELIVERED needs delivered_at");
            assertThrows(SQLException.class, () -> schema.update("UPDATE notification SET delivered_at = NOW(3) WHERE notification_id = ?",
                    ids.get(0)), "only DELIVERED has delivered_at");
            assertThrows(SQLException.class, () -> schema.update("UPDATE notification SET state = 'PARKED' WHERE notification_id = ?",
                    ids.get(0)), "PARKED needs parked_at");
            assertThrows(SQLException.class, () -> schema.update("UPDATE notification SET quantity = 0 WHERE notification_id = ?",
                    ids.get(0)), "a quantity is positive");
            assertEquals(1, schema.notifications("PENDING"));
        }
    }

    /** Selects sale {@code saleId} (2 x A) for {@code subscribers} at {@code at}; returns the IDs in subscriber order. */
    private static List<String> select(NotificationTestSchema schema, String saleId, Instant at, String... subscribers)
            throws Exception {
        List<PlannedNotification> planned = new ArrayList<>();
        for (String subscriber : subscribers) {
            planned.add(new PlannedNotification(EventIds.notificationId(saleId, "A", subscriber), "A", subscriber, 2));
        }
        new JdbcSelectionStore(schema.pool(), Clock.fixed(at, ZoneOffset.UTC)).select(
                new SelectionStore.Sale(saleId, EventIds.saleEventId(saleId), T0.minus(Duration.ofSeconds(5)), List.of(new Line("A", 2))),
                (version, changes) -> new SelectionStore.Plan(planned, planned.size()));
        return planned.stream().map(PlannedNotification::notificationId).toList();
    }
}
