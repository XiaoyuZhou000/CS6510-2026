package database;

import database.NotificationJson.Line;
import database.NotificationStore.Failure;
import database.NotificationStore.Parked;
import database.NotificationStore.Pending;
import database.SelectionStore.PlannedNotification;
import events.EventIds;
import org.junit.jupiter.api.Test;
import support.NotificationTestSchema;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Parking and the operator retry on the real table (007 T096, FR-016, FR-017): {@code retryParked} for one subscriber
 * and for all answers the exact number of rows it moved, resets {@code charged_failures}, increments
 * {@code operator_retries}, makes the rows due at the retry time, and keeps their {@code notification_id};
 * {@code DELIVERED} rows, and {@code PENDING} rows, are never touched. {@code listParked} lists what is parked.
 */
class ParkingAndOperatorRetryTest {
    private static final Instant T0 = Instant.parse("2026-11-12T09:00:00Z");

    @Test
    void retryParkedMovesExactlyTheParkedRowsBackAndKeepsTheirIdentity() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcNotificationStore store = new JdbcNotificationStore(schema.pool());
            List<String> s1 = new ArrayList<>();
            for (int sale = 1; sale <= 4; sale++) s1.add(select(schema, "tx-a" + sale, T0, "s1").get(0));
            List<String> s2 = new ArrayList<>();
            for (int sale = 1; sale <= 2; sale++) s2.add(select(schema, "tx-b" + sale, T0, "s2").get(0));
            String s3 = select(schema, "tx-c", T0, "s3").get(0);

            // s1: one row delivered, three parked (one of them with an uncharged failure first); s2: two parked;
            // s3: delivered.
            assertEquals(2, store.markDelivered(List.of(s1.get(3), s3), T0.plusMillis(10)));
            store.recordFailures(List.of(new Failure(s1.get(0), false, "broker down")), T0.plusMillis(20), 2);
            List<String> parked = new ArrayList<>(s1.subList(0, 3));
            parked.addAll(s2);
            assertEquals(0, store.recordFailures(failures(parked, "rejected 1"), T0.plusMillis(30), 2));
            assertEquals(5, store.recordFailures(failures(parked, "rejected 2"), T0.plusMillis(40), 2));
            assertEquals(5, schema.notifications("PARKED"));
            String deliveredBefore = deliveredRows(schema);

            List<Parked> listed = store.listParked(Optional.empty(), 100);
            assertEquals(Set.copyOf(parked), listed.stream().map(Parked::notificationId).collect(Collectors.toSet()));
            Parked first = store.listParked(Optional.of("s2"), 100).get(0);
            assertEquals("s2", first.subscriberId());
            assertEquals("A", first.sku());
            assertEquals(2, first.chargedFailures());
            assertEquals("rejected 2", first.lastError());
            assertTrue(first.saleId().startsWith("tx-b"));
            assertEquals(1, store.listParked(Optional.empty(), 1).size(), "the limit holds");
            assertTrue(store.listParked(Optional.of("s3"), 100).isEmpty());

            Instant retryAt = T0.plusSeconds(60);
            assertEquals(3, store.retryParked(Optional.of("s1"), retryAt), "exactly s1's parked rows");
            assertEquals(0, store.retryParked(Optional.of("s1"), retryAt), "nothing of s1 is parked any more");
            assertEquals(0, store.retryParked(Optional.of("s3"), retryAt), "a subscriber with only delivered rows");
            assertEquals(Set.copyOf(s1.subList(0, 3)), ids(schema, "s1", "PENDING"), "the identities are kept");
            assertEquals(3, schema.queryLong("SELECT COUNT(*) FROM notification WHERE subscriber_id = 's1' AND state = 'PENDING' "
                    + "AND parked_at IS NULL AND charged_failures = 0 AND operator_retries = 1"));
            assertEquals(1, schema.queryLong("SELECT uncharged_failures FROM notification WHERE notification_id = ?", s1.get(0)),
                    "uncharged failures are not reset");
            assertEquals(2, schema.notifications("PARKED"), "s2's rows stay parked");
            assertDueAt(store, retryAt, Set.copyOf(s1.subList(0, 3)));

            Instant secondRetry = T0.plusSeconds(120);
            assertEquals(2, store.retryParked(Optional.empty(), secondRetry), "all: only s2's rows are still parked");
            assertEquals(Set.copyOf(s2), ids(schema, "s2", "PENDING"));
            assertEquals(2, schema.queryLong("SELECT COUNT(*) FROM notification WHERE subscriber_id = 's2' AND "
                    + "charged_failures = 0 AND operator_retries = 1 AND parked_at IS NULL"));
            assertEquals(3, schema.queryLong("SELECT COUNT(*) FROM notification WHERE subscriber_id = 's1' AND operator_retries = 1"),
                    "a retry of all does not touch PENDING rows");
            assertEquals(0, store.retryParked(Optional.empty(), secondRetry));

            // Park one of s1's rows again: a second operator retry counts again.
            store.recordFailures(failures(List.of(s1.get(1)), "rejected 3"), T0.plusSeconds(130), 1);
            assertEquals(1, store.retryParked(Optional.empty(), T0.plusSeconds(140)));
            assertEquals(2, schema.queryLong("SELECT operator_retries FROM notification WHERE notification_id = ?", s1.get(1)));

            assertEquals(deliveredBefore, deliveredRows(schema), "DELIVERED rows are never touched");
            assertEquals(2, schema.notifications("DELIVERED"));
            assertEquals(0, store.markDelivered(List.of(s3), T0.plusSeconds(200)), "still delivered once");
        }
    }

    private static List<Failure> failures(List<String> ids, String error) {
        return ids.stream().map(id -> new Failure(id, true, error)).toList();
    }

    /** The rows are due at {@code at} and not a millisecond earlier. */
    private static void assertDueAt(JdbcNotificationStore store, Instant at, Set<String> ids) {
        Set<String> dueBefore = store.selectDue(at.minusMillis(1), 1_000, List.of()).stream()
                .map(Pending::notificationId).collect(Collectors.toSet());
        Set<String> dueAt = store.selectDue(at, 1_000, List.of()).stream()
                .map(Pending::notificationId).collect(Collectors.toSet());
        for (String id : ids) {
            assertFalse(dueBefore.contains(id), id + " is not due before the retry time");
            assertTrue(dueAt.contains(id), id + " is due at the retry time");
        }
    }

    private static Set<String> ids(NotificationTestSchema schema, String subscriber, String state) throws Exception {
        Set<String> ids = new java.util.HashSet<>();
        try (var select = schema.admin().prepareStatement(
                "SELECT notification_id FROM notification WHERE subscriber_id = ? AND state = ?")) {
            select.setString(1, subscriber);
            select.setString(2, state);
            try (var rows = select.executeQuery()) {
                while (rows.next()) ids.add(rows.getString(1));
            }
        }
        return ids;
    }

    /** Every column of the delivered rows, as one string. */
    private static String deliveredRows(NotificationTestSchema schema) throws Exception {
        return schema.queryString("SELECT GROUP_CONCAT(CONCAT_WS('|', notification_id, state, delivered_at, attempt_count, "
                + "charged_failures, uncharged_failures, operator_retries, next_attempt_at, COALESCE(parked_at, '-')) "
                + "ORDER BY notification_id SEPARATOR ';') FROM notification WHERE state = 'DELIVERED'");
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
