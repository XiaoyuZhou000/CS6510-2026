package unit.notification;

import database.NotificationStore.Pending;
import events.EnvelopeCodec;
import events.EventIds;
import notification.NotificationDispatcher;
import org.junit.jupiter.api.Test;
import unit.notification.DispatcherFakes.FakeStore;
import unit.notification.DispatcherFakes.Fixture;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The dispatch table of contracts/notification-delivery.md (007 T042), with a {@code CountingRecordSender} over a
 * sender whose confirmations the test completes, a manual clock, and in-memory stores: the immediate path, lanes of
 * at most 64 in flight served round-robin, batched confirmations, the failure backoff, the sweep after a restart, and
 * provisioning before the first send.
 */
class NotificationDispatcherTest {
    private static final Instant T0 = Instant.parse("2026-11-11T12:00:00Z");

    @Test
    void theImmediatePathSendsRowsAsSoonAsTheyAreTakenIn() {
        Fixture fixture = new Fixture();
        fixture.dispatcher.runOnce();   // the startup sweep finds nothing; the next one is a second away
        List<Pending> rows = fixture.store.select("tx-1", "s1", "s2");
        fixture.dispatcher.accept(rows);
        fixture.dispatcher.runOnce();
        assertEquals(2, fixture.counting.sends());
        assertEquals(List.of("chan-s1", "chan-s2"), fixture.transport.topics());
        Map.Entry<String, byte[]> first = fixture.transport.sent.get(0);
        var event = EnvelopeCodec.decode(first.getValue(), "s1");
        assertEquals(EventIds.notificationId("tx-1", "A", "s1"), event.eventId(), "event_id = notification_id");
        assertEquals("tx-1", event.correlationId());
        assertEquals("notification", event.producer());
        assertEquals(T0.toEpochMilli(), event.occurredAtMs(), "occurred_at_ms = the selection time");
    }

    @Test
    void lanesHoldAtMost64InFlightAndAreServedRoundRobin() {
        Fixture fixture = new Fixture();
        List<Pending> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) rows.addAll(fixture.store.select("tx-" + i, "s1"));
        for (int i = 0; i < 5; i++) rows.addAll(fixture.store.select("tx-b" + i, "s2"));
        fixture.dispatcher.accept(rows);
        fixture.dispatcher.runOnce();
        assertEquals(64, fixture.dispatcher.inFlight("s1"));
        assertEquals(36, fixture.dispatcher.queued("s1"));
        assertEquals(5, fixture.dispatcher.inFlight("s2"));
        assertEquals(69, fixture.counting.sends());
        assertEquals(List.of("chan-s1", "chan-s2", "chan-s1", "chan-s2"), fixture.transport.topics().subList(0, 4),
                "the lanes take turns");

        fixture.transport.confirm(10);   // the first ten sends: five on each lane
        fixture.dispatcher.runOnce();
        assertEquals(64, fixture.dispatcher.inFlight("s1"), "confirmed sends make room for queued rows");
        assertEquals(31, fixture.dispatcher.queued("s1"));
        assertEquals(0, fixture.dispatcher.inFlight("s2"));
    }

    @Test
    void confirmationsAreMarkedInBatchesOf20MillisecondsOr200Ids() {
        Fixture fixture = new Fixture();
        fixture.dispatcher.accept(fixture.store.select("tx-1", "s1", "s2", "s3"));
        fixture.dispatcher.runOnce();
        fixture.transport.confirmAll();
        fixture.dispatcher.runOnce();
        assertTrue(fixture.store.markCalls.isEmpty(), "a batch younger than 20 ms waits");
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        assertEquals(List.of(3), fixture.store.markCalls);
        assertEquals(3, fixture.store.count("DELIVERED"));
        assertEquals(0, fixture.dispatcher.tracked());

        List<Pending> many = new ArrayList<>();
        for (int i = 0; i < 200; i++) many.addAll(fixture.store.select("tx-m" + i, "s" + (i % 4)));
        fixture.dispatcher.accept(many);
        fixture.dispatcher.runOnce();
        fixture.transport.confirmAll();
        fixture.dispatcher.runOnce();
        assertEquals(List.of(3, 200), fixture.store.markCalls, "200 IDs are written at once, in one batch");
    }

    @Test
    void failuresBackOffAndAreChargedOnlyWhenAnotherChannelConfirmed() {
        Fixture fixture = new Fixture();
        fixture.dispatcher.accept(fixture.store.select("tx-1", "s1"));
        fixture.dispatcher.runOnce();
        fixture.transport.failAll("broker unavailable");
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        FakeStore.Row row = fixture.store.only("s1");
        assertEquals(1, row.uncharged, "no other channel confirmed: a broker-wide failure");
        assertEquals(0, row.charged);
        assertEquals(fixture.clock.instant().plusMillis(400), row.nextAttemptAt, "200 ms x 2^1");
        assertTrue(row.lastError.contains("broker unavailable"));

        // s2 confirms after s1's next attempt started, so s1's failure is charged.
        fixture.clock.advance(Duration.ofSeconds(1));
        fixture.dispatcher.accept(fixture.store.select("tx-2", "s2"));
        fixture.dispatcher.runOnce();
        assertEquals(2, fixture.transport.pending(), "the sweep re-sent s1's due row and s2's row went out");
        fixture.clock.advance(Duration.ofMillis(1));
        fixture.transport.confirm("s2");
        fixture.transport.fail("s1", "rejected");
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        assertEquals(1, row.charged);
        assertEquals(1, row.uncharged);
        assertEquals(fixture.clock.instant().plusMillis(800), row.nextAttemptAt, "200 ms x 2^2");
    }

    @Test
    void theEighthChargedFailureParksTheRowAndCountsIt() {
        Fixture fixture = new Fixture();
        FakeStore.Row row = fixture.store.rows.get(fixture.store.select("tx-1", "s1").get(0).notificationId());
        row.charged = 7;
        fixture.store.select("tx-2", "s2");
        fixture.dispatcher.runOnce();
        fixture.clock.advance(Duration.ofMillis(1));
        fixture.transport.confirm("s2");
        fixture.transport.fail("s1", "rejected");
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        assertEquals("PARKED", row.state);
        assertEquals(1, fixture.dispatcher.parkedSinceStart());
        assertTrue(fixture.dispatcher.alarms().isEmpty(), "the parked alarm comes from the database count (NotificationStatus)");
    }

    @Test
    void noConfirmedSendFor60SecondsWhileRowsArePendingRaisesTheStallAlarmButParksNothing() {
        Fixture fixture = new Fixture();
        fixture.dispatcher.accept(fixture.store.select("tx-1", "s1", "s2"));
        for (int second = 0; second <= 61; second++) {
            fixture.dispatcher.runOnce();
            fixture.transport.failAll("broker unavailable");
            fixture.clock.advance(Duration.ofSeconds(1));
        }
        fixture.dispatcher.runOnce();
        assertTrue(fixture.dispatcher.stalled());
        assertTrue(fixture.dispatcher.alarms().stream().anyMatch(alarm -> alarm.startsWith("DELIVERY_STALLED")));
        assertEquals(0, fixture.store.count("PARKED"), "a broker-wide outage parks nothing");
        assertEquals(0, fixture.store.rows.values().stream().mapToInt(r -> r.charged).sum());

        fixture.clock.advance(Duration.ofSeconds(6));   // past the 5 s backoff cap: the sweep sends both again
        fixture.dispatcher.runOnce();
        assertEquals(2, fixture.transport.pending());
        fixture.transport.confirmAll();
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        assertFalse(fixture.dispatcher.stalled(), "the broker is back");
        assertEquals(2, fixture.store.count("DELIVERED"));
    }

    @Test
    void theSweepResendsDueRowsAfterARestartWithTheSameIdentity() {
        Fixture fixture = new Fixture();
        List<Pending> rows = fixture.store.select("tx-1", "s1", "s2");
        fixture.dispatcher.accept(rows);
        fixture.dispatcher.runOnce();
        fixture.transport.confirmAll();
        fixture.dispatcher.crash();   // confirmed, but the marks were never written

        NotificationDispatcher restarted = fixture.newDispatcher();
        restarted.runOnce();
        assertEquals(4, fixture.counting.sends(), "both rows are sent again");
        List<String> resent = new ArrayList<>();
        for (Map.Entry<String, byte[]> send : fixture.transport.sent.subList(2, 4)) {
            resent.add(EnvelopeCodec.decode(send.getValue(), send.getKey().substring("chan-".length())).eventId());
        }
        assertEquals(rows.stream().map(Pending::notificationId).sorted().toList(), resent.stream().sorted().toList());
        fixture.transport.confirmAll();
        restarted.flushNow();
        assertEquals(2, fixture.store.count("DELIVERED"));
        fixture.clock.advance(Duration.ofSeconds(2));
        restarted.runOnce();
        assertEquals(4, fixture.counting.sends(), "a delivered row is never swept again");
    }

    @Test
    void aChannelThatIsNotReadyIsProvisionedBeforeItsFirstSend() {
        Fixture fixture = new Fixture();
        fixture.subscriptions.ready.put("s1", false);
        fixture.dispatcher.accept(fixture.store.select("tx-1", "s1"));
        fixture.dispatcher.runOnce();
        assertEquals(List.of("chan-s1"), fixture.provisioned);
        assertEquals(List.of("s1"), fixture.subscriptions.markedReady);
        assertEquals(1, fixture.counting.sends());

        fixture.subscriptions.ready.put("s2", false);
        fixture.failProvisioning = true;
        fixture.dispatcher.accept(fixture.store.select("tx-2", "s2"));
        fixture.dispatcher.runOnce();
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        assertEquals(1, fixture.counting.sends(), "nothing is sent to a channel that does not exist");
        assertEquals(1, fixture.store.only("s2").uncharged, "a provisioning failure is uncharged");
    }

}
