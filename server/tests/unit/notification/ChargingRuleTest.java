package unit.notification;

import notification.ChargingTracker;
import org.junit.jupiter.api.Test;
import unit.notification.DispatcherFakes.FakeStore;
import unit.notification.DispatcherFakes.Fixture;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The charging rule of research N8 and contracts/notification-delivery.md "Dispatch" (007 T095): a failed send is
 * charged to its channel only when another subscriber's channel confirmed a send after the attempt started; the
 * 8th charged failure parks the row, uncharged failures never do, and a broker-wide silence of 60 s while rows are
 * pending raises the stall alarm, which the next confirmation clears.
 */
class ChargingRuleTest {
    private static final Instant T = DispatcherFakes.T0;

    @Test
    void theTrackerChargesOnlyWhenAnotherChannelConfirmedAfterTheAttemptStarted() {
        ChargingTracker tracker = new ChargingTracker();
        assertFalse(tracker.isCharged(T, "s1"), "nothing confirmed yet");
        tracker.confirmed("s1", T.plusMillis(5));
        assertFalse(tracker.isCharged(T, "s1"), "the channel's own confirmation does not charge it");
        tracker.confirmed("s2", T.plusMillis(3));
        assertTrue(tracker.isCharged(T, "s1"), "s2 confirmed after s1's attempt started");
        assertFalse(tracker.isCharged(T.plusMillis(3), "s1"), "a confirmation at the start instant is not after it");
        assertFalse(tracker.isCharged(T.plusMillis(4), "s1"), "s2's confirmation came before the attempt started");
        assertTrue(tracker.isCharged(T.plusMillis(4), "s2"), "s1's later confirmation charges s2");
        assertEquals(T.plusMillis(5), tracker.lastConfirmedSend().orElseThrow());
    }

    @Test
    void aFailureIsChargedWhenAnotherChannelConfirmedASendAfterTheAttemptStarted() {
        Fixture fixture = new Fixture();
        fixture.store.select("tx-1", "s1", "s2");
        fixture.dispatcher.runOnce();   // both attempts start at T
        assertEquals(2, fixture.transport.pending());
        fixture.clock.advance(Duration.ofMillis(1));
        fixture.transport.confirm("s2");
        fixture.transport.fail("s1", "channel rejected");
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        FakeStore.Row row = fixture.store.only("s1");
        assertEquals(1, row.charged);
        assertEquals(0, row.uncharged);
        assertEquals("PENDING", row.state);
        assertEquals("DELIVERED", fixture.store.only("s2").state);
    }

    @Test
    void aFailureWithNoOtherConfirmationIsUncharged() {
        Fixture fixture = new Fixture();
        fixture.store.select("tx-1", "s2");
        fixture.dispatcher.runOnce();
        fixture.transport.confirm("s2");   // confirmed at T, before s1's attempt below starts
        fixture.clock.advance(Duration.ofSeconds(2));
        fixture.dispatcher.accept(fixture.store.select("tx-2", "s1"));
        fixture.dispatcher.runOnce();
        fixture.clock.advance(Duration.ofMillis(1));
        fixture.transport.fail("s1", "broker unavailable");
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();
        FakeStore.Row row = fixture.store.only("s1");
        assertEquals(0, row.charged, "no other channel confirmed after the attempt started");
        assertEquals(1, row.uncharged);
    }

    @Test
    void theEighthChargedFailureParksTheRow() {
        Fixture fixture = new Fixture();
        FakeStore.Row row = fixture.store.rows.get(fixture.store.select("tx-0", "s1").get(0).notificationId());
        for (int failure = 1; failure <= 8; failure++) {
            fixture.store.select("tx-s2-" + failure, "s2");
            fixture.clock.advance(Duration.ofSeconds(5));   // past the backoff cap: s1's row is due again
            fixture.dispatcher.runOnce();
            fixture.clock.advance(Duration.ofMillis(1));
            fixture.transport.confirm("s2");
            fixture.transport.fail("s1", "channel rejected " + failure);
            fixture.clock.advance(Duration.ofMillis(20));
            fixture.dispatcher.runOnce();
            assertEquals(failure, row.charged, "failure " + failure + " is charged");
            assertEquals(failure < 8 ? "PENDING" : "PARKED", row.state, "after charged failure " + failure);
        }
        assertEquals(0, row.uncharged);
        assertEquals(1, fixture.dispatcher.parkedSinceStart());
        assertEquals(8, fixture.store.count("DELIVERED"), "s2's channel kept delivering");

        long sends = fixture.counting.sends();
        fixture.clock.advance(Duration.ofSeconds(10));
        fixture.dispatcher.runOnce();
        assertEquals(sends, fixture.counting.sends(), "a parked row is never swept");
    }

    @Test
    void unchargedFailuresNeverPark() {
        Fixture fixture = new Fixture();
        fixture.store.select("tx-1", "s1", "s2");
        for (int failure = 1; failure <= 20; failure++) {
            fixture.dispatcher.runOnce();
            fixture.clock.advance(Duration.ofMillis(1));
            fixture.transport.failAll("broker unavailable");
            fixture.clock.advance(Duration.ofMillis(20));
            fixture.dispatcher.runOnce();
            fixture.clock.advance(Duration.ofSeconds(5));
        }
        for (String subscriber : new String[] {"s1", "s2"}) {
            FakeStore.Row row = fixture.store.only(subscriber);
            assertEquals("PENDING", row.state, subscriber);
            assertEquals(20, row.uncharged, subscriber);
            assertEquals(0, row.charged, subscriber);
        }
        assertEquals(0, fixture.dispatcher.parkedSinceStart());
    }

    @Test
    void theStallAlarmIsRaisedAfter60SecondsWithoutAConfirmedSendAndClearedByTheNextOne() {
        Fixture fixture = new Fixture();
        fixture.store.select("tx-1", "s1", "s2");
        for (int second = 0; second < 59; second++) {
            fixture.dispatcher.runOnce();
            fixture.transport.failAll("broker unavailable");
            fixture.clock.advance(Duration.ofSeconds(1));
        }
        fixture.dispatcher.runOnce();
        assertFalse(fixture.dispatcher.stalled(), "59 s without a confirmation is not yet a stall");
        assertTrue(fixture.dispatcher.alarms().isEmpty());
        fixture.transport.failAll("broker unavailable");
        fixture.clock.advance(Duration.ofSeconds(1));
        fixture.dispatcher.runOnce();
        assertTrue(fixture.dispatcher.stalled(), "60 s without a confirmation while rows are pending");
        assertTrue(fixture.dispatcher.alarms().stream().anyMatch(alarm -> alarm.startsWith("DELIVERY_STALLED")),
                fixture.dispatcher.alarms().toString());
        assertEquals(0, fixture.store.count("PARKED"));

        fixture.transport.failAll("broker unavailable");
        fixture.clock.advance(Duration.ofMillis(20));
        fixture.dispatcher.runOnce();                   // writes the failures
        fixture.clock.advance(Duration.ofSeconds(6));   // past the backoff cap: the sweep sends both again
        fixture.dispatcher.runOnce();
        assertEquals(2, fixture.transport.pending());
        fixture.transport.confirm("s1");
        fixture.dispatcher.runOnce();
        assertFalse(fixture.dispatcher.stalled(), "the next confirmation clears the alarm");
        assertTrue(fixture.dispatcher.alarms().isEmpty());
    }
}
