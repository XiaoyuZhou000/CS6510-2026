package unit.transaction;

import database.RelayStore;
import org.junit.jupiter.api.Test;
import transaction.ScanEventRelay;
import transaction.ScanEventSink;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The CP3 relay-coalescing switch (R23, T123): {@code OUTBOX_COALESCE_MS} delays the next round only after a
 * partial batch. It is off by default, so the relay keeps starting the next round at once.
 */
class ScanEventRelayCoalescingTest {

    @Test
    void byDefaultTheNextRoundStartsAtOnceAfterAnyDelivery() {
        ScanEventRelay.Settings settings = ScanEventRelay.Settings.fromEnvironment(Map.of());
        assertEquals(Duration.ZERO, settings.coalesce());
        assertEquals(ScanEventRelay.Settings.defaults(), settings);

        ScanEventRelay relay = new ScanEventRelay(new MemoryStore(30), new OkSink(), settings);
        assertEquals(ScanEventRelay.Round.DELIVERED, relay.runOnce());
        assertEquals(Duration.ZERO, relay.delayAfter(ScanEventRelay.Round.DELIVERED));
    }

    @Test
    void coalescingWaitsOnlyAfterAPartialBatch() {
        ScanEventRelay.Settings settings = ScanEventRelay.Settings.fromEnvironment(
                Map.of("OUTBOX_BATCH_SIZE", "10", "OUTBOX_COALESCE_MS", "100"));
        assertEquals(Duration.ofMillis(100), settings.coalesce());
        MemoryStore store = new MemoryStore(25);
        ScanEventRelay relay = new ScanEventRelay(store, new OkSink(), settings);

        assertEquals(ScanEventRelay.Round.DELIVERED, relay.runOnce());
        assertEquals(Duration.ZERO, relay.delayAfter(ScanEventRelay.Round.DELIVERED), "a full batch keeps draining at once");
        assertEquals(ScanEventRelay.Round.DELIVERED, relay.runOnce());
        assertEquals(Duration.ZERO, relay.delayAfter(ScanEventRelay.Round.DELIVERED));
        assertEquals(ScanEventRelay.Round.DELIVERED, relay.runOnce());
        assertEquals(Duration.ofMillis(100), relay.delayAfter(ScanEventRelay.Round.DELIVERED),
                "a partial batch waits so the next round carries more events");
        assertEquals(0, store.rows.size(), "every row is still delivered");
        assertEquals(ScanEventRelay.Round.IDLE, relay.runOnce());
        assertEquals(settings.idlePoll(), relay.delayAfter(ScanEventRelay.Round.IDLE), "the idle poll is unchanged");
    }

    @Test
    void negativeOrOversizedCoalescingIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> ScanEventRelay.Settings.fromEnvironment(Map.of("OUTBOX_COALESCE_MS", "-1")));
        assertThrows(IllegalArgumentException.class,
                () -> ScanEventRelay.Settings.fromEnvironment(Map.of("OUTBOX_COALESCE_MS", "1001")));
    }

    private static final class MemoryStore implements RelayStore {
        final List<UndeliveredScan> rows = new ArrayList<>();

        MemoryStore(int count) {
            for (int i = 1; i <= count; i++) {
                rows.add(new UndeliveredScan(i, "tx-" + i, 1, "SKU-1", "Apple", Instant.now()));
            }
        }

        @Override
        public List<UndeliveredScan> selectUndelivered(int limit) {
            return new ArrayList<>(rows.subList(0, Math.min(limit, rows.size())));
        }

        @Override
        public int markDelivered(List<Long> scanIds, Instant deliveredAt) {
            int before = rows.size();
            rows.removeIf(row -> scanIds.contains(row.scanId()));
            return before - rows.size();
        }

        @Override
        public long countUndelivered() {
            return rows.size();
        }
    }

    private static final class OkSink implements ScanEventSink {
        @Override
        public DeliveryResult deliver(List<RelayStore.UndeliveredScan> events) {
            return new Delivered(events.size(), 0);
        }
    }
}
