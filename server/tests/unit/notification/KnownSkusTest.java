package unit.notification;

import events.ParkReason;
import events.PermanentEventFailure;
import notification.KnownSkus;
import org.junit.jupiter.api.Test;
import support.TestEvents;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Notification's catalog SKU set (007 T038, research N6): empty and not loaded until a snapshot marker whose
 * {@code product_count} matches the received items of its version; a newer version replaces the set; stale items
 * are ignored.
 */
class KnownSkusTest {

    @Test
    void theSetIsEmptyUntilACompleteSnapshotArrives() {
        KnownSkus skus = new KnownSkus();
        assertFalse(skus.loaded());
        skus.handle(TestEvents.item("A", "a", "1.00", "v1"));
        skus.handle(TestEvents.item("B", "b", "1.00", "v1"));
        assertFalse(skus.contains("A"), "items alone change nothing");

        skus.handle(TestEvents.snapshot("v1", 3));
        assertFalse(skus.loaded(), "a marker with fewer buffered SKUs than product_count changes nothing");
        assertEquals(0, skus.size());

        skus.handle(TestEvents.item("C", "c", "1.00", "v1"));
        skus.handle(TestEvents.item("C", "c again", "1.00", "v1"));
        skus.handle(TestEvents.snapshot("v1", 3));
        assertTrue(skus.loaded());
        assertEquals(Optional.of("v1"), skus.version());
        assertEquals(3, skus.size(), "a repeated item is counted once");
        assertTrue(skus.contains("A") && skus.contains("B") && skus.contains("C"));
        assertFalse(skus.contains("D"));
    }

    @Test
    void aNewerVersionReplacesTheSetAndStaleItemsAreIgnored() {
        KnownSkus skus = new KnownSkus();
        skus.handle(TestEvents.item("A", "a", "1.00", "v1"));
        skus.handle(TestEvents.item("B", "b", "1.00", "v1"));
        skus.handle(TestEvents.snapshot("v1", 2));

        skus.handle(TestEvents.item("B", "b", "1.00", "v2"));
        skus.handle(TestEvents.item("D", "d", "1.00", "v2"));
        assertTrue(skus.contains("A"), "the installed set stays until the new version completes");
        skus.handle(TestEvents.snapshot("v2", 2));
        assertEquals(Optional.of("v2"), skus.version());
        assertFalse(skus.contains("A"));
        assertTrue(skus.contains("B") && skus.contains("D"));

        skus.handle(TestEvents.item("A", "a", "1.00", "v1"));
        skus.handle(TestEvents.item("E", "e", "1.00", "v2"));
        skus.handle(TestEvents.snapshot("v2", 2));
        assertFalse(skus.contains("A"), "an item of an older version changes nothing");
        assertFalse(skus.contains("E"), "an item of the installed version changes nothing");
        assertEquals(2, skus.size());
    }

    @Test
    void anOtherEventTypeIsParkedAndOffsetsAreTracked() {
        KnownSkus skus = new KnownSkus();
        PermanentEventFailure failure = assertThrows(PermanentEventFailure.class,
                () -> skus.handle(TestEvents.request("tx-1", 1, TestEvents.line("A", 1))));
        assertEquals(ParkReason.UNKNOWN_TYPE, failure.reason());
        skus.handle(TestEvents.item("A", "a", "1.00", "v1").at("catalog", 0, 41));
        assertEquals(42, skus.nextOffset());
    }
}
