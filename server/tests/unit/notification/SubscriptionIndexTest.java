package unit.notification;

import database.SubscriptionStore.ChangeRow;
import database.SubscriptionStore.ChangeType;
import notification.SubscriptionIndex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The in-memory recipient index (007 T036, research N5): immutable snapshots of SKU → sorted subscriber IDs, changed
 * only by committed history rows in strict version order, and equal to a rebuild from the committed rows.
 */
class SubscriptionIndexTest {

    @Test
    void snapshotsAreImmutableAndMapSkusToSortedSubscribers() {
        SubscriptionIndex index = new SubscriptionIndex();
        index.rebuild(Map.of("s2", List.of("B", "A"), "s1", List.of("B"), "s3", List.of("C")), 3);
        SubscriptionIndex.Snapshot before = index.snapshot();
        assertEquals(3, before.version());
        assertEquals(List.of("s2"), before.lookup("A"));
        assertEquals(List.of("s1", "s2"), before.lookup("B"));
        assertEquals(List.of(), before.lookup("Z"));
        assertThrows(UnsupportedOperationException.class, () -> before.lookup("B").add("s9"));
        assertThrows(UnsupportedOperationException.class, () -> before.subscriptions().put("s9", Set.of("A")));

        assertEquals(SubscriptionIndex.Applied.APPLIED, index.apply(register(4, "s0", "B")));
        assertEquals(List.of("s1", "s2"), before.lookup("B"), "an older snapshot never changes");
        assertEquals(List.of("s0", "s1", "s2"), index.lookup("B"));
        assertEquals(4, index.version());
    }

    @Test
    void applyRequiresTheNextVersionAndReportsAGapForCatchUp() {
        SubscriptionIndex index = new SubscriptionIndex();
        assertEquals(SubscriptionIndex.Applied.GAP, index.apply(register(2, "s1", "A")), "version 1 is missing");
        assertEquals(0, index.version());
        assertTrue(index.lookup("A").isEmpty(), "a gap applies nothing");

        assertEquals(SubscriptionIndex.Applied.APPLIED, index.apply(register(1, "s1", "A")));
        assertEquals(SubscriptionIndex.Applied.ALREADY_APPLIED, index.apply(register(1, "s1", "A")));
        assertEquals(SubscriptionIndex.Applied.APPLIED, index.apply(register(2, "s2", "A")));
        assertFalse(index.apply(List.of(register(3, "s3", "A"), register(5, "s5", "A"))), "the list stops at the gap");
        assertEquals(3, index.version());
        assertEquals(List.of("s1", "s2", "s3"), index.lookup("A"));

        List<ChangeRow> history = List.of(register(4, "s4", "B"), register(5, "s5", "A"));
        SubscriptionIndex.Snapshot caughtUp = index.atVersion(5, (after, upTo) -> {
            assertEquals(3, after);
            assertEquals(5, upTo);
            return history;
        });
        assertEquals(5, caughtUp.version());
        assertEquals(List.of("s1", "s2", "s3", "s5"), caughtUp.lookup("A"));
        assertSame(caughtUp, index.atVersion(5, (after, upTo) -> fail("an index at the version reads nothing")));
        assertThrows(IllegalStateException.class, () -> index.atVersion(4, (after, upTo) -> List.of()),
                "an index ahead of the locked clock is inconsistent");
        assertThrows(IllegalStateException.class, () -> index.atVersion(7, (after, upTo) -> List.of(register(7, "s7", "A"))),
                "a hole in the history is inconsistent");
    }

    @Test
    void resultingSkusReplaceTheSubscribersSetAndCancelRemovesIt() {
        SubscriptionIndex index = new SubscriptionIndex();
        index.apply(register(1, "s1", "A", "B"));
        index.apply(new ChangeRow(2, "s1", ChangeType.UPDATE, List.of("C"), List.of("A"), List.of("B", "C")));
        assertEquals(List.of(), index.lookup("A"));
        assertEquals(List.of("s1"), index.lookup("B"));
        assertEquals(List.of("s1"), index.lookup("C"));

        index.apply(new ChangeRow(3, "s1", ChangeType.CANCEL, List.of(), List.of("B", "C"), List.of()));
        assertTrue(index.snapshot().asMap().isEmpty());
        assertEquals(0, index.snapshot().subscribers());

        index.apply(register(4, "s1", "D"));
        assertEquals(List.of("s1"), index.lookup("D"), "a cancelled subscriber registers again");
    }

    @Test
    void aRebuildFromRowsAndTheClockEqualsIncrementalApplication() {
        List<ChangeRow> history = new ArrayList<>();
        history.add(register(1, "s1", "A", "B"));
        history.add(register(2, "s2", "B"));
        history.add(new ChangeRow(3, "s1", ChangeType.UPDATE, List.of("C"), List.of("A"), List.of("B", "C")));
        history.add(register(4, "s3", "A"));
        history.add(new ChangeRow(5, "s2", ChangeType.CANCEL, List.of(), List.of("B"), List.of()));
        SubscriptionIndex incremental = new SubscriptionIndex();
        assertTrue(incremental.apply(history));

        // The replay rule: the latest row per subscriber with version <= V is its state at V.
        Map<String, List<String>> latest = new HashMap<>();
        for (ChangeRow row : history) {
            if (row.resultingSkus().isEmpty()) latest.remove(row.subscriberId());
            else latest.put(row.subscriberId(), row.resultingSkus());
        }
        SubscriptionIndex rebuilt = new SubscriptionIndex();
        rebuilt.rebuild(latest, 5);
        assertEquals(incremental.snapshot(), rebuilt.snapshot());
        assertEquals(Map.of("A", List.of("s3"), "B", List.of("s1"), "C", List.of("s1")), rebuilt.snapshot().asMap());
    }

    private static ChangeRow register(long version, String subscriber, String... skus) {
        List<String> set = List.of(skus).stream().sorted().toList();
        return new ChangeRow(version, subscriber, ChangeType.REGISTER, set, List.of(), set);
    }
}
