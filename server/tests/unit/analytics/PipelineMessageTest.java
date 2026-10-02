package unit.analytics;

import analytics.PipelineMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PipelineMessageTest {
    @Test
    void acceptedScanValidatesAndTypedMarkersAreStableValues() {
        assertEquals("SKU-1", new PipelineMessage.AcceptedScan("SKU-1").sku());
        assertThrows(IllegalArgumentException.class, () -> new PipelineMessage.AcceptedScan(" "));
        assertThrows(IllegalArgumentException.class,
                () -> new PipelineMessage.AcceptedScan("123456789012345678901"));
        assertEquals(PipelineMessage.IngressEnd.INSTANCE, new PipelineMessage.IngressEnd());
        assertEquals(PipelineMessage.WindowEnd.INSTANCE, new PipelineMessage.WindowEnd());
        assertEquals(PipelineMessage.RankingEnd.INSTANCE, new PipelineMessage.RankingEnd());
    }

    @Test
    void snapshotDefensivelyCopiesAndRejectsInvalidBoundsOrMembers() {
        List<String> mutable = new ArrayList<>(Collections.nCopies(1000, "A"));
        PipelineMessage.WindowSnapshot snapshot =
                new PipelineMessage.WindowSnapshot(1, 1000, mutable);
        mutable.set(0, "B");
        assertEquals("A", snapshot.skus().getFirst());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.skus().set(0, "C"));
        assertThrows(IllegalArgumentException.class,
                () -> new PipelineMessage.WindowSnapshot(0, 999, mutable));
        assertThrows(IllegalArgumentException.class,
                () -> new PipelineMessage.WindowSnapshot(1, 999, mutable));
        assertThrows(IllegalArgumentException.class,
                () -> new PipelineMessage.WindowSnapshot(1, 1000, mutable.subList(0, 999)));
        mutable.set(10, " ");
        assertThrows(IllegalArgumentException.class,
                () -> new PipelineMessage.WindowSnapshot(1, 1000, mutable));
    }

    @Test
    void rankedRecordsAreImmutableContiguousUniqueAndDeterministicallyOrdered() {
        var a = new PipelineMessage.RankedItem(1, "A", 8);
        var b = new PipelineMessage.RankedItem(2, "B", 8);
        List<PipelineMessage.RankedItem> mutable = new ArrayList<>(List.of(a, b));
        var ranked = new PipelineMessage.RankedWindow(1, 1000, mutable);
        mutable.clear();
        assertEquals(List.of(a, b), ranked.ranks());
        assertThrows(UnsupportedOperationException.class, () -> ranked.ranks().add(a));
        assertThrows(IllegalArgumentException.class,
                () -> new PipelineMessage.RankedItem(0, "A", 1));
        assertThrows(IllegalArgumentException.class,
                () -> new PipelineMessage.RankedItem(1, "A", 0));
        assertThrows(IllegalArgumentException.class, () -> new PipelineMessage.RankedWindow(
                1, 1000, List.of(new PipelineMessage.RankedItem(2, "A", 2))));
        assertThrows(IllegalArgumentException.class, () -> new PipelineMessage.RankedWindow(
                1, 1000, List.of(
                        new PipelineMessage.RankedItem(1, "B", 1),
                        new PipelineMessage.RankedItem(2, "A", 1))));
        assertThrows(IllegalArgumentException.class, () -> new PipelineMessage.RankedWindow(
                1, 1000, List.of(
                        new PipelineMessage.RankedItem(1, "A", 2),
                        new PipelineMessage.RankedItem(2, "A", 1))));
    }
}
