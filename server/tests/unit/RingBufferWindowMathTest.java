package unit;

import analytics.WindowMath;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RingBufferWindowMathTest {
    @Test
    void ringIndexWrapsAtExactlyOneThousandScans() {
        assertEquals(0, WindowMath.ringIndex(1));
        assertEquals(499, WindowMath.ringIndex(500));
        assertEquals(999, WindowMath.ringIndex(1000));
        assertEquals(0, WindowMath.ringIndex(1001));
        assertEquals(499, WindowMath.ringIndex(1500));
        assertThrows(IllegalArgumentException.class, () -> WindowMath.ringIndex(0));
    }

    @Test
    void firstFullWindowIsAtOneThousandThenSlidesEveryFiveHundred() {
        assertFalse(WindowMath.isCheckpointBoundary(0));
        assertFalse(WindowMath.isCheckpointBoundary(499));
        assertFalse(WindowMath.isCheckpointBoundary(500));
        assertFalse(WindowMath.isCheckpointBoundary(999));
        assertTrue(WindowMath.isCheckpointBoundary(1000));
        assertEquals(1, WindowMath.windowStart(1000));
        assertEquals(501, WindowMath.windowStart(1500));
        assertThrows(IllegalArgumentException.class, () -> WindowMath.windowStart(500));
        assertThrows(IllegalArgumentException.class, () -> WindowMath.windowStart(1001));
    }

    @Test
    void restartSkipIsConsumedOnlyByTheFirstBoundary() {
        var beforeBoundary = WindowMath.checkpointDecision(1499, true);
        assertFalse(beforeBoundary.checkpoint());
        assertTrue(beforeBoundary.skipNextCheckpoint());

        var skipped = WindowMath.checkpointDecision(1500, beforeBoundary.skipNextCheckpoint());
        assertFalse(skipped.checkpoint());
        assertFalse(skipped.skipNextCheckpoint());

        var resumed = WindowMath.checkpointDecision(2000, skipped.skipNextCheckpoint());
        assertTrue(resumed.checkpoint());
        assertFalse(resumed.skipNextCheckpoint());

        assertFalse(WindowMath.checkpointDecision(500, false).checkpoint(),
            "A fresh run must not persist a partial window");
        assertTrue(WindowMath.checkpointDecision(1000, false).checkpoint(),
            "A fresh run must persist its first full window");
    }
}
