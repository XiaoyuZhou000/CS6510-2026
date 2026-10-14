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
    void restartUsesAbsoluteCheckpointWithoutSkippingTheNextOverlap() {
        assertTrue(WindowMath.isCheckpointBoundary(1000));
        assertTrue(WindowMath.isCheckpointBoundary(1500));
        assertEquals(501, WindowMath.windowStart(1500));
        assertFalse(WindowMath.isCheckpointBoundary(1499));
        assertTrue(WindowMath.isCheckpointBoundary(2000));
    }
}
