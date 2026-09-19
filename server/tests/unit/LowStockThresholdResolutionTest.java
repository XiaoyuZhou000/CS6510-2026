package unit;

import inventory.LowStockThresholds;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class LowStockThresholdResolutionTest {
    @Test
    void resolvesDefaultOrRequestLocalOverride() {
        assertEquals(50, LowStockThresholds.resolve(null));
        assertEquals(7, LowStockThresholds.resolve(7));
        assertEquals(0, LowStockThresholds.resolve(0));
        assertEquals(-1, LowStockThresholds.resolve(-1));
        assertEquals(50, LowStockThresholds.resolve(null),
            "Resolving an override must not mutate the default");
    }

    @Test
    void atOrBelowIsInclusive() {
        assertTrue(LowStockThresholds.isLowStock(2, 3));
        assertTrue(LowStockThresholds.isLowStock(3, 3));
        assertFalse(LowStockThresholds.isLowStock(4, 3));
        assertTrue(LowStockThresholds.isLowStock(0, 0));
    }
}
