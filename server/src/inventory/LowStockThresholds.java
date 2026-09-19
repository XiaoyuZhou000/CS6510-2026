package inventory;

/** Pure policy for low-stock threshold selection and its inclusive boundary. */
public final class LowStockThresholds {
    public static final int DEFAULT_THRESHOLD = 50;

    private LowStockThresholds() {}

    public static int resolve(Integer thresholdOverride) {
        return thresholdOverride == null ? DEFAULT_THRESHOLD : thresholdOverride;
    }

    public static boolean isLowStock(int currentStock, int threshold) {
        return currentStock <= threshold;
    }
}
