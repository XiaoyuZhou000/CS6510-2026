package analytics;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** API-facing analytics contract and accepted-scan ingestion boundary. */
public interface AnalyticsOperations {

    int WINDOW_SIZE = 1_000;
    int SLIDE_INTERVAL = 500;

    /** Records one accepted physical-unit scan without waiting for checkpoint persistence. */
    void recordAcceptedScan(String sku);

    /** Returns the latest successfully persisted result, capped to the requested item count. */
    PopularItemsView latestPopularItems(int limit);

    /** Stops accepting checkpoint work and begins orderly worker shutdown. */
    void shutdown();

    record PopularItemsView(
            int windowSize,
            int slideInterval,
            long windowStart,
            long windowEnd,
            Instant computedAt,
            List<RankedItemView> items) {

        public PopularItemsView {
            if (windowSize <= 0) {
                throw new IllegalArgumentException("windowSize must be positive");
            }
            if (slideInterval <= 0 || slideInterval > windowSize) {
                throw new IllegalArgumentException("slideInterval must be positive and no larger than windowSize");
            }
            boolean emptyWindow = windowStart == 0 && windowEnd == 0;
            if (!emptyWindow && (windowStart <= 0 || windowEnd < windowStart)) {
                throw new IllegalArgumentException("window bounds must be zero/zero or positive and coherent");
            }
            Objects.requireNonNull(computedAt, "computedAt must not be null");
            items = List.copyOf(Objects.requireNonNull(items, "items must not be null"));
            if (items.size() > 10) {
                throw new IllegalArgumentException("popular items may contain at most ten ranks");
            }
            RankedItemView previous = null;
            for (int index = 0; index < items.size(); index++) {
                RankedItemView item = Objects.requireNonNull(items.get(index), "items must not contain null");
                if (item.rank() != index + 1) {
                    throw new IllegalArgumentException("rank positions must be contiguous from 1");
                }
                if (previous != null) {
                    boolean countOutOfOrder = previous.scanCount() < item.scanCount();
                    boolean tieOutOfOrder = previous.scanCount() == item.scanCount()
                            && previous.sku().compareTo(item.sku()) >= 0;
                    if (countOutOfOrder || tieOutOfOrder) {
                        throw new IllegalArgumentException(
                                "items must use scan-count descending and SKU ascending order");
                    }
                }
                previous = item;
            }
            if (emptyWindow && !items.isEmpty()) {
                throw new IllegalArgumentException("an empty window cannot contain ranked items");
            }
        }

        public static PopularItemsView empty() {
            return new PopularItemsView(
                    WINDOW_SIZE, SLIDE_INTERVAL, 0, 0, Instant.EPOCH, List.of());
        }
    }

    record RankedItemView(String sku, String name, long scanCount, int rank) {
        public RankedItemView {
            sku = requiredText(sku, 20, "sku");
            name = requiredText(name, 100, "name");
            if (scanCount <= 0) {
                throw new IllegalArgumentException("scanCount must be positive");
            }
            if (rank < 1 || rank > 10) {
                throw new IllegalArgumentException("rank must be between 1 and 10");
            }
        }
    }

    private static String requiredText(String value, int maximumLength, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        if (value.length() > maximumLength) {
            throw new IllegalArgumentException(field + " must be at most " + maximumLength + " characters");
        }
        return value;
    }
}
