package database;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Database boundary for atomic popular-window header and rank persistence. */
public interface PopularWindowStore {

    /** Returns the maximum committed window end, or zero before the first checkpoint. */
    long readMaxWindowEnd() throws StoreFailure;

    /** Atomically writes one header and all zero-to-ten supplied ranks. */
    void writeWindow(PopularWindowSnapshot snapshot) throws StoreFailure;

    /** Returns the latest committed window with at most {@code limit} ordered ranks. */
    Optional<PopularWindowView> readLatest(int limit) throws StoreFailure;

    record PopularWindowSnapshot(long windowStart, long windowEnd, List<SnapshotRank> ranks) {
        public PopularWindowSnapshot {
            validateWindow(windowStart, windowEnd);
            ranks = immutableRanks(ranks);
        }
    }

    record SnapshotRank(int rank, String sku, long scanCount) implements RankedValue {
        public SnapshotRank {
            validateRank(rank, sku, scanCount);
        }
    }

    record PopularWindowView(
            long windowId,
            long windowStart,
            long windowEnd,
            Instant computedAt,
            List<StoredRank> ranks) {

        public PopularWindowView {
            if (windowId <= 0) {
                throw new IllegalArgumentException("windowId must be positive");
            }
            validateWindow(windowStart, windowEnd);
            Objects.requireNonNull(computedAt, "computedAt must not be null");
            ranks = immutableRanks(ranks);
        }
    }

    record StoredRank(int rank, String sku, String name, long scanCount) implements RankedValue {
        public StoredRank {
            validateRank(rank, sku, scanCount);
            name = requiredText(name, 100, "name");
        }
    }

    interface RankedValue {
        int rank();
        String sku();
        long scanCount();
    }

    private static <T extends RankedValue> List<T> immutableRanks(List<T> source) {
        List<T> ranks = List.copyOf(Objects.requireNonNull(source, "ranks must not be null"));
        if (ranks.size() > 10) {
            throw new IllegalArgumentException("a popular window may contain at most ten ranks");
        }
        T previous = null;
        for (int index = 0; index < ranks.size(); index++) {
            T current = Objects.requireNonNull(ranks.get(index), "ranks must not contain null");
            if (current.rank() != index + 1) {
                throw new IllegalArgumentException("rank positions must be contiguous from 1");
            }
            if (previous != null) {
                boolean countOutOfOrder = previous.scanCount() < current.scanCount();
                boolean tieOutOfOrder = previous.scanCount() == current.scanCount()
                        && previous.sku().compareTo(current.sku()) >= 0;
                if (countOutOfOrder || tieOutOfOrder) {
                    throw new IllegalArgumentException(
                            "snapshot ranks must use scan-count descending and SKU ascending order");
                }
            }
            previous = current;
        }
        return ranks;
    }

    private static void validateWindow(long windowStart, long windowEnd) {
        if (windowStart <= 0 || windowEnd < windowStart) {
            throw new IllegalArgumentException("window bounds must be positive and coherent");
        }
    }

    private static void validateRank(int rank, String sku, long scanCount) {
        if (rank < 1 || rank > 10) {
            throw new IllegalArgumentException("rank must be between 1 and 10");
        }
        requiredText(sku, 20, "sku");
        if (scanCount <= 0) {
            throw new IllegalArgumentException("scanCount must be positive");
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
