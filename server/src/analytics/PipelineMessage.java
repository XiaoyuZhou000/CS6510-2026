package analytics;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable data and control messages exchanged by the analytics pipeline. */
public final class PipelineMessage {
    private static final int MAX_SKU_LENGTH = 20;

    private PipelineMessage() {
    }

    /** Messages accepted by the Window Filter. */
    public sealed interface IngressMessage permits AcceptedScan, IngressEnd {
    }

    /** Messages emitted by the Window Filter and accepted by the Ranking Filter. */
    public sealed interface WindowMessage permits WindowSnapshot, WindowEnd {
    }

    /** Messages emitted by the Ranking Filter and accepted by the Persistence Filter. */
    public sealed interface RankingMessage permits RankedWindow, RankingEnd {
    }

    /** One admitted physical-unit scan. Position assignment belongs to the Window Filter. */
    public record AcceptedScan(String sku) implements IngressMessage {
        public AcceptedScan {
            sku = requiredSku(sku);
        }
    }

    /** Typed end marker for the ingress-to-window pipe. */
    public record IngressEnd() implements IngressMessage {
        public static final IngressEnd INSTANCE = new IngressEnd();
    }

    /** One complete 1,000-event window in chronological analytics order. */
    public record WindowSnapshot(long windowStart, long windowEnd, List<String> skus)
            implements WindowMessage {
        public WindowSnapshot {
            validateCompleteWindow(windowStart, windowEnd);
            skus = List.copyOf(Objects.requireNonNull(skus, "skus must not be null"));
            if (skus.size() != WindowMath.WINDOW_SIZE) {
                throw new IllegalArgumentException("skus must contain exactly 1,000 values");
            }
            for (String sku : skus) {
                requiredSku(sku);
            }
        }
    }

    /** Typed end marker for the window-to-ranking pipe. */
    public record WindowEnd() implements WindowMessage {
        public static final WindowEnd INSTANCE = new WindowEnd();
    }

    /** One deterministic rank in a complete window. */
    public record RankedItem(int rank, String sku, long scanCount) {
        public RankedItem {
            if (rank < 1 || rank > 10) {
                throw new IllegalArgumentException("rank must be between 1 and 10");
            }
            sku = requiredSku(sku);
            if (scanCount <= 0) {
                throw new IllegalArgumentException("scanCount must be positive");
            }
        }
    }

    /** A complete window's zero-to-ten deterministic ranks. */
    public record RankedWindow(long windowStart, long windowEnd, List<RankedItem> ranks)
            implements RankingMessage {
        public RankedWindow {
            validateCompleteWindow(windowStart, windowEnd);
            ranks = List.copyOf(Objects.requireNonNull(ranks, "ranks must not be null"));
            if (ranks.size() > 10) {
                throw new IllegalArgumentException("ranks may contain at most ten items");
            }

            Set<String> uniqueSkus = new HashSet<>();
            RankedItem previous = null;
            for (int index = 0; index < ranks.size(); index++) {
                RankedItem current = Objects.requireNonNull(
                        ranks.get(index), "ranks must not contain null");
                if (current.rank() != index + 1) {
                    throw new IllegalArgumentException("ranks must be contiguous from 1");
                }
                if (!uniqueSkus.add(current.sku())) {
                    throw new IllegalArgumentException("ranked SKUs must be unique within a window");
                }
                if (previous != null) {
                    boolean countOutOfOrder = previous.scanCount() < current.scanCount();
                    boolean tieOutOfOrder = previous.scanCount() == current.scanCount()
                            && previous.sku().compareTo(current.sku()) >= 0;
                    if (countOutOfOrder || tieOutOfOrder) {
                        throw new IllegalArgumentException(
                                "ranks must use scan-count descending and SKU ascending order");
                    }
                }
                previous = current;
            }
        }
    }

    /** Typed end marker for the ranking-to-persistence pipe. */
    public record RankingEnd() implements RankingMessage {
        public static final RankingEnd INSTANCE = new RankingEnd();
    }

    private static void validateCompleteWindow(long windowStart, long windowEnd) {
        if (windowStart <= 0) {
            throw new IllegalArgumentException("windowStart must be positive");
        }
        if (windowEnd <= 0) {
            throw new IllegalArgumentException("windowEnd must be positive");
        }
        if (windowEnd - windowStart + 1 != WindowMath.WINDOW_SIZE) {
            throw new IllegalArgumentException("window bounds must describe exactly 1,000 positions");
        }
    }

    private static String requiredSku(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("sku must not be blank");
        }
        if (value.length() > MAX_SKU_LENGTH) {
            throw new IllegalArgumentException("sku must be at most 20 characters");
        }
        return value;
    }
}
