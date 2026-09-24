package analytics;

import database.PopularWindowStore;
import database.StoreFailure;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Owns accepted-scan window state, deterministic ranking, and ordered persistence. */
public final class AnalyticsService implements AnalyticsOperations {
    private static final int MAX_FAST_RETRIES = 2;

    private final AtomicLong globalScanCounter;
    private final String[] ringBuffer = new String[WindowMath.WINDOW_SIZE];
    private final Object lock = new Object();
    private final PopularWindowStore windowStore;
    private final ExecutorService checkpointExecutor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "analytics-checkpoint");
        thread.setDaemon(true);
        return thread;
    });
    private boolean skipNextCheckpoint;
    private boolean closed;

    public AnalyticsService(PopularWindowStore windowStore) {
        this.windowStore = Objects.requireNonNull(windowStore, "windowStore");
        long maxWindowEnd = windowStore.readMaxWindowEnd();
        globalScanCounter = new AtomicLong(maxWindowEnd);
        skipNextCheckpoint = maxWindowEnd > 0;
    }

    @Override
    public void recordAcceptedScan(String sku) {
        Objects.requireNonNull(sku, "sku");
        if (sku.isBlank()) throw new IllegalArgumentException("sku must not be blank");
        synchronized (lock) {
            if (closed) throw new IllegalStateException("analytics service is shut down");
            long scanNumber = globalScanCounter.incrementAndGet();
            ringBuffer[WindowMath.ringIndex(scanNumber)] = sku;
            WindowMath.CheckpointDecision decision =
                    WindowMath.checkpointDecision(scanNumber, skipNextCheckpoint);
            skipNextCheckpoint = decision.skipNextCheckpoint();
            if (decision.checkpoint()) {
                String[] snapshot = ringBuffer.clone();
                checkpointExecutor.submit(() -> persistInOrder(snapshot,
                        WindowMath.windowStart(scanNumber), scanNumber));
            }
        }
    }

    @Override
    public PopularItemsView latestPopularItems(int limit) {
        if (limit < 0) throw new IllegalArgumentException("limit must be non-negative");
        return windowStore.readLatest(limit).map(window -> new PopularItemsView(
                WINDOW_SIZE, SLIDE_INTERVAL, window.windowStart(), window.windowEnd(),
                window.computedAt(), window.ranks().stream()
                        .map(rank -> new RankedItemView(
                                rank.sku(), rank.name(), rank.scanCount(), rank.rank()))
                        .toList()))
                .orElseGet(PopularItemsView::empty);
    }

    private void persistInOrder(String[] snapshot, long windowStart, long windowEnd) {
        PopularWindowStore.PopularWindowSnapshot window = new PopularWindowStore.PopularWindowSnapshot(
                windowStart, windowEnd, rank(snapshot));
        int failures = 0;
        long[] backoffMillis = {50, 200};
        while (!Thread.currentThread().isInterrupted()) {
            try {
                windowStore.writeWindow(window);
                return;
            } catch (StoreFailure failure) {
                long delay = failures < MAX_FAST_RETRIES ? backoffMillis[failures] : 1_000L;
                failures++;
                if (failures == MAX_FAST_RETRIES + 1) {
                    System.err.println("[analytics] Checkpoint [" + windowStart + "," + windowEnd
                            + "] still failing; retaining it for ordered recovery");
                }
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static List<PopularWindowStore.SnapshotRank> rank(String[] snapshot) {
        Map<String, Long> counts = new HashMap<>();
        for (String sku : snapshot) {
            if (sku != null) counts.merge(sku, 1L, Long::sum);
        }
        List<Map.Entry<String, Long>> ordered = new ArrayList<>(counts.entrySet());
        ordered.sort(Map.Entry.<String, Long>comparingByValue(Comparator.reverseOrder())
                .thenComparing(Map.Entry.comparingByKey()));
        List<PopularWindowStore.SnapshotRank> ranks = new ArrayList<>();
        for (int index = 0; index < Math.min(10, ordered.size()); index++) {
            Map.Entry<String, Long> entry = ordered.get(index);
            ranks.add(new PopularWindowStore.SnapshotRank(
                    index + 1, entry.getKey(), entry.getValue()));
        }
        return List.copyOf(ranks);
    }

    @Override
    public void shutdown() {
        synchronized (lock) {
            if (closed) return;
            closed = true;
            checkpointExecutor.shutdown();
        }
        try {
            if (!checkpointExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                checkpointExecutor.shutdownNow();
                checkpointExecutor.awaitTermination(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException interrupted) {
            checkpointExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
