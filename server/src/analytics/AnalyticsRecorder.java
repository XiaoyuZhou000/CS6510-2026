package analytics;

import catalog.CatalogCache;
import persistence.PopularWindowDao;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public final class AnalyticsRecorder {

    private static final int MAX_RETRIES    = 2;

    private final AtomicLong globalScanCounter;
    private final String[]   ringBuffer = new String[WindowMath.WINDOW_SIZE];

    // guards: counter increment + ring-buffer write + boundary snapshot + task submission
    private final Object lock = new Object();

    private volatile boolean skipNextCheckpoint;

    private final ExecutorService checkpointExecutor =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "analytics-checkpoint");
                t.setDaemon(true);
                return t;
            });

    private final Function<String, String> nameLookup;
    private final AnalyticsWindowStore windowStore;

    public AnalyticsRecorder(CatalogCache catalog, PopularWindowDao windowDao) throws SQLException {
        this(sku -> {
            CatalogCache.CatalogItem item = catalog.get(sku);
            return item == null ? "" : item.name();
        }, windowDao);
    }

    /** Testable constructor for exercising checkpoint persistence and recovery deterministically. */
    public AnalyticsRecorder(Function<String, String> nameLookup,
                             AnalyticsWindowStore windowStore) throws SQLException {
        this.nameLookup = nameLookup;
        this.windowStore = windowStore;
        long maxWindowEnd = windowStore.readMaxWindowEnd();
        this.globalScanCounter  = new AtomicLong(maxWindowEnd);
        this.skipNextCheckpoint = maxWindowEnd > 0;
    }

    /**
     * Records one scan of the given SKU.
     * Increments the global counter, writes to the ring buffer, and — every 500th scan —
     * submits a checkpoint task to the background executor.
     * Never blocks on analytics persistence.
     */
    public void recordScan(String sku) {
        synchronized (lock) {
            long count = globalScanCounter.incrementAndGet();
            int  index = WindowMath.ringIndex(count);
            ringBuffer[index] = sku;

            WindowMath.CheckpointDecision decision =
                    WindowMath.checkpointDecision(count, skipNextCheckpoint);
            skipNextCheckpoint = decision.skipNextCheckpoint();
            if (decision.checkpoint()) {
                // Snapshot under the lock, then submit outside it
                String[] snapshot = new String[WindowMath.WINDOW_SIZE];
                System.arraycopy(ringBuffer, 0, snapshot, 0, WindowMath.WINDOW_SIZE);
                long windowEnd   = count;
                long windowStart = WindowMath.windowStart(windowEnd);

                checkpointExecutor.submit(() -> runCheckpoint(snapshot, windowStart, windowEnd));
            }
        }
    }

    private void runCheckpoint(String[] snapshot, long windowStart, long windowEnd) {
        // Tally counts for the snapshot window
        Map<String, Long> counts = new HashMap<>();
        for (String s : snapshot) {
            if (s != null) counts.merge(s, 1L, Long::sum);
        }

        // Rank top 10 descending by count
        List<PopularWindowDao.PopularEntry> ranked = new ArrayList<>(counts.size());
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            String name = "";
            String resolved = nameLookup.apply(e.getKey());
            if (resolved != null) name = resolved;
            ranked.add(new PopularWindowDao.PopularEntry(e.getKey(), name, e.getValue()));
        }
        ranked.sort((a, b) -> Long.compare(b.scanCount(), a.scanCount()));
        if (ranked.size() > 10) ranked = ranked.subList(0, 10);

        // Keep the oldest failed checkpoint at the head of this single-threaded executor.
        // Fast retries are bounded, then recovery continues at a capped interval until the
        // write succeeds. Scan admission never waits for this worker.
        int failures = 0;
        long[] backoffMs = {50, 200};
        while (!Thread.currentThread().isInterrupted()) {
            try {
                windowStore.writeWindow(windowStart, windowEnd, ranked);
                return;
            } catch (SQLException e) {
                long delay = failures < MAX_RETRIES ? backoffMs[failures] : 1000L;
                failures++;
                if (failures == MAX_RETRIES + 1) {
                    System.err.println("[analytics] Checkpoint [" + windowStart + "," + windowEnd
                        + "] still failing; retaining it for ordered recovery");
                }
                try { Thread.sleep(delay); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    public void shutdown() {
        checkpointExecutor.shutdown();
    }
}
