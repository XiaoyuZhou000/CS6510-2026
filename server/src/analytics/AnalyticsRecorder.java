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

public final class AnalyticsRecorder {

    private static final int WINDOW_SIZE    = 1000;
    private static final int SLIDE_INTERVAL = 500;
    private static final int MAX_RETRIES    = 2;

    private final AtomicLong globalScanCounter;
    private final String[]   ringBuffer = new String[WINDOW_SIZE];

    // guards: counter increment + ring-buffer write + boundary snapshot + task submission
    private final Object lock = new Object();

    private volatile boolean skipNextCheckpoint;

    private final ExecutorService checkpointExecutor =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "analytics-checkpoint");
                t.setDaemon(true);
                return t;
            });

    private final CatalogCache    catalog;
    private final PopularWindowDao windowDao;

    public AnalyticsRecorder(CatalogCache catalog, PopularWindowDao windowDao) throws SQLException {
        this.catalog    = catalog;
        this.windowDao  = windowDao;
        long maxWindowEnd = windowDao.readMaxWindowEnd();
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
            int  index = (int) ((count - 1) % WINDOW_SIZE);
            ringBuffer[index] = sku;

            if (count % SLIDE_INTERVAL == 0) {
                if (skipNextCheckpoint) {
                    skipNextCheckpoint = false;
                    return;
                }
                // Snapshot under the lock, then submit outside it
                String[] snapshot = new String[WINDOW_SIZE];
                System.arraycopy(ringBuffer, 0, snapshot, 0, WINDOW_SIZE);
                long windowEnd   = count;
                long windowStart = windowEnd - WINDOW_SIZE + 1;

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
            CatalogCache.CatalogItem item = catalog.get(e.getKey());
            if (item != null) name = item.name();
            ranked.add(new PopularWindowDao.PopularEntry(e.getKey(), name, e.getValue()));
        }
        ranked.sort((a, b) -> Long.compare(b.scanCount(), a.scanCount()));
        if (ranked.size() > 10) ranked = ranked.subList(0, 10);

        // Persist with bounded retries (in-place, not re-queued)
        int attempts = 0;
        long[] backoffMs = {50, 200};
        while (attempts <= MAX_RETRIES) {
            try {
                windowDao.writeWindow(windowStart, windowEnd, ranked);
                return;
            } catch (SQLException e) {
                if (attempts < MAX_RETRIES) {
                    try { Thread.sleep(backoffMs[attempts]); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                attempts++;
            }
        }
        System.err.println("[analytics] Checkpoint [" + windowStart + "," + windowEnd + "] failed after retries");
    }

    public void shutdown() {
        checkpointExecutor.shutdown();
    }
}
