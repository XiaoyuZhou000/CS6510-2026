package database;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/** Opt-in, aggregate Scan timings. Never logs request data or performs I/O on a request. */
public final class ScanProfiler {
    public enum Phase {
        REQUEST_TOTAL, RESPONSE_WRITE, POOL_ACQUIRE, BEGIN, TRANSACTION_LOCK,
        CATALOG_LOOKUP, LINE_UPSERT, LINE_PRICE, BASKET_AGGREGATE,
        TOTAL_UPDATE, EVENT_INSERT, COMMIT, TRANSACTION_EXECUTION, POOL_RETURN, STORE_TOTAL
    }

    private static final boolean ENABLED = Boolean.parseBoolean(System.getenv("SCAN_PROFILE"));
    private static final Histogram[] HISTOGRAMS = new Histogram[Phase.values().length];
    static {
        for (int i = 0; i < HISTOGRAMS.length; i++) HISTOGRAMS[i] = new Histogram();
    }

    private ScanProfiler() { }
    public static boolean enabled() { return ENABLED; }

    public static void record(Phase phase, long startNanos) {
        if (ENABLED) HISTOGRAMS[phase.ordinal()].add(System.nanoTime() - startNanos);
    }

    public static void log(ConnectionPool pool) {
        if (!ENABLED) return;
        var occupancy = pool.occupancy();
        System.err.printf("[scan-profile] pool size=%d active=%d idle=%d waiters=%d%n",
            occupancy.size(), occupancy.active(), occupancy.idle(), occupancy.waiters());
        for (Phase phase : Phase.values()) {
            String summary = HISTOGRAMS[phase.ordinal()].summary();
            if (summary != null) System.err.printf("[scan-profile] %s %s%n", phase, summary);
        }
    }

    private static final class Histogram {
        // 0.1 ms buckets to 300 ms; overflow is reported as the final bucket.
        private final AtomicLongArray buckets = new AtomicLongArray(3002);
        private final LongAdder count = new LongAdder();
        private final LongAdder totalNanos = new LongAdder();
        private final AtomicLong maxNanos = new AtomicLong();

        void add(long nanos) {
            int bucket = (int) Math.min(3001, Math.max(0, nanos / 100_000));
            buckets.incrementAndGet(bucket);
            count.increment();
            totalNanos.add(nanos);
            maxNanos.accumulateAndGet(nanos, Math::max);
        }

        String summary() {
            long n = count.sum();
            if (n == 0) return null;
            return String.format("count=%d mean_ms=%.3f p50_ms=%.1f p95_ms=%.1f p99_ms=%.1f max_ms=%.3f",
                n, totalNanos.sum() / (n * 1_000_000.0), percentile(n, .50),
                percentile(n, .95), percentile(n, .99), maxNanos.get() / 1_000_000.0);
        }

        private double percentile(long n, double quantile) {
            long target = (long) Math.ceil(n * quantile);
            long seen = 0;
            for (int i = 0; i < buckets.length(); i++) {
                seen += buckets.get(i);
                if (seen >= target) return i / 10.0;
            }
            return 300.1;
        }
    }
}
