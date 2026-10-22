package metrics;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Lock-free latency histogram with fixed log-scale buckets from 50 µs to 30 s (R21).
 *
 * <p>Bucket 0 holds values below 50 µs, buckets 1..N cover {@code [bound(k-1), bound(k))} with
 * eight buckets per doubling (about 9% resolution), and bucket N+1 holds values of 30 s or more.
 * A percentile is estimated as the upper bound of the bucket that contains its rank, capped at the
 * exact maximum.</p>
 */
public final class LatencyHistogram {
    public static final long MIN_BOUND_NANOS = 50_000L;
    public static final long MAX_BOUND_NANOS = 30_000_000_000L;
    private static final int BUCKETS_PER_DOUBLING = 8;
    private static final long[] BOUNDS = bounds();

    private final LongAdder[] buckets = new LongAdder[BOUNDS.length + 1];
    private final LongAdder totalNanos = new LongAdder();
    private final AtomicLong maxNanos = new AtomicLong();

    public LatencyHistogram() {
        for (int i = 0; i < buckets.length; i++) buckets[i] = new LongAdder();
    }

    private static long[] bounds() {
        int steps = (int) Math.ceil(BUCKETS_PER_DOUBLING
                * (Math.log((double) MAX_BOUND_NANOS / MIN_BOUND_NANOS) / Math.log(2)));
        long[] bounds = new long[steps + 1];
        for (int i = 0; i <= steps; i++) {
            bounds[i] = Math.min(MAX_BOUND_NANOS,
                    Math.round(MIN_BOUND_NANOS * Math.pow(2, (double) i / BUCKETS_PER_DOUBLING)));
        }
        bounds[steps] = MAX_BOUND_NANOS;
        return bounds;
    }

    /** Number of buckets, including the underflow and overflow buckets. */
    public static int bucketCount() { return BOUNDS.length + 1; }

    /** Exclusive upper bound of a bucket; the overflow bucket has none. */
    public static long upperBoundNanos(int bucket) {
        if (bucket < 0 || bucket >= BOUNDS.length) throw new IllegalArgumentException("No upper bound: " + bucket);
        return BOUNDS[bucket];
    }

    public static int bucketIndex(long nanos) {
        int found = Arrays.binarySearch(BOUNDS, Math.max(0, nanos));
        return found >= 0 ? found + 1 : -found - 1;
    }

    public void record(long nanos) {
        long value = Math.max(0, nanos);
        buckets[bucketIndex(value)].increment();
        totalNanos.add(value);
        if (value > maxNanos.get()) maxNanos.accumulateAndGet(value, Math::max);
    }

    /** Records the time elapsed since {@code startNanos}, taken from {@link System#nanoTime()}. */
    public void recordSince(long startNanos) {
        record(System.nanoTime() - startNanos);
    }

    /** Zeroes the histogram; not atomic with concurrent records, so for tests only. */
    public void reset() {
        for (LongAdder bucket : buckets) bucket.reset();
        totalNanos.reset();
        maxNanos.set(0);
    }

    public Snapshot snapshot() {
        long[] counts = new long[buckets.length];
        long count = 0;
        for (int i = 0; i < counts.length; i++) {
            counts[i] = buckets[i].sum();
            count += counts[i];
        }
        long max = maxNanos.get();
        return new Snapshot(count, percentile(counts, count, max, 0.50), percentile(counts, count, max, 0.95),
                percentile(counts, count, max, 0.99), max, count == 0 ? 0 : totalNanos.sum() / count);
    }

    private static long percentile(long[] counts, long count, long max, double quantile) {
        if (count == 0) return 0;
        long target = Math.max(1, (long) Math.ceil(count * quantile));
        long seen = 0;
        for (int i = 0; i < counts.length; i++) {
            seen += counts[i];
            if (seen >= target) return i < BOUNDS.length ? Math.min(BOUNDS[i], max) : max;
        }
        return max;
    }

    public record Snapshot(long count, long p50Nanos, long p95Nanos, long p99Nanos, long maxNanos,
                           long meanNanos) {
        public static double millis(long nanos) { return nanos / 1_000_000.0; }
    }
}
