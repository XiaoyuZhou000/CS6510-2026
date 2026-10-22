package metrics;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Process-wide registry of named counters, gauges, and latency timers (R21).
 *
 * <p>Recording is always on and lock-free, so tests can read snapshots whether or not
 * {@link MetricsReporter} prints them. Hot paths should keep the returned {@link LongAdder} or
 * {@link LatencyHistogram} in a static field instead of looking the name up per request.</p>
 */
public final class PerfMetrics {
    private static final ConcurrentHashMap<String, LongAdder> COUNTERS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LatencyHistogram> TIMERS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongSupplier> GAUGES = new ConcurrentHashMap<>();

    private PerfMetrics() { }

    public static LongAdder counter(String name) {
        return COUNTERS.computeIfAbsent(requireName(name), ignored -> new LongAdder());
    }

    public static void increment(String name) { counter(name).increment(); }

    public static void add(String name, long delta) { counter(name).add(delta); }

    public static LatencyHistogram timer(String name) {
        return TIMERS.computeIfAbsent(requireName(name), ignored -> new LatencyHistogram());
    }

    public static void record(String name, long nanos) { timer(name).record(nanos); }

    /** Registers or replaces the supplier sampled whenever the gauge is read. */
    public static void gauge(String name, LongSupplier supplier) {
        if (supplier == null) throw new IllegalArgumentException("gauge supplier is required");
        GAUGES.put(requireName(name), supplier);
    }

    public static long counterValue(String name) {
        LongAdder counter = COUNTERS.get(name);
        return counter == null ? 0 : counter.sum();
    }

    public static LatencyHistogram.Snapshot timerSnapshot(String name) {
        LatencyHistogram timer = TIMERS.get(name);
        return timer == null ? new LatencyHistogram().snapshot() : timer.snapshot();
    }

    /** Current gauge value, or {@code null} when no gauge has that name. */
    public static Long gaugeValue(String name) {
        LongSupplier gauge = GAUGES.get(name);
        return gauge == null ? null : gauge.getAsLong();
    }

    /** One JSON line: {@code {"service":…,"ts":…,"timers":{…},"counters":{…},"gauges":{…}}}. */
    public static String toJson(String service, long timestampMillis) {
        StringBuilder json = new StringBuilder(256);
        json.append("{\"service\":").append(quote(service)).append(",\"ts\":").append(timestampMillis);
        json.append(",\"timers\":{");
        boolean first = true;
        for (Map.Entry<String, LatencyHistogram> timer : new TreeMap<>(TIMERS).entrySet()) {
            LatencyHistogram.Snapshot s = timer.getValue().snapshot();
            if (!first) json.append(',');
            first = false;
            json.append(quote(timer.getKey())).append(":{\"count\":").append(s.count())
                    .append(",\"p50_ms\":").append(millis(s.p50Nanos()))
                    .append(",\"p95_ms\":").append(millis(s.p95Nanos()))
                    .append(",\"p99_ms\":").append(millis(s.p99Nanos()))
                    .append(",\"max_ms\":").append(millis(s.maxNanos()))
                    .append(",\"mean_ms\":").append(millis(s.meanNanos())).append('}');
        }
        json.append("},\"counters\":{");
        first = true;
        for (Map.Entry<String, LongAdder> counter : new TreeMap<>(COUNTERS).entrySet()) {
            if (!first) json.append(',');
            first = false;
            json.append(quote(counter.getKey())).append(':').append(counter.getValue().sum());
        }
        json.append("},\"gauges\":{");
        first = true;
        for (Map.Entry<String, LongSupplier> gauge : new TreeMap<>(GAUGES).entrySet()) {
            long value;
            try {
                value = gauge.getValue().getAsLong();
            } catch (RuntimeException unavailable) {
                continue;
            }
            if (!first) json.append(',');
            first = false;
            json.append(quote(gauge.getKey())).append(':').append(value);
        }
        return json.append("}}").toString();
    }

    /**
     * Zeroes every counter and timer in place, so instances already held in static fields keep
     * reporting; for tests that need a known starting point. Gauges are left registered.
     */
    public static void reset() {
        COUNTERS.values().forEach(LongAdder::reset);
        TIMERS.values().forEach(LatencyHistogram::reset);
    }

    private static String millis(long nanos) {
        return String.format(java.util.Locale.ROOT, "%.3f", LatencyHistogram.Snapshot.millis(nanos));
    }

    private static String requireName(String name) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("metric name is required");
        return name;
    }

    static String quote(String raw) {
        StringBuilder quoted = new StringBuilder(raw.length() + 2).append('"');
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"' -> quoted.append("\\\"");
                case '\\' -> quoted.append("\\\\");
                case '\n' -> quoted.append("\\n");
                case '\r' -> quoted.append("\\r");
                case '\t' -> quoted.append("\\t");
                default -> {
                    if (c < 0x20) quoted.append(String.format("\\u%04x", (int) c));
                    else quoted.append(c);
                }
            }
        }
        return quoted.append('"').toString();
    }
}
