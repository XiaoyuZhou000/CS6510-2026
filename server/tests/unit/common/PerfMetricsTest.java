package unit.common;

import api.json.Json;
import metrics.LatencyHistogram;
import metrics.MetricsReporter;
import metrics.PerfMetrics;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class PerfMetricsTest {
    @Test
    void bucketsAreLogScaleFromFiftyMicrosecondsToThirtySecondsWithUnderAndOverflow() {
        assertEquals(LatencyHistogram.MIN_BOUND_NANOS, LatencyHistogram.upperBoundNanos(0));
        assertEquals(0, LatencyHistogram.bucketIndex(0));
        assertEquals(0, LatencyHistogram.bucketIndex(49_999));
        assertEquals(1, LatencyHistogram.bucketIndex(50_000));
        int last = LatencyHistogram.bucketCount() - 1;
        assertEquals(LatencyHistogram.MAX_BOUND_NANOS, LatencyHistogram.upperBoundNanos(last - 1));
        assertEquals(last, LatencyHistogram.bucketIndex(30_000_000_000L));
        assertEquals(last, LatencyHistogram.bucketIndex(Long.MAX_VALUE));
        assertEquals(last - 1, LatencyHistogram.bucketIndex(29_999_999_999L));
        for (int bucket = 1; bucket < last; bucket++) {
            long lower = LatencyHistogram.upperBoundNanos(bucket - 1);
            long upper = LatencyHistogram.upperBoundNanos(bucket);
            assertTrue(upper > lower, "bounds increase at bucket " + bucket);
            assertTrue((double) upper / lower <= 1.0906, "about 9% resolution at bucket " + bucket);
            assertEquals(bucket, LatencyHistogram.bucketIndex(lower));
            assertEquals(bucket, LatencyHistogram.bucketIndex(upper - 1));
        }
        assertEquals(0, LatencyHistogram.bucketIndex(-5), "negative durations count as zero");
    }

    @Test
    void percentilesAreBucketUpperBoundsCappedAtTheExactMaximum() {
        LatencyHistogram histogram = new LatencyHistogram();
        for (int i = 1; i <= 100; i++) histogram.record(i * 1_000_000L);
        LatencyHistogram.Snapshot snapshot = histogram.snapshot();
        assertEquals(100, snapshot.count());
        assertEquals(100_000_000L, snapshot.maxNanos());
        assertEquals(50_500_000L, snapshot.meanNanos());
        assertWithinBucket(50_000_000L, snapshot.p50Nanos());
        assertWithinBucket(95_000_000L, snapshot.p95Nanos());
        assertWithinBucket(99_000_000L, snapshot.p99Nanos());
        assertTrue(snapshot.p99Nanos() <= snapshot.maxNanos());

        LatencyHistogram single = new LatencyHistogram();
        single.record(1_234_567L);
        LatencyHistogram.Snapshot one = single.snapshot();
        assertEquals(1_234_567L, one.p50Nanos(), "a lone value reports its exact maximum");
        assertEquals(1_234_567L, one.p99Nanos());

        LatencyHistogram.Snapshot empty = new LatencyHistogram().snapshot();
        assertEquals(0, empty.count());
        assertEquals(0, empty.p99Nanos());
    }

    @Test
    void concurrentRecordingIsLossless() throws Exception {
        LatencyHistogram histogram = new LatencyHistogram();
        try (var pool = Executors.newFixedThreadPool(8)) {
            for (int t = 0; t < 8; t++) {
                pool.submit(() -> { for (int i = 0; i < 10_000; i++) histogram.record(100_000L + i); });
            }
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        }
        assertEquals(80_000, histogram.snapshot().count());
        assertEquals(109_999L, histogram.snapshot().maxNanos());
    }

    @Test
    void registryEmitsOneSortedJsonLineWithTimersCountersAndGauges() {
        PerfMetrics.timer("test.json.timer").record(2_000_000L);
        PerfMetrics.add("test.json.counter", 3);
        PerfMetrics.increment("test.json.counter");
        PerfMetrics.gauge("test.json.gauge", () -> 42);
        PerfMetrics.gauge("test.json.broken", () -> { throw new IllegalStateException("unavailable"); });
        assertEquals(4, PerfMetrics.counterValue("test.json.counter"));
        assertEquals(0, PerfMetrics.counterValue("test.json.absent"));
        assertEquals(42L, PerfMetrics.gaugeValue("test.json.gauge"));
        assertNull(PerfMetrics.gaugeValue("test.json.absent"));

        String line = PerfMetrics.toJson("svc \"quoted\"", 1234L);
        assertFalse(line.contains("\n"), "one line");
        Map<String, Object> json = Json.parseObject(line);
        assertEquals("svc \"quoted\"", json.get("service"));
        assertEquals(1234, Json.getInt(json, "ts"));
        Map<String, Object> timer = Json.asObject(Json.asObject(json.get("timers")).get("test.json.timer"));
        assertEquals(1, Json.getInt(timer, "count"));
        for (String field : List.of("p50_ms", "p95_ms", "p99_ms", "max_ms", "mean_ms")) {
            assertEquals(2.0, Json.getDouble(timer, field), 1e-9, field);
        }
        assertEquals(4, Json.getInt(Json.asObject(json.get("counters")), "test.json.counter"));
        Map<String, Object> gauges = Json.asObject(json.get("gauges"));
        assertEquals(42, Json.getInt(gauges, "test.json.gauge"));
        assertFalse(gauges.containsKey("test.json.broken"), "a failing gauge is left out");
        int first = line.indexOf("\"test.json.counter\"");
        assertTrue(first > line.indexOf("\"counters\""));

        LatencyHistogram held = PerfMetrics.timer("test.json.timer");
        PerfMetrics.reset();
        assertSame(held, PerfMetrics.timer("test.json.timer"), "reset keeps instances held by callers");
        assertEquals(0, PerfMetrics.timerSnapshot("test.json.timer").count());
        assertEquals(0, PerfMetrics.counterValue("test.json.counter"));
    }

    @Test
    void reporterPrintsOnlyWhenEnabledAndOnceMoreOnStop() throws Exception {
        ByteArrayOutputStream disabledBytes = new ByteArrayOutputStream();
        MetricsReporter disabled = MetricsReporter.start("quiet", Map.of(),
                new PrintStream(disabledBytes, true, StandardCharsets.UTF_8));
        assertFalse(disabled.enabled());
        disabled.stop();
        assertEquals("", disabledBytes.toString(StandardCharsets.UTF_8));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        MetricsReporter reporter = MetricsReporter.start("reporting",
                Map.of("PERF_METRICS", "true", "PERF_METRICS_INTERVAL_SECONDS", "1"),
                new PrintStream(bytes, true, StandardCharsets.UTF_8));
        assertTrue(reporter.enabled());
        Thread.sleep(1_500);
        reporter.stop();
        reporter.stop();
        List<String> lines = bytes.toString(StandardCharsets.UTF_8).lines().toList();
        assertEquals(2, lines.size(), "one periodic line and one final line: " + lines);
        for (String line : lines) assertEquals("reporting", Json.parseObject(line).get("service"));
        assertThrows(IllegalArgumentException.class, () -> MetricsReporter.start("bad",
                Map.of("PERF_METRICS", "true", "PERF_METRICS_INTERVAL_SECONDS", "0"), System.out));
    }

    private static void assertWithinBucket(long expected, long estimate) {
        assertTrue(estimate >= expected && estimate <= expected * 1.0906,
                "estimate " + estimate + " should be the upper bound of the bucket holding " + expected);
    }
}
