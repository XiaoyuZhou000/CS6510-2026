import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 007 T119: {@link IsolationSummary} takes the median of three {@code endToEndExcluding.p95} values per mode, divides
 * each faulted median by the healthy one, and passes SC-006 only at a ratio of at most 1.20 with every run reconciled
 * and zero missing or duplicate notifications.
 */
class IsolationSummaryTest {

    private static IsolationSummary.Run run(String mode, int n, long p95) {
        return run(mode, n, p95, "PASS", 0, 0);
    }

    private static IsolationSummary.Run run(String mode, int n, long p95, String verdict, long missing, long duplicate) {
        String faults = mode.equals("healthy") ? "none" : mode + ":sub-0001";
        return new IsolationSummary.Run(mode + "-" + n, mode, faults, "sub-0001", p95, verdict, missing, duplicate);
    }

    /** Healthy 100, 120, 110 (median 110); reject and stall as given. */
    private static List<IsolationSummary.Run> runs(long[] reject, long[] stall) {
        List<IsolationSummary.Run> runs = new ArrayList<>(List.of(run("healthy", 1, 100), run("healthy", 2, 120),
                run("healthy", 3, 110)));
        for (int i = 0; i < 3; i++) runs.add(run("reject", i + 1, reject[i]));
        for (int i = 0; i < 3; i++) runs.add(run("stall", i + 1, stall[i]));
        return runs;
    }

    @Test
    void theMedianOfThreeP95ValuesPerModeAndTheRatioAgainstHealthy() {
        IsolationSummary.Summary summary = IsolationSummary.summarize(runs(new long[] {130, 90, 115}, new long[] {200, 121, 125}));
        IsolationSummary.Mode healthy = summary.modes().get(0);
        IsolationSummary.Mode reject = summary.modes().get(1);
        IsolationSummary.Mode stall = summary.modes().get(2);
        assertEquals(110.0, healthy.medianP95());
        assertNull(healthy.ratio());
        assertEquals(115.0, reject.medianP95(), "the median, not the mean (111.7)");
        assertEquals(115.0 / 110.0, reject.ratio(), 1e-9);
        assertEquals(125.0, stall.medianP95(), "one outlier of 200 does not move the median");
        assertEquals(125.0 / 110.0, stall.ratio(), 1e-9);
        assertTrue(summary.passed(), summary.problems().toString());
    }

    @Test
    void aRatioOfExactly120PercentPassesAndAboveItFails() {
        assertTrue(IsolationSummary.summarize(runs(new long[] {132, 132, 132}, new long[] {110, 110, 110})).passed(),
                "132 / 110 = 1.20");
        IsolationSummary.Summary above = IsolationSummary.summarize(runs(new long[] {133, 133, 133}, new long[] {110, 110, 110}));
        assertFalse(above.passed());
        assertTrue(above.problems().get(0).startsWith("reject: median p95 133 ms is 1.21 of healthy 110 ms"), above.problems().toString());
    }

    @Test
    void aMissingOrDuplicateNotificationOrAFailedReconciliationFails() {
        for (IsolationSummary.Run bad : List.of(run("stall", 2, 110, "PASS", 1, 0), run("stall", 2, 110, "PASS", 0, 1),
                run("healthy", 2, 120, "FAIL", 0, 0))) {
            List<IsolationSummary.Run> runs = new ArrayList<>(runs(new long[] {110, 110, 110}, new long[] {110, 110, 110}));
            runs.replaceAll(run -> run.name().equals(bad.name()) ? bad : run);
            IsolationSummary.Summary summary = IsolationSummary.summarize(runs);
            assertFalse(summary.passed(), bad.toString());
            assertTrue(summary.problems().get(0).startsWith(bad.name() + ": reconciliation"), summary.problems().toString());
        }
    }

    @Test
    void fewerThanThreeRunsOfAModeFails() {
        List<IsolationSummary.Run> runs = new ArrayList<>(runs(new long[] {110, 110, 110}, new long[] {110, 110, 110}));
        runs.removeIf(run -> run.name().equals("stall-3"));
        IsolationSummary.Summary summary = IsolationSummary.summarize(runs);
        assertFalse(summary.passed());
        assertEquals(List.of("stall: 2 run(s), at least 3 needed"), summary.problems());
        assertEquals(110.0, summary.modes().get(2).medianP95(), "two values: the mean of both");
    }

    @Test
    void aRunWithTheWrongFaultFails() {
        List<IsolationSummary.Run> runs = new ArrayList<>(runs(new long[] {110, 110, 110}, new long[] {110, 110, 110}));
        runs.set(3, new IsolationSummary.Run("reject-1", "reject", "none", "sub-0001", 110L, "PASS", 0, 0));
        IsolationSummary.Summary summary = IsolationSummary.summarize(runs);
        assertEquals(List.of("reject-1 ran with faults none"), summary.problems());
    }

    @Test
    void readsTheRunFoldersAndWritesTheReadme(@TempDir Path dir) throws Exception {
        long[] p95 = {100, 120, 110, 130, 90, 115, 140, 125, 121};
        int i = 0;
        for (String mode : IsolationSummary.MODES) {
            for (int n = 1; n <= 3; n++) report(dir, mode, n, p95[i++]);
        }
        Files.createDirectories(dir.resolve("not-a-run"));
        assertEquals(0, IsolationSummary.run(new String[] {"--dir=" + dir}));
        String readme = Files.readString(dir.resolve("README.md"), StandardCharsets.UTF_8);
        assertTrue(readme.contains("**SC-006: PASS**"), readme);
        assertTrue(readme.contains("| healthy | 3 | 110 | — |"), readme);
        assertTrue(readme.contains("| reject | 3 | 115 | 1.05 |"), readme);
        assertTrue(readme.contains("| stall | 3 | 125 | 1.14 |"), readme);
        assertTrue(readme.contains("| [stall-3](stall-3/notification-report.md) | `stall:sub-0001` | 121 | PASS | 0 | 0 |"), readme);

        // A fourth, slow reject run: the median of 90, 115, 130, 900 is 122.5, 1.11 of healthy, so it still passes.
        report(dir, "reject", 4, 900);
        assertEquals(0, IsolationSummary.run(new String[] {"--dir=" + dir}));
        assertTrue(Files.readString(dir.resolve("README.md"), StandardCharsets.UTF_8).contains("| reject | 4 | 122.5 | 1.11 |"));
        // Two more slow ones move the median to (130 + 900) / 2 = 515 ms, far above 1.20 of healthy.
        report(dir, "reject", 5, 950);
        report(dir, "reject", 6, 960);
        assertEquals(1, IsolationSummary.run(new String[] {"--dir=" + dir}));
    }

    @Test
    void badArgumentsAndAnUnreadableReportExitWithTwo(@TempDir Path dir) throws Exception {
        assertEquals(2, IsolationSummary.run(new String[] {}));
        assertEquals(2, IsolationSummary.run(new String[] {"--bogus"}));
        Files.createDirectories(dir.resolve("healthy-1"));
        assertEquals(2, IsolationSummary.run(new String[] {"--dir=" + dir}), "a run folder without its report");
    }

    private static void report(Path dir, String mode, int n, long p95) throws Exception {
        Path run = Files.createDirectories(dir.resolve(mode + "-" + n));
        String faults = mode.equals("healthy") ? "none" : mode + ":sub-0001";
        Files.writeString(run.resolve("notification-report.json"), """
                {"run": {"id": "isolation/%s-%d", "faults": "%s"},
                 "latencyMs": {"endToEnd": {"p95": 1}, "endToEndExcluding": {"subscriberId": "sub-0001", "p50": 1, "p95": %d, "p99": 2}},
                 "reconciliation": {"verdict": "PASS", "counts": {"missingNotDelivered": 0, "missingDeliveredNotReceived": 0,
                   "duplicate": 0}}}
                """.formatted(mode, n, faults, p95), StandardCharsets.UTF_8);
    }
}
