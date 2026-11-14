import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@code IsolationSummary --dir=DIR} (007 T125, SC-006, FR-038): the verdict of the isolation runs.
 *
 * <p>It reads {@code DIR/<mode>-<n>/notification-report.json} for the modes {@code healthy}, {@code reject}, and
 * {@code stall}. Per mode it takes the median of the runs' {@code latencyMs.endToEndExcluding.p95}, the p95 end-to-end
 * latency of every subscriber except the faulted one, and divides each faulted mode's median by the healthy median.
 * SC-006 passes when every mode has at least three runs, each faulted ratio is at most {@link #MAX_RATIO}, and every
 * run's reconciliation passed with zero missing and zero duplicate notifications. A faulted run reconciles after its
 * fault is cleared and its parked notifications are retried, so a pass also means nothing owed to the other
 * subscribers was missing or duplicated. It writes {@code DIR/README.md} and prints the verdict.</p>
 *
 * <p>Exit codes: 0 PASS, 1 FAIL, 2 invalid arguments or an unreadable report.</p>
 */
public final class IsolationSummary {
    static final List<String> MODES = List.of("healthy", "reject", "stall");
    static final double MAX_RATIO = 1.20;
    static final int MIN_RUNS = 3;
    private static final Pattern RUN = Pattern.compile("(healthy|reject|stall)-(\\d+)");

    private IsolationSummary() { }

    /** One isolation run, as its notification report gives it. */
    record Run(String name, String mode, String faults, String excludedSubscriber, Long p95Excluding, String verdict,
               long missing, long duplicate) {
        boolean clean() {
            return "PASS".equals(verdict) && missing == 0 && duplicate == 0 && p95Excluding != null;
        }
    }

    /** Per mode: its runs, the median p95 (null without runs), and the ratio against healthy (null for healthy). */
    record Mode(String mode, List<Run> runs, Double medianP95, Double ratio) { }

    record Summary(List<Mode> modes, List<String> problems) {
        boolean passed() { return problems.isEmpty(); }
    }

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        Path dir = null;
        for (String arg : args) {
            if (arg.startsWith("--dir=")) dir = Path.of(arg.substring("--dir=".length()));
            else return usage("Unknown argument: " + arg);
        }
        if (dir == null) return usage("--dir=DIR is required");
        try {
            Summary summary = summarize(read(dir));
            Files.writeString(dir.resolve("README.md"), markdown(summary), StandardCharsets.UTF_8);
            System.out.println("SC-006 " + (summary.passed() ? "PASS" : "FAIL"));
            for (Mode mode : summary.modes()) {
                System.out.println("  " + mode.mode() + ": " + mode.runs().size() + " run(s), median p95 excluding "
                        + format(mode.medianP95()) + " ms" + (mode.ratio() == null ? "" : ", ratio " + ratio(mode.ratio())));
            }
            for (String problem : summary.problems()) System.out.println("  " + problem);
            return summary.passed() ? 0 : 1;
        } catch (IOException | RuntimeException failure) {
            System.err.println("IsolationSummary: " + failure.getMessage());
            return 2;
        }
    }

    /** Every {@code <mode>-<n>/notification-report.json} under {@code dir}, in mode and run order. */
    static List<Run> read(Path dir) throws IOException {
        List<Run> runs = new ArrayList<>();
        try (DirectoryStream<Path> children = Files.newDirectoryStream(dir, Files::isDirectory)) {
            for (Path child : children) {
                Matcher name = RUN.matcher(child.getFileName().toString());
                if (!name.matches()) continue;
                Path report = child.resolve("notification-report.json");
                if (!Files.isRegularFile(report)) throw new IOException("missing " + report);
                runs.add(fromReport(child.getFileName().toString(), name.group(1),
                        Json.parseObject(Files.readString(report, StandardCharsets.UTF_8))));
            }
        }
        runs.sort(Comparator.comparingInt((Run run) -> MODES.indexOf(run.mode()))
                .thenComparingInt(run -> Integer.parseInt(run.name().substring(run.name().indexOf('-') + 1))));
        return runs;
    }

    static Run fromReport(String name, String mode, Map<String, Object> report) {
        Map<String, Object> run = Json.getObject(report, "run");
        Map<String, Object> latency = Json.getObject(report, "latencyMs");
        Object excludingValue = latency.get("endToEndExcluding");
        String excluded = null;
        Long p95 = null;
        if (excludingValue != null) {
            Map<String, Object> excluding = Json.asObject(excludingValue);
            excluded = (String) excluding.get("subscriberId");
            Object value = excluding.get("p95");
            if (value instanceof Number number) p95 = number.longValue();
        }
        Map<String, Object> reconciliation = Json.getObject(report, "reconciliation");
        Map<String, Object> counts = Json.getObject(reconciliation, "counts");
        long missing = Json.getLong(counts, "missingNotDelivered") + Json.getLong(counts, "missingDeliveredNotReceived");
        return new Run(name, mode, (String) run.get("faults"), excluded, p95, Json.getString(reconciliation, "verdict"),
                missing, Json.getLong(counts, "duplicate"));
    }

    static Summary summarize(List<Run> runs) {
        List<String> problems = new ArrayList<>();
        Map<String, List<Run>> byMode = new LinkedHashMap<>();
        for (String mode : MODES) byMode.put(mode, new ArrayList<>());
        for (Run run : runs) byMode.get(run.mode()).add(run);

        String excluded = null;
        for (Run run : runs) {
            if (!run.clean()) {
                problems.add(run.name() + ": reconciliation " + run.verdict() + ", missing " + run.missing() + ", duplicate "
                        + run.duplicate() + (run.p95Excluding() == null ? ", no endToEndExcluding" : ""));
            }
            if (run.excludedSubscriber() != null) {
                if (excluded == null) excluded = run.excludedSubscriber();
                else if (!excluded.equals(run.excludedSubscriber())) {
                    problems.add(run.name() + " excludes " + run.excludedSubscriber() + ", not " + excluded);
                }
            }
            String expectedFaults = run.mode().equals("healthy") ? "none" : run.mode() + ":" + run.excludedSubscriber();
            if (!expectedFaults.equals(run.faults())) problems.add(run.name() + " ran with faults " + run.faults());
        }

        Double healthy = median(byMode.get("healthy"));
        List<Mode> modes = new ArrayList<>();
        for (String mode : MODES) {
            List<Run> modeRuns = byMode.get(mode);
            if (modeRuns.size() < MIN_RUNS) problems.add(mode + ": " + modeRuns.size() + " run(s), at least " + MIN_RUNS + " needed");
            Double median = median(modeRuns);
            Double ratio = null;
            if (!mode.equals("healthy") && median != null && healthy != null && healthy > 0) {
                ratio = median / healthy;
                if (ratio > MAX_RATIO) {
                    problems.add(mode + ": median p95 " + format(median) + " ms is " + ratio(ratio) + " of healthy "
                            + format(healthy) + " ms, above " + ratio(MAX_RATIO));
                }
            }
            modes.add(new Mode(mode, List.copyOf(modeRuns), median, ratio));
        }
        return new Summary(modes, problems);
    }

    /** The median of the runs' p95 values that exist; the mean of the middle two for an even count. */
    static Double median(List<Run> runs) {
        List<Long> values = new ArrayList<>();
        for (Run run : runs) if (run.p95Excluding() != null) values.add(run.p95Excluding());
        if (values.isEmpty()) return null;
        values.sort(Long::compare);
        int middle = values.size() / 2;
        return values.size() % 2 == 1 ? values.get(middle) : (values.get(middle - 1) + values.get(middle)) / 2.0;
    }

    static String markdown(Summary summary) {
        StringBuilder text = new StringBuilder();
        text.append("# Isolation runs (SC-006)\n\n");
        text.append("Written by `subscriber-client/run.sh IsolationSummary --dir=<this folder>` (T125). Each run is ")
                .append("`server/benchmark-notification.ps1 -Workload default -Subscribers 100 -Seed 20261111 ")
                .append("-FaultSubscriber sub-0001 -Fault none|reject|stall`. The latency is each run's ")
                .append("`latencyMs.endToEndExcluding.p95`: the p95 end-to-end latency of every subscriber except the ")
                .append("faulted one. SC-006 needs at least ").append(MIN_RUNS).append(" runs per mode, a faulted median ")
                .append("at most ").append(ratio(MAX_RATIO)).append(" of the healthy median, and every run reconciled ")
                .append("with zero missing and zero duplicate notifications (a faulted run reconciles after its fault ")
                .append("is cleared and its parked notifications are retried).\n\n");
        text.append("**SC-006: ").append(summary.passed() ? "PASS" : "FAIL").append("**\n\n");
        text.append("| Mode | Runs | Median p95 excluding (ms) | Ratio to healthy |\n|---|---:|---:|---:|\n");
        for (Mode mode : summary.modes()) {
            text.append("| ").append(mode.mode()).append(" | ").append(mode.runs().size()).append(" | ")
                    .append(format(mode.medianP95())).append(" | ")
                    .append(mode.ratio() == null ? "—" : ratio(mode.ratio())).append(" |\n");
        }
        text.append("\n| Run | Faults | p95 excluding (ms) | Reconciliation | Missing | Duplicate |\n")
                .append("|---|---|---:|---|---:|---:|\n");
        for (Mode mode : summary.modes()) {
            for (Run run : mode.runs()) {
                text.append("| [").append(run.name()).append("](").append(run.name()).append("/notification-report.md) | `")
                        .append(run.faults()).append("` | ").append(run.p95Excluding() == null ? "—" : run.p95Excluding())
                        .append(" | ").append(run.verdict()).append(" | ").append(run.missing()).append(" | ")
                        .append(run.duplicate()).append(" |\n");
            }
        }
        if (!summary.problems().isEmpty()) {
            text.append("\nProblems:\n\n");
            for (String problem : summary.problems()) text.append("- ").append(problem).append('\n');
        }
        return text.toString();
    }

    private static String format(Double value) {
        if (value == null) return "—";
        return value == Math.rint(value) ? Long.toString(value.longValue()) : String.format(Locale.ROOT, "%.1f", value);
    }

    private static String ratio(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }

    private static int usage(String problem) {
        System.err.println("IsolationSummary: " + problem);
        System.err.println("Usage: IsolationSummary --dir=DIR");
        return 2;
    }
}
