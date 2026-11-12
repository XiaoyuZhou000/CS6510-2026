import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The notification report of one run, {@code notification-report.json} and its Markdown rendering (contracts/
 * subscriber-client-and-reconciliation.md "Notification report", research N9, SC-007). It is separate from the
 * Load Client's report, which stays unmodified.
 *
 * <ul>
 *   <li>End-to-end latency is each owed notification's first receipt minus its sale's completion time (both carried
 *       in the receipt, one machine clock); completion-to-delivered uses Notification's {@code delivered_at}.
 *       Percentiles use the nearest-rank method; a notification never received is excluded from them and counted
 *       as missing.</li>
 *   <li>Rates are over the active window, from the first to the last completed sale.</li>
 *   <li>The drain runs from the final completion to the last first receipt (0 when every receipt came earlier); the
 *       deadline is met when nothing is missing and the drain took at most 60 s.</li>
 * </ul>
 */
public final class NotificationReport {
    public static final long DEADLINE_SECONDS = 60;
    static final List<String> BACKLOG_FIELDS = List.of("saleEventsUnpublished", "saleConsumerLag", "notificationsPending",
            "channelLag");

    private NotificationReport() { }

    /** Nearest-rank percentile of ascending values: the value at rank ceil(p/100 x n). */
    public static long nearestRank(long[] sorted, double percentile) {
        if (sorted.length == 0) throw new IllegalArgumentException("no values");
        int rank = (int) Math.ceil(percentile / 100.0 * sorted.length);
        return sorted[Math.max(1, Math.min(sorted.length, rank)) - 1];
    }

    public static Map<String, Object> build(RunData data, Reconciler.Result result) {
        Map<String, Reconciler.Owed> owed = Reconciler.owed(data);
        Map<String, Map<String, Integer>> sales = Reconciler.completedSales(data);
        long firstCompletion = Long.MAX_VALUE;
        long lastCompletion = Long.MIN_VALUE;
        for (RunData.SaleLine line : data.sales) {
            firstCompletion = Math.min(firstCompletion, line.completedAtMs());
            lastCompletion = Math.max(lastCompletion, line.completedAtMs());
        }
        boolean anySale = !data.sales.isEmpty();
        double windowSeconds = anySale ? (lastCompletion - firstCompletion) / 1000.0 : 0;

        // First receipt and delivery per owed key.
        Map<String, RunData.Receipt> firstReceipt = new HashMap<>();
        for (RunData.Receipt receipt : data.receipts) {
            if (receipt.undecodable()) continue;
            String key = Reconciler.key(receipt.saleId(), receipt.sku(), receipt.subscriberId());
            if (!owed.containsKey(key)) continue;
            RunData.Receipt current = firstReceipt.get(key);
            if (current == null || receipt.receivedAtMs() < current.receivedAtMs()) firstReceipt.put(key, receipt);
        }
        Map<String, RunData.Row> deliveredRow = new HashMap<>();
        for (RunData.Row row : data.rows) {
            String key = Reconciler.key(row.saleId(), row.sku(), row.subscriberId());
            if (owed.containsKey(key) && "DELIVERED".equals(row.state()) && row.deliveredAtMs() != null) {
                deliveredRow.putIfAbsent(key, row);
            }
        }

        String faults = runText(data, "faults", "none");
        String faultSubscriber = runText(data, "faultSubscriber", null);
        if (faultSubscriber == null && faults.contains(":")) faultSubscriber = faults.substring(faults.indexOf(':') + 1);

        List<Long> endToEnd = new ArrayList<>();
        List<Long> excluding = new ArrayList<>();
        long deliveredInWindow = 0;
        long receivedInWindow = 0;
        long allReceivedAt = Long.MIN_VALUE;
        for (Map.Entry<String, RunData.Receipt> entry : firstReceipt.entrySet()) {
            RunData.Receipt receipt = entry.getValue();
            long latency = receipt.receivedAtMs() - receipt.completedAtMs();
            endToEnd.add(latency);
            if (faultSubscriber != null && !faultSubscriber.equals(receipt.subscriberId())) excluding.add(latency);
            if (inWindow(receipt.receivedAtMs(), firstCompletion, lastCompletion)) receivedInWindow++;
            allReceivedAt = Math.max(allReceivedAt, receipt.receivedAtMs());
        }
        List<Long> toDelivered = new ArrayList<>();
        for (RunData.Row row : deliveredRow.values()) {
            toDelivered.add(row.deliveredAtMs() - row.completedAtMs());
            if (inWindow(row.deliveredAtMs(), firstCompletion, lastCompletion)) deliveredInWindow++;
        }

        long charged = 0;
        long uncharged = 0;
        long parked = 0;
        long operatorRetried = 0;
        for (RunData.Row row : data.rows) {
            charged += row.chargedFailures();
            uncharged += row.unchargedFailures();
            operatorRetried += row.operatorRetries();
            if ("PARKED".equals(row.state())) parked++;
        }

        long missing = owed.size() - firstReceipt.size();
        Double drainSeconds;
        if (owed.isEmpty() || firstReceipt.isEmpty()) {
            drainSeconds = owed.isEmpty() ? 0.0 : null;
        } else {
            drainSeconds = Math.max(0, allReceivedAt - lastCompletion) / 1000.0;
        }
        boolean deadlineMet = missing == 0 && drainSeconds != null && drainSeconds <= DEADLINE_SECONDS;

        Map<String, Object> settings = data.snapshot.settings();
        Map<String, Object> loadClient = data.run == null ? null : Json.getObject(data.run, "loadClient");
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("run", Json.object(
                "id", runText(data, "id", data.dir.getFileName().toString()),
                "startedAt", runText(data, "startedAt", null),
                "deploymentCommit", runText(data, "deploymentCommit", null),
                "subscribers", (long) data.snapshot.subscribers().size(),
                "seed", data.snapshot.seed(),
                "skusPerSubscriber", settings == null ? null : settings.get("skusPerSubscriber"),
                "skuDistribution", settings == null ? null : settings.get("skuDistribution"),
                "loadClient", Json.object(
                        "stations", loadClient == null ? null : loadClient.get("stations"),
                        "durationSeconds", loadClient == null ? null : loadClient.get("durationSeconds")),
                "faults", faults));
        report.put("sales", Json.object(
                "completed", (long) sales.size(),
                "activeWindowSeconds", round(windowSeconds),
                "perSecond", windowSeconds > 0 ? round(sales.size() / windowSeconds) : null,
                "backgroundCompletions", data.run == null ? null : data.run.get("backgroundCompletions")));
        report.put("notifications", Json.object(
                "owed", (long) owed.size(),
                "delivered", (long) deliveredRow.size(),
                "received", (long) firstReceipt.size(),
                "redeliveries", result.totals().get("redeliveries"),
                "deliveredPerSecond", windowSeconds > 0 ? round(deliveredInWindow / windowSeconds) : null,
                "receivedPerSecond", windowSeconds > 0 ? round(receivedInWindow / windowSeconds) : null));
        Map<String, Object> latency = new LinkedHashMap<>();
        long[] endToEndSorted = sorted(endToEnd);
        Map<String, Object> endToEndSection = percentiles(endToEndSorted);
        endToEndSection.put("max", endToEndSorted.length == 0 ? null : endToEndSorted[endToEndSorted.length - 1]);
        endToEndSection.put("n", (long) endToEndSorted.length);
        latency.put("endToEnd", endToEndSection);
        latency.put("toDelivered", percentiles(sorted(toDelivered)));
        if (faultSubscriber == null || "none".equals(faults)) {
            latency.put("endToEndExcluding", null);
        } else {
            Map<String, Object> excluded = new LinkedHashMap<>();
            excluded.put("subscriberId", faultSubscriber);
            excluded.putAll(percentiles(sorted(excluding)));
            latency.put("endToEndExcluding", excluded);
        }
        report.put("latencyMs", latency);
        report.put("deliveryFailures", Json.object("chargedFailures", charged, "unchargedFailures", uncharged,
                "parked", parked, "operatorRetried", operatorRetried));
        report.put("backlog", backlog(data.backlog));
        report.put("drain", Json.object(
                "finalCompletionAt", anySale ? Instant.ofEpochMilli(lastCompletion).toString() : null,
                "allReceivedAt", firstReceipt.isEmpty() ? null : Instant.ofEpochMilli(allReceivedAt).toString(),
                "drainSeconds", drainSeconds == null ? null : round(drainSeconds),
                "deadlineSeconds", DEADLINE_SECONDS,
                "deadlineMet", deadlineMet));
        report.put("reconciliation", Json.object("verdict", result.verdict(),
                "counts", new LinkedHashMap<String, Object>(result.counts())));
        return report;
    }

    private static boolean inWindow(long at, long first, long last) {
        return at >= first && at <= last;
    }

    private static String runText(RunData data, String key, String fallback) {
        if (data.run == null || data.run.get(key) == null) return fallback;
        return String.valueOf(data.run.get(key));
    }

    private static long[] sorted(List<Long> values) {
        long[] array = new long[values.size()];
        for (int i = 0; i < array.length; i++) array[i] = values.get(i);
        Arrays.sort(array);
        return array;
    }

    private static Map<String, Object> percentiles(long[] sorted) {
        Map<String, Object> section = new LinkedHashMap<>();
        for (int p : new int[] {50, 95, 99}) section.put("p" + p, sorted.length == 0 ? null : nearestRank(sorted, p));
        return section;
    }

    private static Map<String, Object> backlog(List<Map<String, String>> samples) {
        Map<String, Object> max = new LinkedHashMap<>();
        Map<String, Object> last = new LinkedHashMap<>();
        for (String field : BACKLOG_FIELDS) {
            Long highest = null;
            for (Map<String, String> sample : samples) {
                Long value = value(sample.get(field));
                if (value != null && (highest == null || value > highest)) highest = value;
            }
            max.put(field, highest);
            last.put(field, samples.isEmpty() ? null : value(samples.get(samples.size() - 1).get(field)));
        }
        return Json.object("max", max, "final", last);
    }

    private static Long value(String text) {
        if (text == null || text.isBlank()) return null;
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException notANumber) {
            return null;
        }
    }

    private static double round(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }

    /** Writes the JSON report and its Markdown rendering ({@code .json} replaced by {@code .md}). */
    public static void write(Path jsonFile, Map<String, Object> report) throws IOException {
        RunFiles.writeAtomically(jsonFile, Json.pretty(report));
        String name = jsonFile.getFileName().toString();
        String markdownName = name.endsWith(".json") ? name.substring(0, name.length() - 5) + ".md" : name + ".md";
        RunFiles.writeAtomically(jsonFile.resolveSibling(markdownName), markdown(report));
    }

    @SuppressWarnings("unchecked")
    public static String markdown(Map<String, Object> report) {
        Map<String, Object> run = (Map<String, Object>) report.get("run");
        Map<String, Object> sales = (Map<String, Object>) report.get("sales");
        Map<String, Object> notifications = (Map<String, Object>) report.get("notifications");
        Map<String, Object> latency = (Map<String, Object>) report.get("latencyMs");
        Map<String, Object> failures = (Map<String, Object>) report.get("deliveryFailures");
        Map<String, Object> backlog = (Map<String, Object>) report.get("backlog");
        Map<String, Object> drain = (Map<String, Object>) report.get("drain");
        Map<String, Object> reconciliation = (Map<String, Object>) report.get("reconciliation");
        StringBuilder md = new StringBuilder();
        md.append("# Notification report: ").append(run.get("id")).append("\n\n");
        md.append("Reconciliation: **").append(reconciliation.get("verdict")).append("**. ");
        md.append("Drain deadline (").append(drain.get("deadlineSeconds")).append(" s) met: **")
                .append(drain.get("deadlineMet")).append("**.\n\n");
        md.append("## Run\n\n| Field | Value |\n|---|---|\n");
        for (Map.Entry<String, Object> entry : run.entrySet()) {
            md.append("| ").append(entry.getKey()).append(" | ").append(text(entry.getValue())).append(" |\n");
        }
        md.append("\n## Sales and notifications\n\n| Metric | Value |\n|---|---|\n");
        sales.forEach((k, v) -> md.append("| sales.").append(k).append(" | ").append(text(v)).append(" |\n"));
        notifications.forEach((k, v) -> md.append("| notifications.").append(k).append(" | ").append(text(v)).append(" |\n"));
        failures.forEach((k, v) -> md.append("| deliveryFailures.").append(k).append(" | ").append(text(v)).append(" |\n"));
        md.append("\n## Latency (ms, nearest rank)\n\n| Measure | p50 | p95 | p99 | max | n |\n|---|---|---|---|---|---|\n");
        for (Map.Entry<String, Object> entry : latency.entrySet()) {
            Map<String, Object> section = (Map<String, Object>) entry.getValue();
            if (section == null) continue;
            String label = entry.getKey();
            if (section.containsKey("subscriberId")) label += " " + section.get("subscriberId");
            md.append("| ").append(label).append(" | ").append(text(section.get("p50"))).append(" | ")
                    .append(text(section.get("p95"))).append(" | ").append(text(section.get("p99"))).append(" | ")
                    .append(text(section.get("max"))).append(" | ").append(text(section.get("n"))).append(" |\n");
        }
        md.append("\n## Backlog\n\n| Field | max | final |\n|---|---|---|\n");
        Map<String, Object> max = (Map<String, Object>) backlog.get("max");
        Map<String, Object> last = (Map<String, Object>) backlog.get("final");
        for (String field : BACKLOG_FIELDS) {
            md.append("| ").append(field).append(" | ").append(text(max.get(field))).append(" | ")
                    .append(text(last.get(field))).append(" |\n");
        }
        md.append("\n## Drain\n\n| Field | Value |\n|---|---|\n");
        drain.forEach((k, v) -> md.append("| ").append(k).append(" | ").append(text(v)).append(" |\n"));
        md.append("\n## Reconciliation counts\n\n| Category | Count |\n|---|---|\n");
        ((Map<String, Object>) reconciliation.get("counts")).forEach((k, v) ->
                md.append("| ").append(k).append(" | ").append(text(v)).append(" |\n"));
        return md.toString();
    }

    private static String text(Object value) {
        if (value == null) return "-";
        if (value instanceof Map<?, ?> map) return Json.compact(map);
        if (value instanceof Double d) return Json.compact(d);
        return String.valueOf(value);
    }
}
