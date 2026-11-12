import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * A small run directory that passes reconciliation, laid out as the harness leaves it
 * ({@code subscribers/}, {@code exports/}, {@code run.json}, {@code backlog.tsv}), for T066 and T067.
 *
 * <p>Three subscribers, fixed at version 3: sub-0001 = {SKU-000001, SKU-000002}, sub-0002 = {SKU-000002},
 * sub-0003 = {SKU-000003}. Three sales: txn-1 at T0 (SKU-000001 x3, SKU-000002 x1), txn-2 at T0 + 1 s
 * (SKU-000003 x2, SKU-000004 x1), and txn-3 at T0 + 2 s (SKU-000004 x5, no recipient). The four owed
 * notifications K1..K4 are delivered and first received 10, 20, 30, and 40 ms after their sale completed;
 * K1 is also received a second time (a redelivery).
 */
final class SyntheticRun {
    static final long T0 = 1_800_000_000_000L;
    static final String K1 = Reconciler.key("txn-1", "SKU-000001", "sub-0001");
    static final String K2 = Reconciler.key("txn-1", "SKU-000002", "sub-0001");
    static final String K3 = Reconciler.key("txn-1", "SKU-000002", "sub-0002");
    static final String K4 = Reconciler.key("txn-2", "SKU-000003", "sub-0003");

    private SyntheticRun() { }

    record Owed(String sale, long completedAt, String sku, String subscriber, int quantity, long latencyMs) {
        String id() { return "n-" + sale + "-" + sku + "-" + subscriber; }
    }

    static final List<Owed> OWED = List.of(
            new Owed("txn-1", T0, "SKU-000001", "sub-0001", 3, 10),
            new Owed("txn-1", T0, "SKU-000002", "sub-0001", 1, 20),
            new Owed("txn-1", T0, "SKU-000002", "sub-0002", 1, 30),
            new Owed("txn-2", T0 + 1_000, "SKU-000003", "sub-0003", 2, 40));

    static Path create(Path dir) throws IOException {
        return create(dir, "none");
    }

    static Path create(Path dir, String faults) throws IOException {
        Path subscribers = Files.createDirectories(dir.resolve("subscribers"));
        Path exports = Files.createDirectories(dir.resolve("exports"));
        List<String> catalog = new ArrayList<>();
        for (int i = 1; i <= 10; i++) catalog.add(String.format("SKU-%06d", i));

        List<Assignment.Subscriber> assigned = List.of(
                new Assignment.Subscriber("sub-0001", List.of("SKU-000001", "SKU-000002")),
                new Assignment.Subscriber("sub-0002", List.of("SKU-000002")),
                new Assignment.Subscriber("sub-0003", List.of("SKU-000003")));
        RunFiles.writeSnapshot(subscribers, 42L, new RunFiles.Settings(3, 2, Assignment.UNIFORM, "sub", RunFiles.SEEDED),
                catalog, assigned);
        Map<String, Long> versions = new LinkedHashMap<>();
        Map<String, String> channels = new LinkedHashMap<>();
        for (int i = 0; i < assigned.size(); i++) {
            versions.put(assigned.get(i).id(), (long) (i + 1));
            channels.put(assigned.get(i).id(), "selfcheckout.notify." + assigned.get(i).id() + ".v1");
        }
        RunFiles.writeReady(subscribers, new RunFiles.Ready(Instant.ofEpochMilli(T0 - 5_000), 3L, versions, channels));

        Path receipts = Files.createDirectories(subscribers.resolve("receipts"));
        Map<String, Long> offsets = new LinkedHashMap<>();
        for (Owed owed : OWED) {
            try (ReceiptWriter writer = new ReceiptWriter(receipts, owed.subscriber())) {
                long offset = offsets.merge(owed.subscriber(), 1L, Long::sum) - 1;
                writer.record(owed.id(), owed.sale(), owed.sku(), owed.quantity(), owed.completedAt(),
                        owed.completedAt() + owed.latencyMs(), offset);
                if (owed.equals(OWED.get(0))) {
                    offset = offsets.merge(owed.subscriber(), 1L, Long::sum) - 1;
                    writer.record(owed.id(), owed.sale(), owed.sku(), owed.quantity(), owed.completedAt(),
                            owed.completedAt() + 500, offset);
                }
                writer.flush();
            }
        }

        write(exports.resolve("sales.tsv"), List.of(
                "transaction_id\tcompleted_at_ms\tsku\tquantity",
                "txn-1\t" + T0 + "\tSKU-000001\t3",
                "txn-1\t" + T0 + "\tSKU-000002\t1",
                "txn-2\t" + (T0 + 1_000) + "\tSKU-000003\t2",
                "txn-2\t" + (T0 + 1_000) + "\tSKU-000004\t1",
                "txn-3\t" + (T0 + 2_000) + "\tSKU-000004\t5"));
        String lines1 = "[{\"sku\":\"SKU-000001\",\"quantity\":3},{\"sku\":\"SKU-000002\",\"quantity\":1}]";
        String lines2 = "[{\"sku\":\"SKU-000003\",\"quantity\":2},{\"sku\":\"SKU-000004\",\"quantity\":1}]";
        String lines3 = "[{\"sku\":\"SKU-000004\",\"quantity\":5}]";
        write(exports.resolve("sale-events.tsv"), List.of(
                "event_id\taggregate_key\tpublished_at_ms\tsale_id\tcompleted_at_ms\tlines",
                "e-txn-1\tsale:txn-1\t" + (T0 + 2) + "\ttxn-1\t" + T0 + "\t" + lines1,
                "e-txn-2\tsale:txn-2\t" + (T0 + 1_002) + "\ttxn-2\t" + (T0 + 1_000) + "\t" + lines2,
                "e-txn-3\tsale:txn-3\t" + (T0 + 2_002) + "\ttxn-3\t" + (T0 + 2_000) + "\t" + lines3));
        write(exports.resolve("selections.tsv"), List.of(
                "sale_id\tsale_event_id\tcompleted_at_ms\tlines\tsubscription_version\trecipient_count\tnotification_count\tselected_at_ms",
                "txn-1\te-txn-1\t" + T0 + "\t" + lines1 + "\t3\t2\t3\t" + (T0 + 4),
                "txn-2\te-txn-2\t" + (T0 + 1_000) + "\t" + lines2 + "\t3\t1\t1\t" + (T0 + 1_004),
                "txn-3\te-txn-3\t" + (T0 + 2_000) + "\t" + lines3 + "\t3\t0\t0\t" + (T0 + 2_004)));
        List<String> rows = new ArrayList<>();
        rows.add("notification_id\tsale_id\tsku\tsubscriber_id\tquantity\tcompleted_at_ms\tstate\tattempt_count"
                + "\tcharged_failures\tuncharged_failures\toperator_retries\tcreated_at_ms\tdelivered_at_ms\tparked_at_ms");
        for (Owed owed : OWED) {
            rows.add(owed.id() + "\t" + owed.sale() + "\t" + owed.sku() + "\t" + owed.subscriber() + "\t" + owed.quantity()
                    + "\t" + owed.completedAt() + "\tDELIVERED\t1\t0\t0\t0\t" + (owed.completedAt() + 4) + "\t"
                    + (owed.completedAt() + owed.latencyMs() - 5) + "\tNULL");
        }
        write(exports.resolve("notifications.tsv"), rows);
        write(exports.resolve("history.tsv"), List.of(
                "version\tsubscriber_id\tchange_type\tadded_skus\tremoved_skus\tresulting_skus\tcommitted_at_ms",
                "1\tsub-0001\tREGISTER\t[\"SKU-000001\",\"SKU-000002\"]\t[]\t[\"SKU-000001\",\"SKU-000002\"]\t" + (T0 - 9_000),
                "2\tsub-0002\tREGISTER\t[\"SKU-000002\"]\t[]\t[\"SKU-000002\"]\t" + (T0 - 8_000),
                "3\tsub-0003\tREGISTER\t[\"SKU-000003\"]\t[]\t[\"SKU-000003\"]\t" + (T0 - 7_000)));
        write(exports.resolve("parked-events.tsv"), List.of(
                "dead_letter_id\tevent_id\ttopic\tpartition_no\trecord_offset\tevent_type\treason\tpark_count\tfirst_parked_at_ms"));

        write(dir.resolve("backlog.tsv"), List.of(
                "utcMs\tphase\tsaleEventsUnpublished\tsaleConsumerLag\tnotificationsPending\tchannelLag",
                T0 + "\tload\t2\t5\t7\t3",
                (T0 + 2_000) + "\tload\t9\t1\t4\t11",
                (T0 + 4_000) + "\tdrain\t0\t0\t0\t0"));
        String faultSubscriber = faults.contains(":") ? faults.substring(faults.indexOf(':') + 1) : null;
        Files.writeString(dir.resolve("run.json"), Json.pretty(Json.object(
                "id", "synthetic",
                "startedAt", Instant.ofEpochMilli(T0 - 10_000).toString(),
                "deploymentCommit", "0123456789abcdef",
                "workload", "default",
                "loadClient", Json.object("stations", 10L, "durationSeconds", 60L),
                "faults", faults,
                "faultSubscriber", faultSubscriber,
                "backgroundCompletions", 1L)), StandardCharsets.UTF_8);
        return dir;
    }

    static Path exports(Path dir, String name) { return dir.resolve("exports").resolve(name); }

    static Path receipts(Path dir, String subscriber) {
        return dir.resolve("subscribers").resolve("receipts").resolve(subscriber + ".jsonl");
    }

    static void write(Path file, List<String> lines) throws IOException {
        Files.writeString(file, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
    }

    static void edit(Path file, UnaryOperator<List<String>> change) throws IOException {
        List<String> lines = new ArrayList<>(Files.readAllLines(file, StandardCharsets.UTF_8));
        write(file, change.apply(lines));
    }

    /** Replaces the first line containing {@code marker} by {@code replacement(line)}. */
    static void editLine(Path file, String marker, UnaryOperator<String> replacement) throws IOException {
        edit(file, lines -> {
            for (int i = 0; i < lines.size(); i++) {
                if (lines.get(i).contains(marker)) {
                    lines.set(i, replacement.apply(lines.get(i)));
                    return lines;
                }
            }
            throw new AssertionError("No line contains " + marker + " in " + file);
        });
    }

    static void removeLine(Path file, String marker) throws IOException {
        edit(file, lines -> {
            for (int i = 0; i < lines.size(); i++) {
                if (lines.get(i).contains(marker)) {
                    lines.remove(i);
                    return lines;
                }
            }
            throw new AssertionError("No line contains " + marker + " in " + file);
        });
    }

    /** Replaces the tab-separated field {@code column} (0-based) of the first line containing {@code marker}. */
    static void setField(Path file, String marker, int column, String value) throws IOException {
        editLine(file, marker, line -> {
            String[] fields = line.split("\t", -1);
            fields[column] = value;
            return String.join("\t", fields);
        });
    }
}
