import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code ReconcileMain --mode=history} (007 T086, FR-027, research N12) on a synthetic change-scenario run. The run is
 * {@link SyntheticRun} without a snapshot, with a longer history and a {@code changes.jsonl}:
 *
 * <pre>
 * v1 sub-0001 REGISTER {1, 2}   v2 sub-0002 REGISTER {2}   v3 sub-0003 REGISTER {3}
 * v4 sub-0002 CANCEL            v5 sub-0003 UPDATE   {3, 4}
 * txn-1 and txn-2 selected at v3 (confirmed before: 3), txn-3 at v4 (confirmed before: 4, no recipient)
 * </pre>
 *
 * Each failing case must make exactly its own category non-zero and name its key.
 */
class HistoryReconcileTest {
    static Path historyRun(Path dir) throws IOException {
        Path run = SyntheticRun.create(dir);
        Files.delete(run.resolve("subscribers/snapshot.json"));
        Files.delete(run.resolve("subscribers/ready.json"));
        SyntheticRun.edit(SyntheticRun.exports(run, "history.tsv"), lines -> {
            lines.add("4\tsub-0002\tCANCEL\t[]\t[\"SKU-000002\"]\t[]\t" + (SyntheticRun.T0 + 1_500));
            lines.add("5\tsub-0003\tUPDATE\t[\"SKU-000004\"]\t[]\t[\"SKU-000003\",\"SKU-000004\"]\t" + (SyntheticRun.T0 + 2_500));
            return lines;
        });
        SyntheticRun.setField(SyntheticRun.exports(run, "selections.tsv"), "txn-3\t", 4, "4");
        SyntheticRun.write(run.resolve("subscribers/changes.jsonl"), List.of(
                change("register", "REGISTER", "sub-0001", 201, 1),
                change("register", "REGISTER", "sub-0002", 201, 2),
                change("register", "REGISTER", "sub-0003", 201, 3),
                sale("txn-1", 3),
                sale("txn-2", 3),
                change("cancel", "CANCEL", "sub-0002", 200, 4),
                sale("txn-3", 4),
                change("add", "UPDATE", "sub-0003", 200, 5),
                Json.compact(Json.object("kind", "change", "step", "rejected", "op", "UPDATE", "subscriberId", "guard-0001",
                        "status", 404L, "error", "SUBSCRIBER_NOT_FOUND", "version", null)),
                Json.compact(Json.object("kind", "change", "step", "failed", "op", "REGISTER", "subscriberId", "fail-0001",
                        "status", 500L, "error", "INTERNAL_ERROR", "version", null)),
                guard("guard-0001"),
                guard("fail-0001")));
        return run;
    }

    @Test
    void aSelectionThatMatchesTheReplayedHistoryPasses(@TempDir Path dir) throws Exception {
        Reconciler.Result result = reconcile(historyRun(dir));
        assertEquals("PASS", result.verdict(), result.toJson());
        assertEquals(Reconciler.HISTORY, result.mode());
        assertEquals(4L, result.totals().get("owed"), "the owed set is the verified selections");
        assertEquals(5L, result.totals().get("highestHistoryVersion"));
        assertEquals(1L, result.totals().get("redeliveries"));
    }

    @Test
    void aSelectionMissingASubscriberFailsAsASelectionMismatch(@TempDir Path dir) throws Exception {
        Path run = historyRun(dir);
        SyntheticRun.removeLine(SyntheticRun.exports(run, "notifications.tsv"), "\ttxn-1\tSKU-000002\tsub-0002\t");
        Files.delete(SyntheticRun.receipts(run, "sub-0002"));
        assertFailsWithExactly(run, Reconciler.SELECTION_MISMATCH, "txn-1");
    }

    @Test
    void aSelectionContainingAnExtraSubscriberFailsAsASelectionMismatch(@TempDir Path dir) throws Exception {
        Path run = historyRun(dir);
        // sub-0003 holds SKU-000004 only from v5, after txn-3's selection at v4.
        SyntheticRun.edit(SyntheticRun.exports(run, "notifications.tsv"), lines -> {
            lines.add("n-extra\ttxn-3\tSKU-000004\tsub-0003\t5\t" + (SyntheticRun.T0 + 2_000) + "\tDELIVERED\t1\t0\t0\t0\t"
                    + (SyntheticRun.T0 + 2_004) + "\t" + (SyntheticRun.T0 + 2_030) + "\tNULL");
            return lines;
        });
        try (ReceiptWriter writer = new ReceiptWriter(run.resolve("subscribers/receipts"), "sub-0003")) {
            writer.record("n-extra", "txn-3", "SKU-000004", 5, SyntheticRun.T0 + 2_000, SyntheticRun.T0 + 2_040, 1);
        }
        assertFailsWithExactly(run, Reconciler.SELECTION_MISMATCH, "txn-3");
    }

    @Test
    void aSaleSelectedBelowTheVersionConfirmedBeforeItsCompleteFailsTheCausalityCheck(@TempDir Path dir) throws Exception {
        Path run = historyRun(dir);
        // The driver recorded txn-3's Complete as sent after v5 was confirmed, but it was selected at v4.
        SyntheticRun.editLine(run.resolve("subscribers/changes.jsonl"), "\"txn-3\"", line -> sale("txn-3", 5));
        assertFailsWithExactly(run, Reconciler.HISTORY_RULE, "causality:txn-3");
    }

    @Test
    void aFailedChangeThatAppearsInTheHistoryFails(@TempDir Path dir) throws Exception {
        Path run = historyRun(dir);
        SyntheticRun.edit(SyntheticRun.exports(run, "history.tsv"), lines -> {
            lines.add("6\tfail-0001\tREGISTER\t[\"SKU-000009\"]\t[]\t[\"SKU-000009\"]\t" + (SyntheticRun.T0 + 3_000));
            return lines;
        });
        assertFailsWithExactly(run, Reconciler.HISTORY_RULE, "guard:fail-0001@6");
    }

    @Test
    void aRejectedRequestThatChangedItsSubscriberFails(@TempDir Path dir) throws Exception {
        Path run = historyRun(dir);
        SyntheticRun.edit(SyntheticRun.exports(run, "history.tsv"), lines -> {
            lines.add("6\tguard-0001\tREGISTER\t[\"SKU-000009\"]\t[]\t[\"SKU-000009\"]\t" + (SyntheticRun.T0 + 3_000));
            return lines;
        });
        assertFailsWithExactly(run, Reconciler.HISTORY_RULE, "guard:guard-0001@6");
    }

    @Test
    void aConfirmedChangeMissingFromTheHistoryFails(@TempDir Path dir) throws Exception {
        Path run = historyRun(dir);
        SyntheticRun.edit(run.resolve("subscribers/changes.jsonl"), lines -> {
            lines.add(change("add", "UPDATE", "sub-0001", 200, 7));
            return lines;
        });
        assertFailsWithExactly(run, Reconciler.HISTORY_RULE, "confirmed:sub-0001@7");
    }

    @Test
    void aGapInTheHistoryFails(@TempDir Path dir) throws Exception {
        Path run = historyRun(dir);
        SyntheticRun.edit(SyntheticRun.exports(run, "history.tsv"), lines -> {
            lines.add("7\tsub-0001\tUPDATE\t[\"SKU-000009\"]\t[]\t[\"SKU-000001\",\"SKU-000002\",\"SKU-000009\"]\t"
                    + (SyntheticRun.T0 + 3_000));
            return lines;
        });
        assertFailsWithExactly(run, Reconciler.HISTORY_RULE, "version:6");
    }

    @Test
    void fixedModeStillRejectsAHistoryRunWithoutASnapshot(@TempDir Path dir) throws Exception {
        Path run = historyRun(dir);
        assertThrows(IOException.class, () -> Reconciler.reconcile(RunData.load(run), Reconciler.FIXED));
    }

    private static Reconciler.Result reconcile(Path run) throws IOException {
        return Reconciler.reconcile(RunData.load(run), Reconciler.HISTORY);
    }

    private static void assertFailsWithExactly(Path run, String category, String key) throws IOException {
        Reconciler.Result result = reconcile(run);
        assertEquals("FAIL", result.verdict(), result.toJson());
        for (Map.Entry<String, Long> count : result.counts().entrySet()) {
            if (count.getKey().equals(category)) assertTrue(count.getValue() > 0, result.toJson());
            else assertEquals(0L, count.getValue(), "only " + category + " fails: " + result.toJson());
        }
        assertTrue(result.samples().get(category).contains(key), key + " in " + result.toJson());
    }

    private static String change(String step, String op, String subscriber, long status, long version) {
        return Json.compact(Json.object("kind", "change", "step", step, "op", op, "subscriberId", subscriber,
                "status", status, "error", null, "version", version));
    }

    private static String sale(String transaction, long confirmedVersion) {
        return Json.compact(Json.object("kind", "sale", "step", "confirmed-before", "transactionId", transaction,
                "confirmedVersion", confirmedVersion));
    }

    private static String guard(String subscriber) {
        return Json.compact(Json.object("kind", "guard", "subscriberId", subscriber, "allowedVersions", List.of()));
    }
}
