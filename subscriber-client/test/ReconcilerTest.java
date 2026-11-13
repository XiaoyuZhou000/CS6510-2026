import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** T066: the reconciler passes a clean run and counts redeliveries without failing (FR-024-FR-026, research N11). */
class ReconcilerTest {

    @Test
    void aCleanRunPasses(@TempDir Path dir) throws Exception {
        Reconciler.Result result = Reconciler.reconcile(RunData.load(SyntheticRun.create(dir)), Reconciler.FIXED);
        assertEquals("PASS", result.verdict(), result.toJson());
        assertEquals(Reconciler.FIXED, result.mode());
        for (String category : Reconciler.CATEGORIES) {
            assertEquals(0L, result.counts().get(category), category);
            assertTrue(result.samples().get(category).isEmpty(), category);
        }
        Map<String, Long> totals = result.totals();
        assertEquals(3L, totals.get("completedSales"));
        assertEquals(3L, totals.get("saleEvents"));
        assertEquals(3L, totals.get("selections"));
        assertEquals(4L, totals.get("owed"));
        assertEquals(4L, totals.get("delivered"));
        assertEquals(4L, totals.get("received"));
        assertEquals(5L, totals.get("receipts"));
    }

    @Test
    void redeliveriesAreCountedWithoutFailing(@TempDir Path dir) throws Exception {
        Path run = SyntheticRun.create(dir);
        Reconciler.Result result = Reconciler.reconcile(RunData.load(run), Reconciler.FIXED);
        assertEquals(1L, result.totals().get("redeliveries"));
        assertEquals("PASS", result.verdict());

        // One more re-send of K4 after a crash between send and mark (scenario F item 2) still passes.
        try (ReceiptWriter writer = new ReceiptWriter(run.resolve("subscribers/receipts"), "sub-0003")) {
            writer.record("n-txn-2-SKU-000003-sub-0003", "txn-2", "SKU-000003", 2, SyntheticRun.T0 + 1_000,
                    SyntheticRun.T0 + 9_000, 1L);
            writer.flush();
        }
        result = Reconciler.reconcile(RunData.load(run), Reconciler.FIXED);
        assertEquals(2L, result.totals().get("redeliveries"));
        assertEquals("PASS", result.verdict(), result.toJson());
    }

    @Test
    void aRunDirectoryWithoutASubscribersFolderIsReadInPlace(@TempDir Path dir) throws Exception {
        // Quickstart scenario B passes --out=runs/b to SubscriberMain, so the run files are at the top level.
        Path run = SyntheticRun.create(dir.resolve("nested"));
        Path flat = Files.createDirectories(dir.resolve("flat"));
        for (String name : new String[] {"snapshot.json", "ready.json"}) {
            Files.copy(run.resolve("subscribers").resolve(name), flat.resolve(name));
        }
        Files.createDirectories(flat.resolve("receipts"));
        try (var files = Files.list(run.resolve("subscribers/receipts"))) {
            for (Path file : files.toList()) Files.copy(file, flat.resolve("receipts").resolve(file.getFileName()));
        }
        Files.createDirectories(flat.resolve("exports"));
        try (var files = Files.list(run.resolve("exports"))) {
            for (Path file : files.toList()) Files.copy(file, flat.resolve("exports").resolve(file.getFileName()));
        }
        assertEquals("PASS", Reconciler.reconcile(RunData.load(flat), Reconciler.FIXED).verdict());
    }

    @Test
    void aFixedRunAlsoPassesHistoryModeAndAnotherModeIsRejected(@TempDir Path dir) throws Exception {
        // Every selection of a fixed run is at the ready version, which is the history replayed to that version.
        RunData data = RunData.load(SyntheticRun.create(dir));
        Reconciler.Result history = Reconciler.reconcile(data, Reconciler.HISTORY);
        assertEquals("PASS", history.verdict(), history.toJson());
        assertEquals(Reconciler.owed(data).keySet(), Reconciler.owed(data, Reconciler.HISTORY).keySet());
        assertThrows(IllegalArgumentException.class, () -> Reconciler.reconcile(data, "other"));
    }

    @Test
    void reconcileMainWritesTheVerdictAndExitsNonZeroOnFailure(@TempDir Path dir) throws Exception {
        Path run = SyntheticRun.create(dir);
        assertEquals(0, ReconcileMain.run(new String[] {"--run-dir=" + run, "--mode=fixed"}));
        Map<String, Object> written = Json.parseObject(Files.readString(run.resolve("reconciliation.json")));
        assertEquals("PASS", written.get("verdict"));
        assertEquals("fixed", written.get("mode"));
        assertTrue(written.containsKey("counts"));
        assertTrue(written.containsKey("totals"));
        assertTrue(written.containsKey("samples"));

        SyntheticRun.removeLine(SyntheticRun.exports(run, "notifications.tsv"), "n-txn-2-SKU-000003-sub-0003");
        assertNotEquals(0, ReconcileMain.run(new String[] {"--run-dir=" + run, "--mode=fixed"}));
        written = Json.parseObject(Files.readString(run.resolve("reconciliation.json")));
        assertEquals("FAIL", written.get("verdict"));

        assertNotEquals(0, ReconcileMain.run(new String[] {"--run-dir=" + run, "--mode=history"}), "Unsupported");
        assertNotEquals(0, ReconcileMain.run(new String[] {"--mode=fixed"}), "--run-dir is required");
    }

    @Test
    void anEmptySnapshotOwesNothing(@TempDir Path dir) throws Exception {
        // Scenario A: no subscribers, so only the sale-event check has work to do.
        Path run = SyntheticRun.create(dir);
        Path subscribers = run.resolve("subscribers");
        RunFiles.writeSnapshot(subscribers, 1L, new RunFiles.Settings(0, 0, Assignment.UNIFORM, "sub", RunFiles.FILE),
                java.util.List.of(), java.util.List.of());
        RunFiles.writeReady(subscribers, new RunFiles.Ready(java.time.Instant.EPOCH, 0L, Map.of(), Map.of()));
        try (var files = Files.list(subscribers.resolve("receipts"))) {
            for (Path file : files.toList()) Files.delete(file);
        }
        SyntheticRun.write(SyntheticRun.exports(run, "history.tsv"), java.util.List.of(
                "version\tsubscriber_id\tchange_type\tadded_skus\tremoved_skus\tresulting_skus\tcommitted_at_ms"));
        SyntheticRun.edit(SyntheticRun.exports(run, "notifications.tsv"), lines -> new java.util.ArrayList<>(lines.subList(0, 1)));
        for (String sale : new String[] {"txn-1", "txn-2", "txn-3"}) {
            SyntheticRun.setField(SyntheticRun.exports(run, "selections.tsv"), sale + "\t", 4, "0");
        }
        Reconciler.Result result = Reconciler.reconcile(RunData.load(run), Reconciler.FIXED);
        assertEquals("PASS", result.verdict(), result.toJson());
        assertEquals(0L, result.totals().get("owed"));
    }
}
