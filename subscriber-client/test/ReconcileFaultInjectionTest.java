import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T066, SC-003, quickstart scenario C: each mismatch injected into a passing run fails the reconciliation with
 * exactly its category, naming the affected key.
 */
class ReconcileFaultInjectionTest {

    interface Injection {
        void apply(Path run) throws Exception;
    }

    private static void assertFailsWithExactly(Path dir, String category, String key, Injection injection) throws Exception {
        Path run = SyntheticRun.create(dir);
        assertEquals("PASS", Reconciler.reconcile(RunData.load(run), Reconciler.FIXED).verdict(), "clean copy");
        injection.apply(run);
        Reconciler.Result result = Reconciler.reconcile(RunData.load(run), Reconciler.FIXED);
        assertEquals("FAIL", result.verdict(), result.toJson());
        List<String> failing = new ArrayList<>();
        for (String c : Reconciler.CATEGORIES) if (result.counts().get(c) > 0) failing.add(c);
        assertEquals(List.of(category), failing, result.toJson());
        assertTrue(result.samples().get(category).contains(key), category + " names " + key + ": " + result.toJson());
    }

    @Test
    void aRemovedDeliveredRowIsMissingNotDelivered(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "missingNotDelivered", SyntheticRun.K4, run ->
                SyntheticRun.removeLine(SyntheticRun.exports(run, "notifications.tsv"), "n-txn-2-SKU-000003-sub-0003"));
    }

    @Test
    void aPendingRowIsMissingNotDelivered(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "missingNotDelivered", SyntheticRun.K3, run -> {
            Path rows = SyntheticRun.exports(run, "notifications.tsv");
            SyntheticRun.setField(rows, "n-txn-1-SKU-000002-sub-0002", 6, "PENDING");
            SyntheticRun.setField(rows, "n-txn-1-SKU-000002-sub-0002", 12, "NULL");
        });
    }

    @Test
    void aRemovedReceiptIsMissingDeliveredButNotReceived(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "missingDeliveredNotReceived", SyntheticRun.K3, run ->
                SyntheticRun.removeLine(SyntheticRun.receipts(run, "sub-0002"), "n-txn-1-SKU-000002-sub-0002"));
    }

    @Test
    void anExtraReceiptIsUnexpected(@TempDir Path dir) throws Exception {
        String key = Reconciler.key("txn-3", "SKU-000004", "sub-0002");
        assertFailsWithExactly(dir, "unexpected", key, run -> {
            try (ReceiptWriter writer = new ReceiptWriter(run.resolve("subscribers/receipts"), "sub-0002")) {
                writer.record("n-extra", "txn-3", "SKU-000004", 5, SyntheticRun.T0 + 2_000, SyntheticRun.T0 + 2_050, 1L);
                writer.flush();
            }
        });
    }

    @Test
    void anUnexpectedRowIsUnexpected(@TempDir Path dir) throws Exception {
        String key = Reconciler.key("txn-3", "SKU-000004", "sub-0003");
        assertFailsWithExactly(dir, "unexpected", key, run ->
                SyntheticRun.edit(SyntheticRun.exports(run, "notifications.tsv"), lines -> {
                    lines.add("n-extra\ttxn-3\tSKU-000004\tsub-0003\t5\t" + (SyntheticRun.T0 + 2_000)
                            + "\tPENDING\t0\t0\t0\t0\t" + (SyntheticRun.T0 + 2_004) + "\tNULL\tNULL");
                    return lines;
                }));
    }

    @Test
    void anUndecodableReceiptIsUnexpected(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "unexpected", "receipt:sub-0003@1", run -> {
            try (ReceiptWriter writer = new ReceiptWriter(run.resolve("subscribers/receipts"), "sub-0003")) {
                writer.recordUndecodable(1L, SyntheticRun.T0 + 3_000, "not an EventEnvelope");
                writer.flush();
            }
        });
    }

    @Test
    void aWrongQuantityIsAQuantityMismatch(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "quantityMismatch", SyntheticRun.K1, run ->
                SyntheticRun.setField(SyntheticRun.exports(run, "notifications.tsv"), "n-txn-1-SKU-000001-sub-0001", 4, "2"));
    }

    @Test
    void aWrongReceivedQuantityIsAQuantityMismatch(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "quantityMismatch", SyntheticRun.K4, run ->
                SyntheticRun.editLine(SyntheticRun.receipts(run, "sub-0003"), "n-txn-2-SKU-000003-sub-0003",
                        line -> line.replace("\"quantity\":2", "\"quantity\":7")));
    }

    @Test
    void aSecondIdentityForOneKeyIsADuplicate(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "duplicate", SyntheticRun.K2, run ->
                SyntheticRun.edit(SyntheticRun.exports(run, "notifications.tsv"), lines -> {
                    String original = lines.stream().filter(l -> l.startsWith("n-txn-1-SKU-000002-sub-0001\t")).findFirst().orElseThrow();
                    lines.add(original.replaceFirst("^n-txn-1-SKU-000002-sub-0001", "n-second-identity"));
                    return lines;
                }));
    }

    @Test
    void aSecondReceivedIdentityForOneKeyIsADuplicate(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "duplicate", SyntheticRun.K3, run -> {
            try (ReceiptWriter writer = new ReceiptWriter(run.resolve("subscribers/receipts"), "sub-0002")) {
                writer.record("n-other-identity", "txn-1", "SKU-000002", 1, SyntheticRun.T0, SyntheticRun.T0 + 70, 1L);
                writer.flush();
            }
        });
    }

    @Test
    void aSelectionWithAnotherVersionIsASelectionMismatch(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "selectionMismatch", "txn-2", run ->
                SyntheticRun.setField(SyntheticRun.exports(run, "selections.tsv"), "txn-2\te-txn-2", 4, "2"));
    }

    @Test
    void aMissingSelectionIsASelectionMismatch(@TempDir Path dir) throws Exception {
        // txn-3 has no recipient, so only its selection row is missing (FR-011).
        assertFailsWithExactly(dir, "selectionMismatch", "txn-3", run ->
                SyntheticRun.removeLine(SyntheticRun.exports(run, "selections.tsv"), "txn-3\te-txn-3"));
    }

    @Test
    void aHistoryRowAboveTheReadyVersionViolatesFixedSubscriptions(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "fixedSubscriptions", "version:4", run ->
                SyntheticRun.edit(SyntheticRun.exports(run, "history.tsv"), lines -> {
                    lines.add("4\tsub-0002\tUPDATE\t[\"SKU-000004\"]\t[]\t[\"SKU-000002\",\"SKU-000004\"]\t" + SyntheticRun.T0);
                    return lines;
                }));
    }

    @Test
    void aHistoryThatDisagreesWithTheSnapshotViolatesFixedSubscriptions(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "fixedSubscriptions", "sub-0003", run ->
                SyntheticRun.setField(SyntheticRun.exports(run, "history.tsv"), "3\tsub-0003", 5, "[\"SKU-000003\",\"SKU-000009\"]"));
    }

    @Test
    void aParkedRowIsParked(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "parked", SyntheticRun.K2, run -> {
            Path rows = SyntheticRun.exports(run, "notifications.tsv");
            SyntheticRun.setField(rows, "n-txn-1-SKU-000002-sub-0001", 6, "PARKED");
            SyntheticRun.setField(rows, "n-txn-1-SKU-000002-sub-0001", 12, "NULL");
            SyntheticRun.setField(rows, "n-txn-1-SKU-000002-sub-0001", 13, String.valueOf(SyntheticRun.T0 + 900));
        });
    }

    @Test
    void aParkedEventIsParked(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "parked", "event:e-bad", run ->
                SyntheticRun.edit(SyntheticRun.exports(run, "parked-events.tsv"), lines -> {
                    lines.add("1\te-bad\tselfcheckout.sale-completed.v1\t3\t17\tSaleCompleted\tINVALID_PAYLOAD\t1\t" + SyntheticRun.T0);
                    return lines;
                }));
    }

    @Test
    void aMissingSaleCompletedRowFailsTheSaleEventCheck(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "saleEvents", "txn-2", run ->
                SyntheticRun.removeLine(SyntheticRun.exports(run, "sale-events.tsv"), "e-txn-2\t"));
    }

    @Test
    void aSecondSaleCompletedRowFailsTheSaleEventCheck(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "saleEvents", "txn-1", run ->
                SyntheticRun.edit(SyntheticRun.exports(run, "sale-events.tsv"), lines -> {
                    lines.add(lines.get(1).replaceFirst("^e-txn-1", "e-txn-1-again"));
                    return lines;
                }));
    }

    @Test
    void saleEventLinesThatDifferFromTheSaleFailTheSaleEventCheck(@TempDir Path dir) throws Exception {
        assertFailsWithExactly(dir, "saleEvents", "txn-3", run ->
                SyntheticRun.setField(SyntheticRun.exports(run, "sale-events.tsv"), "e-txn-3\t", 5,
                        "[{\"sku\":\"SKU-000004\",\"quantity\":4}]"));
    }
}
