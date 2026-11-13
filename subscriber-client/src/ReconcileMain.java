import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * {@code ReconcileMain --run-dir=DIR --mode=fixed|history [--report-out=FILE]} (FR-026, research N11).
 *
 * <p>Writes {@code DIR/reconciliation.json} ({@code verdict}, {@code mode}, {@code counts} per category,
 * {@code totals}, and {@code samples}, the first 20 failing keys per category), and with {@code --report-out} the
 * notification report as FILE and its Markdown rendering. Exit codes: 0 PASS, 1 FAIL, 2 invalid arguments, an
 * unknown mode, or an unreadable run directory (including a fixed-mode run without {@code snapshot.json} and
 * {@code ready.json}). History mode also reads {@code changes.jsonl}, written by {@code ChangeScenarioMain}.
 */
public final class ReconcileMain {
    private ReconcileMain() { }

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        Path runDir = null;
        String mode = Reconciler.FIXED;
        Path reportOut = null;
        for (String arg : args) {
            if (arg.startsWith("--run-dir=")) runDir = Path.of(arg.substring("--run-dir=".length()));
            else if (arg.startsWith("--mode=")) mode = arg.substring("--mode=".length());
            else if (arg.startsWith("--report-out=")) reportOut = Path.of(arg.substring("--report-out=".length()));
            else return usage("Unknown argument: " + arg);
        }
        if (runDir == null) return usage("--run-dir=DIR is required");
        try {
            RunData data = RunData.load(runDir);
            Reconciler.Result result = Reconciler.reconcile(data, mode);
            RunFiles.writeAtomically(data.dir.resolve("reconciliation.json"), result.toJson());
            if (reportOut != null) NotificationReport.write(reportOut.toAbsolutePath(), NotificationReport.build(data, result));
            print(result);
            return result.passed() ? 0 : 1;
        } catch (IllegalArgumentException e) {
            return usage(e.getMessage());
        } catch (IOException e) {
            System.err.println("ReconcileMain: cannot read the run directory: " + e);
            return 2;
        }
    }

    private static void print(Reconciler.Result result) {
        StringBuilder line = new StringBuilder("reconciliation ").append(result.verdict()).append(" (").append(result.mode())
                .append("): ");
        Map<String, Long> totals = result.totals();
        line.append("completedSales=").append(totals.get("completedSales")).append(" owed=").append(totals.get("owed"))
                .append(" delivered=").append(totals.get("delivered")).append(" received=").append(totals.get("received"))
                .append(" redeliveries=").append(totals.get("redeliveries"));
        System.out.println(line);
        for (String category : Reconciler.CATEGORIES) {
            long count = result.counts().get(category);
            if (count == 0) continue;
            List<String> samples = result.samples().get(category);
            System.out.println("  " + category + "=" + count + " e.g. " + String.join(", ", samples.subList(0, Math.min(5, samples.size()))));
        }
    }

    private static int usage(String problem) {
        System.err.println("ReconcileMain: " + problem);
        System.err.println("Usage: ReconcileMain --run-dir=DIR --mode=fixed|history [--report-out=FILE]");
        return 2;
    }
}
