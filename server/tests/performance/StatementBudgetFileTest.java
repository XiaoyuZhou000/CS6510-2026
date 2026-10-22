package performance;

import org.junit.jupiter.api.Test;
import support.StatementBudgets;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/** Self-check of the R22 revision rule for {@code statement-budgets.properties}. */
class StatementBudgetFileTest {
    @Test
    void everyCeilingHasADesignValueAndEveryRaisedCeilingHasRecordedEvidence() throws Exception {
        StatementBudgets budgets = StatementBudgets.load();
        String checkpoints = Files.readString(checkpointsFile());
        List<String> problems = new ArrayList<>(problems(budgets, checkpoints));
        assertFalse(budgets.ceilingKeys().isEmpty(), "No statement budgets");
        for (String required : List.of("start.statements", "scan.warm.statements", "scan.warm.selects",
                "scan.warm.commits", "scan.warm.sessionCalls", "scan.cold.extraReads", "complete.t1.statements",
                "complete.t3.statements", "complete.reopen.statements", "inventory.apply.applied.statements",
                "inventory.apply.rejected.statements", "analytics.ingest.statements", "relay.round.statements")) {
            if (!budgets.has(required)) problems.add("Missing budget " + required);
        }
        assertTrue(problems.isEmpty(), String.join(System.lineSeparator(), problems));
    }

    @Test
    void theRuleRejectsMissingDesignsUnrecordedEvidenceAndExceededCeilings() {
        String checkpoints = "| CP3 | `cp3-batch` | default | ... |";
        assertTrue(problems(budgets("a=2", "a.design=2"), checkpoints).isEmpty());
        assertTrue(problems(budgets("a=1", "a.design=2"), checkpoints).isEmpty(), "lowering needs no evidence");
        assertTrue(problems(budgets("a=3", "a.design=2", "a.evidence=cp3-batch"), checkpoints).isEmpty());
        assertFalse(problems(budgets("a=2"), checkpoints).isEmpty(), "a ceiling needs its design value");
        assertFalse(problems(budgets("a=3", "a.design=2"), checkpoints).isEmpty(), "a raise needs evidence");
        assertFalse(problems(budgets("a=3", "a.design=2", "a.evidence=cp3-unknown"), checkpoints).isEmpty(),
                "the evidence label must be recorded in checkpoints.md");
        assertFalse(problems(budgets("a=2", "b.design=2"), checkpoints).isEmpty(), "no orphan design value");

        StatementBudgets within = budgets("a=2", "a.design=2");
        assertDoesNotThrow(() -> within.check("a", 2));
        assertThrows(AssertionError.class, () -> within.check("a", 3));
        assertThrows(AssertionError.class, () -> within.check("unknown", 0));
    }

    /** The R22 revision rule over one budgets file and the text of checkpoints.md. */
    static List<String> problems(StatementBudgets budgets, String checkpoints) {
        List<String> problems = new ArrayList<>();
        for (String key : budgets.allKeys()) {
            String base = key.endsWith(".design") ? key.substring(0, key.length() - ".design".length())
                    : key.endsWith(".evidence") ? key.substring(0, key.length() - ".evidence".length()) : null;
            if (base != null && !budgets.ceilingKeys().contains(base)) problems.add(key + " has no ceiling " + base);
        }
        for (String key : budgets.ceilingKeys()) {
            if (!budgets.has(key + ".design")) {
                problems.add(key + " has no design value " + key + ".design");
                continue;
            }
            long ceiling;
            long design;
            try {
                ceiling = budgets.ceiling(key);
                design = budgets.design(key);
            } catch (AssertionError malformed) {
                problems.add(malformed.getMessage());
                continue;
            }
            if (ceiling > design) {
                String label = budgets.evidence(key);
                if (label == null) {
                    problems.add(key + "=" + ceiling + " exceeds its design value " + design + " without " + key + ".evidence");
                } else if (!checkpoints.contains("`" + label + "`")) {
                    problems.add(key + ".evidence=" + label + " names no A/B record in checkpoints.md");
                }
            }
        }
        return problems;
    }

    private static StatementBudgets budgets(String... entries) {
        Properties properties = new Properties();
        for (String entry : entries) {
            int equals = entry.indexOf('=');
            properties.setProperty(entry.substring(0, equals), entry.substring(equals + 1));
        }
        return StatementBudgets.of(properties);
    }

    private static Path checkpointsFile() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        String relative = "load-client/reports/microservices-architecture/checkpoints.md";
        for (Path candidate : List.of(cwd.resolve(relative), cwd.resolve("..").resolve(relative).normalize())) {
            if (Files.isRegularFile(candidate)) return candidate;
        }
        throw new IllegalStateException(relative + " not found from " + cwd);
    }
}
