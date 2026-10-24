package architecture;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Enforces {@code server/tests/COVERAGE-LEDGER.md} (FR-057, R15).
 *
 * <p>With {@code -Dparity.gate=<gate>} it also enforces a story's parity gate: no row owned by that
 * gate or an earlier one may still be {@code PENDING}, and every row owned by an earlier gate must
 * have a {@code Verified at} value. {@code final} requires every row to be non-{@code PENDING} and
 * verified. A {@code MIGRATED} or {@code SUPERSEDED} row must name its replacement once its own gate
 * is enforced.</p>
 */
class CoverageParityLedgerTest {
    static final List<String> GATES = List.of("Foundational", "US2", "US1", "US3", "US5", "US4", "US6", "US7");
    private static final Set<String> STATUSES = Set.of("RETAINED", "ADAPTED", "PENDING", "MIGRATED", "SUPERSEDED");
    private static final Pattern ROW = Pattern.compile("^\\|\\s*`([^`]+)`\\s*\\|(.*)\\|\\s*$");
    private static final Pattern BACKTICKED = Pattern.compile("`([^`]+)`");
    private static final Pattern GATE = Pattern.compile("\\((Foundational|US[1-7])\\)\\s*$");
    private static final Pattern REPLACEMENT = Pattern.compile("`([A-Za-z0-9_/]+)(?:#([A-Za-z0-9_]+))?`");
    private static final Pattern REQUIREMENT = Pattern.compile("\\b(FR|SC)-\\d{3}\\b");
    /** A test annotation, then any further annotations with arguments, modifiers, and the method name. */
    private static final Pattern TEST_METHOD = Pattern.compile(
            "@(?:Test|ParameterizedTest|RepeatedTest)\\b(?:\\s*\\((?:[^()]|\\([^()]*\\))*\\))?"
                    + "(?:\\s*@[A-Za-z0-9_.]+(?:\\s*\\((?:[^()]|\\([^()]*\\))*\\))?)*"
                    + "\\s*(?:(?:public|protected|private|static|final)\\s+)*void\\s+([A-Za-z0-9_]+)\\s*\\(");

    record Row(int line, String file, String method, boolean fileLevel, String status, String gate,
               String replacement, String note, String verifiedAt) {
        String id() { return file + (fileLevel ? "" : "#" + method); }
    }

    @Test
    void ledgerMatchesTheLegacyFilesAndTheirReplacements() throws Exception {
        Path tests = serverRoot().resolve("tests");
        Path legacy = serverRoot().resolve("tests-legacy");
        List<Row> rows = readLedger(tests.resolve("COVERAGE-LEDGER.md"));
        List<String> violations = new ArrayList<>();
        Map<String, List<Row>> byFile = new HashMap<>();
        for (Row row : rows) byFile.computeIfAbsent(row.file(), ignored -> new ArrayList<>()).add(row);

        // Every @Test method in a parked legacy file has a row.
        if (Files.isDirectory(legacy)) {
            try (Stream<Path> paths = Files.walk(legacy)) {
                for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                    String file = relativeName(legacy, source);
                    List<Row> fileRows = byFile.getOrDefault(file, List.of());
                    if (fileRows.isEmpty()) {
                        violations.add("tests-legacy/" + file + ".java has no ledger row");
                        continue;
                    }
                    for (String method : testMethods(source)) {
                        if (fileRows.stream().noneMatch(row -> !row.fileLevel() && row.method().equals(method))) {
                            violations.add("tests-legacy/" + file + "#" + method + " has no ledger row");
                        }
                    }
                }
            }
        }

        for (Map.Entry<String, List<Row>> entry : byFile.entrySet()) {
            String file = entry.getKey();
            Path parked = legacy.resolve(file + ".java");
            Path original = tests.resolve(file + ".java");
            boolean settled = entry.getValue().stream().allMatch(row ->
                    (row.status().equals("MIGRATED") || row.status().equals("SUPERSEDED")) && !row.verifiedAt().isEmpty());
            if (!Files.isRegularFile(parked) && !Files.isRegularFile(original) && !settled) {
                violations.add(file + " exists in neither tests/ nor tests-legacy/, but not every row is MIGRATED or "
                        + "SUPERSEDED with a Verified at value");
            }
            // A parked copy deleted by T130 leaves at most a same-path replacement, which is not the legacy file.
            for (Row row : entry.getValue()) {
                if (!Files.isRegularFile(parked) && (row.status().equals("MIGRATED") || row.status().equals("SUPERSEDED"))
                        && row.verifiedAt().isEmpty()) {
                    violations.add(row.id() + " is " + row.status() + " without a Verified at value, but tests-legacy/"
                            + file + ".java is gone");
                }
            }
            Path legacyCopy = Files.isRegularFile(parked) ? parked : settled ? null : original;
            Set<String> legacyMethods = legacyCopy != null && Files.isRegularFile(legacyCopy) ? testMethods(legacyCopy) : null;
            Set<String> originalMethods = Files.isRegularFile(original) ? testMethods(original) : Set.of();
            for (Row row : entry.getValue()) {
                if (row.fileLevel()) {
                    if ((row.status().equals("RETAINED") || row.status().equals("ADAPTED")) && !Files.isRegularFile(original)) {
                        violations.add(row.id() + " is " + row.status() + " but tests/" + file + ".java is missing");
                    }
                } else {
                    if (legacyMethods != null && !legacyMethods.contains(row.method())) {
                        violations.add(row.id() + " (line " + row.line() + ") names a method its legacy file "
                                + legacyCopy + " no longer contains");
                    }
                    if ((row.status().equals("RETAINED") || row.status().equals("ADAPTED"))
                            && !originalMethods.contains(row.method())) {
                        violations.add(row.id() + " is " + row.status() + " but the method is missing from tests/" + file + ".java");
                    }
                }
                if (row.status().equals("MIGRATED") || row.status().equals("SUPERSEDED")) {
                    for (String missing : missingReplacements(tests, row.replacement())) {
                        violations.add(row.id() + " names replacement " + missing + " that does not exist under server/tests/");
                    }
                    if (row.status().equals("SUPERSEDED") && !REQUIREMENT.matcher(row.note()).find()) {
                        violations.add(row.id() + " is SUPERSEDED but cites no requirement");
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    @Test
    void parityGateHasNoPendingOrUnverifiedRows() throws Exception {
        String gate = System.getProperty("parity.gate", "").trim();
        org.junit.jupiter.api.Assumptions.assumeFalse(gate.isEmpty(), "No parity gate requested (PARITY_GATE unset)");
        List<Row> rows = readLedger(serverRoot().resolve("tests/COVERAGE-LEDGER.md"));
        assertTrue(gate.equals("final") || GATES.contains(gate), "Unknown parity gate: " + gate + "; use one of "
                + GATES + " or final");
        int enforced = gate.equals("final") ? GATES.size() - 1 : GATES.indexOf(gate);
        List<String> violations = new ArrayList<>();
        for (Row row : rows) {
            int owner = GATES.indexOf(row.gate());
            boolean due = gate.equals("final") || owner <= enforced;
            if (due && row.status().equals("PENDING")) {
                violations.add(row.id() + " (" + row.gate() + ") is still PENDING at gate " + gate);
            }
            boolean earlier = gate.equals("final") || owner < enforced;
            if (earlier && row.verifiedAt().isEmpty()) {
                violations.add(row.id() + " (" + row.gate() + ") has no Verified at value at gate " + gate);
            }
            if (due && (row.status().equals("MIGRATED") || row.status().equals("SUPERSEDED"))
                    && !REPLACEMENT.matcher(row.replacement()).find()) {
                violations.add(row.id() + " is " + row.status() + " but names no replacement at gate " + gate);
            }
        }
        assertTrue(violations.isEmpty(), violations.size() + " parity violation(s):" + System.lineSeparator()
                + String.join(System.lineSeparator(), violations));
    }

    static List<Row> readLedger(Path ledger) throws Exception {
        assertTrue(Files.isRegularFile(ledger), "Missing ledger " + ledger);
        List<Row> rows = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        List<String> lines = Files.readAllLines(ledger);
        for (int i = 0; i < lines.size(); i++) {
            Matcher match = ROW.matcher(lines.get(i));
            if (!match.matches()) continue;
            String[] cells = match.group(2).split("\\|", -1);
            int line = i + 1;
            if (cells.length != 6) {
                problems.add("line " + line + ": expected 6 columns after the file, found " + cells.length);
                continue;
            }
            String file = match.group(1).trim();
            String methodCell = cells[0].trim();
            String status = cells[1].trim();
            String owner = cells[2].trim();
            boolean fileLevel = methodCell.equals("(file)");
            Matcher method = BACKTICKED.matcher(methodCell);
            if (!fileLevel && !method.find()) {
                problems.add("line " + line + ": method cell must hold a backticked method name or (file)");
                continue;
            }
            Matcher gate = GATE.matcher(owner);
            if (!STATUSES.contains(status)) problems.add("line " + line + ": unknown status " + status);
            if (!gate.find()) problems.add("line " + line + ": owner must end with its gate in parentheses: " + owner);
            Row row = new Row(line, file, fileLevel ? "" : method.group(1), fileLevel, status,
                    gate.find(0) ? gate.group(1) : "", cells[3].trim(), cells[4].trim(), cells[5].trim());
            String key = row.id() + "|" + methodCell;
            if (!seen.add(key)) problems.add("line " + line + ": duplicate row " + row.id());
            rows.add(row);
        }
        assertTrue(problems.isEmpty(), String.join(System.lineSeparator(), problems));
        assertFalse(rows.isEmpty(), "The ledger has no rows");
        return rows;
    }

    static Set<String> testMethods(Path source) throws Exception {
        Set<String> methods = new LinkedHashSet<>();
        Matcher match = TEST_METHOD.matcher(Files.readString(source));
        while (match.find()) methods.add(match.group(1));
        return methods;
    }

    private static List<String> missingReplacements(Path tests, String cell) throws Exception {
        List<String> missing = new ArrayList<>();
        Matcher match = REPLACEMENT.matcher(cell);
        while (match.find()) {
            Path file = tests.resolve(match.group(1) + ".java");
            if (!Files.isRegularFile(file)) {
                missing.add(match.group(0));
            } else if (match.group(2) != null && !testMethods(file).contains(match.group(2))) {
                missing.add(match.group(0));
            }
        }
        return missing;
    }

    private static String relativeName(Path root, Path source) {
        String relative = root.relativize(source).toString().replace('\\', '/');
        return relative.substring(0, relative.length() - ".java".length());
    }

    private static Path serverRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("server/common")) ? cwd.resolve("server") : cwd;
    }
}
