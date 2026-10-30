package architecture;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Enforces {@code server/tests/COVERAGE-LEDGER.md} (FR-057, R15; feature 006 FR-065).
 *
 * <p>The ledger has two sections: the feature 005 rows, and the rows under the {@code ## Feature 006} heading, each
 * with its own gate order. With {@code -Dparity.gate=<gate>} it also enforces a story's parity gate on the feature 005
 * section, and with {@code -Dparity.gate=006:<gate>} on the feature 006 section: no row owned by that gate or an
 * earlier one may still be {@code PENDING}, and every row owned by an earlier gate must have a {@code Verified at}
 * value. {@code final} and {@code 006:final} require every row of their section to be non-{@code PENDING} and
 * verified. A {@code MIGRATED} or {@code SUPERSEDED} row must name its replacement once its own gate is enforced.</p>
 */
class CoverageParityLedgerTest {
    /** Gate order of the feature 005 section. */
    static final List<String> GATES = List.of("Foundational", "US2", "US1", "US3", "US5", "US4", "US6", "US7");
    /** Gate order of the feature 006 section, selected with the {@code 006:} prefix. */
    static final List<String> GATES_006 = List.of("Foundational", "US1", "US3", "US4", "US5", "US2", "US6", "US7", "US8");
    static final String PREFIX_006 = "006:";
    private static final Set<String> STATUSES = Set.of("RETAINED", "ADAPTED", "PENDING", "MIGRATED", "SUPERSEDED");
    private static final Pattern ROW = Pattern.compile("^\\|\\s*`([^`]+)`\\s*\\|(.*)\\|\\s*$");
    private static final Pattern SECTION_006 = Pattern.compile("^##\\s+Feature 006\\b.*");
    private static final Pattern BACKTICKED = Pattern.compile("`([^`]+)`");
    private static final Pattern GATE = Pattern.compile("\\((Foundational|US[1-9])\\)\\s*$");
    private static final Pattern REPLACEMENT = Pattern.compile("`([A-Za-z0-9_/]+)(?:#([A-Za-z0-9_]+))?`");
    private static final Pattern PLANNED = Pattern.compile("\\bplanned:");
    private static final Pattern REQUIREMENT = Pattern.compile("\\b(FR|SC)-\\d{3}\\b");
    /** A test annotation, then any further annotations with arguments, modifiers, and the method name. */
    private static final Pattern TEST_METHOD = Pattern.compile(
            "@(?:Test|ParameterizedTest|RepeatedTest)\\b(?:\\s*\\((?:[^()]|\\([^()]*\\))*\\))?"
                    + "(?:\\s*@[A-Za-z0-9_.]+(?:\\s*\\((?:[^()]|\\([^()]*\\))*\\))?)*"
                    + "\\s*(?:(?:public|protected|private|static|final)\\s+)*void\\s+([A-Za-z0-9_]+)\\s*\\(");
    /** Test code that uses gRPC, its generated contracts, the rpc package, or the gRPC-era test doubles (feature 006). */
    private static final Pattern GRPC_REFERENCE = Pattern.compile(
            "(?m)^import\\s+(?:static\\s+)?(?:io\\.grpc|contracts|rpc)\\."
                    + "|^import\\s+(?:static\\s+)?(?:support\\.(?:GrpcTestSupport|FakeInventoryGateway)|transaction\\.InventoryGateway)\\b"
                    + "|\\bio\\.grpc\\.[a-zA-Z]|\\bcontracts\\.(?:inventory|analytics)\\.v\\d"
                    + "|\\b(?:GrpcTestSupport|FakeInventoryGateway|InventoryGateway)\\.[a-zA-Z]");

    record Row(int line, String file, String method, boolean fileLevel, String status, String gate,
               String replacement, String note, String verifiedAt) {
        String id() { return file + (fileLevel ? "" : "#" + method); }

        boolean movedOn() { return status.equals("MIGRATED") || status.equals("SUPERSEDED"); }
    }

    /** The rows of the feature 005 section and of the feature 006 section, in file order. */
    record Ledger(List<Row> feature005, List<Row> feature006) {}

    @Test
    void ledgerMatchesTheLegacyFilesAndTheirReplacements() throws Exception {
        Path tests = serverRoot().resolve("tests");
        Path legacy = serverRoot().resolve("tests-legacy");
        Ledger ledger = readLedger(tests.resolve("COVERAGE-LEDGER.md"));
        List<Row> rows = ledger.feature005();
        List<Row> rows006 = ledger.feature006();
        List<String> violations = new ArrayList<>();
        Map<String, List<Row>> byFile = byFile(rows);

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

        // A file or method that feature 006 moved on (MIGRATED or SUPERSEDED in its section) is exempt where its
        // absence would otherwise break a feature 005 row.
        for (Map.Entry<String, List<Row>> entry : byFile.entrySet()) {
            String file = entry.getKey();
            Path parked = legacy.resolve(file + ".java");
            Path original = tests.resolve(file + ".java");
            boolean settled = entry.getValue().stream().allMatch(row -> row.movedOn() && !row.verifiedAt().isEmpty());
            if (!Files.isRegularFile(parked) && !Files.isRegularFile(original) && !settled
                    && !movedOnBy006(rows006, file, null)) {
                violations.add(file + " exists in neither tests/ nor tests-legacy/, but not every row is MIGRATED or "
                        + "SUPERSEDED with a Verified at value");
            }
            // A parked copy deleted by T130 leaves at most a same-path replacement, which is not the legacy file.
            for (Row row : entry.getValue()) {
                if (!Files.isRegularFile(parked) && row.movedOn() && row.verifiedAt().isEmpty()) {
                    violations.add(row.id() + " is " + row.status() + " without a Verified at value, but tests-legacy/"
                            + file + ".java is gone");
                }
            }
            Path legacyCopy = Files.isRegularFile(parked) ? parked : settled ? null : original;
            Set<String> legacyMethods = legacyCopy != null && Files.isRegularFile(legacyCopy) ? testMethods(legacyCopy) : null;
            Set<String> originalMethods = Files.isRegularFile(original) ? testMethods(original) : Set.of();
            for (Row row : entry.getValue()) {
                if (row.fileLevel()) {
                    if ((row.status().equals("RETAINED") || row.status().equals("ADAPTED")) && !Files.isRegularFile(original)
                            && !movedOnBy006(rows006, file, null)) {
                        violations.add(row.id() + " is " + row.status() + " but tests/" + file + ".java is missing");
                    }
                } else {
                    boolean movedOn = movedOnBy006(rows006, file, row.method());
                    if (legacyMethods != null && !legacyMethods.contains(row.method()) && !movedOn) {
                        violations.add(row.id() + " (line " + row.line() + ") names a method its legacy file "
                                + legacyCopy + " no longer contains");
                    }
                    if ((row.status().equals("RETAINED") || row.status().equals("ADAPTED"))
                            && !originalMethods.contains(row.method()) && !movedOn) {
                        violations.add(row.id() + " is " + row.status() + " but the method is missing from tests/" + file + ".java");
                    }
                }
                if (row.movedOn()) {
                    for (String missing : missingReplacements(tests, row.replacement())) {
                        if (movedOnBy006(rows006, missing)) continue;
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

    /**
     * The feature 006 section (FR-065): its legacy files are those at {@code 8891c57}, and there is no parking
     * directory, so a file may disappear only once every row is moved on; a method may disappear only once a row of
     * it is moved on; every row cites a requirement; and test code that still uses gRPC is recorded here.
     */
    @Test
    void feature006LedgerMatchesTheTestTreeAndItsReplacements() throws Exception {
        Path tests = serverRoot().resolve("tests");
        List<Row> rows = readLedger(tests.resolve("COVERAGE-LEDGER.md")).feature006();
        List<String> violations = new ArrayList<>();
        Map<String, List<Row>> byFile = byFile(rows);
        for (Map.Entry<String, List<Row>> entry : byFile.entrySet()) {
            String file = entry.getKey();
            Path source = tests.resolve(file + ".java");
            boolean present = Files.isRegularFile(source);
            Set<String> current = present ? testMethods(source) : Set.of();
            for (Row row : entry.getValue()) {
                if (!present && !row.movedOn()) {
                    violations.add(row.id() + " (line " + row.line() + ") is " + row.status() + " but tests/" + file
                            + ".java is gone; delete a legacy file only after every row is MIGRATED or SUPERSEDED");
                }
                if (present && !row.fileLevel() && !row.movedOn() && !current.contains(row.method())
                        && !movedOnBy006(rows, file, row.method())) {
                    violations.add(row.id() + " (line " + row.line() + ") is " + row.status()
                            + " but the method is missing from tests/" + file + ".java");
                }
                if (row.movedOn()) {
                    if (!REPLACEMENT.matcher(row.replacement()).find()) {
                        violations.add(row.id() + " (line " + row.line() + ") is " + row.status() + " but names no replacement");
                    }
                    for (String missing : missingReplacements(tests, row.replacement())) {
                        violations.add(row.id() + " names replacement " + missing + " that does not exist under server/tests/");
                    }
                }
                if (!row.status().equals("PENDING") && PLANNED.matcher(row.replacement()).find()) {
                    violations.add(row.id() + " (line " + row.line() + ") is " + row.status()
                            + " but still names a planned: replacement");
                }
                if (!REQUIREMENT.matcher(row.note()).find()) {
                    violations.add(row.id() + " (line " + row.line() + ") cites no requirement");
                }
            }
        }
        try (Stream<Path> paths = Files.walk(tests)) {
            for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                String file = relativeName(tests, source);
                if (!byFile.containsKey(file) && GRPC_REFERENCE.matcher(Files.readString(source)).find()) {
                    violations.add("tests/" + file + ".java uses gRPC test code but has no row in the feature 006 section");
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    @Test
    void parityGateHasNoPendingOrUnverifiedRows() throws Exception {
        String requested = System.getProperty("parity.gate", "").trim();
        org.junit.jupiter.api.Assumptions.assumeFalse(requested.isEmpty(), "No parity gate requested (PARITY_GATE unset)");
        Ledger ledger = readLedger(serverRoot().resolve("tests/COVERAGE-LEDGER.md"));
        boolean feature006 = requested.startsWith(PREFIX_006);
        String gate = feature006 ? requested.substring(PREFIX_006.length()).trim() : requested;
        List<String> gates = feature006 ? GATES_006 : GATES;
        List<Row> rows = feature006 ? ledger.feature006() : ledger.feature005();
        assertTrue(gate.equals("final") || gates.contains(gate), "Unknown parity gate: " + requested + "; use one of "
                + GATES + " or final for feature 005, or " + PREFIX_006 + "<gate> with one of " + GATES_006
                + " or final for feature 006");
        int enforced = gate.equals("final") ? gates.size() - 1 : gates.indexOf(gate);
        List<String> violations = new ArrayList<>();
        for (Row row : rows) {
            int owner = gates.indexOf(row.gate());
            boolean due = gate.equals("final") || owner <= enforced;
            if (due && row.status().equals("PENDING")) {
                violations.add(row.id() + " (" + row.gate() + ") is still PENDING at gate " + requested);
            }
            boolean earlier = gate.equals("final") || owner < enforced;
            if (earlier && row.verifiedAt().isEmpty()) {
                violations.add(row.id() + " (" + row.gate() + ") has no Verified at value at gate " + requested);
            }
            if (due && row.movedOn() && !REPLACEMENT.matcher(row.replacement()).find()) {
                violations.add(row.id() + " is " + row.status() + " but names no replacement at gate " + requested);
            }
        }
        assertTrue(violations.isEmpty(), violations.size() + " parity violation(s):" + System.lineSeparator()
                + String.join(System.lineSeparator(), violations));
    }

    /** Parses both sections; rows after the {@code ## Feature 006} heading belong to the feature 006 section. */
    static Ledger readLedger(Path ledger) throws Exception {
        assertTrue(Files.isRegularFile(ledger), "Missing ledger " + ledger);
        List<Row> rows005 = new ArrayList<>();
        List<Row> rows006 = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        Set<String> seen005 = new LinkedHashSet<>();
        Set<String> seen006 = new LinkedHashSet<>();
        boolean inFeature006 = false;
        List<String> lines = Files.readAllLines(ledger);
        for (int i = 0; i < lines.size(); i++) {
            if (SECTION_006.matcher(lines.get(i)).matches()) {
                if (inFeature006) problems.add("line " + (i + 1) + ": a second feature 006 heading");
                inFeature006 = true;
                continue;
            }
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
            List<String> gates = inFeature006 ? GATES_006 : GATES;
            if (!STATUSES.contains(status)) problems.add("line " + line + ": unknown status " + status);
            if (!gate.find()) {
                problems.add("line " + line + ": owner must end with its gate in parentheses: " + owner);
            } else if (!gates.contains(gate.group(1))) {
                problems.add("line " + line + ": gate " + gate.group(1) + " is not one of the "
                        + (inFeature006 ? "feature 006" : "feature 005") + " gates " + gates);
            }
            Row row = new Row(line, file, fileLevel ? "" : method.group(1), fileLevel, status,
                    gate.find(0) ? gate.group(1) : "", cells[3].trim(), cells[4].trim(), cells[5].trim());
            String key = row.id() + "|" + methodCell;
            if (!(inFeature006 ? seen006 : seen005).add(key)) problems.add("line " + line + ": duplicate row " + row.id());
            (inFeature006 ? rows006 : rows005).add(row);
        }
        assertTrue(problems.isEmpty(), String.join(System.lineSeparator(), problems));
        assertFalse(rows005.isEmpty(), "The ledger has no rows");
        assertFalse(rows006.isEmpty(), "The ledger has no feature 006 rows");
        return new Ledger(rows005, rows006);
    }

    static Set<String> testMethods(Path source) throws Exception {
        Set<String> methods = new LinkedHashSet<>();
        Matcher match = TEST_METHOD.matcher(Files.readString(source));
        while (match.find()) methods.add(match.group(1));
        return methods;
    }

    private static Map<String, List<Row>> byFile(List<Row> rows) {
        Map<String, List<Row>> byFile = new LinkedHashMap<>();
        for (Row row : rows) byFile.computeIfAbsent(row.file(), ignored -> new ArrayList<>()).add(row);
        return byFile;
    }

    /**
     * Whether the feature 006 section moved a legacy file or method on: with a method, some row of that method is
     * {@code MIGRATED} or {@code SUPERSEDED}; without one, the file has rows and all of them are.
     */
    private static boolean movedOnBy006(List<Row> rows006, String file, String method) {
        if (method == null) {
            List<Row> fileRows = rows006.stream().filter(row -> row.file().equals(file)).toList();
            return !fileRows.isEmpty() && fileRows.stream().allMatch(Row::movedOn);
        }
        return rows006.stream().anyMatch(row -> row.file().equals(file) && !row.fileLevel()
                && row.method().equals(method) && row.movedOn());
    }

    /** {@link #movedOnBy006(List, String, String)} for a backticked {@code File#method} replacement reference. */
    private static boolean movedOnBy006(List<Row> rows006, String reference) {
        Matcher match = REPLACEMENT.matcher(reference);
        return match.matches() && movedOnBy006(rows006, match.group(1), match.group(2));
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
