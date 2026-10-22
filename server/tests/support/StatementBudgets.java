package support;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Measured statement-budget ceilings from {@code tests/performance/statement-budgets.properties} (R22).
 *
 * <p>{@link #assertWithin} fails when a measured count exceeds its ceiling and prints every check, so
 * a run's output records the actual counts next to their ceilings.</p>
 */
public final class StatementBudgets {
    public static final String FILE = "tests/performance/statement-budgets.properties";
    private static volatile StatementBudgets loaded;

    private final Properties properties;

    private StatementBudgets(Properties properties) {
        this.properties = properties;
    }

    public static StatementBudgets of(Properties properties) {
        return new StatementBudgets(properties);
    }

    /** The budgets file, read relative to the working directory of {@code run-tests.sh}. */
    public static Path file() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        for (Path candidate : List.of(cwd.resolve(FILE), cwd.resolve("server").resolve(FILE))) {
            if (Files.isRegularFile(candidate)) return candidate;
        }
        throw new IllegalStateException(FILE + " not found from " + cwd);
    }

    public static StatementBudgets load() {
        StatementBudgets budgets = loaded;
        if (budgets == null) {
            Properties properties = new Properties();
            try (Reader reader = Files.newBufferedReader(file(), StandardCharsets.UTF_8)) {
                properties.load(reader);
            } catch (IOException unreadable) {
                throw new IllegalStateException("Cannot read " + FILE, unreadable);
            }
            budgets = new StatementBudgets(properties);
            loaded = budgets;
        }
        return budgets;
    }

    /** Fails when {@code actual} exceeds the ceiling of {@code key} in the budgets file. */
    public static void assertWithin(String key, long actual) {
        load().check(key, actual);
    }

    public void check(String key, long actual) {
        long ceiling = ceiling(key);
        String line = String.format("[statement-budget] %s actual=%d ceiling=%d design=%d", key, actual, ceiling, design(key));
        System.out.println(line);
        if (actual > ceiling) throw new AssertionError(line + ": the count exceeds its ceiling");
    }

    public long ceiling(String key) {
        return number(key);
    }

    public long design(String key) {
        return number(key + ".design");
    }

    /** The A/B evidence label recorded for a raised ceiling, or {@code null}. */
    public String evidence(String key) {
        String label = properties.getProperty(key + ".evidence");
        return label == null || label.isBlank() ? null : label.trim();
    }

    /** Every ceiling key: those without a {@code .design} or {@code .evidence} suffix. */
    public Set<String> ceilingKeys() {
        Set<String> keys = new TreeSet<>();
        for (String key : properties.stringPropertyNames()) {
            if (!key.endsWith(".design") && !key.endsWith(".evidence")) keys.add(key);
        }
        return keys;
    }

    public Set<String> allKeys() {
        return new TreeSet<>(properties.stringPropertyNames());
    }

    public boolean has(String key) {
        return properties.getProperty(key) != null;
    }

    private long number(String key) {
        String value = properties.getProperty(key);
        if (value == null) throw new AssertionError("No statement budget " + key + " in " + FILE);
        try {
            long parsed = Long.parseLong(value.trim());
            if (parsed < 0) throw new AssertionError("Statement budget " + key + " is negative");
            return parsed;
        } catch (NumberFormatException malformed) {
            throw new AssertionError("Statement budget " + key + " is not a whole number: " + value);
        }
    }
}
