package architecture;

import database.DatabaseConfig;
import org.junit.jupiter.api.Test;
import rpc.FaultHooks;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Gates for the microservices constraints: infrastructure, durability, JDBC flags, ownership (R18-R20, R23). */
class MicroserviceBoundaryTest {
    private static final List<String> SERVICES = List.of("transaction", "inventory", "analytics");
    /** The only business components allowed to hold in-memory state such as maps (Implementation Rule 4). */
    private static final Set<String> DESIGNATED_STATE = Set.of("TransactionStateCache", "TransactionLocks",
            "ProductCatalogReplica", "SettlementRegistry", "RecoverySchedule", "CatalogCache", "ProductNameMap");

    @Test
    void noCacheOrMessagingInfrastructureIsOnTheClasspath() throws Exception {
        Pattern forbidden = Pattern.compile("(?i)redis|jedis|lettuce|kafka|rabbit|activemq");
        List<String> violations = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(serverRoot().resolve("lib"))) {
            paths.filter(path -> path.toString().endsWith(".jar"))
                    .filter(path -> forbidden.matcher(path.getFileName().toString()).find())
                    .forEach(path -> violations.add("Forbidden infrastructure dependency: " + path));
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    @Test
    void noProductionSourceChangesServerDurabilitySettings() throws Exception {
        Pattern forbidden = Pattern.compile("(?i)SET\\s+(GLOBAL|PERSIST)|innodb_flush_log_at_trx_commit|sync_binlog|skip-log-bin");
        List<String> violations = new ArrayList<>();
        for (Path source : productionSources()) {
            if (forbidden.matcher(Files.readString(source)).find()) violations.add(source + " touches a durability setting");
        }
        for (Path script : serviceDbFiles()) {
            if (forbidden.matcher(Files.readString(script)).find()) violations.add(script + " touches a durability setting");
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    @Test
    void databaseConfigCarriesEveryR20ConnectorFlag() throws Exception {
        String source = Files.readString(serverRoot().resolve("common/src/database/DatabaseConfig.java"));
        List<String> flags = List.of("useLocalSessionState=true", "useLocalTransactionState=true", "cachePrepStmts=true",
                "prepStmtCacheSize=256", "prepStmtCacheSqlLimit=2048", "rewriteBatchedStatements=true");
        for (String flag : flags) assertTrue(source.contains(flag), "DatabaseConfig.java lacks " + flag);

        DatabaseConfig defaults = DatabaseConfig.fromEnvironment(Map.of("TRANSACTION_DB_PASSWORD", "secret"),
                "TRANSACTION", "cs6510_transaction", "cs6510_transaction_svc", 32, 64);
        for (String flag : flags) assertTrue(defaults.jdbcUrl().contains(flag), defaults.jdbcUrl());
        assertTrue(defaults.jdbcUrl().startsWith("jdbc:mysql://127.0.0.1:3307/cs6510_transaction?"));
        assertFalse(defaults.jdbcUrl().contains("useServerPrepStmts"), "server prepared statements stay a CP3 switch");
        assertEquals("cs6510_transaction_svc", defaults.user());
        assertEquals(32, defaults.initialPoolSize());
        assertEquals(64, defaults.maxPoolSize());
        assertFalse(defaults.toString().contains("secret"), "the password never prints");

        DatabaseConfig tuned = DatabaseConfig.fromEnvironment(Map.of("DB_SERVER_PREPARED_STATEMENTS", "true",
                "DB_HOST", "db.local", "DB_PORT", "3310", "INVENTORY_DB_NAME", "inv", "INVENTORY_DB_USER", "u",
                "INVENTORY_DB_POOL_SIZE", "5", "INVENTORY_DB_POOL_MAX_SIZE", "7"),
                "INVENTORY", "cs6510_inventory", "cs6510_inventory_svc", 16, 48);
        assertTrue(tuned.jdbcUrl().startsWith("jdbc:mysql://db.local:3310/inv?"));
        assertTrue(tuned.jdbcUrl().endsWith("&useServerPrepStmts=true"));
        assertEquals("u", tuned.user());
        assertEquals(5, tuned.initialPoolSize());
        assertEquals(7, tuned.maxPoolSize());

        DatabaseConfig ignoresShared = DatabaseConfig.fromEnvironment(Map.of("DB_NAME", "shared", "DB_USER", "root",
                "DB_PASSWORD", "shared-secret"), "ANALYTICS", "cs6510_analytics", "cs6510_analytics_svc", 10, 10);
        assertEquals("cs6510_analytics", ignoresShared.name(), "the former shared DB_NAME is never read");
        assertEquals("cs6510_analytics_svc", ignoresShared.user(), "the former shared DB_USER is never read");
        assertEquals("", ignoresShared.password(), "the former shared DB_PASSWORD is never read");
    }

    @Test
    void noServiceSourceOrScriptNamesAnotherServicesSchema() throws Exception {
        List<String> violations = new ArrayList<>();
        Path server = serverRoot();
        for (String owner : SERVICES) {
            List<Path> files = new ArrayList<>();
            try (Stream<Path> paths = Files.walk(server.resolve(owner + "/src"))) {
                paths.filter(path -> path.toString().endsWith(".java")).forEach(files::add);
            }
            Path db = repositoryRoot().resolve("db/" + owner);
            assertTrue(Files.isDirectory(db), "Missing " + db);
            try (Stream<Path> paths = Files.walk(db)) {
                paths.filter(Files::isRegularFile).forEach(files::add);
            }
            for (Path file : files) {
                String body = Files.readString(file);
                for (String other : SERVICES) {
                    if (!other.equals(owner) && body.contains("cs6510_" + other)) {
                        violations.add(file + " names another service's schema cs6510_" + other);
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /**
     * The Scan profiler and the SQL materializer are gone (R21, R12): neither source exists, and nothing
     * that builds, runs, or benchmarks the services names {@code SCAN_PROFILE}. The single-database scripts
     * and the CP0 baseline harnesses were retired by T129; CP0 is reproducible from commit 26c89af.
     */
    @Test
    void retiredProfilerAndMaterializerAreGone() throws Exception {
        List<String> violations = new ArrayList<>();
        Path server = serverRoot();
        for (String retired : List.of("db/init.sql", "db/init-db.ps1", "db/validate-invariants.sql",
                "db/migrate-sql-materializer.sql", "server/benchmark-scan.ps1", "server/benchmark-materializer.ps1")) {
            if (Files.exists(repositoryRoot().resolve(retired))) violations.add(retired + " still exists");
        }
        for (String owner : List.of("common", "transaction", "inventory", "analytics")) {
            try (Stream<Path> paths = Files.walk(server.resolve(owner + "/src"))) {
                paths.map(path -> path.getFileName().toString())
                        .filter(name -> name.equals("ScanProfiler.java") || name.equals("JdbcScanMaterializer.java"))
                        .forEach(name -> violations.add(owner + "/src still contains " + name));
            }
        }
        List<Path> checked = new ArrayList<>(productionSources());
        checked.addAll(serverScripts());
        for (Path file : checked) {
            if (Files.readString(file).contains("SCAN_PROFILE")) violations.add(file + " references SCAN_PROFILE");
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /** No service source or launch script reads the former shared DB_NAME, DB_USER, or DB_PASSWORD (R3). */
    @Test
    void noServiceReadsTheFormerSharedDatabaseVariables() throws Exception {
        Pattern javaLiteral = Pattern.compile("\"DB_(NAME|USER|PASSWORD)\"");
        Pattern shellVariable = Pattern.compile("(?<![A-Za-z0-9_])DB_(NAME|USER|PASSWORD)(?![A-Za-z0-9_])");
        List<String> violations = new ArrayList<>();
        for (Path source : productionSources()) {
            if (javaLiteral.matcher(Files.readString(source)).find()) violations.add(source + " reads a shared DB variable");
        }
        for (String service : SERVICES) {
            Path script = serverRoot().resolve("run-" + service + ".sh");
            assertTrue(Files.isRegularFile(script), "Missing " + script);
            if (shellVariable.matcher(Files.readString(script)).find()) violations.add(script + " passes a shared DB variable");
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    @Test
    void faultHooksAreInertWithoutTheProperty() {
        String previous = System.getProperty(FaultHooks.PROPERTY);
        System.clearProperty(FaultHooks.PROPERTY);
        try {
            for (String hook : List.of("inventory.apply.beforeCommit", "inventory.restore.afterCommit", "relay.afterAck",
                    "analytics.ingest.received", "analytics.ingest.beforeCommit", "analytics.ingest.afterCommit")) {
                assertDoesNotThrow(() -> FaultHooks.hit(hook), hook);
            }
        } finally {
            if (previous != null) System.setProperty(FaultHooks.PROPERTY, previous);
        }
    }

    /**
     * Business services own no mutable maps; only the designated cache components may (Implementation
     * Rule 4). The list only grants permission, so a component that does not exist yet passes, and
     * every class added to the service jars later is checked automatically.
     */
    @Test
    void onlyDesignatedComponentsHoldMapsInBusinessPackages() throws Exception {
        List<String> violations = new ArrayList<>();
        int checked = 0;
        Path server = serverRoot();
        List<URL> jars = new ArrayList<>();
        for (String artifact : List.of("common", "contracts", "transaction", "inventory", "analytics")) {
            Path jar = server.resolve("out/" + artifact + ".jar");
            assertTrue(Files.isRegularFile(jar), "Missing artifact " + jar);
            jars.add(jar.toUri().toURL());
        }
        try (URLClassLoader loader = new URLClassLoader(jars.toArray(URL[]::new), getClass().getClassLoader())) {
            for (String service : SERVICES) {
                try (JarFile archive = new JarFile(server.resolve("out/" + service + ".jar").toFile())) {
                    for (String entry : archive.stream().map(java.util.zip.ZipEntry::getName).toList()) {
                        if (!entry.endsWith(".class") || entry.equals("module-info.class")) continue;
                        String className = entry.substring(0, entry.length() - ".class".length()).replace('/', '.');
                        if (!isBusinessPackage(className)) continue;
                        Class<?> type = Class.forName(className, false, loader);
                        checked++;
                        if (DESIGNATED_STATE.contains(outermostSimpleName(className))) continue;
                        for (Field field : type.getDeclaredFields()) {
                            if (java.util.Map.class.isAssignableFrom(field.getType())) {
                                violations.add(className + "." + field.getName() + " is a "
                                        + (Modifier.isStatic(field.getModifiers()) ? "static " : "")
                                        + field.getType().getName() + " outside the designated state components");
                            }
                        }
                    }
                }
            }
        }
        assertTrue(checked > 0, "No business classes found in the service jars");
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    private static boolean isBusinessPackage(String className) {
        int lastDot = className.lastIndexOf('.');
        if (lastDot < 0) return false;
        String packageName = className.substring(0, lastDot);
        return SERVICES.stream().anyMatch(service -> packageName.equals(service) || packageName.startsWith(service + "."));
    }

    private static String outermostSimpleName(String className) {
        String simple = className.substring(className.lastIndexOf('.') + 1);
        int nested = simple.indexOf('$');
        return nested < 0 ? simple : simple.substring(0, nested);
    }

    private static List<Path> productionSources() throws Exception {
        List<Path> sources = new ArrayList<>();
        for (String owner : List.of("common", "transaction", "inventory", "analytics")) {
            try (Stream<Path> paths = Files.walk(serverRoot().resolve(owner + "/src"))) {
                paths.filter(path -> path.toString().endsWith(".java")).forEach(sources::add);
            }
        }
        assertFalse(sources.isEmpty(), "No production sources found");
        return sources;
    }

    /** The build, run, test, and benchmark scripts in {@code server/}. */
    private static List<Path> serverScripts() throws Exception {
        try (Stream<Path> paths = Files.list(serverRoot())) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".sh") || path.toString().endsWith(".ps1"))
                    .toList();
        }
    }

    /** The per-service schema, seed, and validation scripts plus the bootstrap template. */
    private static List<Path> serviceDbFiles() throws Exception {
        List<Path> files = new ArrayList<>();
        Path db = repositoryRoot().resolve("db");
        for (String owner : SERVICES) {
            try (Stream<Path> paths = Files.walk(db.resolve(owner))) {
                paths.filter(path -> path.toString().toLowerCase(Locale.ROOT).endsWith(".sql")).forEach(files::add);
            }
        }
        files.add(db.resolve("bootstrap.sql"));
        return files;
    }

    private static Path serverRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("server/common")) ? cwd.resolve("server") : cwd;
    }

    private static Path repositoryRoot() {
        return serverRoot().getParent();
    }
}
