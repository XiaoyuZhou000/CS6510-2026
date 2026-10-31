package architecture;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Static gate over every production source in the four build roots. */
class LayerDependencyTest {
    private static final Pattern PACKAGE = Pattern.compile("(?m)^package\\s+([a-zA-Z0-9_.]+)\\s*;");
    private static final Pattern SQL = Pattern.compile(
            "(?i)\"\\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\\b");
    private static final Pattern DATABASE_MEMBER = Pattern.compile(
            "(?m)^\\s*(?:private|protected|public)\\s+(?:final\\s+)?(?:database\\.)?"
                    + "(?:CatalogStore|TransactionStore|InventoryStore|CheckoutCompletionStore|PopularWindowStore|"
                    + "ConnectionPool|Jdbc[A-Za-z0-9_]*)\\b");

    @Test
    void everyProductionSourceHasItsDeclaredOwnerAndKeepsSqlAndHttpAtTheirBoundaries() throws Exception {
        Path server = serverRoot();
        List<String> violations = new ArrayList<>();
        int count = 0;
        for (String owner : List.of("common", "transaction", "inventory", "analytics")) {
            Path root = server.resolve(owner + "/src");
            assertTrue(Files.isDirectory(root), "Missing source root: " + root);
            try (Stream<Path> paths = Files.walk(root)) {
                for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                    count++;
                    String body = Files.readString(source);
                    String relative = root.relativize(source).toString().replace('\\', '/');
                    Matcher declared = PACKAGE.matcher(body);
                    if (!declared.find()) {
                        violations.add(source + ": missing package");
                        continue;
                    }
                    String packageName = declared.group(1);
                    if (!relative.startsWith(packageName.replace('.', '/') + "/")) {
                        violations.add(source + ": package/path mismatch");
                    }
                    boolean database = packageName.equals("database") || packageName.startsWith("database.")
                            || packageName.endsWith(".database") || packageName.contains(".database.");
                    boolean api = packageName.equals("api") || packageName.startsWith("api.");
                    boolean compositionRoot = source.getFileName().toString().equals(
                            Character.toUpperCase(owner.charAt(0)) + owner.substring(1) + "Main.java");
                    if (!database && (body.contains("java.sql") || SQL.matcher(body).find())) {
                        violations.add(source + ": SQL outside database adapter");
                    }
                    if (!api && !compositionRoot && (body.contains("com.sun.net.httpserver")
                            || body.contains("HttpExchange") || body.contains("HttpHandler")
                            || Pattern.compile("\\bHTTP_[A-Z_]+\\b").matcher(body).find())) {
                        violations.add(source + ": HTTP outside API or entry point");
                    }
                    if (api && source.getFileName().toString().endsWith("HttpHandler.java")
                            && DATABASE_MEMBER.matcher(body).find()) {
                        violations.add(source + ": handler retains database collaborator");
                    }
                    if (owner.equals("analytics") && packageName.equals("analytics")
                            && body.contains("PopularWindowStore") && !compositionRoot
                            && !List.of("AnalyticsService.java", "PersistenceFilter.java")
                                    .contains(source.getFileName().toString())) {
                        violations.add(source + ": filter bypasses Analytics facade/persistence boundary");
                    }
                }
            }
        }
        assertTrue(count > 0, "No production Java sources found");
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    @Test
    void domainSourcesDoNotImportPeersAndCommonHasNoBusinessPackage() throws Exception {
        Path server = serverRoot();
        List<String> violations = new ArrayList<>();
        for (String owner : List.of("transaction", "inventory", "analytics")) {
            Path root = server.resolve(owner + "/src");
            try (Stream<Path> paths = Files.walk(root)) {
                for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                    String body = Files.readString(source);
                    for (String peer : List.of("transaction", "inventory", "analytics")) {
                        if (!peer.equals(owner) && (Pattern.compile("(?m)^import\\s+" + peer + "\\.").matcher(body).find()
                                || Pattern.compile("\\b" + peer + "\\.[A-Z]").matcher(body).find())) {
                            violations.add(source + " imports peer domain " + peer);
                        }
                    }
                }
            }
        }
        Path jar = server.resolve("out/common.jar");
        assertTrue(Files.isRegularFile(jar), "Missing common artifact: " + jar);
        try (java.util.jar.JarFile archive = new java.util.jar.JarFile(jar.toFile())) {
            archive.stream().map(java.util.zip.ZipEntry::getName)
                    .filter(name -> name.matches("^(transaction|inventory|analytics)/.*\\.class$"))
                    .forEach(name -> violations.add("common.jar contains business class " + name));
        }
        try (Stream<Path> paths = Files.walk(server.resolve("common/src"))) {
            for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                String body = Files.readString(source);
                Matcher declared = PACKAGE.matcher(body);
                if (!declared.find() || List.of("transaction", "inventory", "analytics")
                        .stream().anyMatch(domain -> declared.group(1).equals(domain)
                                || declared.group(1).startsWith(domain + "."))) {
                    violations.add(source + " contains business package");
                }
                for (String domain : List.of("transaction", "inventory", "analytics")) {
                    if (Pattern.compile("(?m)^import\\s+" + domain + "\\.").matcher(body).find()) {
                        violations.add(source + " imports business package " + domain);
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /**
     * {@code messaging} is the only adapter layer (006 Implementation Rule 4, FR-006): the {@code rpc} layer is
     * gone, no source references gRPC or a generated gRPC contract, and a messaging adapter may depend on its
     * business layer, {@code events}, and the rest of common, but no other layer imports it; only the
     * {@code *Main} composition roots wire messaging adapters to {@code events} ports.
     */
    @Test
    void messagingAdaptersAreWiredOnlyByCompositionRoots() throws Exception {
        Path server = serverRoot();
        List<String> violations = new ArrayList<>();
        for (String owner : List.of("common", "transaction", "inventory", "analytics")) {
            if (Files.exists(server.resolve(owner + "/src/rpc"))) violations.add(owner + "/src/rpc/ still exists");
            try (Stream<Path> paths = Files.walk(server.resolve(owner + "/src"))) {
                for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                    String body = Files.readString(source);
                    Matcher declared = PACKAGE.matcher(body);
                    if (!declared.find()) continue;
                    String packageName = declared.group(1);
                    if (packageName.equals("rpc") || packageName.startsWith("rpc.")) {
                        violations.add(source + ": the rpc adapter layer is removed");
                    }
                    if (GRPC_OR_CONTRACTS.matcher(body).find() || RPC_REFERENCE.matcher(body).find()) {
                        violations.add(source + ": references gRPC, a generated gRPC contract, or the rpc package");
                    }
                    boolean messaging = packageName.equals("messaging") || packageName.startsWith("messaging.");
                    boolean compositionRoot = !owner.equals("common") && source.getFileName().toString().equals(
                            Character.toUpperCase(owner.charAt(0)) + owner.substring(1) + "Main.java");
                    if (!messaging && !compositionRoot && MESSAGING_REFERENCE.matcher(body).find()) {
                        violations.add(source + ": imports the messaging adapter layer outside a composition root");
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /**
     * {@code events} is the shared, Kafka-free event framework: the business, database, and messaging layers may
     * import it, and it imports no messaging adapter, Kafka type, or business package (006 Implementation Rule 4).
     */
    @Test
    void eventsPackageImportsNoMessagingKafkaOrBusinessPackage() throws Exception {
        Path root = serverRoot().resolve("common/src/events");
        List<String> violations = new ArrayList<>();
        if (Files.isDirectory(root)) {
            try (Stream<Path> paths = Files.walk(root)) {
                for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                    Matcher imports = IMPORT.matcher(Files.readString(source));
                    while (imports.find()) {
                        String imported = imports.group(1);
                        if (imported.matches("(messaging|org\\.apache\\.kafka|transaction|inventory|analytics)(\\..*)?")) {
                            violations.add(source + " imports " + imported);
                        }
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /**
     * The shared metrics, events, messaging, faults, and time packages carry no business logic, and metrics needs
     * no library (R21). metrics, events, and messaging must exist; faults and time are checked when they exist.
     * events may import the database ports it is built on, such as {@code database.StoreFailure}. The rpc package
     * is gone (006 T153).
     */
    @Test
    void commonSharedPackagesAreBusinessFreeAndMetricsIsDependencyFree() throws Exception {
        Path common = serverRoot().resolve("common/src");
        List<String> violations = new ArrayList<>();
        for (String shared : List.of("metrics", "events", "messaging", "faults", "time")) {
            Path root = common.resolve(shared);
            if (List.of("metrics", "events", "messaging").contains(shared)) {
                assertTrue(Files.isDirectory(root), "Missing common package: " + root);
            } else if (!Files.isDirectory(root)) {
                continue;
            }
            String forbidden = shared.equals("events")
                    ? "(transaction|inventory|analytics|api)(\\..*)?"
                    : "(transaction|inventory|analytics|api|database)(\\..*)?";
            try (Stream<Path> paths = Files.walk(root)) {
                for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                    String body = Files.readString(source);
                    Matcher declared = PACKAGE.matcher(body);
                    if (!declared.find() || !declared.group(1).equals(shared)) {
                        violations.add(source + ": expected package " + shared);
                    }
                    Matcher imports = IMPORT.matcher(body);
                    while (imports.find()) {
                        String imported = imports.group(1);
                        if (imported.matches(forbidden)) {
                            violations.add(source + " imports " + imported);
                        }
                        if (shared.equals("metrics") && !imported.startsWith("java.") && !imported.startsWith("metrics.")) {
                            violations.add(source + ": metrics must stay dependency-free but imports " + imported);
                        }
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /**
     * {@code contracts.jar} holds only the generated event schemas under {@code selfcheckout/events/} (006 D2): no
     * gRPC stub (the gRPC protos are deleted, 006 T154) and no business, api, or database class (R1).
     */
    @Test
    void contractsJarHoldsOnlyGeneratedContracts() throws Exception {
        Path jar = serverRoot().resolve("out/contracts.jar");
        assertTrue(Files.isRegularFile(jar), "Missing contracts artifact: " + jar);
        List<String> violations = new ArrayList<>();
        int events = 0;
        try (java.util.jar.JarFile archive = new java.util.jar.JarFile(jar.toFile())) {
            for (String name : archive.stream().map(java.util.zip.ZipEntry::getName).toList()) {
                if (!name.endsWith(".class")) continue;
                if (name.startsWith("selfcheckout/events/")) events++;
                else violations.add("contracts.jar contains a non-event class " + name);
                if (name.matches("^(transaction|inventory|analytics|api|database|rpc|metrics|events|messaging|contracts)/.*")) {
                    violations.add("contracts.jar contains business, shared, or gRPC class " + name);
                }
                if (name.contains("Grpc")) violations.add("contracts.jar contains a gRPC stub " + name);
            }
        }
        assertTrue(events > 0, "contracts.jar has no generated event classes");
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    private static final Pattern IMPORT = Pattern.compile("(?m)^import\\s+(?:static\\s+)?([a-zA-Z0-9_.]+)\\s*;");
    private static final Pattern MESSAGING_REFERENCE = Pattern.compile(
            "(?m)(^import\\s+(?:static\\s+)?messaging\\.)|(?<![A-Za-z0-9_.])messaging\\.[A-Z]");
    private static final Pattern GRPC_OR_CONTRACTS = Pattern.compile(
            "(?m)(^import\\s+(?:static\\s+)?(?:io\\.grpc|contracts)\\.)|\\bio\\.grpc\\.[a-zA-Z]|\\bcontracts\\.(?:inventory|analytics)\\.v\\d");
    private static final Pattern RPC_REFERENCE = Pattern.compile(
            "(?m)(^import\\s+(?:static\\s+)?rpc\\.)|(?<![A-Za-z0-9_.])rpc\\.[A-Z]");

    private static Path serverRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("server/common")) ? cwd.resolve("server") : cwd;
    }
}