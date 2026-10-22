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

    /** gRPC and generated {@code contracts.*} types stay in rpc adapters and composition roots (R2). */
    @Test
    void grpcAndGeneratedContractsStayInRpcPackagesAndCompositionRoots() throws Exception {
        Path server = serverRoot();
        List<String> violations = new ArrayList<>();
        int rpcSources = 0;
        for (String owner : List.of("common", "transaction", "inventory", "analytics")) {
            try (Stream<Path> paths = Files.walk(server.resolve(owner + "/src"))) {
                for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                    String body = Files.readString(source);
                    Matcher declared = PACKAGE.matcher(body);
                    if (!declared.find()) continue;
                    String packageName = declared.group(1);
                    boolean rpc = packageName.equals("rpc") || packageName.startsWith("rpc.");
                    if (rpc) rpcSources++;
                    boolean compositionRoot = !owner.equals("common") && source.getFileName().toString().equals(
                            Character.toUpperCase(owner.charAt(0)) + owner.substring(1) + "Main.java");
                    if (!rpc && !compositionRoot && GRPC_OR_CONTRACTS.matcher(body).find()) {
                        violations.add(source + ": gRPC or generated contract type outside an rpc package");
                    }
                }
            }
        }
        assertTrue(rpcSources > 0, "No rpc package sources found");
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /** The shared metrics and rpc packages carry no business logic, and metrics needs no library (R21). */
    @Test
    void commonMetricsAndRpcPackagesAreBusinessFreeAndMetricsIsDependencyFree() throws Exception {
        Path common = serverRoot().resolve("common/src");
        List<String> violations = new ArrayList<>();
        for (String shared : List.of("metrics", "rpc")) {
            Path root = common.resolve(shared);
            assertTrue(Files.isDirectory(root), "Missing common package: " + root);
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
                        if (imported.matches("(transaction|inventory|analytics|api|database)(\\..*)?")) {
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

    /** {@code contracts.jar} holds only generated stubs: no business, api, or database class (R1). */
    @Test
    void contractsJarHoldsOnlyGeneratedContracts() throws Exception {
        Path jar = serverRoot().resolve("out/contracts.jar");
        assertTrue(Files.isRegularFile(jar), "Missing contracts artifact: " + jar);
        List<String> violations = new ArrayList<>();
        int generated = 0;
        try (java.util.jar.JarFile archive = new java.util.jar.JarFile(jar.toFile())) {
            for (String name : archive.stream().map(java.util.zip.ZipEntry::getName).toList()) {
                if (!name.endsWith(".class")) continue;
                if (name.startsWith("contracts/")) generated++;
                else violations.add("contracts.jar contains a non-contract class " + name);
                if (name.matches("^(transaction|inventory|analytics|api|database|rpc|metrics)/.*")) {
                    violations.add("contracts.jar contains business or shared class " + name);
                }
            }
        }
        assertTrue(generated > 0, "contracts.jar has no generated classes");
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    private static final Pattern IMPORT = Pattern.compile("(?m)^import\\s+(?:static\\s+)?([a-zA-Z0-9_.]+)\\s*;");
    private static final Pattern GRPC_OR_CONTRACTS = Pattern.compile(
            "(?m)(^import\\s+(?:static\\s+)?(?:io\\.grpc|contracts)\\.)|\\bio\\.grpc\\.[a-zA-Z]|\\bcontracts\\.(?:inventory|analytics)\\.v\\d");

    private static Path serverRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("server/common")) ? cwd.resolve("server") : cwd;
    }
}