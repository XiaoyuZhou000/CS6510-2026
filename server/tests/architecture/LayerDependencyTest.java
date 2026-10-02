package architecture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exhaustive static gate for the four production layers. */
class LayerDependencyTest {
    private static final Pattern PACKAGE = Pattern.compile("(?m)^package\\s+([a-zA-Z0-9_.]+)\\s*;");
    private static final Pattern IMPORT = Pattern.compile("(?m)^import\\s+(?:static\\s+)?([a-zA-Z0-9_.]+)\\s*;");
    private static final Pattern SQL = Pattern.compile(
            "(?i)\\\"\\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\\b");
    private static final Pattern DATABASE_MEMBER = Pattern.compile(
            "(?m)^\\s*(?:private|protected|public)\\s+(?:final\\s+)?(?:database\\.)?"
                    + "(?:CatalogStore|TransactionStore|InventoryStore|CheckoutCompletionStore|PopularWindowStore|"
                    + "ConnectionPool|Jdbc[A-Za-z0-9_]*)\\b");

    @Test
    void everyProductionSourceHasOneLayerAndRespectsDependencies() throws Exception {
        Path sourceRoot = sourceRoot();
        List<String> violations = new ArrayList<>();
        Map<Layer, Set<Layer>> graph = new EnumMap<>(Layer.class);
        for (Layer layer : Layer.values()) graph.put(layer, EnumSet.noneOf(Layer.class));

        List<Path> sources;
        try (Stream<Path> paths = Files.walk(sourceRoot)) {
            sources = paths.filter(path -> path.toString().endsWith(".java")).sorted().toList();
        }
        assertTrue(!sources.isEmpty(), "No production Java sources found under " + sourceRoot);

        for (Path source : sources) {
            String text = Files.readString(source);
            String relative = unix(sourceRoot.relativize(source));
            Matcher packageMatcher = PACKAGE.matcher(text);
            if (!packageMatcher.find()) {
                violations.add(relative + ": missing declared layer package");
                continue;
            }
            Layer owner = Layer.fromPackage(packageMatcher.group(1));
            if (owner == null) {
                violations.add(relative + ": unassigned package " + packageMatcher.group(1));
                continue;
            }
            if (!relative.startsWith(owner.packageName + "/")) {
                violations.add(relative + ": path/package ownership mismatch for " + owner.packageName);
            }

            Matcher imports = IMPORT.matcher(text);
            while (imports.find()) {
                String imported = imports.group(1);
                Layer target = Layer.fromPackage(imported);
                if (target != null && target != owner) {
                    graph.get(owner).add(target);
                    if (!owner.allows(target, source.getFileName().toString())) {
                        violations.add(relative + ": forbidden import " + imported);
                    }
                }
            }

            boolean compositionRoot = relative.equals("api/Main.java");
            if (owner != Layer.DATABASE) {
                if (text.contains("java.sql")) violations.add(relative + ": java.sql outside database");
                if (SQL.matcher(text).find()) violations.add(relative + ": SQL text outside database");
                if (!compositionRoot && text.contains("ConnectionPool")) {
                    violations.add(relative + ": ConnectionPool outside database");
                }
                if (!compositionRoot && Pattern.compile("\\bJdbc[A-Z][A-Za-z0-9_]*").matcher(text).find()) {
                    violations.add(relative + ": JDBC adapter reference outside database");
                }
            }
            if (owner != Layer.API && (text.contains("com.sun.net.httpserver")
                    || text.contains("HttpExchange") || text.contains("HttpHandler")
                    || Pattern.compile("\\bHTTP_[A-Z_]+\\b").matcher(text).find())) {
                violations.add(relative + ": HTTP type/status below API");
            }
            if (owner == Layer.API && source.getFileName().toString().endsWith("HttpHandler.java")
                    && DATABASE_MEMBER.matcher(text).find()) {
                violations.add(relative + ": handler retains a database collaborator");
            }
            if (owner == Layer.ANALYTICS && text.contains("PopularWindowStore")
                    && !Set.of("AnalyticsService.java", "PersistenceFilter.java")
                    .contains(source.getFileName().toString())) {
                violations.add(relative + ": only the facade and persistence filter may use PopularWindowStore");
            }
        }

        for (Layer layer : Layer.values()) {
            detectCycle(layer, layer, graph, new HashSet<>(), violations);
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    private static void detectCycle(Layer origin, Layer current, Map<Layer, Set<Layer>> graph,
                                    Set<Layer> path, List<String> violations) {
        if (!path.add(current)) return;
        for (Layer next : graph.get(current)) {
            if (next == origin) {
                String message = "layer cycle reaches " + origin.packageName;
                if (!violations.contains(message)) violations.add(message);
            } else {
                detectCycle(origin, next, graph, new HashSet<>(path), violations);
            }
        }
    }

    private static Path sourceRoot() throws IOException {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        for (Path candidate : List.of(cwd.resolve("src"), cwd.resolve("server/src"))) {
            if (Files.isDirectory(candidate)) return candidate.toRealPath();
        }
        throw new IOException("Cannot locate server/src from " + cwd);
    }

    private static String unix(Path path) {
        return path.toString().replace('\\', '/');
    }

    private enum Layer {
        API("api"), TRANSACTION("transaction"), ANALYTICS("analytics"), DATABASE("database");

        private final String packageName;

        Layer(String packageName) {
            this.packageName = packageName;
        }

        static Layer fromPackage(String name) {
            for (Layer layer : values()) {
                if (name.equals(layer.packageName) || name.startsWith(layer.packageName + ".")) return layer;
            }
            return null;
        }

        boolean allows(Layer target, String fileName) {
            return switch (this) {
                case API -> target == TRANSACTION || target == ANALYTICS
                        || (fileName.equals("Main.java") && target == DATABASE);
                case TRANSACTION, ANALYTICS -> target == DATABASE;
                case DATABASE -> false;
            };
        }
    }
}
