package architecture;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks each independent composition root and handler injection boundary. */
class CompositionRootTest {
    @Test
    void eachDomainMainConstructsOnlyItsOwnRoutesAndStores() throws Exception {
        for (String domain : List.of("transaction", "inventory", "analytics")) {
            Path main = mainSource(domain);
            String source = Files.readString(main);
            assertTrue(source.contains("HttpServer.create"), main.toString());
            assertTrue(source.contains("createContext("), main.toString());
            assertTrue(source.contains("newVirtualThreadPerTaskExecutor"), main.toString());
            assertTrue(source.contains("new ConnectionPool("), main.toString());
            assertTrue(source.contains("addShutdownHook"), main.toString());
            assertFalse(source.contains(".loadAll("), main.toString());
            assertFalse(source.contains(".findById("), main.toString());
            assertFalse(source.contains(".findLowStock("), main.toString());
            assertFalse(source.contains(".completeAtomically("), main.toString());
            assertFalse(source.contains(".readLatest("), main.toString());
            assertFalse(source.contains(".writeWindow("), main.toString());
            for (String peer : List.of("transaction", "inventory", "analytics")) {
                if (!peer.equals(domain)) {
                    assertFalse(source.matches("(?s).*import\\s+" + peer + "\\..*"),
                            main + " imports peer domain " + peer);
                }
            }
        }
    }

    @Test
    void everyHttpHandlerRetainsOnlyBusinessInterfaces() {
        List<String> violations = new ArrayList<>();
        for (Class<?> handler : List.of(
                api.TransactionHttpHandler.class, api.CatalogHttpHandler.class,
                api.InventoryHttpHandler.class, api.AnalyticsHttpHandler.class)) {
            for (Field field : handler.getDeclaredFields()) {
                if (!isBusinessInterface(field.getType())) {
                    violations.add(handler.getName() + " field " + field.getName()
                            + " is not a business interface: " + field.getType().getName());
                }
            }
            for (Constructor<?> constructor : handler.getDeclaredConstructors()) {
                for (Class<?> parameter : constructor.getParameterTypes()) {
                    if (!isBusinessInterface(parameter)) {
                        violations.add(handler.getName() + " constructor receives " + parameter.getName());
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    @Test
    void analyticsCompositionBuildsTheFacadeAndDelegatesShutdown() throws Exception {
        String source = Files.readString(mainSource("analytics"));
        assertTrue(source.contains("new AnalyticsService("));
        assertFalse(source.contains("new WindowFilter("));
        assertFalse(source.contains("new RankingFilter("));
        assertFalse(source.contains("new PersistenceFilter("));
        assertTrue(source.contains("running.shutdown()"));
        String facade = Files.readString(serverRoot().resolve("analytics/src/analytics/AnalyticsService.java"));
        assertTrue(facade.contains("shutdownTimeoutMillis <= 0"));
    }

    /**
     * Each {@code *Main} wires its own dispatcher, consumers, and stores over its own pool and holds no rpc or gRPC
     * type (006 FR-006, Implementation Rule 4); it reads only its own database settings and starts its metrics and
     * status reporting.
     */
    @Test
    void eachMainWiresItsDispatcherConsumersStoresConfigurationMetricsAndStatus() throws Exception {
        Map<String, List<String>> wiring = Map.of(
                "transaction", List.of("new OutboxDispatcher(", "new JdbcOutboxStore(pool", "new JdbcDeadLetterStore(pool",
                        "CheckoutOutcomeConsumer.create(", "CatalogConsumer.create(", "new ScanEventRelay(",
                        "new KafkaScanEventSink(", "new CheckoutRecoveryWorker("),
                "inventory", List.of("new OutboxDispatcher(", "new JdbcOutboxStore(pool", "new JdbcDeadLetterStore(pool",
                        "CheckoutEventConsumer.create(", "new CatalogPublisher("),
                "analytics", List.of("new JdbcDeadLetterStore(pool", "new JdbcScanIngestStore(pool",
                        "ScanAcceptedConsumer.create("));
        for (String domain : List.of("transaction", "inventory", "analytics")) {
            Path main = mainSource(domain);
            String source = Files.readString(main);
            String prefix = domain.toUpperCase(java.util.Locale.ROOT);
            for (String forbidden : List.of("GrpcServers", "GrpcChannels", "ServerBuilder", "ManagedChannel", "io.grpc", "rpc.")) {
                assertFalse(source.contains(forbidden), main + " holds no rpc or gRPC type: " + forbidden);
            }
            for (String wired : wiring.get(domain)) assertTrue(source.contains(wired), main + " wires " + wired);
            if (domain.equals("analytics")) {
                assertFalse(source.contains("OutboxDispatcher"), main + " produces no event, so it has no dispatcher");
            }
            Matcher configs = Pattern.compile("DatabaseConfig\\.fromEnvironment\\(\\s*\"([A-Z]+)\",\\s*\"([a-z0-9_]+)\","
                    + "\\s*\"([a-z0-9_]+)\"").matcher(source);
            List<String> found = new ArrayList<>();
            while (configs.find()) found.add(configs.group(1) + " " + configs.group(2) + " " + configs.group(3));
            assertEquals(List.of(prefix + " cs6510_" + domain + " cs6510_" + domain + "_svc"), found,
                    main + " reads only its own database settings");
            assertTrue(source.contains("MetricsReporter.start(\"" + domain + "\")"), main + " starts its MetricsReporter");
            String status = Character.toUpperCase(domain.charAt(0)) + domain.substring(1) + "StatusQueries";
            assertTrue(source.contains("StatusReporter.start(\"" + domain + "\""), main + " schedules its status summary");
            assertTrue(source.contains("new " + status + "(pool"), main + " reads its status with its own pool");
            assertTrue(source.contains("status.stop()"), main + " stops its status summary on shutdown");
        }
    }

    /**
     * Each shutdown hook stops HTTP, then the consumers (final {@code commitSync}), then the dispatcher (final
     * mark), then the background workers, then the producers, then the pool
     * (contracts/broker-and-deployment.md "Start, stop, restart", 006 T157). Analytics has no dispatcher or producer.
     */
    @Test
    void eachMainShutsDownInTheContractOrder() throws Exception {
        Map<String, List<List<String>>> stages = Map.of(
                "transaction", List.of(List.of("server.stop("), List.of("outcomes.stop(", "catalogConsumer.stop("),
                        List.of("dispatcher.stop("),
                        List.of("recovery.close()", "relay.close()", "status.stop()", "reporter.stop()"),
                        List.of("checkoutSender.close()", "scanSender.close()"), List.of("pool.close()")),
                "inventory", List.of(List.of("server.stop("), List.of("checkoutConsumer.stop("), List.of("dispatcher.stop("),
                        List.of("status.stop()", "reporter.stop()"), List.of("outcomes.close()", "catalogRecords.close()"),
                        List.of("pool.close()")),
                "analytics", List.of(List.of("server.stop("), List.of("scans.stop("),
                        List.of("running.shutdown()", "status.stop()", "reporter.stop()"), List.of("pool.close()")));
        for (String domain : List.of("transaction", "inventory", "analytics")) {
            Path main = mainSource(domain);
            String hook = shutdownHook(Files.readString(main), main);
            int previousLast = -1;
            String previousStage = "start";
            for (List<String> stage : stages.get(domain)) {
                int first = Integer.MAX_VALUE;
                int last = -1;
                for (String call : stage) {
                    int at = hook.indexOf(call);
                    assertTrue(at >= 0, main + "'s shutdown hook calls " + call);
                    assertEquals(at, hook.lastIndexOf(call), main + "'s shutdown hook calls " + call + " once");
                    first = Math.min(first, at);
                    last = Math.max(last, at);
                }
                assertTrue(first > previousLast, main + " shuts down " + stage + " after " + previousStage);
                previousLast = last;
                previousStage = stage.toString();
            }
        }
    }

    /** The body of the {@code Runtime.addShutdownHook(new Thread(() -> { ... }))} lambda. */
    private static String shutdownHook(String source, Path main) {
        String opening = "addShutdownHook(new Thread(() -> {";
        int start = source.indexOf(opening);
        assertTrue(start >= 0, main + " registers a shutdown hook lambda");
        int end = source.indexOf("}));", start);
        assertTrue(end > start, main + "'s shutdown hook ends with }));");
        return source.substring(start + opening.length(), end);
    }

    private static boolean isBusinessInterface(Class<?> type) {
        Package owner = type.getPackage();
        String packageName = owner == null ? "" : owner.getName();
        return type.isInterface() && (packageName.equals("transaction")
                || packageName.equals("inventory") || packageName.equals("analytics"));
    }

    private static Path mainSource(String domain) {
        String name = Character.toUpperCase(domain.charAt(0)) + domain.substring(1) + "Main.java";
        return serverRoot().resolve(domain + "/src/" + domain + "/" + name);
    }

    private static Path serverRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("server/common")) ? cwd.resolve("server") : cwd;
    }
}