package integration;

import api.json.Json;
import events.TopicNames;
import org.junit.jupiter.api.Test;
import support.KafkaTestSupport;
import support.LiveSchemas;
import support.ServiceHttp;
import support.ServiceProcesses;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Destructive integration test for the assignment baseline (FR-002, FR-016, FR-052; R13).
 *
 * <p>The test is opt-in because it resets the deployed service schemas. When enabled with
 * {@code -DRUN_RESET_BASELINE_TEST=true}, it runs {@code db/reset-service.ps1} for Inventory, Transaction,
 * and Analytics, each with only that service's own credentials, then starts fresh service processes on
 * unused ports and verifies the catalog they serve and every baseline row. Stop the deployment first: a
 * running service would keep state from before the reset.</p>
 */
public final class ResetBaselineTest {
    private static final List<String> RESET_ORDER = List.of("inventory", "transaction", "analytics");
    /** Variables a reset child must not inherit, so it can use nothing but its own service's credentials. */
    private static final List<String> FOREIGN = List.of("DB_USER", "DB_PASSWORD", "DB_NAME", "MYSQL_PWD",
            "MYSQL_ADMIN_PASSWORD");

    /**
     * {@code db/reset-baseline.ps1} refuses while a service port answers (FR-059), before it touches the broker or
     * any schema. Not destructive: the script gets a Kafka home and a mysql.exe that do not exist, so even a
     * regression that skipped the check would stop at resolving them instead of resetting anything.
     */
    @Test
    void resetBaselineRefusesWhileAServicePortAnswers() throws Exception {
        assumeTrue(System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows"),
            "db/reset-baseline.ps1 runs under Windows PowerShell");
        Path missing = Files.createTempDirectory("reset-refusal-");
        ServerSocket listener = null;
        int answering = -1;
        for (int port : List.of(8081, 8082, 8083)) {
            if (ServiceProcesses.httpStatus(port, "/") != -1) {
                answering = port;
                break;
            }
        }
        try {
            for (int port : List.of(8081, 8082, 8083)) {
                if (answering != -1) break;
                try {
                    listener = new ServerSocket(port, 50, InetAddress.getLoopbackAddress());
                    answering = port;
                } catch (java.io.IOException taken) {
                    // Reserved or bound without answering; try the next port.
                }
            }
            assertNotEquals(-1, answering, "No service port answers and none of 8081-8083 can be bound");
            Path script = ServiceProcesses.serverRoot().getParent().resolve("db/reset-baseline.ps1");
            ProcessBuilder builder = new ProcessBuilder(List.of("powershell", "-NoProfile", "-NonInteractive",
                "-ExecutionPolicy", "Bypass", "-File", script.toString(),
                "-KafkaHome", missing.resolve("kafka").toString(), "-MySqlExe", missing.resolve("mysql.exe").toString()));
            builder.redirectErrorStream(true);
            Process process = builder.start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(process.waitFor(60, TimeUnit.SECONDS), "reset-baseline.ps1 did not finish");
            assertEquals(1, process.exitValue(), output);
            assertTrue(output.contains("reset-baseline refused") && output.contains(Integer.toString(answering)), output);
        } finally {
            if (listener != null) listener.close();
            Files.deleteIfExists(missing);
        }
    }

    /**
     * The whole baseline procedure (D13, FR-059): {@code db/reset-baseline.ps1} resets the broker and all three
     * schemas; afterwards every topic is empty with its partitions, and every schema holds exactly the baseline
     * with no outbox, parked, or offset row. Destructive like the test below, and it restarts the broker, so it
     * also needs {@code KAFKA_TEST_BROKER_CONTROL=1} and {@code KAFKA_HOME}.
     */
    @Test
    void resetBaselineEmptiesEveryTopicAndSchema() throws Exception {
        assumeTrue(Boolean.getBoolean("RUN_RESET_BASELINE_TEST"),
            "Destructive reset test disabled; pass -DRUN_RESET_BASELINE_TEST=true to enable");
        assumeTrue(System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows"),
            "db/reset-baseline.ps1 runs under Windows PowerShell");
        KafkaTestSupport.requireBrokerControl();
        for (int port : List.of(8081, 8082, 8083)) {
            assertEquals(-1, ServiceProcesses.httpStatus(port, "/"),
                "Stop the deployment before resetting the baseline: port " + port + " answers");
        }
        Path script = ServiceProcesses.serverRoot().getParent().resolve("db/reset-baseline.ps1");
        ProcessBuilder builder = new ProcessBuilder(List.of("powershell", "-NoProfile", "-NonInteractive",
            "-ExecutionPolicy", "Bypass", "-File", script.toString()));
        String kafkaHome = System.getProperty("KAFKA_HOME", System.getenv("KAFKA_HOME"));
        if (kafkaHome != null && !kafkaHome.isBlank()) builder.environment().put("KAFKA_HOME", kafkaHome);
        String mysql = System.getProperty("MYSQL_EXE", System.getenv("MYSQL_EXE"));
        if (mysql != null && !mysql.isBlank()) {
            builder.command().add("-MySqlExe");
            builder.command().add(mysql);
        }
        builder.redirectErrorStream(true);
        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(process.waitFor(600, TimeUnit.SECONDS), "reset-baseline.ps1 did not finish");
        assertEquals(0, process.exitValue(), "reset-baseline.ps1 failed:" + System.lineSeparator() + output);
        assertExactDatabaseBaseline(false);
        TopicNames production = TopicNames.unprefixed();
        for (TopicNames.Topic topic : TopicNames.Topic.values()) {
            for (int partition = 0; partition < topic.partitions(); partition++) {
                assertEquals(0, KafkaTestSupport.endOffset(production.name(topic), partition),
                    production.name(topic) + "-" + partition + " is empty after the reset");
            }
        }
    }

    @Test
    void resetProducesExactBaselineForFreshServer() throws Exception {
        assumeTrue(Boolean.getBoolean("RUN_RESET_BASELINE_TEST"),
            "Destructive reset test disabled; pass -DRUN_RESET_BASELINE_TEST=true to enable");
        assumeTrue(System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows"),
            "db/reset-service.ps1 runs under Windows PowerShell");
        Map<String, LiveSchemas.Credentials> credentials = Map.of(
            "transaction", LiveSchemas.credentials("transaction"),
            "inventory", LiveSchemas.credentials("inventory"),
            "analytics", LiveSchemas.credentials("analytics"));
        for (int port : List.of(8081, 8082, 8083)) {
            assertEquals(-1, ServiceProcesses.httpStatus(port, "/"),
                "Stop the deployment before resetting its schemas: port " + port + " answers");
        }

        for (String service : RESET_ORDER) {
            String output = reset(service, credentials.get(service));
            assertTrue(output.contains(service + " schema reset."), output);
        }
        assertExactDatabaseBaseline(false);

        // The catalog reaches Transaction through Inventory's events (006 D9), on topics of this run only.
        KafkaTestSupport.requireBroker();
        TopicNames names = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(names);
        // Started in the benchmark order: Inventory, then Transaction (it loads the new replica), then Analytics.
        try (ServiceProcesses.ServiceProcess inventory = ServiceProcesses.service("inventory")
                 .database(credentials.get("inventory")).httpPort(ServiceProcesses.freePort()).kafka(names).start()) {
            inventory.awaitHttp("/items", 200, Duration.ofSeconds(60));
            ServiceHttp inventoryHttp = new ServiceHttp(inventory.httpPort());
            HttpResponse<String> response = inventoryHttp.get("/items");
            assertEquals(200, response.statusCode());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> items = (List<Map<String, Object>>) Json.parseObject(response.body()).get("items");
            assertEquals(2_000, items.size(), "Catalog must contain exactly 2,000 items");
            Set<String> skus = new HashSet<>();
            for (Map<String, Object> item : items) {
                assertTrue(skus.add(item.get("sku").toString()), "Catalog must not contain duplicate SKUs");
            }
            HttpResponse<String> lowStock = inventoryHttp.get("/inventory/low-stock");
            assertEquals(200, lowStock.statusCode());
            assertEquals(List.of(), Json.parseObject(lowStock.body()).get("alerts"), "No SKU starts at or below 50");

            try (ServiceProcesses.ServiceProcess transaction = ServiceProcesses.service("transaction")
                     .database(credentials.get("transaction")).httpPort(ServiceProcesses.freePort()).kafka(names).start()) {
                transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(60));
                awaitReplicaLoaded(transaction, Duration.ofSeconds(60));
                try (ServiceProcesses.ServiceProcess analytics = ServiceProcesses.service("analytics")
                         .database(credentials.get("analytics")).httpPort(ServiceProcesses.freePort())
                         .kafka(names).start()) {
                    analytics.awaitHttp("/analytics/popular-items", 200, Duration.ofSeconds(60));
                    for (ServiceProcesses.ServiceProcess fresh : List.of(inventory, transaction, analytics)) {
                        assertTrue(fresh.isAlive(), "The freshly started " + fresh.service() + " must remain running");
                        assertTrue(fresh.process().pid() > 0,
                            "The fresh " + fresh.service() + " process must have a process id");
                    }
                    awaitCatalogPublished(Duration.ofSeconds(60));
                    assertExactDatabaseBaseline(true);
                }
            }
        }
    }

    /**
     * Every baseline row, each read with only its owning service's credentials. Right after a reset no outbox,
     * parked, or offset row exists; after startup Inventory's outbox holds only its published catalog (D9),
     * Transaction holds nothing but its replica, and Analytics holds nothing.
     */
    private static void assertExactDatabaseBaseline(boolean afterStartup) throws Exception {
        try (Connection connection = LiveSchemas.inventory(); Statement statement = connection.createStatement()) {
            try (ResultSet rows = statement.executeQuery(
                    "SELECT (SELECT COUNT(*) FROM catalog_item) AS catalog_rows, "
                        + "COUNT(*) AS inventory_rows, MIN(stock_quantity) AS min_stock, "
                        + "MAX(stock_quantity) AS max_stock, SUM(stock_quantity) AS total_stock, "
                        + "MIN(low_stock_threshold) AS min_threshold, MAX(low_stock_threshold) AS max_threshold "
                        + "FROM inventory")) {
                assertTrue(rows.next());
                assertEquals(2_000, rows.getInt("catalog_rows"));
                assertEquals(2_000, rows.getInt("inventory_rows"));
                assertEquals(10_000, rows.getInt("min_stock"));
                assertEquals(10_000, rows.getInt("max_stock"));
                assertEquals(20_000_000L, rows.getLong("total_stock"));
                assertEquals(50, rows.getInt("min_threshold"));
                assertEquals(50, rows.getInt("max_threshold"));
            }
            assertEquals(2_000, count(statement, "SELECT product_count FROM catalog_meta WHERE meta_key = 1"));
            assertEmpty(statement, "checkout_operation");
            assertEmpty(statement, "dead_letter_event");
            if (afterStartup) {
                assertEquals(0, count(statement, "SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL"),
                    "Inventory published its whole catalog");
                assertEquals(0, count(statement, "SELECT COUNT(*) FROM outbox_event WHERE event_type NOT IN "
                    + "('CatalogItemPublished', 'CatalogSnapshotPublished')"), "Inventory's outbox holds only its catalog");
            } else {
                assertEmpty(statement, "outbox_event");
            }
        }
        try (Connection connection = LiveSchemas.transaction(); Statement statement = connection.createStatement()) {
            assertEmpty(statement, "`transaction`");
            assertEmpty(statement, "accepted_scan");
            assertEmpty(statement, "checkout_attempt");
            assertEmpty(statement, "outbox_event");
            assertEmpty(statement, "dead_letter_event");
            if (!afterStartup) assertEmpty(statement, "product_replica");
        }
        try (Connection connection = LiveSchemas.analytics(); Statement statement = connection.createStatement()) {
            assertEmpty(statement, "accepted_scan");
            assertEmpty(statement, "popular_window");
            assertEmpty(statement, "popular_item");
            assertEmpty(statement, "dead_letter_event");
            assertEmpty(statement, "consumer_offset");
            assertEquals(0, count(statement, "SELECT last_position FROM scan_sequence WHERE sequence_key = 1"));
            assertEquals(0, count(statement, "SELECT last_window_end FROM analytics_checkpoint WHERE checkpoint_key = 1"));
        }
    }

    /** Waits until Inventory's dispatcher has published every catalog row it wrote at startup. */
    private static void awaitCatalogPublished(Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            try (Connection connection = LiveSchemas.inventory(); Statement statement = connection.createStatement()) {
                if (count(statement, "SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL") == 0) return;
            }
            if (System.nanoTime() > deadline) fail("Inventory's catalog outbox rows were never all published");
            Thread.sleep(200);
        }
    }

    /** A fresh Transaction replaces its empty replica with Inventory's freshly seeded catalog version. */
    private static void awaitReplicaLoaded(ServiceProcesses.ServiceProcess transaction, Duration timeout) throws Exception {
        String version;
        try (Connection connection = LiveSchemas.inventory(); Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery("SELECT catalog_version FROM catalog_meta WHERE meta_key = 1")) {
            assertTrue(row.next());
            version = row.getString(1);
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            try (Connection connection = LiveSchemas.transaction(); Statement statement = connection.createStatement();
                 ResultSet row = statement.executeQuery("SELECT catalog_version, product_count, "
                     + "(SELECT COUNT(*) FROM product_replica) FROM product_replica_meta WHERE meta_key = 1")) {
                assertTrue(row.next());
                if (version.equals(row.getString(1)) && row.getInt(2) == 2_000 && row.getInt(3) == 2_000) return;
            }
            if (System.nanoTime() > deadline) {
                fail("Transaction did not load catalog version " + version + System.lineSeparator() + transaction.output());
            }
            Thread.sleep(200);
        }
    }

    private static long count(Statement statement, String sql) throws SQLException {
        try (ResultSet rows = statement.executeQuery(sql)) {
            assertTrue(rows.next(), sql);
            return rows.getLong(1);
        }
    }

    private static void assertEmpty(Statement statement, String table) throws Exception {
        assertEquals(0L, count(statement, "SELECT COUNT(*) FROM " + table), table + " must be empty after reset");
    }

    /** Runs {@code db/reset-service.ps1 -Service <service>} with that service's credentials and nothing else. */
    private static String reset(String service, LiveSchemas.Credentials own) throws Exception {
        Path script = ServiceProcesses.serverRoot().getParent().resolve("db/reset-service.ps1");
        assertTrue(Files.isRegularFile(script), script.toString());
        ProcessBuilder builder = new ProcessBuilder(List.of("powershell", "-NoProfile", "-NonInteractive",
            "-ExecutionPolicy", "Bypass", "-File", script.toString(), "-Service", service,
            "-MySqlHost", own.host(), "-Port", Integer.toString(own.port())));
        String mysql = System.getProperty("MYSQL_EXE", System.getenv("MYSQL_EXE"));
        if (mysql != null && !mysql.isBlank()) {
            builder.command().add("-MySqlExe");
            builder.command().add(mysql);
        }
        Map<String, String> environment = builder.environment();
        FOREIGN.forEach(environment::remove);
        for (String other : RESET_ORDER) {
            String prefix = other.toUpperCase(Locale.ROOT);
            for (String suffix : List.of("_DB_NAME", "_DB_USER", "_DB_PASSWORD")) environment.remove(prefix + suffix);
        }
        String prefix = service.toUpperCase(Locale.ROOT);
        environment.put(prefix + "_DB_NAME", own.name());
        environment.put(prefix + "_DB_USER", own.user());
        environment.put(prefix + "_DB_PASSWORD", own.password());
        builder.redirectErrorStream(true);
        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(process.waitFor(120, TimeUnit.SECONDS), "reset-service.ps1 -Service " + service + " did not finish");
        assertEquals(0, process.exitValue(), "reset of " + service + " failed:" + System.lineSeparator() + output);
        return output;
    }
}
