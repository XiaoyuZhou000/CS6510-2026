package integration;

import api.json.Json;
import org.junit.jupiter.api.Test;
import support.GrpcTestSupport;
import support.LiveSchemas;
import support.ServiceHttp;
import support.ServiceProcesses;

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
        assertExactDatabaseBaseline();

        int inventoryGrpc = GrpcTestSupport.freePort();
        int analyticsGrpc = GrpcTestSupport.freePort();
        // Started in the benchmark order: Inventory, then Transaction (it loads the new replica), then Analytics.
        try (ServiceProcesses.ServiceProcess inventory = ServiceProcesses.service("inventory")
                 .database(credentials.get("inventory")).httpPort(GrpcTestSupport.freePort())
                 .grpcPort(inventoryGrpc).start()) {
            inventory.awaitHttp("/items", 200, Duration.ofSeconds(60));
            inventory.awaitPort(inventoryGrpc, Duration.ofSeconds(30));
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
                     .database(credentials.get("transaction")).httpPort(GrpcTestSupport.freePort())
                     .grpcTarget("inventory", inventoryGrpc).grpcTarget("analytics", analyticsGrpc).start()) {
                transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(60));
                awaitReplicaLoaded(transaction, Duration.ofSeconds(60));
                try (ServiceProcesses.ServiceProcess analytics = ServiceProcesses.service("analytics")
                         .database(credentials.get("analytics")).httpPort(GrpcTestSupport.freePort())
                         .grpcPort(analyticsGrpc).start()) {
                    analytics.awaitHttp("/analytics/popular-items", 200, Duration.ofSeconds(60));
                    for (ServiceProcesses.ServiceProcess fresh : List.of(inventory, transaction, analytics)) {
                        assertTrue(fresh.isAlive(), "The freshly started " + fresh.service() + " must remain running");
                        assertTrue(fresh.process().pid() > 0,
                            "The fresh " + fresh.service() + " process must have a process id");
                    }
                    assertExactDatabaseBaseline();
                }
            }
        }
    }

    /** Every baseline row, each read with only its owning service's credentials. */
    private static void assertExactDatabaseBaseline() throws Exception {
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
        }
        try (Connection connection = LiveSchemas.transaction(); Statement statement = connection.createStatement()) {
            assertEmpty(statement, "`transaction`");
            assertEmpty(statement, "accepted_scan");
            assertEmpty(statement, "checkout_attempt");
        }
        try (Connection connection = LiveSchemas.analytics(); Statement statement = connection.createStatement()) {
            assertEmpty(statement, "accepted_scan");
            assertEmpty(statement, "popular_window");
            assertEmpty(statement, "popular_item");
            assertEquals(0, count(statement, "SELECT last_position FROM scan_sequence WHERE sequence_key = 1"));
            assertEquals(0, count(statement, "SELECT last_window_end FROM analytics_checkpoint WHERE checkpoint_key = 1"));
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
