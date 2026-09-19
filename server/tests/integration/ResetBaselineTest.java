package integration;

import json.Json;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Destructive integration test for the assignment baseline (FR-002, FR-016).
 *
 * The test is opt-in because db/init.sql drops and recreates the configured database. When
 * enabled, it executes the real reset script, starts a fresh server process, and verifies both
 * the API catalog and every inventory row. Use only with the disposable assignment database.
 */
public final class ResetBaselineTest {
    private static final String DB_URL = System.getProperty("DB_URL",
        "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout"
            + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
    private static final String DB_USER = System.getProperty("DB_USER", "root");
    private static final String DB_PASS = System.getProperty("DB_PASS", "");

    @Test
    void resetProducesExactBaselineForFreshServer() throws Exception {
        assumeTrue(Boolean.getBoolean("RUN_RESET_BASELINE_TEST"),
            "Destructive reset test disabled; pass -DRUN_RESET_BASELINE_TEST=true to enable");

        Path serverDir = Path.of("").toAbsolutePath().normalize();
        Path projectRoot = serverDir.getFileName() != null
                && serverDir.getFileName().toString().equals("server")
            ? serverDir.getParent()
            : serverDir;
        Path initSql = projectRoot.resolve("db").resolve("init.sql");
        Path mainClasses = projectRoot.resolve("server").resolve("out").resolve("main");
        Path libraryDirectory = projectRoot.resolve("server").resolve("lib");

        assertTrue(Files.isRegularFile(initSql), "Missing database reset script: " + initSql);
        assertTrue(Files.isDirectory(mainClasses), "Build the server before running this test");

        executeReset(initSql);

        int port = availablePort();
        Process server = startServer(projectRoot, mainClasses, libraryDirectory, port);
        try {
            URI itemsUri = URI.create("http://127.0.0.1:" + port + "/items");
            HttpResponse<String> response = awaitServer(itemsUri, server);
            assertEquals(200, response.statusCode());

            Map<String, Object> payload = Json.parseObject(response.body());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> items = (List<Map<String, Object>>) payload.get("items");
            assertEquals(2_000, items.size(), "Catalog must contain exactly 2,000 items");

            Set<String> skus = new HashSet<>();
            for (Map<String, Object> item : items) {
                assertTrue(skus.add(item.get("sku").toString()),
                    "Catalog must not contain duplicate SKUs");
            }

            try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                 Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery(
                     "SELECT COUNT(*) AS row_count, MIN(stock_quantity) AS min_stock, "
                         + "MAX(stock_quantity) AS max_stock, SUM(stock_quantity) AS total_stock "
                         + "FROM inventory")) {
                assertTrue(rows.next());
                assertEquals(2_000, rows.getInt("row_count"));
                assertEquals(10_000, rows.getInt("min_stock"));
                assertEquals(10_000, rows.getInt("max_stock"));
                assertEquals(20_000_000L, rows.getLong("total_stock"));
            }
        } finally {
            server.destroy();
            if (!server.waitFor(10, TimeUnit.SECONDS)) {
                server.destroyForcibly();
                server.waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    private static void executeReset(Path initSql) throws Exception {
        String script = Files.readString(initSql);
        String adminUrl = adminUrlWithMultiQueries(DB_URL);
        try (Connection connection = DriverManager.getConnection(adminUrl, DB_USER, DB_PASS);
             Statement statement = connection.createStatement()) {
            statement.execute(script);
            while (statement.getMoreResults() || statement.getUpdateCount() != -1) {
                // Consume every result produced by the multi-statement reset script.
            }
        }
    }

    private static String adminUrlWithMultiQueries(String databaseUrl) {
        int query = databaseUrl.indexOf('?');
        String beforeQuery = query >= 0 ? databaseUrl.substring(0, query) : databaseUrl;
        String parameters = query >= 0 ? databaseUrl.substring(query + 1) : "";
        int lastSlash = beforeQuery.lastIndexOf('/');
        if (lastSlash < "jdbc:mysql://".length()) {
            throw new IllegalArgumentException("Unsupported MySQL JDBC URL: " + databaseUrl);
        }
        String admin = beforeQuery.substring(0, lastSlash + 1);
        return admin + "?" + (parameters.isEmpty() ? "" : parameters + "&")
            + "allowMultiQueries=true";
    }

    private static int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static Process startServer(Path projectRoot, Path mainClasses,
            Path libraryDirectory, int port)
            throws IOException {
        URI database = URI.create(DB_URL.substring("jdbc:".length()));
        String host = database.getHost();
        int dbPort = database.getPort() < 0 ? 3306 : database.getPort();
        String dbName = database.getPath().substring(1);
        String classpath = mainClasses + System.getProperty("path.separator")
            + libraryDirectory + System.getProperty("file.separator") + "*";

        ProcessBuilder builder = new ProcessBuilder(
            javaExecutable(), "-cp", classpath, "Main",
            Integer.toString(port), host, Integer.toString(dbPort), dbName, DB_USER);
        builder.directory(projectRoot.resolve("server").toFile());
        builder.environment().put("DB_PASSWORD", DB_PASS);
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        return builder.start();
    }

    private static String javaExecutable() {
        String executable = System.getProperty("os.name").toLowerCase().contains("win")
            ? "java.exe" : "java";
        return Path.of(System.getProperty("java.home"), "bin", executable).toString();
    }

    private static HttpResponse<String> awaitServer(URI itemsUri, Process server) throws Exception {
        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(1))
            .build();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        Exception lastFailure = null;
        while (System.nanoTime() < deadline) {
            if (!server.isAlive()) {
                throw new AssertionError("Fresh server exited with code " + server.exitValue());
            }
            try {
                return client.send(
                    HttpRequest.newBuilder(itemsUri).timeout(Duration.ofSeconds(2)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            } catch (IOException | InterruptedException failure) {
                lastFailure = failure;
                Thread.sleep(100);
            }
        }
        throw new AssertionError("Fresh server did not become ready within 30 seconds", lastFailure);
    }
}
