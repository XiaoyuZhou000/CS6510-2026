package integration;

import api.json.Json;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.CheckoutDatabase;

import java.io.File;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Inventory restart must not interrupt committed checkout or cache old stock. */
class InventoryServiceIsolationTest {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    @Test
    void inventoryRestartReadsCommittedSaleWhileTransactionRunsDuringOutage() throws Exception {
        try (CheckoutDatabase db = open()) {
            int transactionPort = freePort();
            int inventoryPort = freePort();
            Process transaction = start(db, "transaction", transactionPort);
            Process inventory = start(db, "inventory", inventoryPort);
            String transactions = "http://127.0.0.1:" + transactionPort;
            String queries = "http://127.0.0.1:" + inventoryPort;
            try {
                awaitReady(transaction, transactions + "/transactions/missing", 404);
                awaitReady(inventory, queries + "/items", 200);
                assertEquals(List.of(CheckoutDatabase.OTHER_SKU, CheckoutDatabase.SKU),
                    Json.getList(Json.parseObject(get(queries + "/items").body()), "items")
                        .stream().map(row -> Json.asObject(row).get("sku")).toList());
                assertTrue(Json.getList(Json.parseObject(get(queries + "/inventory/low-stock").body()),
                    "alerts").isEmpty());

                inventory.destroyForcibly();
                assertTrue(inventory.waitFor(5, TimeUnit.SECONDS));
                Map<String,Object> started = Json.parseObject(post(transactions + "/transactions",
                    "{\"stationId\":\"inventory-outage\"}").body());
                String id = started.get("transactionId").toString();
                assertEquals(200, post(transactions + "/transactions/" + id + "/items",
                    "{\"sku\":\"" + CheckoutDatabase.SKU + "\"}").statusCode());
                assertEquals(200, post(transactions + "/transactions/" + id + "/complete", "").statusCode());
                assertEquals(1, db.stock());
                assertTrue(transaction.isAlive());

                inventory = start(db, "inventory", inventoryPort);
                awaitReady(inventory, queries + "/items", 200);
                Map<String,Object> low = Json.parseObject(get(queries + "/inventory/low-stock").body());
                assertEquals(50, ((Number)low.get("threshold")).intValue());
                List<Object> alerts = Json.getList(low, "alerts");
                assertEquals(1, alerts.size());
                Map<String,Object> alert = Json.asObject(alerts.getFirst());
                assertEquals(CheckoutDatabase.SKU, alert.get("sku"));
                assertEquals(1, ((Number)alert.get("currentStock")).intValue());
                assertEquals(1, ((Number)alert.get("threshold")).intValue());
                assertTrue(Json.getList(Json.parseObject(get(queries +
                    "/inventory/low-stock?threshold=0").body()), "alerts").isEmpty());
                HttpResponse<String> invalid = get(queries + "/inventory/low-stock?threshold=-1");
                assertEquals(400, invalid.statusCode());
                assertEquals("INVALID_THRESHOLD", Json.parseObject(invalid.body()).get("error"));
                assertEquals(405, post(queries + "/inventory/low-stock", "").statusCode());
                assertEquals(404, get(queries + "/inventory/low-stock/extra").statusCode());
            } finally {
                inventory.destroyForcibly();
                transaction.destroyForcibly();
                assertTrue(inventory.waitFor(5, TimeUnit.SECONDS));
                assertTrue(transaction.waitFor(5, TimeUnit.SECONDS));
            }
        }
    }

    private static CheckoutDatabase open() throws Exception {
        try { return new CheckoutDatabase(2); }
        catch (java.sql.SQLException unavailable) { throw new TestAbortedException("MySQL unavailable", unavailable); }
    }
    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
    }
    private static Process start(CheckoutDatabase db, String domain, int port) throws Exception {
        Path root = Files.isRegularFile(Path.of("out/transaction.jar")) ? Path.of(".") : Path.of("server");
        String cp = String.join(File.pathSeparator,
            root.resolve("out/common.jar").toAbsolutePath().toString(),
            root.resolve("out/" + domain + ".jar").toAbsolutePath().toString(),
            root.resolve("lib/mysql-connector-j-9.0.0.jar").toAbsolutePath().toString());
        String main = domain + "." + Character.toUpperCase(domain.charAt(0)) + domain.substring(1) + "Main";
        ProcessBuilder builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java")
            .toString(), "-cp", cp, main, Integer.toString(port));
        builder.environment().put("DB_HOST", db.host());
        builder.environment().put("DB_PORT", Integer.toString(db.port()));
        builder.environment().put("DB_NAME", db.databaseName());
        builder.environment().put("DB_USER", db.user());
        builder.environment().put("DB_PASSWORD", db.password());
        builder.environment().put("DB_POOL_SIZE", "4");
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        return builder.start();
    }
    private void awaitReady(Process process, String url, int status) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) fail("Service exited during startup: " + url);
            try {
                if (get(url).statusCode() == status) return;
            } catch (java.io.IOException ignored) { }
            Thread.sleep(50);
        }
        fail("Service did not become ready: " + url);
    }
    private HttpResponse<String> get(String url) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    }
    private HttpResponse<String> post(String url, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url)).header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
}
