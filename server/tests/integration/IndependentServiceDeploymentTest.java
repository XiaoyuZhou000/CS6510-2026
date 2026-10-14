package integration;

import api.json.Json;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.CheckoutDatabase;
import support.PipelineTestSupport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.*;

/** Runs the three checked-in artifacts as separate JVMs against one disposable DB. */
class IndependentServiceDeploymentTest {
    private final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(2)).build();

    @Test
    void eachArtifactRestartsWithoutRestartingItsPeers() throws Exception {
        Path root = Files.isRegularFile(Path.of("out/common.jar"))
            ? Path.of(".") : Path.of("server");
        assertTrue(Files.isRegularFile(root.resolve("out/common.jar")));
        for (String domain : new String[]{"transaction", "inventory", "analytics"}) {
            Path jar = root.resolve("out/" + domain + ".jar");
            assertTrue(Files.isRegularFile(jar), jar.toString());
            try (JarFile artifact = new JarFile(jar.toFile())) {
                String main = domain + "." + Character.toUpperCase(domain.charAt(0))
                    + domain.substring(1) + "Main";
                assertEquals(main, artifact.getManifest().getMainAttributes()
                    .getValue("Main-Class"), jar.toString());
                assertNotNull(artifact.getEntry(main.replace('.', '/') + ".class"));
            }
        }

        try (CheckoutDatabase db = open()) {
            Map<String, Integer> ports = new LinkedHashMap<>();
            Map<String, Process> services = new LinkedHashMap<>();
            for (String domain : new String[]{"transaction", "inventory", "analytics"})
                ports.put(domain, PipelineTestSupport.freePort());
            try {
                for (String domain : ports.keySet()) {
                    services.put(domain, db.startService(domain, ports.get(domain)));
                    ready(services.get(domain), base(ports, domain) + probe(domain));
                }
                String transaction = base(ports, "transaction");
                String inventory = base(ports, "inventory");
                String analytics = base(ports, "analytics");
                String id = Json.parseObject(post(transaction + "/transactions",
                    "{\"stationId\":\"deployment-smoke\"}").body())
                    .get("transactionId").toString();
                assertEquals(200, post(transaction + "/transactions/" + id + "/items",
                    "{\"sku\":\"" + CheckoutDatabase.SKU + "\"}").statusCode());

                for (String domain : ports.keySet()) {
                    stop(services.remove(domain));
                    for (String peer : ports.keySet()) {
                        if (!peer.equals(domain)) {
                            assertTrue(services.get(peer).isAlive(), peer + " stopped with " + domain);
                            assertEquals(peer.equals("transaction") ? 404 : 200,
                                get(base(ports, peer) + probe(peer)).statusCode(), peer);
                        }
                    }
                    services.put(domain, db.startService(domain, ports.get(domain)));
                    ready(services.get(domain), base(ports, domain) + probe(domain));
                }

                Map<String, Object> open = Json.parseObject(
                    get(transaction + "/transactions/" + id).body());
                assertEquals("OPEN", open.get("status"));
                assertEquals(1, ((Number) open.get("itemCount")).intValue());
                assertEquals(200, post(transaction + "/transactions/" + id + "/complete",
                    "").statusCode());
                assertEquals(19, db.stock());
                assertEquals(200, get(inventory + "/items").statusCode());
                assertEquals(200, get(analytics + "/analytics/popular-items").statusCode());
            } finally {
                for (Process process : services.values()) stop(process);
            }
        }
    }

    private static CheckoutDatabase open() throws Exception {
        try { return new CheckoutDatabase(20); }
        catch (SQLException unavailable) {
            throw new TestAbortedException("MySQL unavailable", unavailable);
        }
    }
    private static String base(Map<String, Integer> ports, String domain) {
        return "http://127.0.0.1:" + ports.get(domain);
    }
    private static String probe(String domain) {
        return switch (domain) {
            case "transaction" -> "/transactions/missing";
            case "inventory" -> "/items";
            case "analytics" -> "/analytics/popular-items";
            default -> throw new IllegalArgumentException(domain);
        };
    }
    private static void stop(Process process) throws Exception {
        if (process == null) return;
        process.destroy();
        if (!process.waitFor(8, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            assertTrue(process.waitFor(5, TimeUnit.SECONDS));
        }
    }
    private void ready(Process process, String url) throws Exception {
        int expected = url.endsWith("/transactions/missing") ? 404 : 200;
        PipelineTestSupport.awaitHttpStatus(process, url, expected);
    }
    private HttpResponse<String> get(String url) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    }
    private HttpResponse<String> post(String url, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
            HttpResponse.BodyHandlers.ofString());
    }
}
