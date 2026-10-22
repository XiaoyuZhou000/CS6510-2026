package integration;

import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.ServiceHttp;
import support.ServiceProcesses;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs the three built artifacts as separate JVMs, each over its own disposable schema and user (R3), and
 * restarts each one while its peers keep serving.
 */
class IndependentServiceDeploymentTest {
    private static final List<String> SERVICES = List.of("transaction", "inventory", "analytics");

    @Test
    void eachArtifactRestartsWithoutRestartingItsPeers() throws Exception {
        Path root = ServiceProcesses.serverRoot();
        assertTrue(Files.isRegularFile(root.resolve("out/common.jar")));
        for (String domain : SERVICES) {
            Path jar = root.resolve("out/" + domain + ".jar");
            assertTrue(Files.isRegularFile(jar), jar.toString());
            try (JarFile artifact = new JarFile(jar.toFile())) {
                String main = domain + "." + Character.toUpperCase(domain.charAt(0)) + domain.substring(1) + "Main";
                assertEquals(main, artifact.getManifest().getMainAttributes().getValue("Main-Class"), jar.toString());
                assertNotNull(artifact.getEntry(main.replace('.', '/') + ".class"));
            }
        }

        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(5, 20, null)) {
            deployment.startAnalytics();
            Map<String, ServiceHttp> http = Map.of("transaction", deployment.transactionHttp,
                    "inventory", deployment.inventoryHttp, "analytics", deployment.analyticsHttp);
            String id = deployment.transactionHttp.start("deployment-smoke");
            assertEquals(200, deployment.transactionHttp.scan(id, "SKU-000001").statusCode());

            for (String domain : SERVICES) {
                stop(deployment, domain);
                for (String peer : SERVICES) {
                    if (peer.equals(domain)) continue;
                    assertTrue(process(deployment, peer).isAlive(), peer + " stopped with " + domain);
                    assertEquals(peer.equals("transaction") ? 404 : 200, http.get(peer).get(probe(peer)).statusCode(), peer);
                }
                start(deployment, domain);
            }

            Map<String, Object> open = ServiceHttp.json(deployment.transactionHttp.lookup(id));
            assertEquals("OPEN", open.get("status"));
            assertEquals(1, ((Number) open.get("itemCount")).intValue());
            assertEquals(200, deployment.transactionHttp.complete(id).statusCode());
            assertEquals(19, deployment.inventorySchema.stock("SKU-000001"));
            assertEquals(200, deployment.inventoryHttp.get("/items").statusCode());
            assertEquals(200, deployment.analyticsHttp.get("/analytics/popular-items").statusCode());
            deployment.awaitAnalyticsHoldsEveryScan(Duration.ofSeconds(30));
        }
    }

    private static ServiceProcesses.ServiceProcess process(CheckoutDeployment deployment, String domain) {
        return switch (domain) {
            case "transaction" -> deployment.transactionProcess();
            case "inventory" -> deployment.inventoryProcess();
            case "analytics" -> deployment.analyticsProcess();
            default -> throw new IllegalArgumentException(domain);
        };
    }

    private static void stop(CheckoutDeployment deployment, String domain) throws Exception {
        switch (domain) {
            case "transaction" -> deployment.stopTransaction();
            case "inventory" -> deployment.stopInventory();
            case "analytics" -> deployment.stopAnalytics();
            default -> throw new IllegalArgumentException(domain);
        }
    }

    private static void start(CheckoutDeployment deployment, String domain) throws Exception {
        switch (domain) {
            case "transaction" -> deployment.startTransaction();
            case "inventory" -> deployment.startInventory();
            case "analytics" -> deployment.startAnalytics();
            default -> throw new IllegalArgumentException(domain);
        }
    }

    private static String probe(String domain) {
        return switch (domain) {
            case "transaction" -> "/transactions/missing";
            case "inventory" -> "/items";
            case "analytics" -> "/analytics/popular-items";
            default -> throw new IllegalArgumentException(domain);
        };
    }
}
