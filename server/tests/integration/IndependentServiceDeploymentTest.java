package integration;

import events.TopicNames;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.ServiceHttp;
import support.ServiceProcesses;
import support.TestSchema;
import support.TransactionTestSchema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs the three built artifacts as separate JVMs, each over its own disposable schema and user (R3), and
 * restarts each one while its peers keep serving; starts each one alone, with the broker up and unreachable
 * (006 US7 scenario 1, SC-012).
 */
class IndependentServiceDeploymentTest {
    private static final List<String> SERVICES = List.of("transaction", "inventory", "analytics");

    @Test
    void eachArtifactRestartsWithoutRestartingItsPeers() throws Exception {
        Path root = ServiceProcesses.serverRoot();
        assertTrue(Files.isRegularFile(root.resolve("out/common.jar")));
        String build = Files.readString(root.resolve("build.sh"));
        for (String domain : SERVICES) {
            Path jar = root.resolve("out/" + domain + ".jar");
            assertTrue(Files.isRegularFile(jar), jar.toString());
            try (JarFile artifact = new JarFile(jar.toFile())) {
                String main = domain + "." + Character.toUpperCase(domain.charAt(0)) + domain.substring(1) + "Main";
                assertEquals(main, artifact.getManifest().getMainAttributes().getValue("Main-Class"), jar.toString());
                assertNotNull(artifact.getEntry(main.replace('.', '/') + ".class"));
                // Each artifact carries only its own business classes (006 SC-012).
                for (String peer : SERVICES) {
                    if (peer.equals(domain)) continue;
                    assertFalse(artifact.stream().anyMatch(entry -> entry.getName().startsWith(peer + "/")),
                            jar + " contains classes of " + peer);
                }
            }
            // ./build.sh <service> builds it alone, and run-<service>.sh puts no peer artifact on its classpath.
            assertTrue(build.contains("transaction|inventory|analytics)") && build.contains("build_service \"$1\""),
                    "build.sh builds one service at a time");
            String run = Files.readString(root.resolve("run-" + domain + ".sh"));
            assertTrue(run.contains("jar=out/" + domain + ".jar"), "run-" + domain + ".sh runs out/" + domain + ".jar");
            for (String peer : SERVICES) {
                if (!peer.equals(domain)) assertFalse(run.contains("out/" + peer + ".jar"), "run-" + domain + ".sh uses " + peer);
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

    /**
     * US7 scenario 1 (SC-012): each service starts alone, with no peer running, both while the broker runs and
     * while its broker address refuses every connection, and serves its routes either way. The broker stays up
     * for the other services' sake; the refusing address makes every producer and consumer of the process wait
     * for metadata as with a stopped broker (the live broker-stop case is in {@code ScanWithoutBrokerTest}).
     */
    @Test
    void eachServiceStartsAloneWithTheBrokerUpAndWithItsBrokerUnreachable() throws Exception {
        KafkaTestSupport.requireBroker();
        TopicNames names = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(names);
        TestSchema.Options options = TestSchema.Options.defaults().withOwnUser();
        try (TransactionTestSchema transaction = TransactionTestSchema.create(options);
             InventoryTestSchema inventory = InventoryTestSchema.create(options);
             AnalyticsTestSchema analytics = AnalyticsTestSchema.create(options)) {
            inventory.seed(5, 20);
            Map<String, TestSchema> schemas = Map.of("transaction", transaction, "inventory", inventory,
                    "analytics", analytics);
            for (String domain : SERVICES) {
                for (boolean brokerUp : List.of(true, false)) {
                    ServiceProcesses.Builder builder = ServiceProcesses.service(domain).database(schemas.get(domain))
                            .httpPort(ServiceProcesses.freePort()).kafka(names);
                    if (!brokerUp) builder.env("KAFKA_BOOTSTRAP", "127.0.0.1:" + ServiceProcesses.freePort());
                    String condition = domain + (brokerUp ? " with the broker up" : " with its broker unreachable");
                    try (ServiceProcesses.ServiceProcess process = builder.start()) {
                        process.awaitHttp(probe(domain), domain.equals("transaction") ? 404 : 200, Duration.ofSeconds(60));
                        ServiceHttp http = new ServiceHttp(process.httpPort());
                        switch (domain) {
                            case "transaction" -> {
                                String id = http.start("alone-" + brokerUp);
                                assertEquals("OPEN", ServiceHttp.json(http.lookup(id)).get("status"), condition);
                            }
                            case "inventory" -> assertEquals(5, ((List<?>) ServiceHttp.json(http.get("/items"))
                                    .get("items")).size(), condition);
                            default -> assertEquals(200, http.get("/analytics/popular-items").statusCode(), condition);
                        }
                        assertTrue(process.isAlive(), condition + " stays running" + System.lineSeparator()
                                + process.output());
                    }
                }
            }
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
