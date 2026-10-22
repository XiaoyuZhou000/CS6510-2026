package integration;

import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.ServiceHttp;
import support.TransactionTestSchema;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The real Transaction, Inventory, and Analytics processes, each over its own schema, behind the checked-in
 * route map (US4). A stopped service makes only its own routes answer the gateway's 502; a reachable service
 * reports an Inventory outage on Complete as 500 INTERNAL_ERROR within the client's 10 s timeout, and the
 * pending attempt settles once Inventory returns (spec Availability and Failure Behavior, FR-017, FR-030).
 *
 * <p>{@code PublicRoutingIsolationTest} keeps covering raw-URI forwarding with stand-in owners.</p>
 */
class GatewayServiceOutageTest {
    private static final List<String> SERVICES = List.of("inventory", "analytics", "transaction");
    private static final String SKU = InventoryTestSchema.sku(1);

    @Test
    void stoppingEachServiceAnswers502OnlyOnItsOwnRoutes() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(3, 100, null)) {
            deployment.startAnalytics();
            try (PublicRouterTestSupport router = PublicRouterTestSupport.start(deployment.transactionHttpPort,
                    deployment.inventoryHttpPort, deployment.analyticsHttpPort)) {
                ServiceHttp gateway = new ServiceHttp(router.baseUrl());
                assertOwnersServe(gateway, SERVICES, deployment);

                for (String stopped : SERVICES) {
                    stop(deployment, stopped);
                    String context = stopped + " stopped";
                    assertOwnRoutesUnreachable(gateway, stopped, context);
                    assertOwnersServe(gateway, SERVICES.stream().filter(peer -> !peer.equals(stopped)).toList(),
                            deployment);
                    start(deployment, stopped);
                    awaitServing(gateway, stopped, deployment);
                }
                assertOwnersServe(gateway, SERVICES, deployment);
            }
        }
    }

    @Test
    void inventoryOutageAnswers500ThroughGatewayAndTheAttemptSettlesAfterItReturns() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(3, 100, null)) {
            deployment.startAnalytics();
            try (PublicRouterTestSupport router = PublicRouterTestSupport.start(deployment.transactionHttpPort,
                    deployment.inventoryHttpPort, deployment.analyticsHttpPort)) {
                ServiceHttp gateway = new ServiceHttp(router.baseUrl());
                String completedBefore = gateway.start("gateway-before");
                assertEquals(200, gateway.scan(completedBefore, SKU).statusCode());
                assertEquals(200, gateway.complete(completedBefore).statusCode());
                assertEquals(99, deployment.inventorySchema.stock(SKU));

                deployment.killInventory();
                assertOwnRoutesUnreachable(gateway, "inventory", "inventory killed");
                assertEquals(200, gateway.get("/analytics/popular-items").statusCode(), "Analytics is unaffected");

                // Complete keeps its 404 and 409 answers where they apply (Availability and Failure Behavior).
                assertError(gateway.complete("missing-transaction"), 404, "TRANSACTION_NOT_FOUND");
                assertError(gateway.complete(completedBefore), 409, "TRANSACTION_NOT_OPEN");
                String empty = gateway.start("gateway-empty");
                assertError(gateway.complete(empty), 409, "EMPTY_BASKET");

                String id = gateway.start("gateway-outage");
                assertEquals(200, gateway.scan(id, SKU).statusCode(), "Scan needs no Inventory call");
                assertEquals(200, gateway.scan(id, SKU).statusCode());
                long started = System.nanoTime();
                HttpResponse<String> during = gateway.complete(id);
                long elapsed = System.nanoTime() - started;
                assertError(during, 500, "INTERNAL_ERROR");
                assertEquals("Internal server error", ServiceHttp.json(during).get("message"));
                assertTrue(elapsed < TimeUnit.SECONDS.toNanos(10),
                        "Complete answered after " + TimeUnit.NANOSECONDS.toMillis(elapsed) + " ms");
                assertEquals("OPEN", ServiceHttp.json(gateway.lookup(id)).get("status"),
                        "lookup never waits and shows OPEN while the attempt is pending");
                assertEquals(1, pendingAttempts(deployment, id), "the attempt stays pending");
                assertEquals(99, deployment.inventorySchema.stock(SKU), "no stock effect during the outage");

                // The frozen basket: a Scan on the pending transaction waits out its bound and answers 500.
                started = System.nanoTime();
                HttpResponse<String> frozen = gateway.scan(id, SKU);
                elapsed = System.nanoTime() - started;
                assertError(frozen, 500, "INTERNAL_ERROR");
                assertTrue(elapsed < TimeUnit.SECONDS.toNanos(10),
                        "Scan answered after " + TimeUnit.NANOSECONDS.toMillis(elapsed) + " ms");

                deployment.startInventory();
                awaitServing(gateway, "inventory", deployment);
                awaitCompleted(deployment, id);
                Map<String, Object> settled = ServiceHttp.json(gateway.lookup(id));
                assertEquals("COMPLETED", settled.get("status"), "settled without manual steps");
                assertEquals(2, ((Number) settled.get("itemCount")).intValue(), "the frozen basket was settled");
                assertEquals(97, deployment.inventorySchema.stock(SKU));
                assertError(gateway.complete(id), 409, "TRANSACTION_NOT_OPEN");
                deployment.awaitAnalyticsHoldsEveryScan(Duration.ofSeconds(30));
            }
        }
    }

    private static void assertOwnersServe(ServiceHttp gateway, List<String> owners, CheckoutDeployment deployment)
            throws Exception {
        for (String owner : owners) {
            switch (owner) {
                case "inventory" -> {
                    assertEquals(200, gateway.get("/items").statusCode(), "/items");
                    assertEquals(200, gateway.get("/inventory/low-stock").statusCode(), "/inventory/low-stock");
                }
                case "analytics" -> assertEquals(200, gateway.get("/analytics/popular-items").statusCode(),
                        "/analytics/popular-items");
                case "transaction" -> {
                    String id = gateway.start("gateway-" + owner);
                    assertEquals(200, gateway.scan(id, SKU).statusCode(), "scan " + deployment.logs());
                    assertEquals("OPEN", ServiceHttp.json(gateway.lookup(id)).get("status"));
                    // Checkout needs Inventory; with Inventory down it is covered by the outage test.
                    if (owners.contains("inventory")) {
                        assertEquals(200, gateway.complete(id).statusCode(), "complete " + deployment.logs());
                    }
                }
                default -> throw new IllegalArgumentException(owner);
            }
        }
    }

    private static void assertOwnRoutesUnreachable(ServiceHttp gateway, String owner, String context) throws Exception {
        List<HttpResponse<String>> responses = switch (owner) {
            case "inventory" -> List.of(gateway.get("/items"), gateway.get("/inventory/low-stock"));
            case "analytics" -> List.of(gateway.get("/analytics/popular-items"));
            case "transaction" -> List.of(gateway.post("/transactions", "{\"stationId\":\"gateway\"}"),
                    gateway.scan("any-transaction", SKU), gateway.complete("any-transaction"),
                    gateway.lookup("any-transaction"));
            default -> throw new IllegalArgumentException(owner);
        };
        for (HttpResponse<String> response : responses) {
            assertEquals(502, response.statusCode(), context + ": " + response.request().method() + " "
                    + response.uri().getPath());
        }
    }

    private static void assertError(HttpResponse<String> response, int status, String error) {
        assertEquals(status, response.statusCode(), response.body());
        assertEquals(error, ServiceHttp.json(response).get("error"), response.body());
        for (String leak : List.of("grpc", "Grpc", "UNAVAILABLE", "DEADLINE", "SQL", "jdbc", "cs6510")) {
            assertFalse(response.body().contains(leak), "leaked " + leak + ": " + response.body());
        }
    }

    private static long pendingAttempts(CheckoutDeployment deployment, String id) throws Exception {
        return deployment.transactionSchema.attempts(id).stream().filter(row -> row.state().equals("PENDING")).count();
    }

    /** A restarted owner is reachable through the gateway again before the next step. */
    private static void awaitServing(ServiceHttp gateway, String owner, CheckoutDeployment deployment) throws Exception {
        String probe = switch (owner) {
            case "inventory" -> "/items";
            case "analytics" -> "/analytics/popular-items";
            case "transaction" -> "/transactions/missing";
            default -> throw new IllegalArgumentException(owner);
        };
        int expected = owner.equals("transaction") ? 404 : 200;
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (gateway.get(probe).statusCode() != expected) {
            if (System.nanoTime() > deadline) fail(owner + " never served through the gateway " + deployment.logs());
            Thread.sleep(100);
        }
    }

    private static void awaitCompleted(CheckoutDeployment deployment, String id) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            List<TransactionTestSchema.AttemptRow> attempts = deployment.transactionSchema.attempts(id);
            if (attempts.stream().anyMatch(row -> row.state().equals("COMPLETED"))) return;
            if (System.nanoTime() > deadline) fail("the pending checkout never settled " + attempts + deployment.logs());
            Thread.sleep(100);
        }
    }

    private static void stop(CheckoutDeployment deployment, String service) throws Exception {
        switch (service) {
            case "transaction" -> deployment.stopTransaction();
            case "inventory" -> deployment.stopInventory();
            case "analytics" -> deployment.stopAnalytics();
            default -> throw new IllegalArgumentException(service);
        }
    }

    private static void start(CheckoutDeployment deployment, String service) throws Exception {
        switch (service) {
            case "transaction" -> deployment.startTransaction();
            case "inventory" -> deployment.startInventory();
            case "analytics" -> deployment.startAnalytics();
            default -> throw new IllegalArgumentException(service);
        }
    }
}
