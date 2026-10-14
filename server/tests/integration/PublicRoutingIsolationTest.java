package integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import support.PipelineTestSupport;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the real Nginx route map with independent owner processes. */
class PublicRoutingIsolationTest {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    @Test
    void checkedInRouteMapKeepsExactPublicOwnershipAndRawUriForwarding() throws Exception {
        Path config = Files.isRegularFile(Path.of("../deploy/nginx.conf"))
            ? Path.of("../deploy/nginx.conf") : Path.of("deploy/nginx.conf");
        String source = Files.readString(config).replace("\r\n", "\n");
        assertTrue(source.contains("listen 8080;"));
        for (String upstream : List.of("transaction_service", "inventory_service", "analytics_service"))
            assertTrue(source.contains("upstream " + upstream + " {"));
        assertTrue(source.contains("server 127.0.0.1:8081;"));
        assertTrue(source.contains("server 127.0.0.1:8082;"));
        assertTrue(source.contains("server 127.0.0.1:8083;"));
        assertTrue(source.contains("proxy_http_version 1.1;"));
        assertTrue(source.contains("proxy_set_header Connection \"\";"));
        for (String route : List.of("= /items", "= /inventory/low-stock"))
            assertTrue(source.contains("location " + route + " {\n            proxy_pass http://inventory_service;"));
        assertTrue(source.contains("location = /analytics/popular-items {\n            proxy_pass http://analytics_service;"));
        for (String route : List.of("= /transactions", "^~ /transactions/"))
            assertTrue(source.contains("location " + route + " {\n            proxy_pass http://transaction_service;"));
        assertFalse(source.contains("proxy_pass http://transaction_service/;"),
            "proxy_pass with a URI suffix could rewrite the raw client path");
    }

    @Test
    void forwardsAllSevenRoutesAndIsolatesStoppedOwners() throws Exception {
        try (Owner transaction = owner("transaction");
             Owner inventory = owner("inventory");
             Owner analytics = owner("analytics");
             PublicRouterTestSupport router = PublicRouterTestSupport.start(
                 transaction.port(), inventory.port(), analytics.port())) {
            String base = router.baseUrl();
            List<Route> routes = List.of(
                new Route("GET", "/items?order=sku", "", "inventory"),
                new Route("GET", "/inventory/low-stock?threshold=7", "", "inventory"),
                new Route("GET", "/analytics/popular-items?limit=3", "", "analytics"),
                new Route("POST", "/transactions", "{\"stationId\":\"s1\"}", "transaction"),
                new Route("POST", "/transactions/a%2Db/items", "{\"sku\":\"SKU-1\"}", "transaction"),
                new Route("POST", "/transactions/a-b/complete", "{}", "transaction"),
                new Route("GET", "/transactions/a-b?view=full", "", "transaction"));
            for (Route route : routes) {
                HttpResponse<String> response = send(base, route.method(), route.path(), route.body());
                assertEquals(200, response.statusCode(), route.path());
                assertEquals(route.owner() + "|" + route.method() + "|" + route.path() + "|" + route.body()
                    + "|application/json", response.body(), route.path());
            }
            HttpResponse<String> applicationError = send(base, "POST", "/transactions?error=1", "{}");
            assertEquals(409, applicationError.statusCode());
            assertEquals("{\"error\":\"TRANSACTION_NOT_OPEN\"}", applicationError.body());
            assertEquals("application/json", applicationError.headers().firstValue("Content-Type").orElseThrow());

            transaction.close();
            assertUpstreamFailure(base, "/transactions/a-b");
            assertEquals(200, send(base, "GET", "/items", "").statusCode());
            assertEquals(200, send(base, "GET", "/analytics/popular-items", "").statusCode());
            inventory.close();
            assertUpstreamFailure(base, "/items");
            assertUpstreamFailure(base, "/inventory/low-stock");
            assertEquals(200, send(base, "GET", "/analytics/popular-items", "").statusCode());
            analytics.close();
            assertUpstreamFailure(base, "/analytics/popular-items");
        }
    }

    private static Owner owner(String name) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> respond(name, exchange));
        server.start();
        return new Owner(server);
    }

    private static void respond(String owner, HttpExchange exchange) throws java.io.IOException {
        boolean error = "error=1".equals(exchange.getRequestURI().getRawQuery());
        String body = error ? "{\"error\":\"TRANSACTION_NOT_OPEN\"}" : owner + "|"
            + exchange.getRequestMethod() + "|" + exchange.getRequestURI().toASCIIString() + "|"
            + new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8) + "|"
            + exchange.getRequestHeaders().getFirst("Content-Type");
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(error ? 409 : 200, bytes.length);
        try (var out = exchange.getResponseBody()) { out.write(bytes); }
    }

    private HttpResponse<String> send(String base, String method, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(base + path))
            .header("Content-Type", "application/json")
            .method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private void assertUpstreamFailure(String base, String path) throws Exception {
        int status = send(base, "GET", path, "").statusCode();
        assertTrue(status == 502 || status == 503, path + " returned " + status);
    }

    private record Route(String method, String path, String body, String owner) { }

    private record Owner(HttpServer server) implements AutoCloseable {
        int port() { return server.getAddress().getPort(); }
        @Override public void close() { server.stop(0); }
    }
}
