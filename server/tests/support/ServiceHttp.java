package support;

import api.json.Json;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/** JSON requests to one service's own HTTP port, for process-level tests. */
public final class ServiceHttp {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final String base;

    public ServiceHttp(int port) {
        this.base = "http://127.0.0.1:" + port;
    }

    public HttpResponse<String> post(String path, String body) throws IOException, InterruptedException {
        return client.send(HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> get(String path) throws IOException, InterruptedException {
        return client.send(HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(15)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /** Starts a transaction and returns its ID; fails unless the response is 201. */
    public String start(String stationId) throws IOException, InterruptedException {
        HttpResponse<String> response = post("/transactions", "{\"stationId\":\"" + stationId + "\"}");
        if (response.statusCode() != 201) throw new AssertionError("Start answered " + response.statusCode() + ": " + response.body());
        return Json.parseObject(response.body()).get("transactionId").toString();
    }

    public HttpResponse<String> scan(String transactionId, String sku) throws IOException, InterruptedException {
        return post("/transactions/" + transactionId + "/items", "{\"sku\":\"" + sku + "\"}");
    }

    /** Completes with the empty body the Load Client sends. */
    public HttpResponse<String> complete(String transactionId) throws IOException, InterruptedException {
        return post("/transactions/" + transactionId + "/complete", "");
    }

    public HttpResponse<String> lookup(String transactionId) throws IOException, InterruptedException {
        return get("/transactions/" + transactionId);
    }

    public static Map<String, Object> json(HttpResponse<String> response) {
        return Json.parseObject(response.body());
    }
}
