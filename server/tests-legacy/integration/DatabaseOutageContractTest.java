package integration;

import api.json.Json;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.CheckoutDatabase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** A reachable owner must report database failures without acknowledging unfinished work. */
class DatabaseOutageContractTest {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    @Test
    void failedScanAndCompletionReturnInternalErrorThenCommittedStateRecovers() throws Exception {
        try (CheckoutDatabase db = open()) {
            int port = support.PipelineTestSupport.freePort();
            Process service = db.startService("transaction", port);
            String base = "http://127.0.0.1:" + port;
            try {
                support.PipelineTestSupport.awaitHttpStatus(service, base + "/transactions/missing", 404);
                HttpResponse<String> started = post(base + "/transactions", "{\"stationId\":\"db-outage\"}");
                assertEquals(201, started.statusCode());
                String id = Json.parseObject(started.body()).get("transactionId").toString();
                long beforePosition = db.eventCount();

                db.setTransactionTableUnavailable(true);
                try {
                    assertInternal(post(base + "/transactions/" + id + "/items",
                        "{\"sku\":\"" + CheckoutDatabase.SKU + "\"}"));
                    assertInternal(post(base + "/transactions/" + id + "/complete", "{}"));
                } finally {
                    db.setTransactionTableUnavailable(false);
                }
                assertEquals(beforePosition, db.eventCount(), "failed scan must not commit an event");
                assertEquals(2, db.stock(), "failed completion must not deduct stock");
                Map<String,Object> afterOutage = Json.parseObject(get(base + "/transactions/" + id).body());
                assertEquals("OPEN", afterOutage.get("status"));
                assertEquals(0, ((Number) afterOutage.get("itemCount")).intValue());

                assertEquals(200, post(base + "/transactions/" + id + "/items",
                    "{\"sku\":\"" + CheckoutDatabase.SKU + "\"}").statusCode());
                assertEquals(beforePosition + 1, db.eventCount());
                db.setTransactionTableUnavailable(true);
                try {
                    assertInternal(post(base + "/transactions/" + id + "/complete", "{}"));
                } finally {
                    db.setTransactionTableUnavailable(false);
                }
                assertEquals(2, db.stock());
                assertEquals("OPEN", Json.parseObject(get(base + "/transactions/" + id).body()).get("status"));
                assertEquals(200, post(base + "/transactions/" + id + "/complete", "{}").statusCode());
                assertEquals(1, db.stock());
                assertEquals("COMPLETED", Json.parseObject(get(base + "/transactions/" + id).body()).get("status"));
            } finally {
                service.destroyForcibly();
                assertTrue(service.waitFor(5, TimeUnit.SECONDS));
            }
        }
    }

    private static CheckoutDatabase open() throws Exception {
        try { return new CheckoutDatabase(2); }
        catch (java.sql.SQLException unavailable) { throw new TestAbortedException("MySQL unavailable", unavailable); }
    }

    private static void assertInternal(HttpResponse<String> response) {
        assertEquals(500, response.statusCode());
        assertEquals("INTERNAL_ERROR", Json.parseObject(response.body()).get("error"));
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
