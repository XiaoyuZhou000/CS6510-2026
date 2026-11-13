package contract;

import api.json.Json;
import faults.FaultHooks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import support.InProcessNotification;
import support.NotificationTestSchema;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code POST /notifications/ops/faults} (007 T090, contracts/notification-http.md "Operational endpoints"): with
 * {@code NOTIFICATION_TEST_FAULTS=1} it sets or clears the {@code selfcheckout.faults} property of the running
 * service, and without it the route does not exist (404).
 */
class OpsFaultsEndpointTest {
    private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @AfterEach
    void clearFaults() {
        System.clearProperty(FaultHooks.PROPERTY);
    }

    @Test
    void withTestFaultsEnabledTheRouteSetsAndClearsTheProperty() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create();
             InProcessNotification notification = InProcessNotification.over(schema, Map.of("NOTIFICATION_TEST_FAULTS", "1"))) {
            String value = "notification.subscription.beforeCommit:throw";
            HttpResponse<String> set = post(notification, "{\"faults\":\"" + value + "\"}");
            assertEquals(200, set.statusCode(), set.body());
            assertEquals(Map.of("faults", value), Json.parseObject(set.body()));
            assertEquals(value, System.getProperty(FaultHooks.PROPERTY));

            String flags = "notification.channel.stall:sub-0001,notification.channel.reject:sub-0002";
            assertEquals(200, post(notification, "{\"faults\":\"" + flags + "\"}").statusCode(), "channel flags are accepted");
            assertEquals(flags, System.getProperty(FaultHooks.PROPERTY));

            HttpResponse<String> cleared = post(notification, "{\"faults\":\"\"}");
            assertEquals(200, cleared.statusCode(), cleared.body());
            assertEquals(Map.of("faults", ""), Json.parseObject(cleared.body()));
            assertNull(System.getProperty(FaultHooks.PROPERTY), "an empty value clears the property");
        }
    }

    @Test
    void withTestFaultsEnabledAnInvalidBodyIs400AndChangesNothing() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create();
             InProcessNotification notification = InProcessNotification.over(schema, Map.of("NOTIFICATION_TEST_FAULTS", "1"))) {
            for (String body : List.of("{not json", "{}", "{\"faults\":3}", "{\"faults\":\"no-action\"}",
                    "{\"faults\":\"notification.channel.stall:\"}")) {
                HttpResponse<String> response = post(notification, body);
                assertEquals(400, response.statusCode(), body + " -> " + response.body());
                assertEquals("INVALID_REQUEST", Json.parseObject(response.body()).get("error"), body);
                assertNull(System.getProperty(FaultHooks.PROPERTY), body);
            }
            HttpResponse<String> get = CLIENT.send(HttpRequest.newBuilder(uri(notification)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(405, get.statusCode());
        }
    }

    @Test
    void withoutTestFaultsTheRouteIs404AndThePropertyIsUntouched() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create();
             InProcessNotification notification = InProcessNotification.over(schema)) {
            HttpResponse<String> response = post(notification, "{\"faults\":\"notification.subscription.beforeCommit:throw\"}");
            assertEquals(404, response.statusCode(), response.body());
            assertEquals("NOT_FOUND", Json.parseObject(response.body()).get("error"));
            assertNull(System.getProperty(FaultHooks.PROPERTY));
            assertEquals(200, CLIENT.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + notification.httpPort()
                    + "/notifications/ops/status")).GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode(),
                    "the other ops routes still answer");
        }
    }

    private static HttpResponse<String> post(InProcessNotification notification, String body) throws Exception {
        return CLIENT.send(HttpRequest.newBuilder(uri(notification)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static URI uri(InProcessNotification notification) {
        return URI.create("http://127.0.0.1:" + notification.httpPort() + "/notifications/ops/faults");
    }
}
