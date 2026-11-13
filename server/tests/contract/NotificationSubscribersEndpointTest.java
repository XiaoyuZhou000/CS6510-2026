package contract;

import api.json.Json;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import support.InProcessNotification;
import support.NotificationTestSchema;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code POST /notifications/subscribers} and {@code GET /notifications/subscribers/{id}} (007 T043), every row of
 * contracts/notification-http.md, on Notification's real HTTP handler over an in-process service: a disposable
 * schema and an in-memory bus, so the contract is checked without a running deployment. Also checks that
 * {@code spec/notification-openapi.yaml} documents what is tested here (T062).
 */
class NotificationSubscribersEndpointTest {
    private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private static final List<String> CATALOG = List.of("SKU-000001", "SKU-000002", "SKU-000003", "SKU-000004");
    private static NotificationTestSchema schema;
    private static InProcessNotification notification;

    @BeforeAll
    static void startService() throws Exception {
        schema = NotificationTestSchema.create();
        notification = InProcessNotification.over(schema);
        // Before any catalog arrives, a registration cannot be validated: 500, and nothing is written.
        HttpResponse<String> early = post("{\"subscriberId\":\"early\",\"skus\":[\"SKU-000001\"]}");
        assertError(early, 500, "INTERNAL_ERROR");
        assertEquals(0, schema.subscriptionVersion());
        notification.publishCatalog("v1", CATALOG);
        notification.awaitCatalog(Duration.ofSeconds(10));
    }

    @AfterAll
    static void stopService() throws Exception {
        if (notification != null) notification.close();
        if (schema != null) schema.close();
    }

    @Test
    void aNewSubscriberIs201WithTheSubscriberViewAndARepeatIs200AtTheSameVersion() throws Exception {
        HttpResponse<String> created = post("{\"subscriberId\":\"sub-0001\",\"skus\":[\"SKU-000003\",\"SKU-000001\"]}");
        assertEquals(201, created.statusCode(), created.body());
        assertEquals("application/json", created.headers().firstValue("Content-Type").orElse(""));
        Map<String, Object> view = Json.parseObject(created.body());
        assertEquals(Map.of("subscriberId", "sub-0001", "status", "ACTIVE", "skus", List.of("SKU-000001", "SKU-000003"),
                "channel", "selfcheckout.notify.sub-0001.v1", "channelReady", true), withoutVersion(view));
        long version = ((Number) view.get("subscriptionVersion")).longValue();
        assertTrue(version >= 1);
        assertTrue(notification.bus.hasTopic("selfcheckout.notify.sub-0001.v1"), "the channel was provisioned");

        HttpResponse<String> repeat = post("{\"subscriberId\":\"sub-0001\",\"skus\":[\"SKU-000001\",\"SKU-000003\",\"SKU-000001\"]}");
        assertEquals(200, repeat.statusCode(), repeat.body());
        assertEquals(view, Json.parseObject(repeat.body()), "an idempotent repeat answers the change that produced it");

        HttpResponse<String> read = get("sub-0001");
        assertEquals(200, read.statusCode());
        assertEquals(view, Json.parseObject(read.body()));
    }

    @Test
    void anActiveSubscriberWithADifferentSetIs409AndChangesNothing() throws Exception {
        assertEquals(201, post("{\"subscriberId\":\"sub-0002\",\"skus\":[\"SKU-000002\"]}").statusCode());
        long version = schema.subscriptionVersion();
        assertError(post("{\"subscriberId\":\"sub-0002\",\"skus\":[\"SKU-000002\",\"SKU-000004\"]}"), 409, "SUBSCRIBER_EXISTS");
        assertError(post("{\"subscriberId\":\"sub-0002\",\"skus\":[\"SKU-999999\"]}"), 409, "SUBSCRIBER_EXISTS",
                "a state conflict comes before SKU validity");
        assertEquals(version, schema.subscriptionVersion());
        assertEquals(List.of("SKU-000002"), Json.getList(Json.parseObject(get("sub-0002").body()), "skus"));
    }

    @Test
    void invalidSubscriberIdsAre400InvalidSubscriberId() throws Exception {
        for (String body : List.of("{\"skus\":[\"SKU-000001\"]}", "{\"subscriberId\":\"\",\"skus\":[\"SKU-000001\"]}",
                "{\"subscriberId\":\"has space\",\"skus\":[\"SKU-000001\"]}", "{\"subscriberId\":\"a.b\",\"skus\":[\"SKU-000001\"]}",
                "{\"subscriberId\":\"" + "x".repeat(41) + "\",\"skus\":[\"SKU-000001\"]}",
                "{\"subscriberId\":42,\"skus\":[\"SKU-000001\"]}",
                "{\"subscriberId\":\"bad/id\"}")) {
            assertError(post(body), 400, "INVALID_SUBSCRIBER_ID", body);
        }
        assertEquals(201, post("{\"subscriberId\":\"" + "y".repeat(40) + "\",\"skus\":[\"SKU-000001\"]}").statusCode(),
                "40 characters is the limit");
    }

    @Test
    void missingOrEmptySkusAre400MissingSkus() throws Exception {
        assertError(post("{\"subscriberId\":\"sub-0003\"}"), 400, "MISSING_SKUS");
        assertError(post("{\"subscriberId\":\"sub-0003\",\"skus\":[]}"), 400, "MISSING_SKUS");
        assertError(post("{\"subscriberId\":\"sub-0003\",\"skus\":null}"), 400, "MISSING_SKUS");
        assertEquals(404, get("sub-0003").statusCode(), "nothing was registered");
    }

    @Test
    void anSkuOutsideTheCatalogIs400UnknownSkuNamingIt() throws Exception {
        HttpResponse<String> unknown = post("{\"subscriberId\":\"sub-0004\",\"skus\":[\"SKU-000001\",\"SKU-777777\"]}");
        assertError(unknown, 400, "UNKNOWN_SKU");
        assertTrue(Json.getString(Json.parseObject(unknown.body()), "message").contains("SKU-777777"), unknown.body());
        assertEquals(404, get("sub-0004").statusCode());
    }

    @Test
    void malformedRequestsAre400InvalidRequest() throws Exception {
        for (String body : List.of("{not json", "[\"sub\"]", "", "{\"subscriberId\":\"sub-0005\",\"skus\":\"SKU-000001\"}",
                "{\"subscriberId\":\"sub-0005\",\"skus\":[1,2]}")) {
            assertError(post(body), 400, "INVALID_REQUEST", body);
        }
    }

    @Test
    void anUnknownSubscriberIs404AndAnInvalidIdIs400() throws Exception {
        assertError(get("nobody-here"), 404, "SUBSCRIBER_NOT_FOUND");
        assertError(get("bad.id"), 400, "INVALID_SUBSCRIBER_ID");
    }

    /** T085: PATCH, an effective change and a no-op. */
    @Test
    void anEffectivePatchIs200AtANewVersionAndANoOpIs200AtTheSameVersion() throws Exception {
        assertEquals(201, post("{\"subscriberId\":\"sub-0101\",\"skus\":[\"SKU-000001\",\"SKU-000002\"]}").statusCode());
        long registered = version(get("sub-0101"));
        HttpResponse<String> changed = patch("sub-0101", "{\"add\":[\"SKU-000003\"],\"remove\":[\"SKU-000001\"]}");
        assertEquals(200, changed.statusCode(), changed.body());
        Map<String, Object> view = Json.parseObject(changed.body());
        assertEquals(List.of("SKU-000002", "SKU-000003"), view.get("skus"));
        assertEquals("ACTIVE", view.get("status"));
        assertEquals(schema.subscriptionVersion(), version(changed), "the confirmation carries the commit's version");
        assertTrue(version(changed) > registered);

        long before = schema.subscriptionVersion();
        HttpResponse<String> noOp = patch("sub-0101", "{\"add\":[\"SKU-000002\"],\"remove\":[\"SKU-000004\"]}");
        assertEquals(200, noOp.statusCode(), noOp.body());
        assertEquals(view, Json.parseObject(noOp.body()), "a no-op answers the current view");
        assertEquals(before, schema.subscriptionVersion(), "a no-op writes no version");
        assertEquals(view, Json.parseObject(get("sub-0101").body()));
        assertEquals(200, patch("sub-0101", "{\"remove\":[\"SKU-000002\"]}").statusCode(), "add may be omitted");
        assertEquals(200, patch("sub-0101", "{\"add\":[\"SKU-000004\"]}").statusCode(), "remove may be omitted");
    }

    @Test
    void aPatchThatWouldLeaveNoSkuIs409LastSku() throws Exception {
        assertEquals(201, post("{\"subscriberId\":\"sub-0102\",\"skus\":[\"SKU-000001\"]}").statusCode());
        long before = schema.subscriptionVersion();
        assertError(patch("sub-0102", "{\"remove\":[\"SKU-000001\"]}"), 409, "LAST_SKU");
        assertError(patch("sub-0102", "{\"remove\":[\"SKU-000001\",\"SKU-999999\"]}"), 409, "LAST_SKU",
                "a state conflict comes before SKU validity");
        assertEquals(before, schema.subscriptionVersion());
        assertEquals(List.of("SKU-000001"), Json.getList(Json.parseObject(get("sub-0102").body()), "skus"));
    }

    @Test
    void deleteCancelsOnceAndARepeatedDeleteIsIdempotent() throws Exception {
        assertEquals(201, post("{\"subscriberId\":\"sub-0103\",\"skus\":[\"SKU-000001\",\"SKU-000002\"]}").statusCode());
        HttpResponse<String> cancelled = delete("sub-0103");
        assertEquals(200, cancelled.statusCode(), cancelled.body());
        Map<String, Object> view = Json.parseObject(cancelled.body());
        assertEquals("CANCELLED", view.get("status"));
        assertEquals(List.of(), view.get("skus"));
        assertEquals("selfcheckout.notify.sub-0103.v1", view.get("channel"));
        assertEquals(schema.subscriptionVersion(), version(cancelled));

        long before = schema.subscriptionVersion();
        HttpResponse<String> repeat = delete("sub-0103");
        assertEquals(200, repeat.statusCode(), repeat.body());
        assertEquals(view, Json.parseObject(repeat.body()), "a repeated DELETE answers the first one's version");
        assertEquals(before, schema.subscriptionVersion());
        assertEquals(view, Json.parseObject(get("sub-0103").body()));

        assertError(patch("sub-0103", "{\"add\":[\"SKU-000003\"]}"), 409, "SUBSCRIBER_CANCELLED");
        assertError(patch("sub-0103", "{\"add\":[\"SKU-999999\"]}"), 409, "SUBSCRIBER_CANCELLED",
                "a state conflict comes before SKU validity");
        HttpResponse<String> again = post("{\"subscriberId\":\"sub-0103\",\"skus\":[\"SKU-000004\"]}");
        assertEquals(201, again.statusCode(), "a cancelled subscriber registers again");
        assertEquals(List.of("SKU-000004"), Json.parseObject(again.body()).get("skus"));
    }

    @Test
    void aPatchOrDeleteOfAnUnknownSubscriberIs404() throws Exception {
        long before = schema.subscriptionVersion();
        assertError(patch("nobody-0104", "{\"add\":[\"SKU-000001\"]}"), 404, "SUBSCRIBER_NOT_FOUND");
        assertError(patch("nobody-0104", "{\"add\":[\"SKU-999999\"]}"), 404, "SUBSCRIBER_NOT_FOUND",
                "existence comes before SKU validity");
        assertError(delete("nobody-0104"), 404, "SUBSCRIBER_NOT_FOUND");
        assertEquals(before, schema.subscriptionVersion());
    }

    @Test
    void anInvalidPatchBodyIs400InvalidRequestAndAnInvalidIdIs400InvalidSubscriberId() throws Exception {
        assertEquals(201, post("{\"subscriberId\":\"sub-0105\",\"skus\":[\"SKU-000001\"]}").statusCode());
        long before = schema.subscriptionVersion();
        for (String body : List.of("{not json", "", "[]", "{}", "{\"add\":[],\"remove\":[]}",
                "{\"add\":[\"SKU-000002\"],\"remove\":[\"SKU-000002\"]}", "{\"add\":\"SKU-000002\"}", "{\"add\":[2]}",
                "{\"remove\":null,\"add\":null}")) {
            assertError(patch("sub-0105", body), 400, "INVALID_REQUEST", body);
        }
        assertError(patch("nobody-0105", "{\"add\":[\"SKU-000002\"],\"remove\":[\"SKU-000002\"]}"), 400, "INVALID_REQUEST",
                "the request shape comes before existence");
        assertError(patch("bad.id", "{\"add\":[\"SKU-000002\"]}"), 400, "INVALID_SUBSCRIBER_ID");
        assertError(delete("bad.id"), 400, "INVALID_SUBSCRIBER_ID");
        assertEquals(before, schema.subscriptionVersion());
    }

    @Test
    void aPatchWithAnSkuOutsideTheCatalogIs400UnknownSku() throws Exception {
        assertEquals(201, post("{\"subscriberId\":\"sub-0106\",\"skus\":[\"SKU-000001\"]}").statusCode());
        long before = schema.subscriptionVersion();
        HttpResponse<String> unknown = patch("sub-0106", "{\"add\":[\"SKU-000002\",\"SKU-888888\"]}");
        assertError(unknown, 400, "UNKNOWN_SKU");
        assertTrue(Json.getString(Json.parseObject(unknown.body()), "message").contains("SKU-888888"), unknown.body());
        assertError(patch("sub-0106", "{\"remove\":[\"SKU-888888\"]}"), 400, "UNKNOWN_SKU");
        assertEquals(before, schema.subscriptionVersion());
        assertEquals(List.of("SKU-000001"), Json.getList(Json.parseObject(get("sub-0106").body()), "skus"));
    }

    @Test
    void otherMethodsOnASubscriberAre405() throws Exception {
        HttpResponse<String> put = CLIENT.send(HttpRequest.newBuilder(uri("/sub-0101"))
                .PUT(HttpRequest.BodyPublishers.ofString("{}")).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(405, put.statusCode());
    }

    @Test
    void theOpenApiDocumentDescribesTheseRoutesAndCodes() throws Exception {
        Path spec = Path.of("").toAbsolutePath().normalize();
        Path file = Files.isRegularFile(spec.resolve("../spec/notification-openapi.yaml"))
                ? spec.resolve("../spec/notification-openapi.yaml") : spec.resolve("spec/notification-openapi.yaml");
        String yaml = Files.readString(file);
        assertTrue(yaml.startsWith("openapi: 3.0.3"), "OpenAPI 3.0.3");
        assertTrue(yaml.contains("version: \"1.0.0\""), "info.version 1.0.0");
        for (String required : List.of("/notifications/subscribers:", "/notifications/subscribers/{subscriberId}:",
                "\"201\":", "\"200\":", "\"400\":", "\"404\":", "\"409\":", "\"500\":", "SubscriberView:", "ApiError:",
                "INVALID_SUBSCRIBER_ID", "MISSING_SKUS", "UNKNOWN_SKU", "INVALID_REQUEST", "SUBSCRIBER_EXISTS",
                "SUBSCRIBER_NOT_FOUND", "INTERNAL_ERROR", "channelReady", "subscriptionVersion",
                "patch:", "delete:", "SubscriptionChange:", "LAST_SKU", "SUBSCRIBER_CANCELLED", "CANCELLED")) {
            assertTrue(yaml.contains(required), "notification-openapi.yaml documents " + required);
        }
    }

    private static Map<String, Object> withoutVersion(Map<String, Object> view) {
        Map<String, Object> copy = new java.util.HashMap<>(view);
        copy.remove("subscriptionVersion");
        return copy;
    }

    private static void assertError(HttpResponse<String> response, int status, String code) {
        assertError(response, status, code, "");
    }

    private static void assertError(HttpResponse<String> response, int status, String code, String context) {
        assertEquals(status, response.statusCode(), context + " -> " + response.body());
        Map<String, Object> body = Json.parseObject(response.body());
        assertEquals(code, body.get("error"), context);
        String message = String.valueOf(body.get("message"));
        assertFalse(message.isBlank());
        String lower = message.toLowerCase(Locale.ROOT);
        for (String internal : List.of("kafka", "offset", "sql", "jdbc", "exception")) {
            assertFalse(lower.contains(internal), "the message names " + internal + ": " + message);
        }
    }

    private static HttpResponse<String> post(String body) throws Exception {
        return CLIENT.send(HttpRequest.newBuilder(uri("")).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> patch(String subscriberId, String body) throws Exception {
        return CLIENT.send(HttpRequest.newBuilder(uri("/" + subscriberId)).header("Content-Type", "application/json")
                .method("PATCH", HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> delete(String subscriberId) throws Exception {
        return CLIENT.send(HttpRequest.newBuilder(uri("/" + subscriberId)).DELETE().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static long version(HttpResponse<String> response) {
        return ((Number) Json.parseObject(response.body()).get("subscriptionVersion")).longValue();
    }

    private static HttpResponse<String> get(String subscriberId) throws Exception {
        return CLIENT.send(HttpRequest.newBuilder(uri("/" + subscriberId)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static URI uri(String suffix) {
        return URI.create("http://127.0.0.1:" + notification.httpPort() + "/notifications/subscribers" + suffix);
    }
}
