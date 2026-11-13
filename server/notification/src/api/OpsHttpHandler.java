package api;

import api.json.Json;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import database.NotificationStatusQueries.ParkedEvent;
import database.NotificationStore.Parked;
import events.TopicNames;
import faults.FaultHooks;
import notification.NotificationStatus;
import notification.ParkedOperations;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Notification's operational routes under {@code /notifications/ops} (007 contracts/notification-http.md
 * "Operational endpoints"; not part of the public contract). {@code GET /notifications/ops/status} returns
 * {@code {subscriptionVersion, indexVersion, activeSubscribers, catalogLoaded, salesSelected, notifications: {pending,
 * delivered, parked}, failures: {charged, uncharged}, parkedEvents, alarms, saleConsumerLag, lastConfirmedSendAt}}.
 * {@code GET /notifications/ops/parked} lists parked notifications and parked records, and
 * {@code POST /notifications/ops/parked/retry} moves parked notifications back to {@code PENDING} (T101).
 * With {@code NOTIFICATION_TEST_FAULTS=1} only, {@code POST /notifications/ops/faults} sets or clears the test-fault
 * property; otherwise that route is 404 like any unknown one.
 */
public final class OpsHttpHandler implements HttpHandler {
    public static final String CONTEXT = "/notifications/ops";

    public static final int DEFAULT_PARKED_LIMIT = 100;
    public static final int MAX_PARKED_LIMIT = 1_000;

    private final Supplier<NotificationStatus.Snapshot> status;
    private final ParkedOperations parked;
    private final boolean testFaults;

    public OpsHttpHandler(Supplier<NotificationStatus.Snapshot> status) {
        this(status, null, false);
    }

    /** {@code testFaults} ({@code NOTIFICATION_TEST_FAULTS=1}) enables {@code POST /notifications/ops/faults}. */
    public OpsHttpHandler(Supplier<NotificationStatus.Snapshot> status, boolean testFaults) {
        this(status, null, testFaults);
    }

    /** With {@code parked}, the parked listing and the operator retry (T101); without it those routes are 404. */
    public OpsHttpHandler(Supplier<NotificationStatus.Snapshot> status, ParkedOperations parked, boolean testFaults) {
        this.status = Objects.requireNonNull(status, "status");
        this.parked = parked;
        this.testFaults = testFaults;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod().toUpperCase();
        String path = exchange.getRequestURI().getPath();
        try {
            if (path.equals(CONTEXT + "/status")) {
                if (!"GET".equals(method)) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                HttpSupport.respond(exchange, 200, serialize(status.get()));
                return;
            }
            if (parked != null && path.equals(CONTEXT + "/parked")) {
                if (!"GET".equals(method)) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                listParked(exchange);
                return;
            }
            if (parked != null && path.equals(CONTEXT + "/parked/retry")) {
                if (!"POST".equals(method)) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                retryParked(exchange);
                return;
            }
            if (testFaults && path.equals(CONTEXT + "/faults")) {
                if (!"POST".equals(method)) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                setFaults(exchange);
                return;
            }
            HttpSupport.respondError(exchange, 404, "NOT_FOUND", "No such route");
        } catch (RuntimeException failure) {
            System.err.println("[notification] Request " + method + " " + path + " failed: " + failure);
            HttpSupport.respondError(exchange, 500, NotificationErrorMapper.INTERNAL_ERROR, "The status could not be read");
        }
    }

    /**
     * T090, test-only (research N13): {@code {"faults": "<selfcheckout.faults value>"}} sets the property of this
     * process, and an empty value clears it. A value that does not parse is 400 and changes nothing, so a test cannot
     * silently miss a hook. The answer is {@code {"faults": "<value>"}}.
     */
    private static void setFaults(HttpExchange exchange) throws IOException {
        Object value;
        try {
            value = Json.parseObject(HttpSupport.readBody(exchange)).get("faults");
        } catch (RuntimeException malformed) {
            HttpSupport.respondError(exchange, 400, NotificationErrorMapper.INVALID_REQUEST, "The body must be a JSON object");
            return;
        }
        if (!(value instanceof String faults)) {
            HttpSupport.respondError(exchange, 400, NotificationErrorMapper.INVALID_REQUEST, "faults must be a string");
            return;
        }
        try {
            FaultHooks.parse(faults);
            FaultHooks.flags(faults);
        } catch (IllegalArgumentException invalid) {
            HttpSupport.respondError(exchange, 400, NotificationErrorMapper.INVALID_REQUEST,
                    "faults is not a valid fault list: " + invalid.getMessage());
            return;
        }
        if (faults.isEmpty()) System.clearProperty(FaultHooks.PROPERTY);
        else System.setProperty(FaultHooks.PROPERTY, faults);
        System.out.println("[notification] Test faults set to '" + faults + "'");
        HttpSupport.respond(exchange, 200, Json.object("faults", Json.quote(faults)));
    }

    /**
     * {@code GET /notifications/ops/parked?subscriberId=&limit=}: {@code {"notifications": [...], "events": [...]}},
     * each list at most {@code limit} (default 100, at most 1,000) long, oldest first. A malformed subscriber ID or
     * limit is 400.
     */
    private void listParked(HttpExchange exchange) throws IOException {
        Optional<String> subscriberId;
        int limit;
        try {
            subscriberId = subscriber(HttpSupport.queryParam(exchange, "subscriberId"));
            String rawLimit = HttpSupport.queryParam(exchange, "limit");
            limit = rawLimit == null || rawLimit.isEmpty() ? DEFAULT_PARKED_LIMIT : Integer.parseInt(rawLimit);
            if (limit < 1 || limit > MAX_PARKED_LIMIT) throw new IllegalArgumentException("limit must be 1.." + MAX_PARKED_LIMIT);
        } catch (IllegalArgumentException invalid) {
            HttpSupport.respondError(exchange, 400, NotificationErrorMapper.INVALID_REQUEST, invalid.getMessage());
            return;
        }
        ParkedOperations.View view = parked.list(subscriberId, limit);
        List<String> notifications = new ArrayList<>();
        for (Parked row : view.notifications()) {
            notifications.add(Json.object(
                    "notificationId", Json.quote(row.notificationId()),
                    "saleId", Json.quote(row.saleId()),
                    "sku", Json.quote(row.sku()),
                    "subscriberId", Json.quote(row.subscriberId()),
                    "chargedFailures", Json.number(row.chargedFailures()),
                    "lastError", quoteOrNull(row.lastError()),
                    "parkedAt", Json.quote(row.parkedAt().toString())));
        }
        List<String> events = new ArrayList<>();
        for (ParkedEvent event : view.events()) {
            events.add(Json.object(
                    "eventId", quoteOrNull(event.eventId()),
                    "topic", Json.quote(event.topic()),
                    "partition", Json.number(event.partition()),
                    "offset", Json.number(event.offset()),
                    "eventType", quoteOrNull(event.eventType()),
                    "schemaVersion", event.schemaVersion() == null ? "null" : Json.number(event.schemaVersion()),
                    "reason", Json.quote(event.reason()),
                    "detail", quoteOrNull(event.detail()),
                    "firstParkedAt", Json.quote(event.firstParkedAt().toString()),
                    "lastParkedAt", Json.quote(event.lastParkedAt().toString()),
                    "parkCount", Json.number(event.parkCount())));
        }
        HttpSupport.respond(exchange, 200, Json.object("notifications", Json.array(notifications),
                "events", Json.array(events)));
    }

    /**
     * {@code POST /notifications/ops/parked/retry} with {@code {"subscriberId": optional}} (an empty body retries
     * every parked row): answers {@code {"retried": n}}.
     */
    private void retryParked(HttpExchange exchange) throws IOException {
        Optional<String> subscriberId;
        try {
            String body = HttpSupport.readBody(exchange);
            Map<String, Object> request = body == null || body.isBlank() ? Map.of() : Json.parseObject(body);
            Object value = request.get("subscriberId");
            if (value != null && !(value instanceof String)) throw new IllegalArgumentException("subscriberId must be a string");
            subscriberId = subscriber((String) value);
        } catch (IllegalArgumentException invalid) {
            HttpSupport.respondError(exchange, 400, NotificationErrorMapper.INVALID_REQUEST, invalid.getMessage());
            return;
        } catch (RuntimeException malformed) {
            HttpSupport.respondError(exchange, 400, NotificationErrorMapper.INVALID_REQUEST, "The body must be a JSON object");
            return;
        }
        int retried = parked.retry(subscriberId);
        HttpSupport.respond(exchange, 200, Json.object("retried", Json.number(retried)));
    }

    private static Optional<String> subscriber(String raw) {
        if (raw == null || raw.isEmpty()) return Optional.empty();
        if (!TopicNames.SUBSCRIBER_ID.matcher(raw).matches()) {
            throw new IllegalArgumentException("subscriberId must be 1 to 40 letters, digits, '_' or '-'");
        }
        return Optional.of(raw);
    }

    private static String quoteOrNull(String value) {
        return value == null ? "null" : Json.quote(value);
    }

    public static String serialize(NotificationStatus.Snapshot snapshot) {
        List<String> alarms = new ArrayList<>();
        for (String alarm : snapshot.alarms()) alarms.add(Json.quote(alarm));
        return Json.object(
                "subscriptionVersion", Json.number(snapshot.subscriptionVersion()),
                "indexVersion", Json.number(snapshot.indexVersion()),
                "activeSubscribers", Json.number(snapshot.activeSubscribers()),
                "catalogLoaded", Boolean.toString(snapshot.catalogLoaded()),
                "salesSelected", Json.number(snapshot.salesSelected()),
                "notifications", Json.object("pending", Json.number(snapshot.pending()),
                        "delivered", Json.number(snapshot.delivered()), "parked", Json.number(snapshot.parked())),
                "failures", Json.object("charged", Json.number(snapshot.chargedFailures()),
                        "uncharged", Json.number(snapshot.unchargedFailures())),
                "parkedEvents", Json.number(snapshot.parkedEvents()),
                "alarms", Json.array(alarms),
                "saleConsumerLag", Json.number(snapshot.saleConsumerLag()),
                "lastConfirmedSendAt", snapshot.lastConfirmedSendAt().map(Instant::toString).map(Json::quote).orElse("null"));
    }
}
