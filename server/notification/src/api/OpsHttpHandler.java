package api;

import api.json.Json;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import faults.FaultHooks;
import notification.NotificationStatus;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Notification's operational routes under {@code /notifications/ops} (007 contracts/notification-http.md
 * "Operational endpoints"; not part of the public contract). {@code GET /notifications/ops/status} returns
 * {@code {subscriptionVersion, indexVersion, activeSubscribers, catalogLoaded, salesSelected, notifications: {pending,
 * delivered, parked}, failures: {charged, uncharged}, parkedEvents, alarms, saleConsumerLag, lastConfirmedSendAt}}.
 * With {@code NOTIFICATION_TEST_FAULTS=1} only, {@code POST /notifications/ops/faults} sets or clears the test-fault
 * property; otherwise that route is 404 like any unknown one.
 */
public final class OpsHttpHandler implements HttpHandler {
    public static final String CONTEXT = "/notifications/ops";

    private final Supplier<NotificationStatus.Snapshot> status;
    private final boolean testFaults;

    public OpsHttpHandler(Supplier<NotificationStatus.Snapshot> status) {
        this(status, false);
    }

    /** {@code testFaults} ({@code NOTIFICATION_TEST_FAULTS=1}) enables {@code POST /notifications/ops/faults}. */
    public OpsHttpHandler(Supplier<NotificationStatus.Snapshot> status, boolean testFaults) {
        this.status = Objects.requireNonNull(status, "status");
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
