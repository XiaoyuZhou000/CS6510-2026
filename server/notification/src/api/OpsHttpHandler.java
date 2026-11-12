package api;

import api.json.Json;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
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
 */
public final class OpsHttpHandler implements HttpHandler {
    public static final String CONTEXT = "/notifications/ops";

    private final Supplier<NotificationStatus.Snapshot> status;

    public OpsHttpHandler(Supplier<NotificationStatus.Snapshot> status) {
        this.status = Objects.requireNonNull(status, "status");
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
            HttpSupport.respondError(exchange, 404, "NOT_FOUND", "No such route");
        } catch (RuntimeException failure) {
            System.err.println("[notification] Request " + method + " " + path + " failed: " + failure);
            HttpSupport.respondError(exchange, 500, NotificationErrorMapper.INTERNAL_ERROR, "The status could not be read");
        }
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
