package api;

import api.json.Json;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import database.SubscriptionStore.SubscriberState;
import metrics.PerfMetrics;
import notification.SubscriptionFailure;
import notification.SubscriptionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The public subscription routes of Notification Service (007 contracts/notification-http.md), served under
 * {@code /notifications/subscribers}:
 *
 * <ul>
 *   <li>{@code POST /notifications/subscribers}: register; 201 for a new or re-registered subscriber, 200 for an
 *       idempotent repeat;</li>
 *   <li>{@code GET /notifications/subscribers/{subscriberId}}: the committed subscriber view, or 404;</li>
 *   <li>{@code PATCH /notifications/subscribers/{subscriberId}}: add and remove SKUs; 200 at the new version, or at
 *       the current one for a no-op (T089);</li>
 *   <li>{@code DELETE /notifications/subscribers/{subscriberId}}: cancel; 200, idempotent.</li>
 * </ul>
 *
 * <p>Validation precedence: request shape (400: malformed JSON is {@code INVALID_REQUEST}, then
 * {@code INVALID_SUBSCRIBER_ID}, then {@code MISSING_SKUS}), then subscriber existence (404), then state conflicts
 * (409), then SKU validity (400 {@code UNKNOWN_SKU}). Failures map through {@link NotificationErrorMapper}.</p>
 */
public final class NotificationHttpHandler implements HttpHandler {
    public static final String CONTEXT = "/notifications/subscribers";
    private static final Pattern ROOT = Pattern.compile("^/notifications/subscribers/?$");
    private static final Pattern BY_ID = Pattern.compile("^/notifications/subscribers/([^/]+)$");

    private final SubscriptionService subscriptions;

    public NotificationHttpHandler(SubscriptionService subscriptions) {
        this.subscriptions = Objects.requireNonNull(subscriptions, "subscriptions");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod().toUpperCase();
        String path = exchange.getRequestURI().getPath();
        try {
            if (ROOT.matcher(path).matches()) {
                if ("POST".equals(method)) {
                    register(exchange);
                    return;
                }
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            Matcher byId = BY_ID.matcher(path);
            if (byId.matches()) {
                String subscriberId = HttpSupport.urlDecode(byId.group(1));
                switch (method) {
                    case "GET" -> get(exchange, subscriberId);
                    case "PATCH" -> update(exchange, subscriberId);
                    case "DELETE" -> cancel(exchange, subscriberId);
                    default -> exchange.sendResponseHeaders(405, -1);
                }
                return;
            }
            HttpSupport.respondError(exchange, 404, "NOT_FOUND", "No such route");
        } catch (InvalidRequest invalid) {
            HttpSupport.respondError(exchange, 400, NotificationErrorMapper.INVALID_REQUEST, invalid.getMessage());
        } catch (RuntimeException failure) {
            NotificationErrorMapper.ApiError error = NotificationErrorMapper.map(failure);
            if (error.httpStatus() == 500 && !(failure instanceof SubscriptionFailure)) {
                System.err.println("[notification] Request " + method + " " + path + " failed: " + failure);
            }
            HttpSupport.respondError(exchange, error.httpStatus(), error.error(), error.message());
        }
    }

    private void register(HttpExchange exchange) throws IOException {
        long started = System.nanoTime();
        try {
            Map<String, Object> body = body(exchange);
            Object id = body.get("subscriberId");
            if (!(id instanceof String subscriberId)) {
                throw new SubscriptionFailure(SubscriptionFailure.Code.INVALID_SUBSCRIBER_ID,
                        "subscriberId is required and must be a string");
            }
            Object rawSkus = body.get("skus");
            List<String> skus = new ArrayList<>();
            if (rawSkus != null) {
                if (!(rawSkus instanceof List<?> list)) throw new InvalidRequest("skus must be an array of SKU strings");
                for (Object sku : list) {
                    if (!(sku instanceof String text)) throw new InvalidRequest("skus must be an array of SKU strings");
                    skus.add(text);
                }
            }
            // The service checks the ID's format before the SKU list (MISSING_SKUS), as the precedence requires.
            SubscriptionService.Registration registration = subscriptions.register(subscriberId, skus);
            HttpSupport.respond(exchange, registration.created() ? 201 : 200, view(registration.state()));
        } finally {
            PerfMetrics.record("notification.http.register", System.nanoTime() - started);
        }
    }

    /** {@code PATCH}: {@code {"add": [...], "remove": [...]}}, either list optional but not both empty. */
    private void update(HttpExchange exchange, String subscriberId) throws IOException {
        long started = System.nanoTime();
        try {
            Map<String, Object> body = body(exchange);
            List<String> add = skuList(body, "add");
            List<String> remove = skuList(body, "remove");
            HttpSupport.respond(exchange, 200, view(subscriptions.update(subscriberId, add, remove)));
        } finally {
            PerfMetrics.record("notification.http.update", System.nanoTime() - started);
        }
    }

    /** {@code DELETE}: cancel; a repeat answers the same view and version. Any request body is ignored. */
    private void cancel(HttpExchange exchange, String subscriberId) throws IOException {
        long started = System.nanoTime();
        try {
            HttpSupport.readBody(exchange);
            HttpSupport.respond(exchange, 200, view(subscriptions.cancel(subscriberId)));
        } finally {
            PerfMetrics.record("notification.http.cancel", System.nanoTime() - started);
        }
    }

    /** An optional array of SKU strings; absent or null is empty. */
    private static List<String> skuList(Map<String, Object> body, String field) {
        Object raw = body.get(field);
        List<String> skus = new ArrayList<>();
        if (raw == null) return skus;
        if (!(raw instanceof List<?> list)) throw new InvalidRequest(field + " must be an array of SKU strings");
        for (Object sku : list) {
            if (!(sku instanceof String text)) throw new InvalidRequest(field + " must be an array of SKU strings");
            skus.add(text);
        }
        return skus;
    }

    private void get(HttpExchange exchange, String subscriberId) throws IOException {
        long started = System.nanoTime();
        try {
            HttpSupport.respond(exchange, 200, view(subscriptions.find(subscriberId)));
        } finally {
            PerfMetrics.record("notification.http.get", System.nanoTime() - started);
        }
    }

    private static Map<String, Object> body(HttpExchange exchange) throws IOException {
        String text = HttpSupport.readBody(exchange);
        try {
            return Json.parseObject(text);
        } catch (RuntimeException malformed) {
            throw new InvalidRequest("the request body must be a JSON object");
        }
    }

    /** The subscriber view of the contract. */
    public static String view(SubscriberState state) {
        List<String> skus = new ArrayList<>();
        for (String sku : state.skus()) skus.add(Json.quote(sku));
        return Json.object("subscriberId", Json.quote(state.subscriberId()),
                "status", Json.quote(state.status().name()),
                "skus", Json.array(skus),
                "channel", Json.quote(state.channelTopic()),
                "channelReady", Boolean.toString(state.channelReady()),
                "subscriptionVersion", Json.number(state.updatedVersion()));
    }

    /** Input that is not even the request's shape: 400 {@code INVALID_REQUEST}. */
    private static final class InvalidRequest extends RuntimeException {
        InvalidRequest(String message) {
            super(message);
        }
    }
}
