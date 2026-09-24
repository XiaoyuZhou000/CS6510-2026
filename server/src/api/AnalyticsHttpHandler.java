package api;

import analytics.AnalyticsOperations;
import api.json.Json;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** HTTP-only translation for the latest popular-items view. */
public final class AnalyticsHttpHandler implements HttpHandler {
    private final AnalyticsOperations analytics;

    public AnalyticsHttpHandler(AnalyticsOperations analytics) {
        this.analytics = Objects.requireNonNull(analytics, "analytics");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"/analytics/popular-items".equals(exchange.getRequestURI().getPath())) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
            return;
        }
        if (!"GET".equals(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().set("Allow", "GET");
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }
        int limit;
        try {
            String value = HttpSupport.queryParam(exchange, "limit");
            limit = value == null ? 10 : Integer.parseInt(value);
            if (limit < 0) throw new IllegalArgumentException();
        } catch (IllegalArgumentException invalid) {
            HttpSupport.respondError(exchange, 400, "INVALID_LIMIT",
                    "limit must be a non-negative integer");
            return;
        }
        try {
            HttpSupport.respond(exchange, 200, serialize(analytics.latestPopularItems(limit)));
        } catch (RuntimeException failure) {
            ApiErrorMapper.ApiError error = ApiErrorMapper.map(failure);
            HttpSupport.respondError(exchange, error.httpStatus(), error.error(), error.message());
        }
    }

    private static String serialize(AnalyticsOperations.PopularItemsView view) {
        List<String> items = new ArrayList<>();
        for (AnalyticsOperations.RankedItemView item : view.items()) {
            items.add(Json.object("sku", Json.quote(item.sku()), "name", Json.quote(item.name()),
                    "scanCount", Json.number(item.scanCount()), "rank", Json.number(item.rank())));
        }
        return Json.object("windowSize", Json.number(view.windowSize()),
                "slideInterval", Json.number(view.slideInterval()),
                "windowStart", Json.number(view.windowStart()), "windowEnd", Json.number(view.windowEnd()),
                "computedAt", Json.quote(view.computedAt().toString()), "items", Json.array(items));
    }
}
