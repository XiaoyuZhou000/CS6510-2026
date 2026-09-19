package analytics;

import api.HttpSupport;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import json.Json;
import persistence.PopularWindowDao;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Reports the latest committed checkpoint, independent of in-memory scan progress. */
public final class AnalyticsHandlers implements HttpHandler {
    private final PopularWindowDao windows;

    public AnalyticsHandlers(PopularWindowDao windows) {
        this.windows = windows;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        if (!"/analytics/popular-items".equals(ex.getRequestURI().getPath())) {
            ex.sendResponseHeaders(404, -1);
            ex.close();
            return;
        }
        if (!"GET".equals(ex.getRequestMethod())) {
            ex.getResponseHeaders().set("Allow", "GET");
            ex.sendResponseHeaders(405, -1);
            ex.close();
            return;
        }
        int limit;
        try {
            String value = HttpSupport.queryParam(ex, "limit");
            limit = value == null ? 10 : Integer.parseInt(value);
            if (limit < 0) throw new IllegalArgumentException();
        } catch (IllegalArgumentException e) {
            HttpSupport.respondError(ex, 400, "INVALID_LIMIT", "limit must be a non-negative integer");
            return;
        }
        try {
            PopularWindowDao.WindowRow window = windows.readLatestWindow(limit);
            List<String> items = new ArrayList<>();
            if (window != null) {
                for (PopularWindowDao.RankedItem item : window.items()) {
                    items.add(Json.object(
                        "sku", Json.quote(item.sku()),
                        "name", Json.quote(item.name()),
                        "scanCount", Json.number(item.scanCount()),
                        "rank", Json.number(item.rank())));
                }
            }
            // Before the first checkpoint, zero bounds and epoch denote no computed window.
            HttpSupport.respond(ex, 200, Json.object(
                "windowSize", Json.number(1000),
                "slideInterval", Json.number(500),
                "windowStart", Json.number(window == null ? 0 : window.windowStart()),
                "windowEnd", Json.number(window == null ? 0 : window.windowEnd()),
                "computedAt", Json.quote((window == null ? Instant.EPOCH : window.computedAt()).toString()),
                "items", Json.array(items)));
        } catch (SQLException e) {
            System.err.println("[analytics] DB error: " + e.getMessage());
            HttpSupport.respondError(ex, 500, "INTERNAL_ERROR", "Internal server error");
        }
    }
}
