package api;

import api.json.Json;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import transaction.InventoryOperations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** HTTP-only translation for current committed low-stock reporting. */
public final class InventoryHttpHandler implements HttpHandler {
    private final InventoryOperations inventory;

    public InventoryHttpHandler(InventoryOperations inventory) {
        this.inventory = Objects.requireNonNull(inventory, "inventory");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"/inventory/low-stock".equals(exchange.getRequestURI().getPath())) {
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
        Integer threshold;
        try {
            String value = HttpSupport.queryParam(exchange, "threshold");
            threshold = value == null ? null : Integer.valueOf(value);
            if (threshold != null && threshold < 0) throw new IllegalArgumentException();
        } catch (IllegalArgumentException invalid) {
            HttpSupport.respondError(exchange, 400, "INVALID_THRESHOLD",
                    "threshold must be a non-negative integer");
            return;
        }
        try {
            HttpSupport.respond(exchange, 200, serialize(inventory.listLowStock(threshold)));
        } catch (RuntimeException failure) {
            ApiErrorMapper.ApiError error = ApiErrorMapper.map(failure);
            HttpSupport.respondError(exchange, error.httpStatus(), error.error(), error.message());
        }
    }

    private static String serialize(InventoryOperations.LowStockView view) {
        List<String> alerts = new ArrayList<>();
        for (InventoryOperations.LowStockItemView alert : view.alerts()) {
            alerts.add(Json.object("sku", Json.quote(alert.sku()), "name", Json.quote(alert.name()),
                    "currentStock", Json.number(alert.currentStock()),
                    "threshold", Json.number(alert.threshold()),
                    "triggeredAt", Json.quote(alert.triggeredAt().toString())));
        }
        return Json.object("threshold", Json.number(view.threshold()),
                "generatedAt", Json.quote(view.generatedAt().toString()),
                "alerts", Json.array(alerts));
    }
}
