package inventory;

import api.HttpSupport;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import json.Json;
import persistence.InventoryDao;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class InventoryHandlers implements HttpHandler {
    private final InventoryDao inventory;

    public InventoryHandlers(InventoryDao inventory) {
        this.inventory = inventory;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        if (!"/inventory/low-stock".equals(ex.getRequestURI().getPath())) {
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
        Integer override;
        try {
            String value = HttpSupport.queryParam(ex, "threshold");
            override = value == null ? null : Integer.valueOf(value);
        } catch (IllegalArgumentException e) {
            HttpSupport.respondError(ex, 400, "INVALID_THRESHOLD", "threshold must be an integer");
            return;
        }

        try {
            List<InventoryDao.LowStockRow> rows = inventory.listLowStock(override);
            String generatedAt = Instant.now().toString();
            List<String> alerts = new ArrayList<>();
            for (InventoryDao.LowStockRow row : rows) {
                alerts.add(Json.object(
                    "sku", Json.quote(row.sku()),
                    "name", Json.quote(row.name()),
                    "currentStock", Json.number(row.currentStock()),
                    "threshold", Json.number(row.threshold()),
                    "triggeredAt", Json.quote(generatedAt)));
            }
            HttpSupport.respond(ex, 200, Json.object(
                "threshold", Json.number(LowStockThresholds.resolve(override)),
                "generatedAt", Json.quote(generatedAt),
                "alerts", Json.array(alerts)));
        } catch (SQLException e) {
            System.err.println("[inventory] DB error: " + e.getMessage());
            HttpSupport.respondError(ex, 500, "INTERNAL_ERROR", "Internal server error");
        }
    }
}
