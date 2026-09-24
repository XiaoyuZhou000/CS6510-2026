package api;

import api.json.Json;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import transaction.CatalogOperations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** HTTP-only translation for the stable catalog listing. */
public final class CatalogHttpHandler implements HttpHandler {
    private final CatalogOperations catalog;

    public CatalogHttpHandler(CatalogOperations catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"/items".equals(exchange.getRequestURI().getPath())) {
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
        try {
            List<String> items = new ArrayList<>();
            for (CatalogOperations.CatalogItemView item : catalog.listItems()) {
                items.add(Json.object("sku", Json.quote(item.sku()), "name", Json.quote(item.name()),
                        "price", Json.number(item.price())));
            }
            HttpSupport.respond(exchange, 200, Json.object("items", Json.array(items)));
        } catch (RuntimeException failure) {
            ApiErrorMapper.ApiError error = ApiErrorMapper.map(failure);
            HttpSupport.respondError(exchange, error.httpStatus(), error.error(), error.message());
        }
    }
}
