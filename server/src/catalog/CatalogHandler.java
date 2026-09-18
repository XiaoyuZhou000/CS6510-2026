package catalog;

import api.HttpSupport;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import json.Json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class CatalogHandler implements HttpHandler {

    private final CatalogCache catalog;

    public CatalogHandler(CatalogCache catalog) {
        this.catalog = catalog;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            ex.sendResponseHeaders(405, -1);
            return;
        }
        List<String> items = new ArrayList<>();
        for (CatalogCache.CatalogItem item : catalog.all()) {
            items.add(Json.object(
                "sku",   Json.quote(item.sku()),
                "name",  Json.quote(item.name()),
                "price", Json.number(item.price())
            ));
        }
        String json = Json.object("items", Json.array(items));
        HttpSupport.respond(ex, 200, json);
    }
}
