package api;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import api.json.Json;
import metrics.PerfMetrics;
import transaction.TransactionOperations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;

/**
 * HTTP-only translation for the transaction lifecycle and lookup routes. Only a request the client got wrong
 * (unreadable JSON, a malformed path, a field outside its limits) is answered 400 INVALID_REQUEST, decided before
 * the business contract is called; every failure from behind it maps through {@link ApiErrorMapper}, so an
 * internal {@code IllegalArgumentException} is a 500 like any other and never echoes its message (FR-016, FR-024).
 */
public final class TransactionHttpHandler implements HttpHandler {
    private final TransactionOperations transactions;

    public TransactionHttpHandler(TransactionOperations transactions) {
        this.transactions = Objects.requireNonNull(transactions, "transactions");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod().toUpperCase();
        String path = exchange.getRequestURI().getPath();
        try {
            Matcher matcher;
            if ("POST".equals(method)) {
                if (HttpSupport.TRANSACTIONS_ROOT.matcher(path).matches()) {
                    start(exchange);
                    return;
                }
                matcher = HttpSupport.TRANSACTIONS_ITEMS.matcher(path);
                if (matcher.matches()) {
                    scan(exchange, pathVar(matcher));
                    return;
                }
                matcher = HttpSupport.TRANSACTIONS_COMPLETE.matcher(path);
                if (matcher.matches()) {
                    complete(exchange, pathVar(matcher));
                    return;
                }
            } else if ("GET".equals(method)) {
                matcher = HttpSupport.TRANSACTIONS_BY_ID.matcher(path);
                if (matcher.matches()) {
                    get(exchange, pathVar(matcher));
                    return;
                }
            }
            exchange.sendResponseHeaders(405, -1);
        } catch (InvalidRequest invalidRequest) {
            HttpSupport.respondError(exchange, 400, "INVALID_REQUEST", invalidRequest.getMessage());
        } catch (RuntimeException failure) {
            ApiErrorMapper.ApiError error = ApiErrorMapper.map(failure);
            HttpSupport.respondError(exchange, error.httpStatus(), error.error(), error.message());
        }
    }

    private void start(HttpExchange exchange) throws IOException {
        long requestStart = System.nanoTime();
        try {
            String stationId = parsed(() -> Json.getString(HttpSupport.readJsonBody(exchange), "stationId"));
            if (stationId == null || stationId.isBlank()) {
                HttpSupport.respondError(exchange, 400, ApiErrors.MISSING_STATION_ID,
                        "stationId is required and must not be blank");
                return;
            }
            TransactionOperations.StartCommand command = parsed(() -> new TransactionOperations.StartCommand(stationId));
            HttpSupport.respond(exchange, 201, serialize(transactions.start(command)));
        } finally {
            PerfMetrics.record("transaction.http.start", System.nanoTime() - requestStart);
        }
    }

    private void scan(HttpExchange exchange, String transactionId) throws IOException {
        long requestStart = System.nanoTime();
        try {
            String sku = parsed(() -> Json.getString(HttpSupport.readJsonBody(exchange), "sku"));
            if (sku == null || sku.isBlank()) {
                HttpSupport.respondError(exchange, 400, "MISSING_SKU", "sku is required and must not be blank");
                return;
            }
            TransactionOperations.ScanCommand command =
                    parsed(() -> new TransactionOperations.ScanCommand(transactionId, sku));
            HttpSupport.respond(exchange, 200, serialize(transactions.scan(command)));
        } finally {
            PerfMetrics.record("transaction.http.scan", System.nanoTime() - requestStart);
        }
    }

    private void complete(HttpExchange exchange, String transactionId) throws IOException {
        long requestStart = System.nanoTime();
        try {
            // The fixed client sends either an empty body or {}; neither carries domain input.
            HttpSupport.respond(exchange, 200, serialize(transactions.complete(transactionId)));
        } finally {
            PerfMetrics.record("transaction.http.complete", System.nanoTime() - requestStart);
        }
    }

    private void get(HttpExchange exchange, String transactionId) throws IOException {
        long requestStart = System.nanoTime();
        try {
            HttpSupport.respond(exchange, 200, serialize(transactions.get(transactionId)));
        } finally {
            PerfMetrics.record("transaction.http.get", System.nanoTime() - requestStart);
        }
    }

    private static String pathVar(Matcher matcher) throws IOException {
        return parsed(() -> HttpSupport.pathVar(matcher));
    }

    /** Reads or validates client input; an {@code IllegalArgumentException} here is the client's error. */
    private static <T> T parsed(RequestInput<T> input) throws IOException {
        try {
            return input.read();
        } catch (IllegalArgumentException invalid) {
            throw new InvalidRequest(invalid);
        }
    }

    @FunctionalInterface
    private interface RequestInput<T> {
        T read() throws IOException;
    }

    /** Client input that failed to parse or validate: the only failure answered 400 INVALID_REQUEST. */
    private static final class InvalidRequest extends RuntimeException {
        InvalidRequest(IllegalArgumentException cause) {
            super(cause.getMessage(), cause);
        }
    }

    private static String serialize(TransactionOperations.TransactionView view) {
        return Json.object("transactionId", Json.quote(view.transactionId()),
                "stationId", Json.quote(view.stationId()), "status", Json.quote(view.status().name()),
                "itemCount", Json.number(view.itemCount()), "runningTotal", Json.number(view.runningTotal()),
                "startedAt", Json.quote(view.startedAt().toString()));
    }

    private static String serialize(TransactionOperations.ScanView view) {
        return Json.object("transactionId", Json.quote(view.transactionId()), "sku", Json.quote(view.sku()),
                "name", Json.quote(view.name()), "unitPrice", Json.number(view.unitPrice()),
                "itemCount", Json.number(view.itemCount()), "runningTotal", Json.number(view.runningTotal()));
    }

    private static String serialize(TransactionOperations.ReceiptView view) {
        List<String> lines = new ArrayList<>();
        for (TransactionOperations.ReceiptLineView line : view.lines()) {
            lines.add(Json.object("sku", Json.quote(line.sku()), "name", Json.quote(line.name()),
                    "unitPrice", Json.number(line.unitPrice()), "quantity", Json.number(line.quantity())));
        }
        return Json.object("transactionId", Json.quote(view.transactionId()),
                "stationId", Json.quote(view.stationId()), "itemCount", Json.number(view.itemCount()),
                "totalAmount", Json.number(view.totalAmount()), "startedAt", Json.quote(view.startedAt().toString()),
                "completedAt", Json.quote(view.completedAt().toString()), "lines", Json.array(lines));
    }
}
