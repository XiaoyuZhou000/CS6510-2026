package checkout;

import api.ApiErrors;
import api.HttpSupport;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import json.Json;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Routes all /transactions/... requests to CheckoutService and serializes results.
 *
 * Phase 4 covers: POST /transactions, POST /transactions/{id}/items,
 *                 POST /transactions/{id}/complete.
 * Phase 7 (T040) will add GET /transactions/{id}.
 */
public final class CheckoutHandlers implements HttpHandler {

    private final CheckoutService service;

    public CheckoutHandlers(CheckoutService service) {
        this.service = service;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod().toUpperCase();
        String path   = ex.getRequestURI().getPath();

        try {
            Matcher m;

            if ("POST".equals(method)) {
                m = HttpSupport.TRANSACTIONS_ROOT.matcher(path);
                if (m.matches()) { handleStart(ex); return; }

                m = HttpSupport.TRANSACTIONS_ITEMS.matcher(path);
                if (m.matches()) { handleScan(ex, HttpSupport.pathVar(m)); return; }

                m = HttpSupport.TRANSACTIONS_COMPLETE.matcher(path);
                if (m.matches()) { handleComplete(ex, HttpSupport.pathVar(m)); return; }
            }

            if ("GET".equals(method)) {
                m = HttpSupport.TRANSACTIONS_BY_ID.matcher(path);
                if (m.matches()) { handleGet(ex, HttpSupport.pathVar(m)); return; }
            }

            ex.sendResponseHeaders(405, -1);

        } catch (SQLException e) {
            System.err.println("[checkout] DB error: " + e.getMessage());
            HttpSupport.respondError(ex, 500, "INTERNAL_ERROR", "Internal server error");
        }
    }

    // ------------------------------------------------------------------ POST /transactions

    private void handleStart(HttpExchange ex) throws IOException, SQLException {
        Map<String, Object> body = HttpSupport.readJsonBody(ex);
        String stationId = Json.getString(body, "stationId");
        try {
            CheckoutService.TransactionView tx = service.start(stationId);
            HttpSupport.respond(ex, 201, serializeTransaction(tx));
        } catch (CheckoutException e) {
            HttpSupport.respondError(ex, e.httpStatus, e.errorCode, e.getMessage());
        }
    }

    // ------------------------------------------------------------------ POST /transactions/{id}/items

    private void handleScan(HttpExchange ex, String transactionId) throws IOException, SQLException {
        Map<String, Object> body = HttpSupport.readJsonBody(ex);
        String sku = Json.getString(body, "sku");
        try {
            CheckoutService.ScanView scan = service.scan(transactionId, sku);
            HttpSupport.respond(ex, 200, serializeScanResult(scan));
        } catch (CheckoutException e) {
            HttpSupport.respondError(ex, e.httpStatus, e.errorCode, e.getMessage());
        }
    }

    // ------------------------------------------------------------------ POST /transactions/{id}/complete

    private void handleComplete(HttpExchange ex, String transactionId) throws IOException, SQLException {
        try {
            CheckoutService.ReceiptView receipt = service.complete(transactionId);
            HttpSupport.respond(ex, 200, serializeReceipt(receipt));
        } catch (CheckoutException e) {
            HttpSupport.respondError(ex, e.httpStatus, e.errorCode, e.getMessage());
        }
    }

    // ------------------------------------------------------------------ GET /transactions/{id} (T039/T040)

    private void handleGet(HttpExchange ex, String transactionId) throws IOException, SQLException {
        // Serve in-memory data for OPEN transactions.
        // T039/T040 will add the DB fallback for completed transactions.
        CheckoutService.TransactionView inMem = service.getInMemory(transactionId);
        if (inMem != null) {
            HttpSupport.respond(ex, 200, serializeTransaction(inMem));
            return;
        }
        HttpSupport.respondError(ex, 404, ApiErrors.TRANSACTION_NOT_FOUND,
            "Transaction not found: " + transactionId);
    }

    // ------------------------------------------------------------------ serializers

    private static String serializeTransaction(CheckoutService.TransactionView tx) {
        return Json.object(
            "transactionId", Json.quote(tx.transactionId()),
            "stationId",     Json.quote(tx.stationId()),
            "status",        Json.quote(tx.status()),
            "itemCount",     Json.number(tx.itemCount()),
            "runningTotal",  Json.number(tx.runningTotal()),
            "startedAt",     Json.quote(tx.startedAt().toString())
        );
    }

    private static String serializeScanResult(CheckoutService.ScanView scan) {
        return Json.object(
            "transactionId", Json.quote(scan.transactionId()),
            "sku",           Json.quote(scan.sku()),
            "name",          Json.quote(scan.name()),
            "unitPrice",     Json.number(scan.unitPrice()),
            "itemCount",     Json.number(scan.itemCount()),
            "runningTotal",  Json.number(scan.runningTotal())
        );
    }

    private static String serializeReceipt(CheckoutService.ReceiptView receipt) {
        List<String> lines = new ArrayList<>();
        for (CheckoutService.ReceiptLineView line : receipt.lines()) {
            lines.add(Json.object(
                "sku",       Json.quote(line.sku()),
                "name",      Json.quote(line.name()),
                "unitPrice", Json.number(line.unitPrice()),
                "quantity",  Json.number(line.quantity())
            ));
        }
        return Json.object(
            "transactionId", Json.quote(receipt.transactionId()),
            "stationId",     Json.quote(receipt.stationId()),
            "itemCount",     Json.number(receipt.itemCount()),
            "totalAmount",   Json.number(receipt.totalAmount()),
            "startedAt",     Json.quote(receipt.startedAt().toString()),
            "completedAt",   Json.quote(receipt.completedAt().toString()),
            "lines",         Json.array(lines)
        );
    }
}
