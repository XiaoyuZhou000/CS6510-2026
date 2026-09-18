package api;

import com.sun.net.httpserver.HttpExchange;
import json.Json;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class HttpSupport {

    // Path patterns for /transactions/{id}/... routes
    public static final Pattern TRANSACTIONS_ROOT     = Pattern.compile("^/transactions$");
    public static final Pattern TRANSACTIONS_ITEMS    = Pattern.compile("^/transactions/([^/]+)/items$");
    public static final Pattern TRANSACTIONS_COMPLETE = Pattern.compile("^/transactions/([^/]+)/complete$");
    public static final Pattern TRANSACTIONS_BY_ID    = Pattern.compile("^/transactions/([^/]+)$");

    private HttpSupport() {}

    /** Reads the entire request body as a UTF-8 string. */
    public static String readBody(HttpExchange ex) throws IOException {
        return new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    /** Parses the request body as a JSON object. */
    public static Map<String, Object> readJsonBody(HttpExchange ex) throws IOException {
        return Json.parseObject(readBody(ex));
    }

    /** Sends a JSON response with the given status code. */
    public static void respond(HttpExchange ex, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    /** Sends an ApiError JSON response. */
    public static void respondError(HttpExchange ex, int status, String errorCode, String message)
            throws IOException {
        respond(ex, status, ApiErrors.toJson(errorCode, message));
    }

    /** URL-decodes a path segment. */
    public static String urlDecode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    /**
     * Extracts a named query parameter from the request URI's query string.
     * Returns null if the parameter is absent.
     */
    public static String queryParam(HttpExchange ex, String name) {
        String query = ex.getRequestURI().getQuery();
        if (query == null) return null;
        for (String part : query.split("&")) {
            int eq = part.indexOf('=');
            if (eq < 0) continue;
            String key = URLDecoder.decode(part.substring(0, eq), StandardCharsets.UTF_8);
            if (key.equals(name)) {
                return URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    /** Extracts capture group 1 from a path pattern match. */
    public static String pathVar(Matcher m) {
        return urlDecode(m.group(1));
    }
}
