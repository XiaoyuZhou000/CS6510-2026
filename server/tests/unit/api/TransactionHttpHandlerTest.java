package unit.api;

import api.TransactionHttpHandler;
import api.json.Json;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import database.StoreFailure;
import org.junit.jupiter.api.Test;
import transaction.CheckoutPendingFailure;
import transaction.CheckoutRejectedFailure;
import transaction.DependencyUnavailableFailure;
import transaction.ProductCatalogUnavailableFailure;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TransactionHttpHandlerTest {
    private static final Instant STARTED = Instant.parse("2026-09-23T10:15:30Z");
    private static final Instant COMPLETED = Instant.parse("2026-09-23T10:16:30Z");

    @Test
    void validatesStartBeforeDelegationAndSerializesSynchronousSuccess() throws Exception {
        FakeTransactions transactions = new FakeTransactions();
        TransactionHttpHandler handler = new TransactionHttpHandler(transactions);

        TestExchange invalid = TestExchange.request("POST", "/transactions", "{}");
        handler.handle(invalid);
        assertEquals(400, invalid.status());
        assertEquals("MISSING_STATION_ID", invalid.json().get("error"));
        assertEquals(0, transactions.startCalls);

        TestExchange valid = TestExchange.request(
                "POST", "/transactions", "{\"stationId\":\"station-7\"}");
        handler.handle(valid);
        assertEquals(201, valid.status());
        assertEquals("application/json", valid.getResponseHeaders().getFirst("Content-Type"));
        assertEquals("station-7", transactions.lastStart.stationId());
        assertEquals("tx-1", valid.json().get("transactionId"));
        assertEquals(0.0, ((Number) valid.json().get("runningTotal")).doubleValue());
    }

    @Test
    void delegatesScanLookupAndCompletionAndSerializesTheirDistinctShapes() throws Exception {
        FakeTransactions transactions = new FakeTransactions();
        TransactionHttpHandler handler = new TransactionHttpHandler(transactions);

        TestExchange scan = TestExchange.request(
                "POST", "/transactions/tx-1/items", "{\"sku\":\"A\"}");
        handler.handle(scan);
        assertEquals(200, scan.status());
        assertEquals(new TransactionOperations.ScanCommand("tx-1", "A"), transactions.lastScan);
        assertEquals("Apple", scan.json().get("name"));
        assertEquals(1.25, ((Number) scan.json().get("unitPrice")).doubleValue());

        TestExchange lookup = TestExchange.request("GET", "/transactions/tx-1", "");
        handler.handle(lookup);
        assertEquals(200, lookup.status());
        assertEquals("tx-1", transactions.lastGet);
        assertEquals("OPEN", lookup.json().get("status"));

        TestExchange completion = TestExchange.request(
                "POST", "/transactions/tx-1/complete", "{}");
        handler.handle(completion);
        assertEquals(200, completion.status());
        assertEquals("tx-1", transactions.lastComplete);
        assertEquals(2.50, ((Number) completion.json().get("totalAmount")).doubleValue());
        assertEquals(1, ((List<?>) completion.json().get("lines")).size());
    }

    @Test
    void mapsDomainFailuresAndSanitizesInfrastructureFailures() throws Exception {
        FakeTransactions transactions = new FakeTransactions();
        TransactionHttpHandler handler = new TransactionHttpHandler(transactions);

        transactions.failure = new TransactionFailure(
                TransactionFailure.Code.UNKNOWN_SKU, "SKU not found: secret-sku");
        TestExchange domain = TestExchange.request(
                "POST", "/transactions/tx-1/items", "{\"sku\":\"secret-sku\"}");
        handler.handle(domain);
        assertEquals(404, domain.status());
        assertEquals("SKU_NOT_FOUND", domain.json().get("error"));
        assertEquals("SKU not found: secret-sku", domain.json().get("message"));

        transactions.failure = new IllegalStateException("jdbc:mysql://root:password@private");
        TestExchange infrastructure = TestExchange.request("GET", "/transactions/tx-1", "");
        handler.handle(infrastructure);
        assertEquals(500, infrastructure.status());
        assertEquals("INTERNAL_ERROR", infrastructure.json().get("error"));
        assertEquals("Internal server error", infrastructure.json().get("message"));
        assertFalse(infrastructure.body().contains("password"));
    }

    @Test
    void internalCheckoutCatalogAndDependencyFailuresAnswerSanitized500() throws Exception {
        FakeTransactions transactions = new FakeTransactions();
        TransactionHttpHandler handler = new TransactionHttpHandler(transactions);
        StoreFailure sql = new StoreFailure("SQLException: Table 'cs6510_transaction.checkout_attempt'",
                new IllegalStateException("jdbc:mysql://cs6510_transaction_svc:secret@127.0.0.1:3307"), true);
        Map<String, RuntimeException> failures = new LinkedHashMap<>();
        failures.put("pending", new CheckoutPendingFailure("tx-1"));
        failures.put("catalog", new ProductCatalogUnavailableFailure());
        failures.put("dependency", new DependencyUnavailableFailure(
                "io.grpc.StatusRuntimeException: UNAVAILABLE: io exception",
                new IllegalStateException("DEADLINE_EXCEEDED after 2s on selfcheckout.inventory.v1")));
        failures.put("invalid request", new CheckoutRejectedFailure("tx-1", "INVALID_REQUEST", "SKU-000042"));
        failures.put("stock restored", new CheckoutRejectedFailure("tx-1", "STOCK_RESTORED", null));
        failures.put("store", sql);
        failures.put("unexpected", new IllegalStateException("NullPointerException in grpc-default-executor"));

        for (Map.Entry<String, RuntimeException> failure : failures.entrySet()) {
            transactions.failure = failure.getValue();
            for (TestExchange exchange : List.of(
                    TestExchange.request("POST", "/transactions/tx-1/complete", ""),
                    TestExchange.request("POST", "/transactions/tx-1/items", "{\"sku\":\"SKU-000042\"}"),
                    TestExchange.request("GET", "/transactions/tx-1", ""),
                    TestExchange.request("POST", "/transactions", "{\"stationId\":\"station-7\"}"))) {
                handler.handle(exchange);
                String context = failure.getKey() + " " + exchange.getRequestMethod() + " "
                        + exchange.getRequestURI();
                assertEquals(500, exchange.status(), context);
                assertEquals("application/json", exchange.getResponseHeaders().getFirst("Content-Type"), context);
                assertEquals(Map.of("error", "INTERNAL_ERROR", "message", "Internal server error"),
                        exchange.json(), context);
                for (String leak : List.of("grpc", "UNAVAILABLE", "DEADLINE", "SQL", "jdbc", "cs6510",
                        "secret", "SKU-000042", "STOCK_RESTORED", "INVALID_REQUEST", "pending", "tx-1")) {
                    assertFalse(exchange.body().contains(leak), context + " leaked " + leak);
                }
            }
        }
    }

    @Test
    void publicClientErrorMappingsAreUnchanged() throws Exception {
        FakeTransactions transactions = new FakeTransactions();
        TransactionHttpHandler handler = new TransactionHttpHandler(transactions);

        TestExchange missingSku = TestExchange.request("POST", "/transactions/tx-1/items", "{}");
        handler.handle(missingSku);
        assertEquals(400, missingSku.status());
        assertEquals("MISSING_SKU", missingSku.json().get("error"));
        assertNull(transactions.lastScan, "a 400 is decided before delegation");

        TestExchange blankStation = TestExchange.request("POST", "/transactions", "{\"stationId\":\" \"}");
        handler.handle(blankStation);
        assertEquals(400, blankStation.status());
        assertEquals("MISSING_STATION_ID", blankStation.json().get("error"));

        Map<TransactionFailure.Code, Object[]> expected = new EnumMap<>(TransactionFailure.Code.class);
        expected.put(TransactionFailure.Code.NOT_FOUND, new Object[] {404, "TRANSACTION_NOT_FOUND"});
        expected.put(TransactionFailure.Code.UNKNOWN_SKU, new Object[] {404, "SKU_NOT_FOUND"});
        expected.put(TransactionFailure.Code.NOT_OPEN, new Object[] {409, "TRANSACTION_NOT_OPEN"});
        expected.put(TransactionFailure.Code.EMPTY_BASKET, new Object[] {409, "EMPTY_BASKET"});
        expected.put(TransactionFailure.Code.INSUFFICIENT_STOCK, new Object[] {409, "INSUFFICIENT_STOCK"});
        assertEquals(EnumSet.allOf(TransactionFailure.Code.class), expected.keySet(), "every domain code is mapped");
        for (Map.Entry<TransactionFailure.Code, Object[]> mapping : expected.entrySet()) {
            String message = "message for " + mapping.getKey();
            transactions.failure = new TransactionFailure(mapping.getKey(), message);
            TestExchange exchange = TestExchange.request("POST", "/transactions/tx-1/complete", "{}");
            handler.handle(exchange);
            assertEquals(mapping.getValue()[0], exchange.status(), mapping.getKey().name());
            assertEquals(mapping.getValue()[1], exchange.json().get("error"), mapping.getKey().name());
            assertEquals(message, exchange.json().get("message"), mapping.getKey().name());
        }
    }

    @Test
    void rejectsMethodMismatchWithoutCallingTheBusinessContract() throws Exception {
        FakeTransactions transactions = new FakeTransactions();
        TestExchange exchange = TestExchange.request("PUT", "/transactions", "{}");

        new TransactionHttpHandler(transactions).handle(exchange);

        assertEquals(405, exchange.status());
        assertEquals(0, transactions.totalCalls());
    }

    private static final class FakeTransactions implements TransactionOperations {
        private int startCalls;
        private StartCommand lastStart;
        private ScanCommand lastScan;
        private String lastComplete;
        private String lastGet;
        private RuntimeException failure;

        @Override
        public TransactionView start(StartCommand command) {
            startCalls++;
            lastStart = command;
            failIfConfigured();
            return transaction();
        }

        @Override
        public ScanView scan(ScanCommand command) {
            lastScan = command;
            failIfConfigured();
            return new ScanView(command.transactionId(), command.sku(), "Apple",
                    new BigDecimal("1.25"), 1, new BigDecimal("1.25"));
        }

        @Override
        public ReceiptView complete(String transactionId) {
            lastComplete = transactionId;
            failIfConfigured();
            return new ReceiptView(transactionId, "station-7", 2, new BigDecimal("2.50"),
                    STARTED, COMPLETED,
                    List.of(new ReceiptLineView("A", "Apple", new BigDecimal("1.25"), 2)));
        }

        @Override
        public TransactionView get(String transactionId) {
            lastGet = transactionId;
            failIfConfigured();
            return transaction();
        }

        private TransactionView transaction() {
            return new TransactionView("tx-1", "station-7", TransactionStatus.OPEN,
                    0, new BigDecimal("0.00"), STARTED);
        }

        private void failIfConfigured() {
            if (failure != null) throw failure;
        }

        private int totalCalls() {
            return startCalls + (lastScan == null ? 0 : 1)
                    + (lastComplete == null ? 0 : 1) + (lastGet == null ? 0 : 1);
        }
    }
}

/** Minimal in-memory exchange used by all API-layer isolation tests. */
final class TestExchange extends HttpExchange {
    private final Headers requestHeaders = new Headers();
    private final Headers responseHeaders = new Headers();
    private final Map<String, Object> attributes = new HashMap<>();
    private final URI uri;
    private final String method;
    private InputStream requestBody;
    private OutputStream responseBody = new ByteArrayOutputStream();
    private int status = -1;

    private TestExchange(String method, String uri, String body) {
        this.method = method;
        this.uri = URI.create(uri);
        this.requestBody = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
    }

    static TestExchange request(String method, String uri, String body) {
        return new TestExchange(method, uri, body);
    }

    int status() {
        return status;
    }

    String body() {
        return ((ByteArrayOutputStream) responseBody).toString(StandardCharsets.UTF_8);
    }

    Map<String, Object> json() {
        return Json.parseObject(body());
    }

    @Override public Headers getRequestHeaders() { return requestHeaders; }
    @Override public Headers getResponseHeaders() { return responseHeaders; }
    @Override public URI getRequestURI() { return uri; }
    @Override public String getRequestMethod() { return method; }
    @Override public HttpContext getHttpContext() { return null; }
    @Override public void close() { }
    @Override public InputStream getRequestBody() { return requestBody; }
    @Override public OutputStream getResponseBody() { return responseBody; }
    @Override public void sendResponseHeaders(int responseCode, long responseLength) { status = responseCode; }
    @Override public InetSocketAddress getRemoteAddress() { return new InetSocketAddress(0); }
    @Override public int getResponseCode() { return status; }
    @Override public InetSocketAddress getLocalAddress() { return new InetSocketAddress(0); }
    @Override public String getProtocol() { return "HTTP/1.1"; }
    @Override public Object getAttribute(String name) { return attributes.get(name); }
    @Override public void setAttribute(String name, Object value) { attributes.put(name, value); }
    @Override public void setStreams(InputStream input, OutputStream output) {
        requestBody = input;
        responseBody = output;
    }
    @Override public HttpPrincipal getPrincipal() { return null; }
}
