package unit.api;

import api.TransactionHttpHandler;
import api.json.Json;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import org.junit.jupiter.api.Test;
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
import java.util.HashMap;
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
