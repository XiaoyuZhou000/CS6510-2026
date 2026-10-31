package integration;

import api.TransactionHttpHandler;
import api.json.Json;
import com.sun.net.httpserver.HttpServer;
import events.EventTypes;
import org.junit.jupiter.api.Test;
import support.FakeInventoryReactor;
import support.FakeInventoryReactor.Reply;
import support.InMemoryEventBus;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.ServiceHttp;
import support.ServiceProcesses;
import support.TestSchema;
import support.TransactionTestSchema;
import transaction.CheckoutOrchestrator;
import transaction.TransactionOperations;

import java.net.InetSocketAddress;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * User Story 6 scenario 3, FR-016 and FR-024 (contracts/public-http-and-routing.md): every internal condition of
 * the mapping table reaches the client as the established response, through the real {@link TransactionHttpHandler}
 * on an HTTP port, over a Transaction wired as in production on an {@link InMemoryEventBus} whose broker can be
 * paused or made to refuse sends, with a {@link FakeInventoryReactor} answering checkout requests.
 *
 * <ul>
 *   <li>Broker unavailable or refusing publishes, and no outcome received: Complete answers 500 within its budget
 *       and the attempt stays pending, never cancelled; Start, Scan, and Lookup are unaffected.</li>
 *   <li>{@code StockRejected} with INVALID_REQUEST, {@code StockDeductionRefused}, and {@code StockRestored}: 500
 *       with the transaction OPEN; {@code StockRejected} with INSUFFICIENT_STOCK: 409 naming the SKU.</li>
 *   <li>A pending attempt still unsettled at the bound: Scan and Complete answer 500 and change nothing.</li>
 *   <li>No catalog replica yet: Scan answers 500, not 404.</li>
 *   <li>An internal failure is never answered as a validation error, whatever exception carries it.</li>
 *   <li>An owning service that is down: the gateway's 502 on each of its routes.</li>
 * </ul>
 *
 * No body, success or error, names Kafka, a topic, an offset, a partition, the broker, or an event type.
 */
class BrokerConditionErrorMappingTest {
    private static final String SKU = InventoryTestSchema.sku(1);
    /** A short budget keeps the 500 cases fast; the mapping does not depend on its length. */
    private static final CheckoutOrchestrator.Settings SETTINGS = CheckoutOrchestrator.Settings.defaults()
            .withResponseBudget(Duration.ofMillis(1_500)).withWaitBackstop(Duration.ofMillis(250));
    private static final List<String> MESSAGING_WORDS = messagingWords();

    @Test
    void anUnavailableOrRefusingBrokerAnswersComplete500AndLeavesStartScanAndLookupUnaffected() throws Exception {
        try (Fixture fixture = Fixture.start(FakeInventoryReactor::deducting)) {
            ServiceHttp http = fixture.http;
            for (String condition : List.of("paused", "refusing")) {
                if (condition.equals("paused")) fixture.bus.pauseBroker(true);
                else fixture.bus.refuseSends(true);

                int requestsBefore = fixture.reactor.requests();
                String id = started(http, "outage-" + condition);
                assertSuccess(http.scan(id, SKU), 200);
                assertSuccess(http.lookup(id), 200);
                assertInternal(timedComplete(http, id), condition);
                assertEquals("OPEN", ServiceHttp.json(assertSuccess(http.lookup(id), 200)).get("status"));
                assertEquals(1, fixture.pending(id), condition + ": the attempt stays pending");
                assertEquals(requestsBefore, fixture.reactor.requests(), condition + ": nothing reached Inventory");

                fixture.bus.pauseBroker(false);
                fixture.bus.refuseSends(false);
                // A 500 is never a cancellation: once the broker accepts the request, the attempt completes.
                fixture.awaitAttempt(id, "COMPLETED");
                assertEquals("COMPLETED", ServiceHttp.json(assertSuccess(http.lookup(id), 200)).get("status"));
                assertError(http.complete(id), 409, "TRANSACTION_NOT_OPEN");
            }
        }
    }

    @Test
    void aMissingOutcomeAnswers500AndThePendingAttemptFreezesScanAndCompleteAt500() throws Exception {
        try (Fixture fixture = Fixture.start((bus, names) -> FakeInventoryReactor.deducting(bus, names)
                .then(Reply.ignore()))) {
            ServiceHttp http = fixture.http;
            String id = started(http, "no-outcome");
            assertSuccess(http.scan(id, SKU), 200);
            assertSuccess(http.scan(id, SKU), 200);
            assertInternal(timedComplete(http, id), "no outcome");
            assertEquals(1, fixture.pending(id), "the attempt stays pending");

            long started = System.nanoTime();
            assertInternal(http.scan(id, SKU), "Scan on a pending attempt");
            assertWithinClientTimeout(started, "Scan");
            assertInternal(timedComplete(http, id), "Complete on a pending attempt");
            Map<String, Object> unchanged = ServiceHttp.json(assertSuccess(http.lookup(id), 200));
            assertEquals("OPEN", unchanged.get("status"));
            assertEquals(2, ((Number) unchanged.get("itemCount")).intValue(), "nothing joined the frozen basket");
            assertEquals(1, fixture.schema.attempts(id).size(), "no second attempt began");
        }
    }

    @Test
    void refusalsWithoutAPublicCodeAnswer500AndInsufficientStockAnswers409NamingTheSku() throws Exception {
        Map<String, Reply> refusals = new LinkedHashMap<>();
        refusals.put("StockRejected INVALID_REQUEST", Reply.invalid(SKU));
        refusals.put("StockDeductionRefused", Reply.refuse());
        refusals.put("StockRestored", Reply.restored());
        try (Fixture fixture = Fixture.start((bus, names) -> {
            FakeInventoryReactor reactor = FakeInventoryReactor.deducting(bus, names);
            refusals.values().forEach(reactor::then);
            return reactor.then(Reply.insufficient(SKU));
        })) {
            ServiceHttp http = fixture.http;
            for (String condition : refusals.keySet()) {
                String id = started(http, "refused");
                assertSuccess(http.scan(id, SKU), 200);
                assertInternal(timedComplete(http, id), condition);
                Map<String, Object> reopened = ServiceHttp.json(assertSuccess(http.lookup(id), 200));
                assertEquals("OPEN", reopened.get("status"), condition);
                assertEquals(1, ((Number) reopened.get("itemCount")).intValue(), condition);
                assertEquals(0, fixture.pending(id), condition + ": the attempt is settled");
            }

            String id = started(http, "insufficient");
            assertSuccess(http.scan(id, SKU), 200);
            HttpResponse<String> insufficient = http.complete(id);
            assertError(insufficient, 409, "INSUFFICIENT_STOCK");
            assertEquals("Insufficient stock for SKU: " + SKU, ServiceHttp.json(insufficient).get("message"));
            assertEquals("OPEN", ServiceHttp.json(assertSuccess(http.lookup(id), 200)).get("status"));
            assertEquals(refusals.size() + 1, fixture.reactor.requests());
        }
    }

    @Test
    void aScanWithoutACatalogReplicaAnswers500() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create(TestSchema.Options.defaults())) {
            var names = events.TopicNames.unprefixed();
            InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 6510);
            try (InProcessTransaction transaction = InProcessTransaction.over(schema).bus(bus, names)
                    .catalogFromEvents().settings(SETTINGS).build();
                 Served served = Served.over(transaction.service)) {
                String id = started(served.http, "no-catalog");
                assertInternal(served.http.scan(id, SKU), "no replica");
                Map<String, Object> lookup = ServiceHttp.json(assertSuccess(served.http.lookup(id), 200));
                assertEquals(0, ((Number) lookup.get("itemCount")).intValue());
            }
        }
    }

    @Test
    void internalFailuresAreNeverAnsweredAsValidationErrors() throws Exception {
        List<RuntimeException> failures = List.of(
                new IllegalArgumentException("No outbox row 7f3a for topic checkout"),
                new NumberFormatException("For input string: \"partition-3\""),
                new IllegalStateException("Kafka broker unavailable: offset 17 of partition 3"),
                new RuntimeException(new TimeoutException("StockDeducted not received")));
        for (RuntimeException failure : failures) {
            try (Served served = Served.over(new Failing(failure))) {
                ServiceHttp http = served.http;
                String context = failure.getClass().getSimpleName();
                assertInternal(http.post("/transactions", "{\"stationId\":\"internal\"}"), context + " Start");
                assertInternal(http.scan("tx-internal", SKU), context + " Scan");
                assertInternal(http.complete("tx-internal"), context + " Complete");
                assertInternal(http.lookup("tx-internal"), context + " Lookup");
                // Requests that are malformed keep their 400s, decided before the service is called.
                assertError(http.post("/transactions", "{\"stationId\":\"" + "s".repeat(41) + "\"}"), 400,
                        "INVALID_REQUEST");
                assertError(http.post("/transactions", "{not json"), 400, "INVALID_REQUEST");
                assertError(http.post("/transactions/tx-internal/items", "{\"sku\":\"" + "K".repeat(21) + "\"}"),
                        400, "INVALID_REQUEST");
                assertError(http.post("/transactions", "{}"), 400, "MISSING_STATION_ID");
                assertError(http.post("/transactions/tx-internal/items", "{}"), 400, "MISSING_SKU");
            }
        }
    }

    @Test
    void anOwningServiceThatIsDownAnswersTheGateways502OnEachOfItsRoutes() throws Exception {
        int transaction = ServiceProcesses.freePort();
        int inventory = ServiceProcesses.freePort();
        int analytics = ServiceProcesses.freePort();
        try (PublicRouterTestSupport router = PublicRouterTestSupport.start(transaction, inventory, analytics)) {
            ServiceHttp gateway = new ServiceHttp(router.baseUrl());
            for (HttpResponse<String> response : List.of(
                    gateway.post("/transactions", "{\"stationId\":\"down\"}"), gateway.scan("tx-down", SKU),
                    gateway.complete("tx-down"), gateway.lookup("tx-down"), gateway.get("/items"),
                    gateway.get("/inventory/low-stock"), gateway.get("/analytics/popular-items"))) {
                String route = response.request().method() + " " + response.uri().getPath();
                assertEquals(502, response.statusCode(), route);
                assertNoMessagingDetail(response, route);
            }
        }
    }

    // ---- assertions ----

    private static String started(ServiceHttp http, String station) throws Exception {
        HttpResponse<String> response = assertSuccess(http.post("/transactions",
                "{\"stationId\":\"" + station + "\"}"), 201);
        return ServiceHttp.json(response).get("transactionId").toString();
    }

    private static HttpResponse<String> timedComplete(ServiceHttp http, String id) throws Exception {
        long started = System.nanoTime();
        HttpResponse<String> response = http.complete(id);
        assertWithinClientTimeout(started, "Complete");
        return response;
    }

    private static void assertWithinClientTimeout(long startedNanos, String route) {
        long elapsed = System.nanoTime() - startedNanos;
        assertTrue(elapsed < TimeUnit.SECONDS.toNanos(10),
                route + " answered after " + TimeUnit.NANOSECONDS.toMillis(elapsed) + " ms");
    }

    private static HttpResponse<String> assertSuccess(HttpResponse<String> response, int status) {
        assertEquals(status, response.statusCode(), response.body());
        assertNoMessagingDetail(response, response.uri().getPath());
        return response;
    }

    private static void assertInternal(HttpResponse<String> response, String context) {
        assertEquals(500, response.statusCode(), context + ": " + response.body());
        assertEquals(Map.of("error", "INTERNAL_ERROR", "message", "Internal server error"),
                Json.parseObject(response.body()), context);
        assertNoMessagingDetail(response, context);
    }

    private static void assertError(HttpResponse<String> response, int status, String error) {
        assertEquals(status, response.statusCode(), response.body());
        Map<String, Object> body = Json.parseObject(response.body());
        assertEquals(List.of("error", "message"), body.keySet().stream().sorted().toList(), response.body());
        assertEquals(error, body.get("error"), response.body());
        assertNoMessagingDetail(response, error);
    }

    private static void assertNoMessagingDetail(HttpResponse<String> response, String context) {
        String body = response.body().toLowerCase(Locale.ROOT);
        for (String word : MESSAGING_WORDS) {
            assertFalse(body.contains(word), context + " revealed '" + word + "': " + response.body());
        }
    }

    private static List<String> messagingWords() {
        List<String> words = new ArrayList<>(List.of("kafka", "topic", "offset", "partition", "broker"));
        for (EventTypes type : EventTypes.values()) words.add(type.typeName().toLowerCase(Locale.ROOT));
        return List.copyOf(words);
    }

    // ---- fixtures ----

    @FunctionalInterface
    private interface ReactorFactory {
        FakeInventoryReactor create(InMemoryEventBus bus, events.TopicNames names);
    }

    /** A seeded Transaction schema, its service on the bus behind HTTP, and a scripted Inventory. */
    private static final class Fixture implements AutoCloseable {
        final TransactionTestSchema schema;
        final InMemoryEventBus bus;
        final InProcessTransaction transaction;
        final FakeInventoryReactor reactor;
        final Served served;
        final ServiceHttp http;

        private Fixture(TransactionTestSchema schema, InMemoryEventBus bus, InProcessTransaction transaction,
                        FakeInventoryReactor reactor, Served served) {
            this.schema = schema;
            this.bus = bus;
            this.transaction = transaction;
            this.reactor = reactor;
            this.served = served;
            this.http = served.http;
        }

        static Fixture start(ReactorFactory reactors) throws Exception {
            TransactionTestSchema schema = TransactionTestSchema.create(TestSchema.Options.defaults());
            try {
                schema.seedReplica(5, UUID.randomUUID().toString());
                var names = events.TopicNames.unprefixed();
                InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 6510);
                InProcessTransaction transaction = InProcessTransaction.over(schema).bus(bus, names)
                        .settings(SETTINGS).build();
                FakeInventoryReactor reactor = reactors.create(bus, names).start();
                return new Fixture(schema, bus, transaction, reactor, Served.over(transaction.service));
            } catch (Exception | Error failure) {
                schema.close();
                throw failure;
            }
        }

        long pending(String id) throws Exception {
            return schema.attempts(id).stream().filter(row -> row.state().equals("PENDING")).count();
        }

        void awaitAttempt(String id, String state) throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (schema.attempts(id).stream().noneMatch(row -> row.state().equals(state))) {
                if (System.nanoTime() > deadline) fail("no " + state + " attempt for " + id + ": " + schema.attempts(id));
                Thread.sleep(50);
            }
        }

        @Override
        public void close() throws Exception {
            try (schema) {
                served.close();
                reactor.close();
                transaction.close();
            }
        }
    }

    /** The production HTTP handler for {@code /transactions} on a loopback port. */
    private static final class Served implements AutoCloseable {
        final HttpServer server;
        final ServiceHttp http;

        private Served(HttpServer server) {
            this.server = server;
            this.http = new ServiceHttp(server.getAddress().getPort());
        }

        static Served over(TransactionOperations operations) throws Exception {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/transactions", new TransactionHttpHandler(operations));
            server.start();
            return new Served(server);
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    /** Every operation fails with one internal exception. */
    private record Failing(RuntimeException failure) implements TransactionOperations {
        @Override
        public TransactionView start(StartCommand command) { throw failure; }

        @Override
        public ScanView scan(ScanCommand command) { throw failure; }

        @Override
        public ReceiptView complete(String transactionId) { throw failure; }

        @Override
        public TransactionView get(String transactionId) { throw failure; }
    }
}
