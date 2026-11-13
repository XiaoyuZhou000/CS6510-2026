package integration;

import api.TransactionHttpHandler;
import com.sun.net.httpserver.HttpServer;
import events.TopicNames;
import faults.FaultHooks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import support.InMemoryEventBus;
import support.InProcessInventory;
import support.InProcessNotification;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.NotificationTestSchema;
import support.ServiceHttp;
import support.TestSchema;
import support.TransactionTestSchema;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 007 US6 acceptance scenarios 1 and 2 (T104, FR-031, SC-008): Transaction does not depend on Notification. The real
 * Transaction (behind its own {@link TransactionHttpHandler}) and Inventory run over one {@link InMemoryEventBus}.
 * <ul>
 *   <li>With Notification stopped, one fixed script of Start, Scan, Lookup, and Complete requests, successes and errors,
 *       gets the same responses (IDs and times normalized), the same Transaction commits (apart from the outbox marker's
 *       timing-dependent batching), and the same rows as on a
 *       deployment that has no Notification at all, as in 006. After Notification starts, every sale is selected
 *       once.</li>
 *   <li>With a slow Notification consumer (each selection waits 2 s before its commit), Complete latency is unchanged
 *       within the tolerance, and no sale event stays unpublished: the events wait on the bus, not in the outbox.</li>
 * </ul>
 */
class NotificationOutageCheckoutTest {
    private static final int PRODUCTS = 5;
    private static final int STOCK = 100;
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final Duration WAIT = Duration.ofSeconds(60);
    /** Median Complete latency may grow by at most this much with the slow consumer. */
    private static final long TOLERANCE_MS = 150;

    @AfterEach
    void clearFaults() {
        System.clearProperty(FaultHooks.PROPERTY);
    }

    @Test
    void withNotificationStoppedCheckoutAnswersAndCommitsAsWithoutItAndEverySaleIsSelectedOnceAfterItStarts()
            throws Exception {
        Observed without;
        try (Deployment deployment = Deployment.start()) {
            without = deployment.runScript();
        }

        Observed stopped;
        try (Deployment deployment = Deployment.start();
             NotificationTestSchema schema = NotificationTestSchema.create()) {
            InProcessNotification notification = InProcessNotification.over(schema.pool(), deployment.bus, deployment.names);
            try {
                deployment.inventory.publishCatalog();
                notification.awaitCatalog(WAIT);
                assertTrue(notification.service().register("outage-s1", List.of(A, B)).state().channelReady());
                Observed[] during = new Observed[1];
                notification.crash(() -> {
                    try {
                        during[0] = deployment.runScript();
                    } catch (Exception failure) {
                        throw new IllegalStateException(failure);
                    }
                });
                stopped = during[0];

                // Started again: each of the script's sales is selected exactly once (sale_selection is keyed by sale).
                long sales = deployment.transactionSchema.queryLong(
                        "SELECT COUNT(*) FROM outbox_event WHERE event_type = 'SaleCompleted'");
                assertEquals(stopped.completedSales().size(), sales);
                await(() -> query(schema::saleSelections) == sales, "every sale is selected after Notification starts");
                Thread.sleep(300);
                assertEquals(sales, schema.saleSelections(), "no sale is selected twice");
                for (String sale : stopped.completedSales()) {
                    assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM sale_selection WHERE sale_id = ?", sale), sale);
                }
                await(() -> query(schema::notifications) > 0 && query(() -> schema.notifications("DELIVERED")) == query(schema::notifications),
                        "the owed notifications are delivered");
            } finally {
                notification.close();
            }
        }

        assertEquals(without.responses(), stopped.responses(), "the same responses with Notification stopped");
        assertEquals(without.commits(), stopped.commits(), "the same Transaction commits");
        assertEquals(without.rows(), stopped.rows(), "the same Transaction and Inventory rows");
    }

    @Test
    void aSlowNotificationConsumerLeavesCompleteLatencyUnchangedAndNoSaleEventUnpublished() throws Exception {
        try (Deployment deployment = Deployment.start();
             NotificationTestSchema schema = NotificationTestSchema.create()) {
            InProcessNotification notification = InProcessNotification.over(schema.pool(), deployment.bus, deployment.names);
            try {
                deployment.inventory.publishCatalog();
                notification.awaitCatalog(WAIT);
                assertTrue(notification.service().register("slow-s1", List.of(A)).state().channelReady());
                ServiceHttp http = deployment.http;
                for (int i = 0; i < 3; i++) deployment.sale(http, "warm-" + i, A);   // JIT and pool warm-up

                List<Long> healthy = new ArrayList<>();
                for (int i = 0; i < 10; i++) healthy.add(deployment.sale(http, "healthy-" + i, A));
                long healthySales = 13;
                await(() -> query(schema::saleSelections) == healthySales, "the healthy sales are selected");

                System.setProperty(FaultHooks.PROPERTY, "notification.selection.beforeCommit:delay:2000");
                List<Long> slow = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    slow.add(deployment.sale(http, "slow-" + i, A));
                    await(() -> query(deployment.transactionSchema::unpublishedOutboxCount) == 0,
                            "the sale event is published while Notification is behind");
                }
                long selected = schema.saleSelections();
                assertTrue(selected < healthySales + 10, "Notification is behind: " + selected + " selected");
                assertEquals(0, deployment.transactionSchema.queryLong("SELECT COUNT(*) FROM outbox_event "
                        + "WHERE event_type = 'SaleCompleted' AND published_at IS NULL"), "saleEventsUnpublished stays 0");

                long healthyMedian = median(healthy);
                long slowMedian = median(slow);
                System.out.println("[slow-consumer] Complete median healthy " + healthyMedian + " ms, slow " + slowMedian
                        + " ms, slow max " + slow.stream().mapToLong(Long::longValue).max().orElse(0) + " ms");
                assertTrue(slowMedian <= healthyMedian + TOLERANCE_MS,
                        "Complete median " + slowMedian + " ms vs " + healthyMedian + " ms healthy");
                assertTrue(slow.stream().allMatch(ms -> ms < 2_000), "no Complete waits for a selection: " + slow);

                System.clearProperty(FaultHooks.PROPERTY);
                await(() -> query(schema::saleSelections) == healthySales + 10, "the backlog drains");
                assertEquals(healthySales + 10, schema.notifications(), "one notification per sale for slow-s1");
            } finally {
                notification.close();
            }
        }
    }

    /** What one run of the script observed: responses, Transaction commits, and rows. */
    private record Observed(List<String> responses, long commits, Map<String, Long> rows, List<String> completedSales) {
    }

    /** Transaction (over HTTP, with its connections counted) and Inventory on one bus. */
    private static final class Deployment implements AutoCloseable {
        final TransactionTestSchema transactionSchema;
        final InventoryTestSchema inventorySchema;
        final TopicNames names = TopicNames.unprefixed();
        final InMemoryEventBus bus;
        /** Commits on every thread but the outbox marker (see {@link #countCommitsOutsideTheMarker}). */
        final AtomicLong commits = new AtomicLong();
        final InProcessInventory inventory;
        final InProcessTransaction transaction;
        final HttpServer server;
        final ServiceHttp http;

        private Deployment() throws Exception {
            transactionSchema = TransactionTestSchema.create(TestSchema.Options.defaults().pool(4, 24));
            inventorySchema = InventoryTestSchema.create(TestSchema.Options.defaults().pool(4, 24));
            transactionSchema.seedReplica(PRODUCTS, inventorySchema.seed(PRODUCTS, STOCK));
            bus = InMemoryEventBus.withCatalogTopics(names, 6510);
            inventory = InProcessInventory.over(inventorySchema, bus, names);
            transaction = InProcessTransaction.over(transactionSchema).pool(4, 24)
                    .decorator(this::countCommitsOutsideTheMarker).bus(bus, names).build();
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/transactions", new TransactionHttpHandler(transaction.service));
            server.start();
            http = new ServiceHttp(server.getAddress().getPort());
        }

        static Deployment start() throws Exception {
            return new Deployment();
        }

        /**
         * Counts the commits of every thread except the outbox dispatcher's marker. The marker gathers confirmations
         * for up to {@code OUTBOX_MARK_BATCH_MS} before marking them published, so whether a sale's
         * {@code CheckoutRequested} and {@code SaleCompleted} rows are marked in one commit or two depends on timing
         * alone, in either run. The rows themselves are compared, and every one is published before the next request.
         */
        private Connection countCommitsOutsideTheMarker(Connection connection) {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("commit") && !Thread.currentThread().getName().endsWith("-outbox-marker")) {
                            commits.incrementAndGet();
                        }
                        try {
                            return method.invoke(connection, args);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
        }

        /**
         * One fixed script, each request after the previous one's effects settled (no pending attempt, nothing
         * unpublished), so that the commit count does not depend on batching.
         */
        Observed runScript() throws Exception {
            settle();
            long commitsBefore = commits.get();
            List<String> responses = new ArrayList<>();
            List<String> sales = new ArrayList<>();
            Map<String, String> ids = new LinkedHashMap<>();

            String first = record(responses, ids, http.post("/transactions", "{\"stationId\":\"script-1\"}"), "T1");
            record(responses, ids, http.scan(first, A), null);
            record(responses, ids, http.scan(first, A), null);
            record(responses, ids, http.scan(first, B), null);
            record(responses, ids, http.scan(first, "SKU-999999"), null);
            record(responses, ids, http.post("/transactions/" + first + "/items", "{}"), null);
            record(responses, ids, http.lookup(first), null);
            record(responses, ids, http.complete(first), null);
            sales.add(first);
            record(responses, ids, http.complete(first), null);
            record(responses, ids, http.scan(first, A), null);
            record(responses, ids, http.lookup(first), null);

            String empty = record(responses, ids, http.post("/transactions", "{\"stationId\":\"script-2\"}"), "T2");
            record(responses, ids, http.complete(empty), null);
            record(responses, ids, http.post("/transactions", "{}"), null);
            record(responses, ids, http.complete("missing-transaction"), null);
            record(responses, ids, http.lookup("missing-transaction"), null);

            String second = record(responses, ids, http.post("/transactions", "{\"stationId\":\"script-3\"}"), "T3");
            record(responses, ids, http.scan(second, B), null);
            record(responses, ids, http.complete(second), null);
            sales.add(second);
            record(responses, ids, http.lookup(second), null);

            long scriptCommits = commits.get() - commitsBefore;
            return new Observed(responses, scriptCommits, rows(), sales);
        }

        /** One sale of one unit of {@code sku}, its Complete latency in ms; returns after its effects settled. */
        long sale(ServiceHttp client, String station, String sku) throws Exception {
            String id = client.start(station);
            assertEquals(200, client.scan(id, sku).statusCode());
            long started = System.nanoTime();
            HttpResponse<String> response = client.complete(id);
            long elapsed = Duration.ofNanos(System.nanoTime() - started).toMillis();
            assertEquals(200, response.statusCode(), response.body());
            return elapsed;
        }

        private String record(List<String> responses, Map<String, String> ids, HttpResponse<String> response,
                              String label) throws Exception {
            String id = null;
            if (label != null) {
                assertEquals(201, response.statusCode(), response.body());
                id = (String) ServiceHttp.json(response).get("transactionId");
                ids.put(id, label);
            }
            String body = response.body();
            for (Map.Entry<String, String> entry : ids.entrySet()) body = body.replace(entry.getKey(), entry.getValue());
            body = body.replaceAll("\\d{4}-\\d{2}-\\d{2}T[0-9:.]+Z", "<time>");
            String path = response.uri().getPath();
            for (Map.Entry<String, String> entry : ids.entrySet()) path = path.replace(entry.getKey(), entry.getValue());
            responses.add(response.request().method() + " " + path + " -> " + response.statusCode() + " " + body);
            settle();
            return id;
        }

        private void settle() throws Exception {
            await(() -> query(transactionSchema::pendingAttemptCount) == 0
                    && query(transactionSchema::unpublishedOutboxCount) == 0, "the request's effects settle");
        }

        private Map<String, Long> rows() throws Exception {
            Map<String, Long> rows = new LinkedHashMap<>();
            for (String table : List.of("`transaction`", "accepted_scan", "checkout_attempt", "outbox_event")) {
                rows.put(table, transactionSchema.queryLong("SELECT COUNT(*) FROM " + table));
            }
            rows.put("SaleCompleted", transactionSchema.queryLong(
                    "SELECT COUNT(*) FROM outbox_event WHERE event_type = 'SaleCompleted'"));
            rows.put("stock " + A, (long) inventorySchema.stock(A));
            rows.put("stock " + B, (long) inventorySchema.stock(B));
            return rows;
        }

        @Override
        public void close() throws Exception {
            try (transactionSchema; inventorySchema) {
                server.stop(0);
                transaction.close();
                inventory.close();
            }
        }
    }

    private static long median(List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(null);
        return sorted.get(sorted.size() / 2);
    }

    private interface Query {
        long get() throws Exception;
    }

    private static long query(Query query) {
        try {
            return query.get();
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "timed out waiting: " + what);
            Thread.sleep(20);
        }
    }
}
