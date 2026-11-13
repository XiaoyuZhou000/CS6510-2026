package integration;

import api.json.Json;
import database.JdbcNotificationStore;
import events.ConsumerLoop;
import events.EnvelopeCodec;
import events.ParkingPolicy;
import events.RecordSender;
import faults.FaultHooks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.SubscriberNotified;
import support.InProcessCheckout;
import support.InProcessNotification;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.NotificationTestSchema;
import transaction.TransactionOperations;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The fault and recovery matrix of contracts/notification-delivery.md (007 T094, T102, US5, SC-005) on the real
 * Transaction, Inventory, and Notification components over one {@link support.InMemoryEventBus}. Each row injects
 * its fault once, recovers the way the matrix says (a restart of the crashed component without the fault, a
 * redelivery, or the outbox sweep), and then checks the outcome: one {@code sale_selection} per sale, one
 * {@code notification} row per (sale, SKU, subscriber), every row delivered, and the expected number of redeliveries
 * on the subscribers' channels.
 *
 * <p>A crash is a {@code throw} hook (a {@code halt} would end this JVM) followed by a stop of the component as a
 * killed process would leave it and a fresh start without the fault; {@link FaultHooks#fired} says when the armed
 * hook took effect. The consumers retry a failed record up to 1,000 times here, so a record never parks while a
 * crash is being staged.</p>
 *
 * <p>Subscriptions: s1 = {A, B}, s2 = {B}, s3 = {A}. Each sale is 2 x A + 1 x B, so it owes four notifications:
 * (s1, A, 2), (s1, B, 1), (s2, B, 1), and (s3, A, 2).</p>
 */
class NotificationCrashMatrixTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final int OWED = 4;
    private static final Map<String, String> RETRY_FOREVER = Map.of("CONSUMER_MAX_ATTEMPTS", "1000");

    @AfterEach
    void clearFaults() {
        System.clearProperty(FaultHooks.PROPERTY);
    }

    /** Crash before the T3 commit: no completion and no event until Transaction restarts; then one set. */
    @Test
    void aTransactionCrashBeforeTheT3CommitGivesOneSetAfterRecovery() throws Exception {
        try (Matrix matrix = Matrix.start(RETRY_FOREVER)) {
            String hook = "transaction.sale.beforeT3Commit";
            long fired = FaultHooks.fired(hook);
            System.setProperty(FaultHooks.PROPERTY, hook + ":throw");
            Future<?> complete = matrix.completeInBackground(matrix.basket());
            matrix.await(() -> FaultHooks.fired(hook) > fired, "the T3 fault fires");
            assertEquals(0, matrix.saleEvents(), "nothing was committed, so no event exists");
            matrix.restartTransaction(UnaryOperator.identity());
            matrix.awaitSettled(complete);

            String sale = matrix.lastSale;
            assertEquals("COMPLETED", matrix.saga.transactionSchema.attempts(sale).get(0).state());
            matrix.assertExactlyOnce(List.of(sale), 0);
            assertEquals(1, matrix.saleEvents(), "T3 recorded the event once");
        }
    }

    /** Crash after the T3 commit, before the outbox send: the sweep publishes the row; one set. */
    @Test
    void aTransactionCrashAfterTheT3CommitIsPublishedByTheSweep() throws Exception {
        try (Matrix matrix = Matrix.start(RETRY_FOREVER)) {
            String hook = "transaction.sale.afterT3Commit";
            long fired = FaultHooks.fired(hook);
            System.setProperty(FaultHooks.PROPERTY, hook + ":throw");
            Future<?> complete = matrix.completeInBackground(matrix.basket());
            matrix.await(() -> FaultHooks.fired(hook) > fired, "the fault after T3 fires");
            matrix.restartTransaction(UnaryOperator.identity());
            matrix.awaitSettled(complete);

            matrix.assertExactlyOnce(List.of(matrix.lastSale), 0);
            assertEquals(1, matrix.saleEvents());
        }
    }

    /** The broker stored the event but its confirmation was lost: the outbox re-sends it, and Notification no-ops. */
    @Test
    void aLostPublishConfirmationGivesADuplicateEventAndOneSet() throws Exception {
        try (Matrix matrix = Matrix.start(RETRY_FOREVER)) {
            AtomicBoolean lose = new AtomicBoolean(true);
            matrix.restartTransaction(sender -> (topic, key, value) -> {
                CompletableFuture<Void> stored = sender.send(topic, key, value);
                if (topic.equals(matrix.saga.names.saleCompleted()) && lose.getAndSet(false)) {
                    return stored.thenCompose(ignored ->
                            CompletableFuture.failedFuture(new IllegalStateException("confirmation lost")));
                }
                return stored;
            });
            String sale = matrix.sell();
            matrix.await(() -> matrix.saleEvents() == 2, "the outbox re-sent the unconfirmed event");
            matrix.assertExactlyOnce(List.of(sale), 0);
            assertEquals(2, matrix.saleEvents(), "two records of one sale on the topic");
        }
    }

    /** A duplicated {@code SaleCompleted}, delivered after the first was selected and delivered: a no-op. */
    @Test
    void aDuplicatedSaleCompletedIsANoOp() throws Exception {
        try (Matrix matrix = Matrix.start(RETRY_FOREVER)) {
            String sale = matrix.sell();
            matrix.assertExactlyOnce(List.of(sale), 0);
            byte[] event = matrix.saga.bus.values(matrix.saga.names.saleCompleted()).get(0);
            matrix.saga.bus.send(matrix.saga.names.saleCompleted(), sale, event).join();
            matrix.awaitSaleTopicConsumed();
            matrix.assertExactlyOnce(List.of(sale), 0);
            assertEquals(2, matrix.saleEvents());
        }
    }

    /** Crash before the selection commit: the offset is not committed, the event is redelivered and selected once. */
    @Test
    void aNotificationCrashBeforeTheSelectionCommitSelectsOnceAfterRedelivery() throws Exception {
        try (Matrix matrix = Matrix.start(RETRY_FOREVER)) {
            String hook = "notification.selection.beforeCommit";
            long fired = FaultHooks.fired(hook);
            System.setProperty(FaultHooks.PROPERTY, hook + ":throw");
            String sale = matrix.sell();
            matrix.await(() -> FaultHooks.fired(hook) > fired, "the fault before the selection commit fires");
            assertEquals(0, matrix.schema.saleSelections(), "the selection rolled back");
            matrix.notification.crash(() -> System.clearProperty(FaultHooks.PROPERTY));
            matrix.assertExactlyOnce(List.of(sale), 0);
        }
    }

    /** Crash after the selection commit, before the offset commit: a duplicate {@code sale_id}, a no-op. */
    @Test
    void aNotificationCrashAfterTheSelectionCommitIsANoOpOnRedelivery() throws Exception {
        try (Matrix matrix = Matrix.start(RETRY_FOREVER)) {
            String hook = "notification.selection.afterCommit";
            long fired = FaultHooks.fired(hook);
            System.setProperty(FaultHooks.PROPERTY, hook + ":throw");
            String sale = matrix.sell();
            matrix.await(() -> FaultHooks.fired(hook) > fired, "the fault after the selection commit fires");
            assertEquals(1, matrix.schema.saleSelections(), "the selection committed");
            matrix.notification.crash(() -> System.clearProperty(FaultHooks.PROPERTY));
            matrix.assertExactlyOnce(List.of(sale), 0);
        }
    }

    /** Crash before the first send: the restarted dispatcher's sweep sends the {@code PENDING} rows once each. */
    @Test
    void aNotificationCrashBeforeTheFirstSendIsSentByTheSweep() throws Exception {
        try (Matrix matrix = Matrix.start(RETRY_FOREVER)) {
            // Every channel stalls, so the dispatcher holds the rows in flight and nothing reaches a channel.
            System.setProperty(FaultHooks.PROPERTY, "notification.channel.stall:s1,notification.channel.stall:s2,"
                    + "notification.channel.stall:s3");
            String sale = matrix.sell();
            matrix.await(() -> matrix.notification.dispatcher().inFlight() == OWED, "all four sends are in flight");
            assertEquals(0, matrix.notification.channelSends.sends(), "nothing reached a channel");
            matrix.notification.crash(() -> System.clearProperty(FaultHooks.PROPERTY));
            matrix.assertExactlyOnce(List.of(sale), 0);
        }
    }

    /** Crash after a confirmed send, before the mark: re-sent with the same identity, one redelivery per row. */
    @Test
    void aNotificationCrashAfterAConfirmedSendGivesOneRedeliveryPerRow() throws Exception {
        try (Matrix matrix = Matrix.start(RETRY_FOREVER)) {
            String hook = "notification.delivery.afterSend";
            System.setProperty(FaultHooks.PROPERTY, hook + ":throw");
            String sale = matrix.sell();
            matrix.await(() -> matrix.channelRecords() == OWED, "every notification reaches its channel");
            matrix.await(() -> FaultHooks.fired(hook) > 0, "the marking fault fires");
            assertEquals(OWED, matrix.schema.notifications("PENDING"), "confirmed but never marked");
            matrix.notification.crash(() -> System.clearProperty(FaultHooks.PROPERTY));
            matrix.assertExactlyOnce(List.of(sale), OWED);
        }
    }

    /**
     * T102: a restart while rows are parked keeps them parked and alarmed (from the database counts, with the parked
     * record of a malformed event), the parked listing shows them, and an operator retry delivers each once.
     */
    @Test
    void aRestartWhileRowsAreParkedKeepsThemParkedUntilAnOperatorRetry() throws Exception {
        try (Matrix matrix = Matrix.start(Map.of())) {
            System.setProperty(FaultHooks.PROPERTY, "notification.channel.reject:s1");
            String sale = matrix.sell();
            matrix.await(() -> matrix.delivered() == 2, "s2 and s3 are delivered");
            JdbcNotificationStore store = new JdbcNotificationStore(matrix.schema.pool());
            matrix.await(() -> matrix.uncharged("s1") >= 2, "s1's sends fail");
            List<String> s1Rows = matrix.ids("s1");
            assertEquals(2, s1Rows.size());
            assertEquals(2, store.park(s1Rows, Instant.now()), "park s1's rows, as eight charged failures would");
            matrix.saga.bus.send(matrix.saga.names.saleCompleted(), "poison", "not an envelope".getBytes(StandardCharsets.UTF_8)).join();
            matrix.await(() -> matrix.parkedEvents() == 1, "the record is parked");

            matrix.notification.crash(() -> System.clearProperty(FaultHooks.PROPERTY));
            Map<String, Object> status = matrix.ops("GET", "/status", null);
            List<?> alarms = (List<?>) status.get("alarms");
            assertTrue(alarms.stream().anyMatch(alarm -> alarm.toString().startsWith("PARKED_NOTIFICATIONS: 2")), alarms.toString());
            assertTrue(alarms.stream().anyMatch(alarm -> alarm.toString().startsWith("PARKED_EVENTS: 1")), alarms.toString());
            assertEquals(2, ((Number) ((Map<?, ?>) status.get("notifications")).get("parked")).intValue());
            assertEquals(1, ((Number) status.get("parkedEvents")).intValue());

            Map<String, Object> parked = matrix.ops("GET", "/parked?subscriberId=s1&limit=10", null);
            List<?> notifications = (List<?>) parked.get("notifications");
            assertEquals(2, notifications.size());
            Set<Object> listed = new HashSet<>();
            for (Object row : notifications) {
                Map<?, ?> fields = (Map<?, ?>) row;
                listed.add(fields.get("notificationId"));
                assertEquals(sale, fields.get("saleId"));
                assertEquals("s1", fields.get("subscriberId"));
                assertNotNull(fields.get("parkedAt"));
            }
            assertEquals(Set.copyOf(s1Rows), listed);
            List<?> events = (List<?>) parked.get("events");
            assertEquals(1, events.size());
            assertEquals("UNDECODABLE", ((Map<?, ?>) events.get(0)).get("reason"));
            assertEquals(400, matrix.opsStatus("GET", "/parked?limit=0", null));
            assertEquals(400, matrix.opsStatus("POST", "/parked/retry", "{\"subscriberId\":\"no spaces\"}"));

            Thread.sleep(1_500);   // a sweep or more after the restart
            assertEquals(0, matrix.channel("s1").size(), "a parked row is not sent after a restart");
            assertEquals(2, matrix.schema.notifications("PARKED"));

            assertEquals(0, retried(matrix.ops("POST", "/parked/retry", "{\"subscriberId\":\"s2\"}")));
            assertEquals(2, retried(matrix.ops("POST", "/parked/retry", "{\"subscriberId\":\"s1\"}")));
            matrix.assertExactlyOnce(List.of(sale), 0);
            assertEquals(2, matrix.schema.queryLong("SELECT COUNT(*) FROM notification WHERE subscriber_id = 's1' "
                    + "AND operator_retries = 1 AND state = 'DELIVERED'"));
            List<?> after = (List<?>) matrix.ops("GET", "/status", null).get("alarms");
            assertTrue(after.stream().noneMatch(alarm -> alarm.toString().startsWith("PARKED_NOTIFICATIONS")), after.toString());
            assertEquals(0, retried(matrix.ops("POST", "/parked/retry", "")));
        }
    }

    // ------------------------------------------------------------------ fixture

    /** Transaction, Inventory, and Notification on one bus, the catalog loaded, s1..s3 registered. */
    private static final class Matrix implements AutoCloseable {
        private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

        final InProcessCheckout saga;
        final NotificationTestSchema schema;
        final InProcessNotification notification;
        final ExecutorService stations = Executors.newCachedThreadPool();
        final List<InProcessTransaction> replaced = new ArrayList<>();
        InProcessTransaction transaction;
        String lastSale;

        private Matrix(InProcessCheckout saga, NotificationTestSchema schema, InProcessNotification notification) {
            this.saga = saga;
            this.schema = schema;
            this.notification = notification;
            this.transaction = saga.transaction;
        }

        static Matrix start(Map<String, String> notificationSettings) throws Exception {
            InProcessCheckout saga = InProcessCheckout.start(5, 1_000);
            NotificationTestSchema schema = NotificationTestSchema.create();
            InProcessNotification notification = InProcessNotification.over(schema.pool(), saga.bus, saga.names,
                    notificationSettings);
            saga.inventory.publishCatalog();
            notification.awaitCatalog(WAIT);
            Matrix matrix = new Matrix(saga, schema, notification);
            matrix.restartTransaction(UnaryOperator.identity());
            notification.service().register("s1", List.of(A, B));
            notification.service().register("s2", List.of(B));
            notification.service().register("s3", List.of(A));
            return matrix;
        }

        /** Stops Transaction as a crash leaves it and starts it again over its schema, with {@code wrap} on its sender. */
        void restartTransaction(UnaryOperator<RecordSender> wrap) throws Exception {
            transaction.close();
            System.clearProperty(FaultHooks.PROPERTY);
            ConsumerLoop.Settings consumers = ConsumerLoop.Settings.defaults().withPolicy(new ParkingPolicy(1_000,
                    Duration.ofMillis(200), Duration.ofMillis(100), Duration.ofSeconds(5), Duration.ofSeconds(60)));
            transaction = InProcessTransaction.over(saga.transactionSchema).pool(4, 24).bus(saga.bus, saga.names)
                    .consumer(consumers).wrapSender(wrap).build();
            replaced.add(transaction);
        }

        /** A basket of 2 x A + 1 x B. */
        String basket() {
            TransactionOperations service = transaction.service;
            String id = service.start(new TransactionOperations.StartCommand("matrix-station")).transactionId();
            for (String sku : List.of(A, B, A)) service.scan(new TransactionOperations.ScanCommand(id, sku));
            lastSale = id;
            return id;
        }

        String sell() {
            String id = basket();
            transaction.service.complete(id);
            return id;
        }

        /** Completes in the background: the station's request may outlive a Transaction crash. */
        Future<?> completeInBackground(String id) {
            TransactionOperations service = transaction.service;
            return stations.submit(() -> {
                try {
                    service.complete(id);
                } catch (RuntimeException expected) {
                    // The crashed process dropped the request, or answered that the outcome is pending.
                }
            });
        }

        void awaitSettled(Future<?> complete) throws Exception {
            complete.get(60, TimeUnit.SECONDS);
            await(() -> {
                try {
                    return saga.transactionSchema.attempts(lastSale).get(0).state().equals("COMPLETED");
                } catch (Exception failure) {
                    throw new IllegalStateException(failure);
                }
            }, "the attempt completes after the restart");
        }

        /** {@code SaleCompleted} records on the bus. */
        long saleEvents() {
            return saga.bus.values(saga.names.saleCompleted()).size();
        }

        void awaitSaleTopicConsumed() throws InterruptedException {
            String group = saga.names.group(events.TopicNames.NOTIFICATION_SALE_COMPLETED_GROUP);
            String topic = saga.names.saleCompleted();
            await(() -> {
                for (int p = 0; p < saga.bus.partitions(topic); p++) {
                    long end = saga.bus.endOffset(topic, p);
                    Long committed = saga.bus.committedOffset(group, topic, p);
                    if (end > 0 && (committed == null || committed < end)) return false;
                }
                return true;
            }, "Notification consumed the sale topic");
        }

        /**
         * One selection per sale, one row per owed key, every row delivered, and {@code redeliveries} channel
         * records beyond the first copy of each notification.
         */
        void assertExactlyOnce(List<String> sales, int redeliveries) throws Exception {
            await(() -> delivered() == (long) OWED * sales.size(), "every owed notification is delivered");
            awaitSaleTopicConsumed();
            Thread.sleep(300);   // a duplicate would show by now
            assertEquals(sales.size(), schema.saleSelections(), "one selection per sale");
            for (String sale : sales) {
                assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM sale_selection WHERE sale_id = ?", sale));
                assertEquals(OWED, schema.queryLong("SELECT COUNT(*) FROM notification WHERE sale_id = ?", sale),
                        "one row per (sale, SKU, subscriber)");
            }
            assertEquals((long) OWED * sales.size(), schema.notifications("DELIVERED"));
            Map<String, Integer> expected = Map.of("s1", 2, "s2", 1, "s3", 1);
            int extra = 0;
            for (var subscriber : expected.entrySet()) {
                List<SubscriberNotified> received = channel(subscriber.getKey());
                Map<String, SubscriberNotified> distinct = new HashMap<>();
                for (SubscriberNotified notified : received) {
                    SubscriberNotified first = distinct.putIfAbsent(notified.getNotificationId(), notified);
                    if (first != null) assertEquals(first, notified, "a redelivery keeps the identity and contents");
                }
                assertEquals(subscriber.getValue() * sales.size(), distinct.size(), subscriber.getKey() + "'s notifications");
                extra += received.size() - distinct.size();
            }
            assertEquals(redeliveries, extra, "redeliveries on the channels");
        }

        long delivered() {
            try {
                return schema.notifications("DELIVERED");
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        }

        long parkedEvents() {
            try {
                return schema.queryLong("SELECT COUNT(*) FROM dead_letter_event");
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        }

        long uncharged(String subscriber) {
            try {
                return schema.queryLong("SELECT COALESCE(SUM(uncharged_failures + charged_failures), 0) FROM notification "
                        + "WHERE subscriber_id = ?", subscriber);
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        }

        List<String> ids(String subscriber) throws Exception {
            List<String> ids = new ArrayList<>();
            try (var select = schema.admin().prepareStatement("SELECT notification_id FROM notification WHERE subscriber_id = ?")) {
                select.setString(1, subscriber);
                try (var rows = select.executeQuery()) {
                    while (rows.next()) ids.add(rows.getString(1));
                }
            }
            return ids;
        }

        int channelRecords() {
            int total = 0;
            for (String subscriber : List.of("s1", "s2", "s3")) total += channel(subscriber).size();
            return total;
        }

        List<SubscriberNotified> channel(String subscriber) {
            String topic = saga.names.channel(subscriber);
            List<SubscriberNotified> received = new ArrayList<>();
            if (!saga.bus.hasTopic(topic)) return received;
            for (byte[] value : saga.bus.values(topic)) {
                received.add(EnvelopeCodec.decode(value, subscriber).payload(SubscriberNotified.class));
            }
            return received;
        }

        Map<String, Object> ops(String method, String path, String body) throws Exception {
            HttpResponse<String> response = send(method, path, body);
            assertEquals(200, response.statusCode(), method + " " + path + ": " + response.body());
            return Json.parseObject(response.body());
        }

        int opsStatus(String method, String path, String body) throws Exception {
            return send(method, path, body).statusCode();
        }

        private HttpResponse<String> send(String method, String path, String body) throws Exception {
            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + notification.httpPort()
                    + "/notifications/ops" + path)).timeout(Duration.ofSeconds(10));
            request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
            return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        }

        void await(BooleanSupplier condition, String what) throws InterruptedException {
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (!condition.getAsBoolean()) {
                assertTrue(System.nanoTime() < deadline, "timed out waiting: " + what);
                Thread.sleep(5);
            }
        }

        @Override
        public void close() throws Exception {
            stations.shutdownNow();
            try (saga; schema) {
                notification.close();
                for (InProcessTransaction each : replaced) each.close();
            }
        }
    }

    private static int retried(Map<String, Object> answer) {
        assertEquals(Set.of("retried"), answer.keySet());
        return ((Number) answer.get("retried")).intValue();
    }
}
