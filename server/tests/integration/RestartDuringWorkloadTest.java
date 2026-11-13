package integration;

import events.EnvelopeCodec;
import events.EventIds;
import events.EventTypes;
import messaging.KafkaClients;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.SubscriberNotified;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.NotificationTestSchema;
import support.ServiceHttp;
import support.ServiceProcesses;
import support.TestSchema;

import java.net.http.HttpResponse;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Restart during a workload (SC-009): concurrent stations keep starting, scanning, and completing while
 * Inventory, Transaction, and Analytics are each restarted once, and in a second run while the broker is
 * restarted; during each outage the other services answer and, unless Transaction is down, scans keep being
 * acknowledged (006 US7 scenario 4, SC-012). Once pending work drains, every scan a
 * client saw acknowledged is stored once in both Transaction and Analytics, every SKU reconciles (SC-002),
 * and positions and every eligible window match an independent recomputation (SC-006).
 */
class RestartDuringWorkloadTest {
    private static final int PRODUCTS = 20;
    private static final int STOCK = 10_000;
    private static final int STATIONS = 6;
    /** Acknowledged scans between restarts, and in total; 1,500 makes at least two eligible windows likely. */
    private static final int SCANS_PER_PHASE = 250;
    /** Scans acknowledged while Inventory, Analytics, or the broker is down (006 US2, US7 scenario 4). */
    private static final int SCANS_WHILE_DOWN = 50;
    private static final int MIN_SCANS = 1_500;
    private static final Duration WORKLOAD_LIMIT = Duration.ofMinutes(4);
    private static final Duration DRAIN = Duration.ofSeconds(90);
    /** 007 T113: subscribers and the workload length around the Notification restart. */
    private static final int SUBSCRIBERS = 10;
    private static final Duration NOTIFICATION_WORKLOAD = Duration.ofSeconds(30);

    @Test
    void restartingEachServiceOnceLosesNoAcknowledgedScanAndKeepsEveryInvariant() throws Exception {
        restartDuringWorkload(List.of("inventory", "transaction", "analytics"));
    }

    /**
     * US7 scenario 4 for the broker (006 SC-012): the live broker is stopped and started during the workload; while
     * it is down every service answers and scans keep being acknowledged, and afterwards every invariant holds.
     */
    @Test
    void restartingTheBrokerDuringTheWorkloadLosesNoAcknowledgedScanAndKeepsEveryInvariant() throws Exception {
        KafkaTestSupport.requireBrokerControl();
        restartDuringWorkload(List.of("broker"));
    }

    /**
     * 007 US7 scenario 4 (T113, SC-012): 10 subscribers registered, a 30-second workload, and Notification killed and
     * started again in the middle of it. The other services answer while it is down and every request of the workload
     * succeeds; afterwards each completed sale is selected once, every owed notification (one per sale, sold SKU, and
     * subscriber of that SKU) is delivered, and each channel holds exactly the owed IDs, duplicates allowed.
     */
    @Test
    void restartingNotificationDuringAThirtySecondWorkloadDeliversEveryOwedNotificationOnce() throws Exception {
        String run = UUID.randomUUID().toString().substring(0, 6);
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(PRODUCTS, STOCK, null);
             NotificationTestSchema schema = NotificationTestSchema.create(TestSchema.Options.defaults().withOwnUser())) {
            deployment.startAnalytics();
            int port = ServiceProcesses.freePort();
            ServiceProcesses.Builder launcher = ServiceProcesses.service("notification")
                    .env("NOTIFICATION_DB_POOL_MAX_SIZE", "20").database(schema).httpPort(port).kafka(deployment.names);
            ServiceProcesses.ServiceProcess notification = launcher.start();
            try {
                ServiceHttp notificationHttp = new ServiceHttp(port);
                awaitCatalogLoaded(notification, notificationHttp);
                Map<String, Set<String>> subscriptions = new LinkedHashMap<>();
                Random choice = new Random(20261113L);
                for (int s = 1; s <= SUBSCRIBERS; s++) {
                    String subscriber = "rdw-" + run + "-" + s;
                    Set<String> skus = new TreeSet<>();
                    for (int n = 1; n <= PRODUCTS; n++) if (choice.nextInt(3) == 0) skus.add(InventoryTestSchema.sku(n));
                    if (skus.isEmpty()) skus.add(InventoryTestSchema.sku(s));
                    String body = "{\"subscriberId\":\"" + subscriber + "\",\"skus\":[\"" + String.join("\",\"", skus) + "\"]}";
                    HttpResponse<String> registered = notificationHttp.post("/notifications/subscribers", body);
                    assertEquals(201, registered.statusCode(), registered.body());
                    subscriptions.put(subscriber, skus);
                }

                Queue<String> errors = new ConcurrentLinkedQueue<>();
                AtomicBoolean running = new AtomicBoolean(true);
                AtomicInteger requests = new AtomicInteger();
                ExecutorService stations = Executors.newFixedThreadPool(STATIONS);
                List<Future<?>> workers = new ArrayList<>();
                long started = System.nanoTime();
                try {
                    for (int station = 0; station < STATIONS; station++) {
                        String stationId = "rdw-" + station;
                        workers.add(stations.submit(() -> {
                            shopWithoutErrors(deployment.transactionHttp, stationId, running, requests, errors);
                            return null;
                        }));
                    }
                    sleepUntil(started, NOTIFICATION_WORKLOAD.dividedBy(3));
                    notification.kill();
                    long down = System.nanoTime();
                    Map<String, ServiceHttp> peers = Map.of("transaction", deployment.transactionHttp,
                            "inventory", deployment.inventoryHttp, "analytics", deployment.analyticsHttp);
                    int before = requests.get();
                    sleepUntil(down, Duration.ofSeconds(5));
                    for (Map.Entry<String, ServiceHttp> peer : peers.entrySet()) {
                        String probe = switch (peer.getKey()) {
                            case "transaction" -> "/transactions/missing";
                            case "inventory" -> "/items";
                            default -> "/analytics/popular-items";
                        };
                        assertEquals(peer.getKey().equals("transaction") ? 404 : 200,
                                peer.getValue().get(probe).statusCode(), peer.getKey() + " answers while Notification is down");
                    }
                    assertTrue(requests.get() > before, "the workload kept being served while Notification was down");
                    notification = launcher.start();
                    notification.awaitHttp("/notifications/ops/status", 200, Duration.ofSeconds(60));
                    sleepUntil(started, NOTIFICATION_WORKLOAD);
                } finally {
                    running.set(false);
                    stations.shutdown();
                    assertTrue(stations.awaitTermination(60, TimeUnit.SECONDS), "stations did not stop");
                }
                for (Future<?> worker : workers) worker.get();
                assertEquals(List.of(), List.copyOf(errors), "every request of the workload succeeded");

                awaitNoPendingAttempts(deployment);
                Map<String, Set<String>> soldSkus = soldSkusBySale(deployment);
                Map<String, Set<String>> owed = new LinkedHashMap<>();
                long total = 0;
                for (Map.Entry<String, Set<String>> subscriber : subscriptions.entrySet()) {
                    Set<String> ids = new HashSet<>();
                    soldSkus.forEach((sale, skus) -> {
                        for (String sku : skus) {
                            if (subscriber.getValue().contains(sku)) ids.add(EventIds.notificationId(sale, sku, subscriber.getKey()));
                        }
                    });
                    owed.put(subscriber.getKey(), ids);
                    total += ids.size();
                }
                int sales = soldSkus.size();
                long owedTotal = total;
                assertTrue(sales > 0 && owedTotal > 0, "the workload completed sales that were owed notifications");
                awaitNotification(() -> schema.saleSelections() == sales, "every completed sale is selected once", notification);
                awaitNotification(() -> schema.notifications("DELIVERED") == owedTotal,
                        "every owed notification is delivered", notification);
                assertEquals(sales, schema.saleSelections(), "one selection per sale");
                assertEquals(owedTotal, schema.notifications(), "one row per owed notification");
                assertEquals(0, schema.notifications("PARKED"), "nothing parked");
                assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM dead_letter_event"), "no sale event parked");
                long copies = 0;
                for (Map.Entry<String, Set<String>> subscriber : owed.entrySet()) {
                    Map<String, Integer> received = channel(deployment.names.channel(subscriber.getKey()),
                            subscriber.getValue().size());
                    assertEquals(subscriber.getValue(), received.keySet(),
                            subscriber.getKey() + " received every owed notification and nothing else");
                    for (int count : received.values()) copies += count;
                }
                System.out.println("[notification-restart] " + sales + " sales, " + owedTotal + " owed notifications, "
                        + (copies - owedTotal) + " redelivered copies, " + requests.get() + " requests");
            } finally {
                notification.close();
            }
        }
    }

    private static void restartDuringWorkload(List<String> restarts) throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(PRODUCTS, STOCK, null)) {
            deployment.startAnalytics();
            Set<String> acknowledged = ConcurrentHashMap.newKeySet();
            Set<String> completed = ConcurrentHashMap.newKeySet();
            AtomicBoolean running = new AtomicBoolean(true);
            ExecutorService stations = Executors.newFixedThreadPool(STATIONS);
            List<Future<?>> workers = new ArrayList<>();
            try {
                for (int station = 0; station < STATIONS; station++) {
                    String stationId = "restart-" + station;
                    workers.add(stations.submit(() -> {
                        shop(deployment.transactionHttp, stationId, running, acknowledged, completed);
                        return null;
                    }));
                }
                long deadline = System.nanoTime() + WORKLOAD_LIMIT.toNanos();
                for (String component : restarts) {
                    awaitMoreScans(acknowledged, acknowledged.size() + SCANS_PER_PHASE, deadline, deployment);
                    restart(deployment, component, acknowledged, deadline);
                }
                awaitMoreScans(acknowledged, Math.max(MIN_SCANS, acknowledged.size() + SCANS_PER_PHASE), deadline,
                        deployment);
            } finally {
                running.set(false);
                stations.shutdown();
                assertTrue(stations.awaitTermination(60, TimeUnit.SECONDS), "stations did not stop");
            }
            for (Future<?> worker : workers) worker.get();

            awaitNoPendingAttempts(deployment);
            deployment.awaitAnalyticsHoldsEveryScan(DRAIN);
            long highest = highestEligibleBoundary(deployment.analyticsSchema.lastPosition());
            awaitCheckpoint(deployment, highest);

            Set<String> transactionScans = deployment.transactionSchema.identities();
            Set<String> analyticsScans = deployment.analyticsSchema.identities();
            Set<String> lost = new HashSet<>(acknowledged);
            lost.removeAll(transactionScans);
            assertEquals(Set.of(), lost, "acknowledged scans missing from Transaction");
            lost = new HashSet<>(acknowledged);
            lost.removeAll(analyticsScans);
            assertEquals(Set.of(), lost, "acknowledged scans missing from Analytics");
            assertTrue(highest >= 1_000, "the workload published at least one window: " + highest);

            for (String id : completed) {
                assertEquals("COMPLETED", deployment.transactionSchema.transaction(id).orElseThrow().status(),
                        "an acknowledged Complete stays COMPLETED: " + id);
            }
            assertEquals(0, deployment.transactionSchema.pendingAttemptCount());
            Map<String, Long> sold = soldBySku(deployment);
            for (int n = 1; n <= PRODUCTS; n++) {
                String sku = InventoryTestSchema.sku(n);
                int stock = deployment.inventorySchema.stock(sku);
                assertTrue(stock >= 0, sku + " stock is nonnegative");
                assertEquals(sold.getOrDefault(sku, 0L), (long) (STOCK - stock),
                        sku + ": initial minus final stock equals the units in COMPLETED transactions");
            }
            deployment.analyticsSchema.assertWindowsMatchPositions(highest);
        }
    }

    /** One station as in {@link #shop}, but every request must succeed; a failure is recorded and ends the station. */
    private static void shopWithoutErrors(ServiceHttp http, String stationId, AtomicBoolean running,
                                          AtomicInteger requests, Queue<String> errors) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        try {
            while (running.get()) {
                String id = http.start(stationId);
                requests.incrementAndGet();
                int items = 3 + random.nextInt(4);
                for (int i = 0; i < items; i++) {
                    HttpResponse<String> scan = http.scan(id, InventoryTestSchema.sku(1 + random.nextInt(PRODUCTS)));
                    requests.incrementAndGet();
                    if (scan.statusCode() != 200) throw new AssertionError("scan " + scan.statusCode() + " " + scan.body());
                }
                HttpResponse<String> complete = http.complete(id);
                requests.incrementAndGet();
                if (complete.statusCode() != 200) {
                    throw new AssertionError("complete " + complete.statusCode() + " " + complete.body());
                }
            }
        } catch (Exception | AssertionError failure) {
            errors.add(stationId + ": " + failure);
        }
    }

    private static void sleepUntil(long since, Duration elapsed) throws InterruptedException {
        long remaining = since + elapsed.toNanos() - System.nanoTime();
        if (remaining > 0) TimeUnit.NANOSECONDS.sleep(remaining);
    }

    /** Waits until Notification has read Inventory's catalog, so registrations name known SKUs. */
    private static void awaitCatalogLoaded(ServiceProcesses.ServiceProcess notification, ServiceHttp http) throws Exception {
        notification.awaitHttp("/notifications/ops/status", 200, Duration.ofSeconds(60));
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!"true".equals(String.valueOf(ServiceHttp.json(http.get("/notifications/ops/status")).get("catalogLoaded")))) {
            if (System.nanoTime() > deadline) fail("Notification never loaded the catalog" + System.lineSeparator() + notification.output());
            Thread.sleep(200);
        }
    }

    /** The distinct SKUs of each COMPLETED sale, from Transaction's own records. */
    private static Map<String, Set<String>> soldSkusBySale(CheckoutDeployment deployment) throws Exception {
        Map<String, Set<String>> sold = new HashMap<>();
        try (PreparedStatement sql = deployment.transactionSchema.admin().prepareStatement(
                "SELECT DISTINCT s.transaction_id, s.sku FROM accepted_scan s JOIN `transaction` t "
                        + "ON t.transaction_id = s.transaction_id WHERE t.status = 'COMPLETED'");
             ResultSet row = sql.executeQuery()) {
            while (row.next()) sold.computeIfAbsent(row.getString(1), sale -> new HashSet<>()).add(row.getString(2));
        }
        long completed = deployment.transactionSchema.queryLong("SELECT COUNT(*) FROM `transaction` WHERE status = 'COMPLETED'");
        assertEquals(completed, sold.size(), "every completed sale has at least one scan");
        return sold;
    }

    private interface Check {
        boolean holds() throws Exception;
    }

    private static void awaitNotification(Check check, String what, ServiceProcesses.ServiceProcess notification)
            throws Exception {
        long deadline = System.nanoTime() + DRAIN.toNanos();
        while (!check.holds()) {
            if (System.nanoTime() > deadline) fail("timed out waiting: " + what + System.lineSeparator() + notification.output());
            Thread.sleep(250);
        }
    }

    /** The notification IDs on a channel with the number of copies of each, once {@code expected} distinct IDs arrived. */
    private static Map<String, Integer> channel(String topic, int expected) {
        Map<String, Integer> copies = new HashMap<>();
        TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaConsumer<String, byte[]> consumer = KafkaClients.consumer(KafkaTestSupport.clientEnvironment(),
                "rdw-reader-" + UUID.randomUUID(), null)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            int quietPolls = 0;
            while ((copies.size() < expected || quietPolls < 5) && System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(200));
                quietPolls = records.isEmpty() ? quietPolls + 1 : 0;
                for (ConsumerRecord<String, byte[]> record : records) {
                    var event = EnvelopeCodec.decode(record.value(), record.key());
                    assertEquals(EventTypes.SUBSCRIBER_NOTIFIED, event.type());
                    copies.merge(event.payload(SubscriberNotified.class).getNotificationId(), 1, Integer::sum);
                }
            }
        }
        return copies;
    }

    /** One station: Start, three to six Scans, Complete, repeatedly; every failure just moves on. */
    private static void shop(ServiceHttp http, String stationId, AtomicBoolean running, Set<String> acknowledged,
                             Set<String> completed) throws InterruptedException {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (running.get()) {
            String id;
            try {
                id = http.start(stationId);
            } catch (Exception | AssertionError unavailable) {
                Thread.sleep(50);
                continue;
            }
            int items = 3 + random.nextInt(4);
            for (int i = 0; i < items && running.get(); i++) {
                String sku = InventoryTestSchema.sku(1 + random.nextInt(PRODUCTS));
                try {
                    HttpResponse<String> scan = http.scan(id, sku);
                    if (scan.statusCode() == 200) {
                        Object itemCount = ServiceHttp.json(scan).get("itemCount");
                        acknowledged.add(id + "#" + ((Number) itemCount).intValue());
                    }
                } catch (Exception unavailable) {
                    Thread.sleep(50);
                }
            }
            try {
                if (http.complete(id).statusCode() == 200) completed.add(id);
            } catch (Exception unavailable) {
                Thread.sleep(50);
            }
        }
    }

    /**
     * Stops {@code component}, checks that every other service still answers and, unless Transaction is the one
     * stopped, that scans keep being acknowledged; then starts it again.
     */
    private static void restart(CheckoutDeployment deployment, String component, Set<String> acknowledged,
                                long deadline) throws Exception {
        switch (component) {
            case "inventory" -> deployment.stopInventory();
            case "transaction" -> deployment.stopTransaction();
            case "analytics" -> deployment.stopAnalytics();
            case "broker" -> KafkaTestSupport.stopBroker();
            default -> throw new IllegalArgumentException(component);
        }
        try {
            Map<String, ServiceHttp> http = Map.of("transaction", deployment.transactionHttp,
                    "inventory", deployment.inventoryHttp, "analytics", deployment.analyticsHttp);
            for (String peer : List.of("transaction", "inventory", "analytics")) {
                if (peer.equals(component)) continue;
                String probe = switch (peer) {
                    case "transaction" -> "/transactions/missing";
                    case "inventory" -> "/items";
                    default -> "/analytics/popular-items";
                };
                assertEquals(peer.equals("transaction") ? 404 : 200, http.get(peer).get(probe).statusCode(),
                        peer + " answers while " + component + " is down");
            }
            if (!component.equals("transaction")) {
                awaitMoreScans(acknowledged, acknowledged.size() + SCANS_WHILE_DOWN, deadline, deployment);
            }
        } finally {
            switch (component) {
                case "inventory" -> deployment.startInventory();
                case "transaction" -> deployment.startTransaction();
                case "analytics" -> deployment.startAnalytics();
                default -> KafkaTestSupport.startBroker();
            }
        }
    }

    private static void awaitMoreScans(Set<String> acknowledged, int target, long deadline,
                                       CheckoutDeployment deployment) throws InterruptedException {
        while (acknowledged.size() < target) {
            if (System.nanoTime() > deadline) {
                fail("the workload acknowledged only " + acknowledged.size() + " of " + target + " scans"
                        + System.lineSeparator() + deployment.logs());
            }
            Thread.sleep(100);
        }
    }

    private static void awaitNoPendingAttempts(CheckoutDeployment deployment) throws Exception {
        long deadline = System.nanoTime() + DRAIN.toNanos();
        while (deployment.transactionSchema.pendingAttemptCount() > 0) {
            if (System.nanoTime() > deadline) fail("checkout attempts stayed pending" + System.lineSeparator() + deployment.logs());
            Thread.sleep(200);
        }
    }

    private static void awaitCheckpoint(CheckoutDeployment deployment, long highest) throws Exception {
        long deadline = System.nanoTime() + DRAIN.toNanos();
        while (deployment.analyticsSchema.checkpoint() != highest) {
            if (System.nanoTime() > deadline) {
                fail("Analytics checkpoint " + deployment.analyticsSchema.checkpoint() + " never reached " + highest
                        + System.lineSeparator() + deployment.logs());
            }
            Thread.sleep(200);
        }
    }

    /** 0 below 1,000 positions; otherwise the highest of 1,000, 1,500, ... at or below {@code lastPosition}. */
    private static long highestEligibleBoundary(long lastPosition) {
        return lastPosition < 1_000 ? 0 : 1_000 + (lastPosition - 1_000) / 500 * 500;
    }

    /** Units per SKU in COMPLETED transactions, from Transaction's own basket records. */
    private static Map<String, Long> soldBySku(CheckoutDeployment deployment) throws Exception {
        Map<String, Long> sold = new HashMap<>();
        try (PreparedStatement sql = deployment.transactionSchema.admin().prepareStatement(
                "SELECT s.sku, COUNT(*) FROM accepted_scan s JOIN `transaction` t ON t.transaction_id = s.transaction_id "
                        + "WHERE t.status = 'COMPLETED' GROUP BY s.sku");
             ResultSet row = sql.executeQuery()) {
            while (row.next()) sold.put(row.getString(1), row.getLong(2));
        }
        return sold;
    }
}
