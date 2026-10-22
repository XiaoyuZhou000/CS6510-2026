package integration;

import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.ServiceHttp;

import java.net.http.HttpResponse;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Restart during a workload (SC-009): concurrent stations keep starting, scanning, and completing while
 * Inventory, Transaction, and Analytics are each restarted once. Once pending work drains, every scan a
 * client saw acknowledged is stored once in both Transaction and Analytics, every SKU reconciles (SC-002),
 * and positions and every eligible window match an independent recomputation (SC-006).
 */
class RestartDuringWorkloadTest {
    private static final int PRODUCTS = 20;
    private static final int STOCK = 10_000;
    private static final int STATIONS = 6;
    /** Acknowledged scans between restarts, and in total; 1,500 makes at least two eligible windows likely. */
    private static final int SCANS_PER_PHASE = 250;
    private static final int MIN_SCANS = 1_500;
    private static final Duration WORKLOAD_LIMIT = Duration.ofMinutes(4);
    private static final Duration DRAIN = Duration.ofSeconds(90);

    @Test
    void restartingEachServiceOnceLosesNoAcknowledgedScanAndKeepsEveryInvariant() throws Exception {
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
                for (String service : List.of("inventory", "transaction", "analytics")) {
                    awaitMoreScans(acknowledged, acknowledged.size() + SCANS_PER_PHASE, deadline, deployment);
                    restart(deployment, service);
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

    private static void restart(CheckoutDeployment deployment, String service) throws Exception {
        switch (service) {
            case "inventory" -> {
                deployment.stopInventory();
                deployment.startInventory();
            }
            case "transaction" -> {
                deployment.stopTransaction();
                deployment.startTransaction();
            }
            case "analytics" -> {
                deployment.stopAnalytics();
                deployment.startAnalytics();
            }
            default -> throw new IllegalArgumentException(service);
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
