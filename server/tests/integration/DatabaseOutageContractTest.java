package integration;

import api.json.Json;
import events.TopicNames;
import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.ServiceHttp;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A reachable owner must report database failures without acknowledging unfinished work (US6, FR-011, FR-024).
 * Transaction's own {@code transaction} table goes away and comes back, renamed through TransactionTestSchema,
 * while Inventory keeps serving from its own schema. Inventory's {@code inventory} table goes away while a
 * checkout request waits on the broker: its low-stock route answers 500, its checkout consumer commits no offset
 * until the table returns, Transaction and Analytics keep serving, and the request is then applied exactly once.
 */
class DatabaseOutageContractTest {
    private static final String SKU = InventoryTestSchema.sku(1);

    @Test
    void failedScanAndCompletionReturnInternalErrorThenCommittedStateRecovers() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(5, 2)) {
            ServiceHttp http = deployment.transactionHttp;
            String id = http.start("db-outage");
            long beforePosition = deployment.transactionSchema.scanCount();

            deployment.transactionSchema.setTableUnavailable("transaction", true);
            try {
                assertInternal(http.scan(id, SKU));
                assertInternal(http.post("/transactions/" + id + "/complete", "{}"));
                assertEquals(200, deployment.inventoryHttp.get("/items").statusCode(),
                        "Inventory's routes are unaffected by Transaction's outage");
            } finally {
                deployment.transactionSchema.setTableUnavailable("transaction", false);
            }
            assertEquals(beforePosition, deployment.transactionSchema.scanCount(), "failed scan must not commit an event");
            assertEquals(2, deployment.inventorySchema.stock(SKU), "failed completion must not deduct stock");
            Map<String, Object> afterOutage = Json.parseObject(http.lookup(id).body());
            assertEquals("OPEN", afterOutage.get("status"));
            assertEquals(0, ((Number) afterOutage.get("itemCount")).intValue());

            assertEquals(200, http.scan(id, SKU).statusCode());
            assertEquals(beforePosition + 1, deployment.transactionSchema.scanCount());
            deployment.transactionSchema.setTableUnavailable("transaction", true);
            try {
                assertInternal(http.post("/transactions/" + id + "/complete", "{}"));
                assertEquals(200, deployment.inventoryHttp.get("/inventory/low-stock").statusCode());
            } finally {
                deployment.transactionSchema.setTableUnavailable("transaction", false);
            }
            assertEquals(2, deployment.inventorySchema.stock(SKU));
            assertEquals("OPEN", Json.parseObject(http.lookup(id).body()).get("status"));
            HttpResponse<String> completed = http.post("/transactions/" + id + "/complete", "{}");
            assertEquals(200, completed.statusCode(), completed.body() + deployment.logs());
            assertEquals(1, deployment.inventorySchema.stock(SKU));
            assertEquals("COMPLETED", Json.parseObject(http.lookup(id).body()).get("status"));
        }
    }

    @Test
    void inventoryDatabaseOutageAnswers500AndItsConsumerCommitsNoOffsetUntilTheDatabaseReturns() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.withAnalytics(5, 2, null)) {
            deployment.startAnalytics();
            ServiceHttp http = deployment.transactionHttp;
            String group = deployment.names.group(TopicNames.INVENTORY_CHECKOUT_GROUP);
            String topic = deployment.names.checkout();
            String id = http.start("inventory-db-outage");
            assertEquals(200, http.scan(id, SKU).statusCode());
            long committedBefore = committed(group, topic);
            long publishedBefore = published(topic);

            deployment.inventorySchema.setTableUnavailable("inventory", true);
            try {
                assertInternal(deployment.inventoryHttp.get("/inventory/low-stock"));
                long started = System.nanoTime();
                assertInternal(http.complete(id));
                assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(10), "Complete stays bounded");
                assertTrue(published(topic) > publishedBefore, "the checkout request reached the broker");
                Thread.sleep(2_000);
                assertEquals(committedBefore, committed(group, topic),
                        "Inventory's consumer commits no offset while its database cannot apply the request");
                assertEquals(1, deployment.transactionSchema.attempts(id).stream()
                        .filter(row -> row.state().equals("PENDING")).count(), "the attempt stays pending");

                String other = http.start("inventory-db-outage-peer");
                assertEquals(200, http.scan(other, SKU).statusCode(), "Transaction keeps scanning");
                assertEquals("OPEN", Json.parseObject(http.lookup(other).body()).get("status"));
                assertEquals(200, deployment.analyticsHttp.get("/analytics/popular-items").statusCode(),
                        "Analytics keeps serving");
            } finally {
                deployment.inventorySchema.setTableUnavailable("inventory", false);
            }

            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (deployment.transactionSchema.attempts(id).stream().noneMatch(row -> row.state().equals("COMPLETED"))) {
                if (System.nanoTime() > deadline) fail("the request was never applied " + deployment.logs());
                Thread.sleep(100);
            }
            assertEquals("COMPLETED", Json.parseObject(http.lookup(id).body()).get("status"));
            assertEquals(1, deployment.inventorySchema.stock(SKU), "deducted exactly once after the outage");
            assertEquals(200, deployment.inventoryHttp.get("/inventory/low-stock").statusCode());
            KafkaTestSupport.awaitNoLag(group, topic, Duration.ofSeconds(30));
            assertTrue(committed(group, topic) > committedBefore, "the offset is committed once the request is applied");
        }
    }

    /** The group's committed offsets summed over the topic's partitions; a partition never committed counts 0. */
    private static long committed(String group, String topic) {
        long sum = 0;
        for (int partition = 0; partition < TopicNames.Topic.CHECKOUT.partitions(); partition++) {
            sum += Math.max(0, KafkaTestSupport.committedOffset(group, topic, partition));
        }
        return sum;
    }

    private static long published(String topic) {
        long sum = 0;
        for (int partition = 0; partition < TopicNames.Topic.CHECKOUT.partitions(); partition++) {
            sum += KafkaTestSupport.endOffset(topic, partition);
        }
        return sum;
    }

    private static void assertInternal(HttpResponse<String> response) {
        assertEquals(500, response.statusCode(), response.body());
        assertEquals("INTERNAL_ERROR", Json.parseObject(response.body()).get("error"));
    }
}
