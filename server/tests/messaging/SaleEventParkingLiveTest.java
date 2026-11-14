package messaging;

import api.OpsHttpHandler;
import api.json.Json;
import events.ConsumerLoop;
import events.EnvelopeCodec;
import events.EventIds;
import events.EventTypes;
import events.TopicNames;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.SoldLine;
import support.KafkaTestSupport;
import support.LiveNotification;
import support.NotificationTestSchema;
import support.PublishRawRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 007 US5 acceptance scenario 4 (T098) on the live broker: {@link PublishRawRecord} sends a malformed
 * {@code SaleCompleted} and one with an unsupported schema version to the sale topic. Each is attempted five times
 * and parked in Notification's {@code dead_letter_event}; {@code ops/status} then shows {@code parkedEvents = 2} and
 * an alarm, and valid sales keep flowing, on other partitions while the bad records are retried and on the same
 * partitions after them.
 */
class SaleEventParkingLiveTest {
    private static final String SKU = "SKU-000001";
    private static final Duration WAIT = Duration.ofSeconds(60);
    private static final int SALE_PARTITIONS = 16;

    @Test
    void malformedAndUnsupportedSaleEventsAreParkedWhileValidSalesKeepFlowing() throws Exception {
        KafkaTestSupport.requireBroker();
        TopicNames names = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(names);
        String run = UUID.randomUUID().toString().substring(0, 6);
        String subscriber = "park-" + run;
        try (NotificationTestSchema schema = NotificationTestSchema.create();
             KafkaRecordSender saleSender = KafkaTestSupport.sender("park-sales", 0);
             LiveNotification notification = LiveNotification.start(schema, names, List.of(SKU), "park-sale-consumer", 4)) {
            ConsumerLoop sales = notification.sales;
            {
                notification.service.register(subscriber, List.of(SKU));
                notification.awaitAssigned(SALE_PARTITIONS, Duration.ofSeconds(30));

                // The bad records share their partitions with two valid sales published right after them.
                String malformedKey = "park-" + run + "-malformed";
                String unsupportedKey = "park-" + run + "-unsupported";
                Map<String, String> environment = KafkaTestSupport.clientEnvironment();
                PublishRawRecord.publish(environment, names.saleCompleted(), malformedKey,
                        "not an EventEnvelope".getBytes(StandardCharsets.UTF_8));
                String unsupportedSale = "park-" + run + "-v2";
                byte[] versionTwo = EventEnvelope.parseFrom(event(unsupportedSale)).toBuilder().setSchemaVersion(2)
                        .setRoutingKey(unsupportedKey).build().toByteArray();
                PublishRawRecord.publish(environment, names.saleCompleted(), unsupportedKey, versionTwo);
                List<String> valid = new ArrayList<>();
                valid.add(sameKeyPartitionSale(run, malformedKey));
                valid.add(sameKeyPartitionSale(run, unsupportedKey));
                for (int i = 0; i < 8; i++) valid.add("park-" + run + "-ok-" + i);
                for (String sale : valid) saleSender.send(names.saleCompleted(), sale, event(sale)).get();

                int malformedPartition = partition(malformedKey);
                int unsupportedPartition = partition(unsupportedKey);
                List<String> elsewhere = valid.stream()
                        .filter(sale -> partition(sale) != malformedPartition && partition(sale) != unsupportedPartition).toList();
                assertFalse(elsewhere.isEmpty(), "some valid sales use other partitions");
                await(() -> elsewhere.stream().allMatch(sale -> selected(schema, sale)),
                        "sales on other partitions are selected while the bad records are retried");
                assertEquals(partition(valid.get(0)), malformedPartition);
                assertEquals(partition(valid.get(1)), unsupportedPartition);

                await(() -> parkedEvents(schema) == 2, "both bad records are parked");
                await(() -> valid.stream().allMatch(sale -> selected(schema, sale)), "every valid sale is selected");
                await(() -> delivered(schema) == valid.size(), () -> "every valid sale is delivered; " + rows(schema));
                assertEquals(valid.size(), schema.saleSelections(), "only the valid sales are selected");
                assertEquals(2, sales.parked());

                assertEquals(Set.of("UNDECODABLE", "UNSUPPORTED_VERSION"), Set.of(
                        schema.queryString("SELECT reason FROM dead_letter_event WHERE event_id IS NULL"),
                        schema.queryString("SELECT reason FROM dead_letter_event WHERE event_id IS NOT NULL")));
                assertEquals(EventIds.saleEventId(unsupportedSale),
                        schema.queryString("SELECT event_id FROM dead_letter_event WHERE reason = 'UNSUPPORTED_VERSION'"));
                assertEquals(2, schema.queryLong("SELECT COUNT(*) FROM dead_letter_event WHERE topic = ? AND park_count = 1",
                        names.saleCompleted()));

                Map<String, Object> shown = Json.parseObject(OpsHttpHandler.serialize(notification.status.read()));
                assertEquals(2, ((Number) shown.get("parkedEvents")).intValue());
                List<?> alarms = (List<?>) shown.get("alarms");
                assertTrue(alarms.stream().anyMatch(alarm -> alarm.toString().startsWith("PARKED_EVENTS: 2")), alarms.toString());
            }
        }
    }

    /** A valid sale ID whose key lands on {@code key}'s partition. */
    private static String sameKeyPartitionSale(String run, String key) {
        int target = partition(key);
        for (int i = 0; ; i++) {
            // Named after the bad record it follows, so two bad records on one partition get two different sales.
            String sale = key + "-same-" + i;
            if (partition(sale) == target) return sale;
        }
    }

    /** Kafka's default partitioner for a keyed record on the sale topic. */
    private static int partition(String key) {
        return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % SALE_PARTITIONS;
    }

    private static byte[] event(String saleId) {
        long now = System.currentTimeMillis();
        SaleCompleted sale = SaleCompleted.newBuilder().setSaleId(saleId).setTransactionId(saleId).setAttemptNumber(1)
                .setCompletedAtMs(now).setStationId("park-station")
                .addLines(SoldLine.newBuilder().setSku(SKU).setQuantity(1)).build();
        return EnvelopeCodec.encode(EventIds.saleEventId(saleId), EventTypes.SALE_COMPLETED, saleId, saleId,
                EventTypes.TRANSACTION, now, sale);
    }

    private static boolean selected(NotificationTestSchema schema, String sale) {
        try {
            return schema.queryLong("SELECT COUNT(*) FROM sale_selection WHERE sale_id = ?", sale) == 1;
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static long parkedEvents(NotificationTestSchema schema) {
        try {
            return schema.queryLong("SELECT COUNT(*) FROM dead_letter_event");
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static long delivered(NotificationTestSchema schema) {
        try {
            return schema.notifications("DELIVERED");
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    /** The notification rows by state with their attempt and failure counts, for a timeout message. */
    private static String rows(NotificationTestSchema schema) {
        try {
            return schema.queryString("SELECT GROUP_CONCAT(CONCAT(state, ' attempts=', attempt_count, ' charged=', "
                    + "charged_failures, ' uncharged=', uncharged_failures) SEPARATOR '; ') FROM notification") + " | selections: "
                    + schema.queryString("SELECT GROUP_CONCAT(CONCAT(sale_id, ' version=', subscription_version, ' recipients=', "
                    + "recipient_count, ' rows=', notification_count) SEPARATOR '; ') FROM sale_selection") + " | clock "
                    + schema.subscriptionVersion() + ", changes " + schema.subscriptionChanges();
        } catch (Exception failure) {
            return "rows unreadable: " + failure;
        }
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        await(condition, () -> what);
    }

    private static void await(BooleanSupplier condition, java.util.function.Supplier<String> what) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) fail("timed out waiting: " + what.get());
            Thread.sleep(50);
        }
    }
}
