import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.SubscriberNotified;

import java.io.PrintStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** T074: one subscriber thread decodes its channel, records receipts, and resumes from them (research N10). */
class SubscriberThreadTest {
    static final String CHANNEL = "selfcheckout.notify.sub-0001.v1";
    static final TopicPartition PARTITION = new TopicPartition(CHANNEL, 0);
    static final PrintStream QUIET = new PrintStream(OutputStream.nullOutputStream());

    static byte[] notified(String id, String subscriber, String sale, String sku, int quantity) {
        SubscriberNotified payload = SubscriberNotified.newBuilder().setNotificationId(id).setSubscriberId(subscriber)
                .setSaleId(sale).setSku(sku).setQuantity(quantity).setCompletedAtMs(1_000L).setSubscriptionVersion(3)
                .build();
        return EventEnvelope.newBuilder().setEventId(id).setEventType("SubscriberNotified").setSchemaVersion(1)
                .setRoutingKey(subscriber).setCorrelationId(sale).setProducer("notification")
                .setPayload(payload.toByteString()).build().toByteArray();
    }

    static ConsumerRecord<String, byte[]> record(long offset, byte[] value) {
        return new ConsumerRecord<>(CHANNEL, 0, offset, "sub-0001", value);
    }

    static MockConsumer<String, byte[]> mock(long endOffset, List<ConsumerRecord<String, byte[]>> records) {
        MockConsumer<String, byte[]> consumer = new MockConsumer<>("earliest");
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        consumer.updateEndOffsets(Map.of(PARTITION, endOffset));
        consumer.schedulePollTask(() -> records.forEach(consumer::addRecord));
        return consumer;
    }

    static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("Timed out waiting for " + what);
            Thread.sleep(20);
        }
    }

    static List<Map<String, Object>> receipts(Path dir) throws Exception {
        return Files.readAllLines(dir.resolve("sub-0001.jsonl"), StandardCharsets.UTF_8).stream()
                .filter(l -> !l.isBlank()).map(Json::parseObject).toList();
    }

    private static SubscriberThread start(Path dir, Deque<Consumer<String, byte[]>> consumers) throws Exception {
        SubscriberThread subscriber = new SubscriberThread("sub-0001", CHANNEL, new ReceiptWriter(dir, "sub-0001"), QUIET,
                consumers::removeFirst);
        Thread runner = new Thread(subscriber, "test-subscriber");
        runner.setDaemon(true);
        runner.start();
        return subscriber;
    }

    @Test
    void recordsNotificationsAndUndecodableRecords(@TempDir Path dir) throws Exception {
        Deque<Consumer<String, byte[]>> consumers = new ArrayDeque<>();
        consumers.add(mock(6, List.of(
                record(0, notified("n-1", "sub-0001", "txn-1", "SKU-000001", 3)),
                record(1, "garbage".getBytes(StandardCharsets.UTF_8)),
                record(2, notified("n-2", "sub-0002", "txn-1", "SKU-000002", 1)),
                record(3, notified("n-1", "sub-0001", "txn-1", "SKU-000001", 3)),
                record(4, null),
                record(5, notified("n-3", "sub-0001", "txn-2", "SKU-000002", 1)))));
        SubscriberThread subscriber = start(dir, consumers);
        await("six records", () -> {
            StatusWriter.Stats s = subscriber.stats();
            return s.received() + s.redeliveries() + s.undecodable() == 6;
        });
        await("lag", () -> subscriber.stats().lag() == 0);
        subscriber.stop();
        await("stop", subscriber::finished);
        assertNull(subscriber.failure());

        StatusWriter.Stats stats = subscriber.stats();
        assertEquals(2, stats.received());
        assertEquals(1, stats.redeliveries());
        assertEquals(3, stats.undecodable());
        List<Map<String, Object>> lines = receipts(dir);
        assertEquals(6, lines.size());
        assertEquals("n-1", lines.get(0).get("notificationId"));
        assertEquals(3L, lines.get(0).get("quantity"));
        assertEquals(Boolean.TRUE, lines.get(1).get("undecodable"));
        assertTrue(String.valueOf(lines.get(2).get("error")).contains("addressed to sub-0002"));
        assertEquals(Boolean.TRUE, lines.get(3).get("redelivery"));
        assertEquals(Boolean.TRUE, lines.get(4).get("undecodable"));
        assertEquals(5L, lines.get(5).get("channelOffset"));
    }

    @Test
    void resumesAfterTheHighestRecordedOffset(@TempDir Path dir) throws Exception {
        try (ReceiptWriter earlier = new ReceiptWriter(dir, "sub-0001")) {
            earlier.record("n-1", "txn-1", "SKU-000001", 3, 1_000L, 1_100L, 0L);
            earlier.record("n-2", "txn-2", "SKU-000001", 1, 1_000L, 1_100L, 1L);
            earlier.flush();
        }
        Deque<Consumer<String, byte[]>> consumers = new ArrayDeque<>();
        // The channel still holds offsets 0 and 1; only 2 and 3 are new.
        consumers.add(mock(4, List.of(
                record(0, notified("n-1", "sub-0001", "txn-1", "SKU-000001", 3)),
                record(1, notified("n-2", "sub-0001", "txn-2", "SKU-000001", 1)),
                record(2, notified("n-3", "sub-0001", "txn-3", "SKU-000001", 2)),
                record(3, notified("n-4", "sub-0001", "txn-4", "SKU-000001", 1)))));
        SubscriberThread subscriber = start(dir, consumers);
        await("two new records", () -> subscriber.stats().received() == 4);
        subscriber.stop();
        await("stop", subscriber::finished);
        assertEquals(0, subscriber.stats().redeliveries(), "records before the resume offset are not read again");
        assertEquals(List.of(0L, 1L, 2L, 3L), receipts(dir).stream().map(r -> r.get("channelOffset")).toList());
    }

    @Test
    void aFailedConsumerIsReplacedAndResumes(@TempDir Path dir) throws Exception {
        MockConsumer<String, byte[]> failing = new MockConsumer<>("earliest");
        failing.updateBeginningOffsets(Map.of(PARTITION, 0L));
        failing.updateEndOffsets(Map.of(PARTITION, 1L));
        failing.schedulePollTask(() -> {
            failing.addRecord(record(0, notified("n-1", "sub-0001", "txn-1", "SKU-000001", 3)));
        });
        failing.schedulePollTask(() -> failing.setPollException(new KafkaException("broker went away")));
        Deque<Consumer<String, byte[]>> consumers = new ArrayDeque<>();
        consumers.add(failing);
        consumers.add(mock(2, List.of(
                record(0, notified("n-1", "sub-0001", "txn-1", "SKU-000001", 3)),
                record(1, notified("n-2", "sub-0001", "txn-2", "SKU-000001", 1)))));
        SubscriberThread subscriber = start(dir, consumers);
        await("both records", () -> subscriber.stats().received() == 2);
        subscriber.stop();
        await("stop", subscriber::finished);
        assertNull(subscriber.failure(), "one failure is ridden out");
        assertEquals(0, subscriber.stats().redeliveries());
        assertEquals(List.of(0L, 1L), receipts(dir).stream().map(r -> r.get("channelOffset")).toList());
    }
}
