import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 007 US5 acceptance scenario 6 (T100) on the live broker, skipped when {@code KAFKA_TEST_BOOTSTRAP} is unset: 100
 * notifications are published to a test channel; the subscriber thread is stopped once it recorded 40 of them; 60
 * more are published while it is down, and a new thread resumes from the receipt file. All 160 are recorded once,
 * and at most one poll batch (10 records here) is recorded again, flagged as redeliveries.
 */
class SubscriberResumeLiveTest {
    private static final int POLL_BATCH = 10;

    @Test
    void aStoppedSubscriberResumesFromItsReceiptsWithoutLosingOrDuplicating(@TempDir Path dir) throws Exception {
        String bootstrap = setting("KAFKA_TEST_BOOTSTRAP");
        assumeTrue(bootstrap != null, "KAFKA_TEST_BOOTSTRAP is not set; the live resume test needs a broker");
        String run = UUID.randomUUID().toString().substring(0, 8);
        String subscriber = "resume-" + run;
        String channel = "t-" + run + ".selfcheckout.notify." + subscriber + ".v1";
        createChannel(bootstrap, channel);
        Supplier<Consumer<String, byte[]>> consumers = () -> consumer(bootstrap, subscriber);

        try (KafkaProducer<String, byte[]> producer = producer(bootstrap)) {
            publish(producer, channel, subscriber, 0, 100);

            SubscriberThread first = new SubscriberThread(subscriber, channel, new ReceiptWriter(dir, subscriber),
                    SubscriberThreadTest.QUIET, consumers);
            Thread firstRunner = start(first);
            SubscriberThreadTest.await("40 receipts", () -> first.stats().received() >= 40);
            first.stop();
            firstRunner.join(30_000);
            assertTrue(first.finished(), "the first thread stopped");
            assertNull(first.failure());
            long recordedBefore = first.stats().received();
            assertTrue(recordedBefore >= 40 && recordedBefore <= 100, "recorded before the stop: " + recordedBefore);

            publish(producer, channel, subscriber, 100, 160);

            SubscriberThread resumed = new SubscriberThread(subscriber, channel, new ReceiptWriter(dir, subscriber),
                    SubscriberThreadTest.QUIET, consumers);
            assertEquals(recordedBefore, resumed.stats().received(), "the receipts were read back");
            Thread resumedRunner = start(resumed);
            SubscriberThreadTest.await("all 160 receipts", () -> resumed.stats().received() == 160);
            Thread.sleep(1_000);   // a duplicate would show by now
            resumed.stop();
            resumedRunner.join(30_000);
            assertTrue(resumed.finished());
            assertNull(resumed.failure());
        }

        List<Map<String, Object>> receipts = receipts(dir, subscriber);
        Map<String, Integer> firstCopies = new HashMap<>();
        int redeliveries = 0;
        for (Map<String, Object> receipt : receipts) {
            assertFalse(Json.getBoolean(receipt, "undecodable"), receipt.toString());
            String id = Json.getString(receipt, "notificationId");
            if (Json.getBoolean(receipt, "redelivery")) {
                redeliveries++;
                assertTrue(firstCopies.containsKey(id), "a redelivery follows a first copy: " + id);
            } else {
                assertNull(firstCopies.put(id, 1), "recorded once as new: " + id);
            }
        }
        assertEquals(160, firstCopies.size(), "every notification is recorded once");
        for (int i = 0; i < 160; i++) assertTrue(firstCopies.containsKey(id(run, i)), id(run, i));
        assertTrue(redeliveries <= POLL_BATCH, "at most one poll batch is received again: " + redeliveries);
    }

    private static Thread start(SubscriberThread subscriber) {
        Thread runner = new Thread(subscriber, "resume-subscriber");
        runner.setDaemon(true);
        runner.start();
        return runner;
    }

    private static String id(String run, int i) {
        return "n-" + run + "-" + i;
    }

    private static void publish(KafkaProducer<String, byte[]> producer, String channel, String subscriber, int from, int to)
            throws Exception {
        String run = channel.substring(2, 10);
        for (int i = from; i < to; i++) {
            producer.send(new ProducerRecord<>(channel, subscriber, SubscriberThreadTest.notified(id(run, i), subscriber,
                    "txn-" + run + "-" + i, "SKU-000001", 1))).get(30, TimeUnit.SECONDS);
        }
    }

    /** One partition, and the never-rolling segments the Windows broker needs. */
    private static void createChannel(String bootstrap, String channel) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(properties)) {
            NewTopic topic = new NewTopic(channel, 1, (short) 1).configs(Map.of("segment.bytes", "1073741824",
                    "segment.ms", Long.toString(Long.MAX_VALUE), "cleanup.policy", "delete", "retention.ms", "604800000"));
            admin.createTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
        }
    }

    private static KafkaProducer<String, byte[]> producer(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    /** The application's consumer settings, with small poll batches so the stop lands mid-stream. */
    private static Consumer<String, byte[]> consumer(String bootstrap, String subscriber) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "subscriber-" + subscriber);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(POLL_BATCH));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    private static List<Map<String, Object>> receipts(Path dir, String subscriber) throws Exception {
        return java.nio.file.Files.readAllLines(dir.resolve(subscriber + ".jsonl"), java.nio.charset.StandardCharsets.UTF_8)
                .stream().filter(line -> !line.isBlank()).map(Json::parseObject).toList();
    }

    private static String setting(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) value = System.getenv(name);
        return value == null || value.isBlank() ? null : value.trim();
    }

}
