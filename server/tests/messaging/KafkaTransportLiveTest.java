package messaging;

import events.ConsumerLoop;
import events.EnvelopeCodec;
import events.EventTypes;
import events.ParkReason;
import events.RecordStream;
import events.TransientEventFailure;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.AttemptId;
import selfcheckout.events.v1.CatalogItemPublished;
import selfcheckout.events.v1.StockDeducted;
import support.InMemoryDeadLetterStore;
import support.KafkaTestSupport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Kafka adapters and the shared consumer loop against the real broker (D5, D6, D14). Skipped without a
 * broker at {@code KAFKA_TEST_BOOTSTRAP}, and failing under {@code STRICT_LIVE_GATES=1}; each test uses its own
 * topics under a fresh prefix and never deletes them.
 */
class KafkaTransportLiveTest {
    private final String prefix = "t-" + UUID.randomUUID().toString().substring(0, 8) + ".";
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable closeable : closeables) closeable.close();
    }

    @Test
    void sendsCompleteAfterAcksAllAndTheIdempotentProducerKeepsPerKeyOrder() throws Exception {
        KafkaTestSupport.requireBroker();
        Properties properties = KafkaClients.producerProperties(KafkaTestSupport.clientEnvironment(), "order", 0);
        assertEquals("all", properties.get(ProducerConfig.ACKS_CONFIG));
        assertEquals("true", properties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("5", properties.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        assertEquals("1000", properties.get(ProducerConfig.MAX_BLOCK_MS_CONFIG));

        String topic = prefix + "ordering";
        KafkaTestSupport.createTopic(topic, 16, "delete");
        KafkaRecordSender sender = keep(KafkaTestSupport.sender("order", 0));
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            String key = "tx-" + (i % 25);
            futures.add(sender.send(topic, key, (key + "#" + i).getBytes(StandardCharsets.UTF_8)));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);

        RecordStream stream = keep(KafkaTestSupport.streams("order-reader", null).get());
        List<RecordStream.Partition> partitions = new ArrayList<>();
        for (int p = 0; p < 16; p++) partitions.add(new RecordStream.Partition(topic, p));
        stream.assign(partitions);
        for (RecordStream.Partition partition : partitions) stream.seek(partition, 0);
        List<RecordStream.Record> read = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (read.size() < 1_000 && System.nanoTime() < deadline) read.addAll(stream.poll(Duration.ofMillis(200)));
        assertEquals(1_000, read.size(), "every confirmed record is in the log");
        Map<String, Integer> last = new HashMap<>();
        Map<String, Integer> partitionOfKey = new HashMap<>();
        for (RecordStream.Record record : read) {
            String[] parts = new String(record.value(), StandardCharsets.UTF_8).split("#");
            Integer previous = last.put(parts[0], Integer.parseInt(parts[1]));
            assertTrue(previous == null || previous < Integer.parseInt(parts[1]), "per-key order for " + parts[0]);
            Integer partition = partitionOfKey.putIfAbsent(record.key(), record.partition());
            assertTrue(partition == null || partition == record.partition(), "a key stays on one partition");
        }
    }

    @Test
    void anUnreachableBrokerFailsTheSendWithinMaxBlockWithoutBlockingTheCaller() throws Exception {
        Map<String, String> nowhere = Map.of(KafkaClients.BOOTSTRAP_VARIABLE, "127.0.0.1:1");
        KafkaRecordSender sender = keep(new KafkaRecordSender(KafkaClients.producer(nowhere, "unreachable", 0)));
        long started = System.nanoTime();
        CompletableFuture<Void> future = sender.send(prefix + "nowhere", "k", new byte[] {1});
        long returned = System.nanoTime() - started;
        assertTrue(returned < TimeUnit.MILLISECONDS.toNanos(1_500), "send returned after " + returned / 1_000_000 + " ms");
        ExecutionException failure = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertNotNull(failure.getCause());
        assertTrue(System.nanoTime() - started < TimeUnit.MILLISECONDS.toNanos(2_500), "failed within about max.block.ms");
    }

    @Test
    void aConsumerRestartedAfterAHandlerFailureReadsTheSameRecordAgain() throws Exception {
        KafkaTestSupport.requireBroker();
        String topic = prefix + "redelivery";
        String group = prefix + "redelivery-group";
        KafkaTestSupport.createTopic(topic, 16, "delete");
        KafkaRecordSender sender = keep(KafkaTestSupport.sender("redelivery", 0));
        sender.send(topic, "tx-r", deducted("first", "tx-r")).get(10, TimeUnit.SECONDS);

        List<String> failedAttempts = new CopyOnWriteArrayList<>();
        ConsumerLoop failing = ConsumerLoop.group("test", group, topic, KafkaTestSupport.streams("redelivery", group),
                event -> {
                    failedAttempts.add(event.eventId());
                    throw new TransientEventFailure("database down");
                }, new InMemoryDeadLetterStore(), ConsumerLoop.Settings.defaults());
        failing.start();
        await(() -> !failedAttempts.isEmpty(), 30);
        failing.stop(Duration.ofSeconds(10));

        List<String> handled = new CopyOnWriteArrayList<>();
        ConsumerLoop healthy = keepLoop(ConsumerLoop.group("test", group, topic, KafkaTestSupport.streams("redelivery", group),
                event -> handled.add(event.eventId()), new InMemoryDeadLetterStore(), ConsumerLoop.Settings.defaults()));
        healthy.start();
        await(() -> handled.contains("first"), 30);
        assertEquals(List.of("first"), handled, "no offset was committed past the failed record");
    }

    @Test
    void aPoisonRecordIsParkedWithinAboutOneSecondWhileAnotherPartitionKeepsFlowing() throws Exception {
        KafkaTestSupport.requireBroker();
        String topic = prefix + "poison";
        String group = prefix + "poison-group";
        KafkaTestSupport.createTopic(topic, 16, "delete");
        InMemoryDeadLetterStore parked = new InMemoryDeadLetterStore();
        List<String> handled = new CopyOnWriteArrayList<>();
        ConsumerLoop loop = keepLoop(ConsumerLoop.group("test", group, topic, KafkaTestSupport.streams("poison", group),
                event -> handled.add(event.eventId()), parked, ConsumerLoop.Settings.defaults()));
        loop.start();
        KafkaRecordSender sender = keep(KafkaTestSupport.sender("poison", 0));
        sender.send(topic, "tx-warmup", deducted("warmup", "tx-warmup")).get(10, TimeUnit.SECONDS);
        await(() -> handled.contains("warmup"), 30);

        String poisonKey = "tx-poison";
        String cleanKey = keyOnAnotherPartition(poisonKey);
        long sentPoison = System.nanoTime();
        sender.send(topic, poisonKey, "\u0000not an envelope".getBytes(StandardCharsets.ISO_8859_1)).get(10, TimeUnit.SECONDS);
        sender.send(topic, cleanKey, deducted("clean", cleanKey)).get(10, TimeUnit.SECONDS);
        await(() -> handled.contains("clean"), 10);
        assertTrue(System.nanoTime() - sentPoison < TimeUnit.MILLISECONDS.toNanos(700),
                "the clean partition does not wait for the poison record");
        await(() -> parked.countParked() == 1, 10);
        long parkedAfter = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sentPoison);
        assertTrue(parkedAfter < 2_500, "parked after " + parkedAfter + " ms");
        assertEquals(ParkReason.UNDECODABLE, parked.rows().get(0).first.reason());
        assertNull(parked.rows().get(0).first.eventId());
    }

    @Test
    void manualAssignmentReplaysACompactedTopicFromOffsetZero() throws Exception {
        KafkaTestSupport.requireBroker();
        String topic = prefix + "catalog";
        KafkaTestSupport.createTopic(topic, 1, "compact");
        KafkaRecordSender sender = keep(KafkaTestSupport.sender("catalog", 5));
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            String sku = "SKU-" + (i % 20);
            CatalogItemPublished item = CatalogItemPublished.newBuilder().setSku(sku).setName("n" + i).setPrice("1.00")
                    .setCatalogVersion("v").build();
            futures.add(sender.send(topic, sku, EnvelopeCodec.encode("c" + i, EventTypes.CATALOG_ITEM_PUBLISHED, sku, "v",
                    EventTypes.INVENTORY, i, item)));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        for (int run = 0; run < 2; run++) {
            List<String> seen = new CopyOnWriteArrayList<>();
            ConsumerLoop reader = keepLoop(ConsumerLoop.manual("test", "catalog-" + run,
                    List.of(new RecordStream.Partition(topic, 0)), ignored -> 0L, KafkaTestSupport.streams("catalog", null),
                    event -> seen.add(event.eventId()), new InMemoryDeadLetterStore(), ConsumerLoop.Settings.defaults()));
            reader.start();
            await(() -> seen.size() == 50, 30);
            assertEquals("c0", seen.get(0), "replay starts at offset 0");
            assertEquals("c49", seen.get(49));
            reader.stop(Duration.ofSeconds(5));
        }
    }

    private static byte[] deducted(String eventId, String key) {
        StockDeducted payload = StockDeducted.newBuilder()
                .setAttempt(AttemptId.newBuilder().setTransactionId(key).setAttemptNumber(1)).build();
        return EnvelopeCodec.encode(eventId, EventTypes.STOCK_DEDUCTED, key, key, EventTypes.INVENTORY,
                System.currentTimeMillis(), payload);
    }

    private static String keyOnAnotherPartition(String key) {
        support.InMemoryEventBus partitioner = new support.InMemoryEventBus(0);
        partitioner.createTopic("p", 16);
        int partition = partitioner.partitionFor("p", key);
        for (int i = 0; ; i++) {
            if (partitioner.partitionFor("p", "tx-clean-" + i) != partition) return "tx-clean-" + i;
        }
    }

    private <T extends AutoCloseable> T keep(T closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private ConsumerLoop keepLoop(ConsumerLoop loop) {
        closeables.add(0, loop);
        return loop;
    }

    private static void await(BooleanSupplier condition, int seconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("condition not reached within " + seconds + " s");
            Thread.sleep(10);
        }
    }
}
