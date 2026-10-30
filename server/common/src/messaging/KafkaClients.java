package messaging;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import events.RecordStream;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Builds the Kafka producers and consumers with the configuration of research D1 and the event catalog
 * ("Delivery semantics"), from {@code KAFKA_BOOTSTRAP} (default {@code 127.0.0.1:19092}).
 *
 * <ul>
 *   <li><b>Producers</b>: {@code acks=all}, {@code enable.idempotence=true},
 *       {@code max.in.flight.requests.per.connection=5}, {@code delivery.timeout.ms=30000},
 *       {@code max.block.ms=1000}, and a per-producer {@code linger.ms}: 0 for checkout and outcome events,
 *       {@code SCAN_PRODUCER_LINGER_MS} (5) for scan and catalog batches. {@code request.timeout.ms} is 10,000,
 *       because Kafka requires {@code delivery.timeout.ms} to be at least {@code linger.ms} plus it.</li>
 *   <li><b>Consumers</b>: {@code enable.auto.commit=false}, {@code auto.offset.reset=earliest},
 *       {@code isolation.level=read_uncommitted}, {@code fetch.min.bytes=1}, {@code fetch.max.wait.ms=50},
 *       {@code max.poll.records=500}, and {@code session.timeout.ms=10000} with {@code heartbeat.interval.ms=3000},
 *       so a crashed member's partitions are reassigned within about 10 s. A consumer without a group (the
 *       catalog reader) never commits.</li>
 * </ul>
 */
public final class KafkaClients {
    public static final String BOOTSTRAP_VARIABLE = "KAFKA_BOOTSTRAP";
    public static final String DEFAULT_BOOTSTRAP = "127.0.0.1:19092";
    public static final String SCAN_LINGER_VARIABLE = "SCAN_PRODUCER_LINGER_MS";
    public static final int IMMEDIATE_LINGER_MS = 0;
    public static final int DEFAULT_BATCH_LINGER_MS = 5;
    /**
     * A crashed member's partitions move to the survivors after this long instead of Kafka's 45 s, so a
     * restarted service resumes its share of a group within seconds (US3, SC-005).
     */
    static final String SESSION_TIMEOUT_MS = "10000";
    static final String HEARTBEAT_INTERVAL_MS = "3000";

    private KafkaClients() { }

    public static String bootstrap(Map<String, String> environment) {
        String value = environment.get(BOOTSTRAP_VARIABLE);
        return value == null || value.isBlank() ? DEFAULT_BOOTSTRAP : value.trim();
    }

    /** {@code SCAN_PRODUCER_LINGER_MS}, the {@code linger.ms} of the scan and catalog producers (default 5). */
    public static int batchLingerMs(Map<String, String> environment) {
        String value = environment.get(SCAN_LINGER_VARIABLE);
        if (value == null || value.isBlank()) return DEFAULT_BATCH_LINGER_MS;
        int linger = Integer.parseInt(value.trim());
        if (linger < 0) throw new IllegalArgumentException(SCAN_LINGER_VARIABLE + " must not be negative");
        return linger;
    }

    public static Properties producerProperties(Map<String, String> environment, String clientId, int lingerMs) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap(environment));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(lingerMs));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return properties;
    }

    public static KafkaProducer<String, byte[]> producer(Map<String, String> environment, String clientId, int lingerMs) {
        return new KafkaProducer<>(producerProperties(environment, clientId, lingerMs));
    }

    /** {@code group} is null for a reader that never commits. */
    public static Properties consumerProperties(Map<String, String> environment, String clientId, String group) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap(environment));
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        if (group != null) properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "50");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, HEARTBEAT_INTERVAL_MS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return properties;
    }

    public static KafkaConsumer<String, byte[]> consumer(Map<String, String> environment, String clientId, String group) {
        return new KafkaConsumer<>(consumerProperties(environment, clientId, group));
    }

    /**
     * Opens a new {@link KafkaRecordStream} on every call, for a {@code ConsumerLoop} thread or its
     * replacement after a stream failure; each gets the client ID {@code clientId-<n>}.
     */
    public static Supplier<RecordStream> streams(Map<String, String> environment, String clientId, String group) {
        Map<String, String> copy = Map.copyOf(environment);
        AtomicInteger next = new AtomicInteger();
        return () -> {
            String name = clientId + "-" + next.incrementAndGet();
            return new KafkaRecordStream(consumer(copy, name, group), group, name);
        };
    }

    /** A {@link RecordSender} over a new producer with the given {@code linger.ms}. */
    public static KafkaRecordSender sender(Map<String, String> environment, String clientId, int lingerMs) {
        return new KafkaRecordSender(producer(environment, clientId, lingerMs));
    }
}
