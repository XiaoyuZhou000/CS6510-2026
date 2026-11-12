package messaging;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;

/**
 * The one producer shared by every subscriber channel (007 contracts/notification-delivery.md "Dispatch"): the
 * 006 producer settings ({@code acks=all}, idempotence) with {@code linger.ms=5}, {@code delivery.timeout.ms=10000},
 * and {@code max.block.ms=1000}, so a stalled send fails within 10 s. {@code request.timeout.ms} is lowered to
 * 5,000 because Kafka requires {@code delivery.timeout.ms} to be at least {@code linger.ms} plus it. It is created on
 * its first send, so Notification starts with the broker down.
 */
public final class ChannelSender {
    public static final int LINGER_MS = 5;
    public static final int DELIVERY_TIMEOUT_MS = 10_000;

    private ChannelSender() { }

    public static Properties properties(Map<String, String> environment) {
        Properties properties = KafkaClients.producerProperties(environment, "notification-channels", LINGER_MS);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(DELIVERY_TIMEOUT_MS));
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return properties;
    }

    public static KafkaRecordSender lazy(Map<String, String> environment) {
        Map<String, String> copy = Map.copyOf(environment);
        return KafkaRecordSender.lazy(() -> new KafkaProducer<>(properties(copy)));
    }
}
