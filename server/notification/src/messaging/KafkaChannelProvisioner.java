package messaging;

import notification.ChannelProvisioner;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Creates subscriber channel topics with the Kafka {@code Admin} client (007 research N7, T051): one partition,
 * replication 1, {@code cleanup.policy=delete}, the 006 Windows segment pins ({@code segment.bytes=1073741824},
 * {@code segment.ms=9223372036854775807}), {@code segment.index.bytes=1048576} to bound the memory-mapped index files
 * of a hundred or more topics, and no retention. {@code TopicExistsException} is success, so provisioning is
 * idempotent. The admin client is created on first use, so startup never waits for the broker.
 */
public final class KafkaChannelProvisioner implements ChannelProvisioner, AutoCloseable {
    static final Map<String, String> CHANNEL_CONFIG = Map.of(
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
            TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824",
            TopicConfig.SEGMENT_MS_CONFIG, Long.toString(Long.MAX_VALUE),
            TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, "1048576",
            TopicConfig.RETENTION_MS_CONFIG, "-1",
            TopicConfig.RETENTION_BYTES_CONFIG, "-1");

    private final String bootstrap;
    private Admin admin;
    private boolean closed;

    public KafkaChannelProvisioner(String bootstrap) {
        this.bootstrap = Objects.requireNonNull(bootstrap, "bootstrap");
    }

    /** From {@code KAFKA_BOOTSTRAP}. */
    public static KafkaChannelProvisioner fromEnvironment(Map<String, String> environment) {
        return new KafkaChannelProvisioner(KafkaClients.bootstrap(environment));
    }

    @Override
    public void provision(String topic) {
        NewTopic channel = new NewTopic(topic, 1, (short) 1).configs(CHANNEL_CONFIG);
        try {
            admin().createTopics(List.of(channel)).all().get(15, TimeUnit.SECONDS);
        } catch (ExecutionException failure) {
            if (failure.getCause() instanceof TopicExistsException) return;
            throw new IllegalStateException("channel topic " + topic + " could not be created: " + failure.getCause(), failure);
        } catch (TimeoutException failure) {
            throw new IllegalStateException("channel topic " + topic + " was not confirmed in time", failure);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while creating channel topic " + topic, interrupted);
        }
    }

    private synchronized Admin admin() {
        if (closed) throw new IllegalStateException("the channel provisioner is closed");
        if (admin == null) admin = KafkaClients.admin(bootstrap);
        return admin;
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (admin != null) admin.close(java.time.Duration.ofSeconds(5));
    }
}
