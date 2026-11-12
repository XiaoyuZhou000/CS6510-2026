package notification;

/**
 * Creates a subscriber's channel topic (007 research N7), behind which the messaging adapter uses the Kafka
 * {@code Admin} client. Creation is idempotent: a topic that already exists is success.
 */
@FunctionalInterface
public interface ChannelProvisioner {
    /** Creates {@code topic} if it does not exist; throws when that could not be confirmed. */
    void provision(String topic);
}
