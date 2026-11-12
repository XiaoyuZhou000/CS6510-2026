package events;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The five fixed topics and four consumer groups of the event catalog (006 contracts/event-catalog.md
 * "Topics", D4, and 007 contracts/sale-completed-event.md), and the per-subscriber channel topics
 * {@code selfcheckout.notify.<subscriberId>.v1} that Notification Service creates itself.
 *
 * <p>Every name carries the prefix {@code KAFKA_TOPIC_PREFIX} (default empty). Production runs leave it
 * empty; tests set a per-run prefix so their topics and groups never meet another run's records or
 * committed offsets. Group names are prefixed too, so one group never subscribes to two runs' topics.</p>
 */
public final class TopicNames {
    public static final String PREFIX_VARIABLE = "KAFKA_TOPIC_PREFIX";
    public static final String INVENTORY_CHECKOUT_GROUP = "inventory-checkout";
    public static final String TRANSACTION_CHECKOUT_OUTCOME_GROUP = "transaction-checkout-outcome";
    public static final String ANALYTICS_SCAN_INGEST_GROUP = "analytics-scan-ingest";
    public static final String NOTIFICATION_SALE_COMPLETED_GROUP = "notification-sale-completed";
    /** A subscriber ID, and so the variable part of its channel topic name (007 data-model.md). */
    public static final Pattern SUBSCRIBER_ID = Pattern.compile("^[A-Za-z0-9_-]{1,40}$");
    private static final String CHANNEL_START = "selfcheckout.notify.";
    private static final String CHANNEL_END = ".v1";

    /** A topic of the catalog, by its unprefixed name. */
    public enum Topic {
        CATALOG("selfcheckout.catalog.v1", 1),
        CHECKOUT("selfcheckout.checkout.v1", 16),
        CHECKOUT_OUTCOME("selfcheckout.checkout-outcome.v1", 16),
        SCAN_ACCEPTED("selfcheckout.scan-accepted.v1", 1),
        SALE_COMPLETED("selfcheckout.sale-completed.v1", 16);

        private final String baseName;
        private final int partitions;

        Topic(String baseName, int partitions) {
            this.baseName = baseName;
            this.partitions = partitions;
        }

        public String baseName() { return baseName; }

        public int partitions() { return partitions; }
    }

    private final String prefix;

    public TopicNames(String prefix) {
        this.prefix = prefix == null ? "" : prefix;
    }

    public static TopicNames unprefixed() {
        return new TopicNames("");
    }

    public static TopicNames fromEnvironment(Map<String, String> environment) {
        String prefix = environment.get(PREFIX_VARIABLE);
        return new TopicNames(prefix == null ? "" : prefix.trim());
    }

    public String prefix() { return prefix; }

    public String name(Topic topic) { return prefix + topic.baseName(); }

    public String catalog() { return name(Topic.CATALOG); }

    public String checkout() { return name(Topic.CHECKOUT); }

    public String checkoutOutcome() { return name(Topic.CHECKOUT_OUTCOME); }

    public String scanAccepted() { return name(Topic.SCAN_ACCEPTED); }

    public String saleCompleted() { return name(Topic.SALE_COMPLETED); }

    /**
     * The channel topic of one subscriber: {@code <prefix>selfcheckout.notify.<subscriberId>.v1}.
     *
     * @throws IllegalArgumentException if {@code subscriberId} does not match {@link #SUBSCRIBER_ID}
     */
    public String channel(String subscriberId) {
        if (subscriberId == null || !SUBSCRIBER_ID.matcher(subscriberId).matches()) {
            throw new IllegalArgumentException("subscriber ID must match " + SUBSCRIBER_ID.pattern() + ": " + subscriberId);
        }
        return prefix + CHANNEL_START + subscriberId + CHANNEL_END;
    }

    /** Whether {@code name} is a channel topic of this prefix, with a valid subscriber ID. */
    public boolean isChannel(String name) {
        if (name == null || !name.startsWith(prefix + CHANNEL_START) || !name.endsWith(CHANNEL_END)) return false;
        String subscriberId = name.substring(prefix.length() + CHANNEL_START.length(), name.length() - CHANNEL_END.length());
        return SUBSCRIBER_ID.matcher(subscriberId).matches();
    }

    /** Every fixed topic name, in catalog order; channel topics are not included. */
    public List<String> all() {
        return List.of(catalog(), checkout(), checkoutOutcome(), scanAccepted(), saleCompleted());
    }

    /** The prefixed name of one of the group constants above. */
    public String group(String baseGroup) { return prefix + baseGroup; }

    /** The fixed topic a prefixed name denotes, or empty for a channel or a name outside this prefix's catalog. */
    public Optional<Topic> topicOf(String name) {
        for (Topic topic : Topic.values()) {
            if (name(topic).equals(name)) return Optional.of(topic);
        }
        return Optional.empty();
    }
}
