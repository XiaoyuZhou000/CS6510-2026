package events;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The four topics and three consumer groups of the event catalog (contracts/event-catalog.md "Topics", D4).
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

    /** A topic of the catalog, by its unprefixed name. */
    public enum Topic {
        CATALOG("selfcheckout.catalog.v1", 1),
        CHECKOUT("selfcheckout.checkout.v1", 16),
        CHECKOUT_OUTCOME("selfcheckout.checkout-outcome.v1", 16),
        SCAN_ACCEPTED("selfcheckout.scan-accepted.v1", 1);

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

    /** Every topic name, in catalog order. */
    public List<String> all() {
        return List.of(catalog(), checkout(), checkoutOutcome(), scanAccepted());
    }

    /** The prefixed name of one of the group constants above. */
    public String group(String baseGroup) { return prefix + baseGroup; }

    /** The topic a prefixed name denotes, or empty for a name outside this prefix's catalog. */
    public Optional<Topic> topicOf(String name) {
        for (Topic topic : Topic.values()) {
            if (name(topic).equals(name)) return Optional.of(topic);
        }
        return Optional.empty();
    }
}
