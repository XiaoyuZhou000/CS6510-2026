package events;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import events.TopicNames.Topic;
import selfcheckout.events.v1.AttemptId;
import selfcheckout.events.v1.CatalogItemPublished;
import selfcheckout.events.v1.CatalogSnapshotPublished;
import selfcheckout.events.v1.CheckoutRequested;
import selfcheckout.events.v1.RestorationNotNeeded;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.ScanAccepted;
import selfcheckout.events.v1.StockDeducted;
import selfcheckout.events.v1.StockDeductionRefused;
import selfcheckout.events.v1.StockRejected;
import selfcheckout.events.v1.StockRestorationRequested;
import selfcheckout.events.v1.StockRestored;
import selfcheckout.events.v1.SubscriberNotified;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * The twelve event types of the catalog (006 contracts/event-catalog.md "Event types", D3, and 007
 * contracts/sale-completed-event.md): for each, its topic, its single owning producer (FR-009), its
 * supported schema versions, its payload parser, and the payload field that must equal the envelope's
 * {@code routing_key}.
 *
 * <p>{@link #SUBSCRIBER_NOTIFIED} is a channel-family type: each record goes to its subscriber's own channel
 * ({@link TopicNames#channel}), so it has no fixed {@link #topic()}.</p>
 */
public enum EventTypes {
    CATALOG_ITEM_PUBLISHED("CatalogItemPublished", Topic.CATALOG, EventTypes.INVENTORY,
            CatalogItemPublished.parser(), payload -> ((CatalogItemPublished) payload).getSku()),
    CATALOG_SNAPSHOT_PUBLISHED("CatalogSnapshotPublished", Topic.CATALOG, EventTypes.INVENTORY,
            CatalogSnapshotPublished.parser(), payload -> EventTypes.SNAPSHOT_KEY),
    CHECKOUT_REQUESTED("CheckoutRequested", Topic.CHECKOUT, EventTypes.TRANSACTION,
            CheckoutRequested.parser(), payload -> transactionOf(((CheckoutRequested) payload).getAttempt())),
    STOCK_RESTORATION_REQUESTED("StockRestorationRequested", Topic.CHECKOUT, EventTypes.TRANSACTION,
            StockRestorationRequested.parser(),
            payload -> transactionOf(((StockRestorationRequested) payload).getAttempt())),
    STOCK_DEDUCTED("StockDeducted", Topic.CHECKOUT_OUTCOME, EventTypes.INVENTORY,
            StockDeducted.parser(), payload -> transactionOf(((StockDeducted) payload).getAttempt())),
    STOCK_REJECTED("StockRejected", Topic.CHECKOUT_OUTCOME, EventTypes.INVENTORY,
            StockRejected.parser(), payload -> transactionOf(((StockRejected) payload).getAttempt())),
    STOCK_DEDUCTION_REFUSED("StockDeductionRefused", Topic.CHECKOUT_OUTCOME, EventTypes.INVENTORY,
            StockDeductionRefused.parser(), payload -> transactionOf(((StockDeductionRefused) payload).getAttempt())),
    STOCK_RESTORED("StockRestored", Topic.CHECKOUT_OUTCOME, EventTypes.INVENTORY,
            StockRestored.parser(), payload -> transactionOf(((StockRestored) payload).getAttempt())),
    RESTORATION_NOT_NEEDED("RestorationNotNeeded", Topic.CHECKOUT_OUTCOME, EventTypes.INVENTORY,
            RestorationNotNeeded.parser(), payload -> transactionOf(((RestorationNotNeeded) payload).getAttempt())),
    SCAN_ACCEPTED("ScanAccepted", Topic.SCAN_ACCEPTED, EventTypes.TRANSACTION,
            ScanAccepted.parser(), payload -> ((ScanAccepted) payload).getTransactionId()),
    SALE_COMPLETED("SaleCompleted", Topic.SALE_COMPLETED, EventTypes.TRANSACTION,
            SaleCompleted.parser(), payload -> ((SaleCompleted) payload).getTransactionId()),
    /** On the subscriber's channel topic, not a fixed one. */
    SUBSCRIBER_NOTIFIED("SubscriberNotified", null, EventTypes.NOTIFICATION,
            SubscriberNotified.parser(), payload -> ((SubscriberNotified) payload).getSubscriberId());

    public static final String TRANSACTION = "transaction";
    public static final String INVENTORY = "inventory";
    public static final String NOTIFICATION = "notification";
    /** Routing key of {@code CatalogSnapshotPublished}. */
    public static final String SNAPSHOT_KEY = "__snapshot__";
    /** Every type of this feature is version 1 (D3). */
    public static final int CURRENT_VERSION = 1;

    private final String typeName;
    private final Topic topic;
    private final String producer;
    private final Parser<? extends Message> parser;
    private final Function<Message, String> routingKeyOf;

    EventTypes(String typeName, Topic topic, String producer, Parser<? extends Message> parser,
               Function<Message, String> routingKeyOf) {
        this.typeName = typeName;
        this.topic = topic;
        this.producer = producer;
        this.parser = parser;
        this.routingKeyOf = routingKeyOf;
    }

    /** The {@code event_type} string, for example {@code CheckoutRequested}. */
    public String typeName() { return typeName; }

    /** The fixed topic of this type, or empty for a channel-family type ({@link #isChannelType()}). */
    public Optional<Topic> topic() { return Optional.ofNullable(topic); }

    /** Whether each record of this type goes to its subscriber's channel topic instead of a fixed topic. */
    public boolean isChannelType() { return topic == null; }

    /** {@code transaction}, {@code inventory}, or {@code notification}. */
    public String producer() { return producer; }

    public Set<Integer> supportedVersions() { return Set.of(CURRENT_VERSION); }

    public Parser<? extends Message> parser() { return parser; }

    /** The payload's identity field that the envelope's {@code routing_key} must equal. */
    public String routingKeyOf(Message payload) { return routingKeyOf.apply(payload); }

    public static Optional<EventTypes> byName(String typeName) {
        for (EventTypes type : values()) {
            if (type.typeName.equals(typeName)) return Optional.of(type);
        }
        return Optional.empty();
    }

    /** The deterministic {@code event_id} of a scan: {@code scan:<transaction_id>:<scan_sequence>} (D3). */
    public static String scanEventId(String transactionId, int scanSequence) {
        return "scan:" + transactionId + ":" + scanSequence;
    }

    /** The outbox {@code aggregate_key} of a checkout event: {@code <transaction_id>:<attempt_number>}. */
    public static String attemptKey(String transactionId, int attemptNumber) {
        return transactionId + ":" + attemptNumber;
    }

    private static String transactionOf(AttemptId attempt) {
        return attempt.getTransactionId();
    }
}
