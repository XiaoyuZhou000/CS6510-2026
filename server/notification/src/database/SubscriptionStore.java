package database;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;

/**
 * Notification's subscription state and its ordered history (007 data-model.md section 2, research N4): the
 * {@code subscription_clock}, {@code subscriber}, {@code subscription}, and {@code subscription_change} tables.
 *
 * <p>Every change runs the change transaction of contracts/notification-delivery.md "Subscription change
 * transaction": the clock is locked {@code FOR UPDATE}, so changes commit one at a time and version order is
 * commit order. A rejected or no-op request rolls back and writes nothing.</p>
 */
public interface SubscriptionStore {
    enum Status { ACTIVE, CANCELLED }

    enum ChangeType { REGISTER, UPDATE, CANCEL }

    /** A subscriber's committed state; {@code skus} sorted, empty when cancelled. */
    record SubscriberState(String subscriberId, Status status, List<String> skus, String channelTopic,
                           boolean channelReady, long createdVersion, long updatedVersion) {
        public SubscriberState {
            skus = List.copyOf(skus);
        }
    }

    /** One immutable {@code subscription_change} row; each list sorted. */
    record ChangeRow(long version, String subscriberId, ChangeType type, List<String> addedSkus,
                     List<String> removedSkus, List<String> resultingSkus) {
        public ChangeRow {
            addedSkus = List.copyOf(addedSkus);
            removedSkus = List.copyOf(removedSkus);
            resultingSkus = List.copyOf(resultingSkus);
        }
    }

    /** The active subscriptions at one history version, read in one consistent unit. */
    record IndexRows(long version, Map<String, List<String>> skusBySubscriber) {
        public IndexRows {
            skusBySubscriber = Map.copyOf(skusBySubscriber);
        }
    }

    /** A subscriber's channel topic and whether it was created. */
    record Channel(String subscriberId, String topic, boolean ready) { }

    /** What a change request did. */
    sealed interface ChangeResult permits Applied, Unchanged, Rejected { }

    /** The change committed at {@code change.version()}; {@code created} for a new or re-registered subscriber. */
    record Applied(SubscriberState state, ChangeRow change, boolean created) implements ChangeResult { }

    /** An idempotent repeat: nothing was written, and {@code state} is the current one. */
    record Unchanged(SubscriberState state) implements ChangeResult { }

    /** Why a request was rejected; {@link SubscriptionChangeRules} decides it in the HTTP contract's precedence. */
    enum Rejection { INVALID_REQUEST, SUBSCRIBER_NOT_FOUND, SUBSCRIBER_EXISTS, SUBSCRIBER_CANCELLED, LAST_SKU, UNKNOWN_SKU }

    /** Rejected under the clock lock; nothing was written. */
    record Rejected(Rejection reason, String detail) implements ChangeResult { }

    /**
     * Registers {@code subscriberId} for {@code skus}, or registers a cancelled subscriber again. An active
     * subscriber with the same set is {@link Unchanged}; with another set it is rejected as
     * {@link Rejection#SUBSCRIBER_EXISTS}. {@code firstUnknownSku}, when not null, is the first requested SKU
     * outside the catalog: after the state checks the request is rejected as {@link Rejection#UNKNOWN_SKU}
     * (contracts/notification-http.md "Validation precedence").
     */
    ChangeResult register(String subscriberId, SortedSet<String> skus, String channelTopic, String firstUnknownSku,
                          Instant at);

    /**
     * Adds {@code add} to and removes {@code remove} from an active subscriber's SKUs ({@link SubscriptionChangeRules#update}):
     * an effective change is one {@code UPDATE} row with the effective SKUs; adding held SKUs and removing absent ones
     * is {@link Unchanged}.
     */
    ChangeResult update(String subscriberId, SortedSet<String> add, SortedSet<String> remove, String firstUnknownSku,
                        Instant at);

    /**
     * Cancels every subscription of {@code subscriberId} with one {@code CANCEL} row, keeping its identity and channel
     * for a later registration; a cancelled subscriber is {@link Unchanged} at the version that cancelled it.
     */
    ChangeResult cancel(String subscriberId, Instant at);

    Optional<SubscriberState> find(String subscriberId);

    /** The active subscriptions and the clock, read under the clock's shared lock. */
    IndexRows loadIndex();

    /** The history rows with {@code version} in {@code (after, upTo]}, in version order. */
    List<ChangeRow> changesAfter(long after, long upTo);

    /** {@code subscription_clock.version}. */
    long currentVersion();

    Optional<Channel> channel(String subscriberId);

    void markChannelReady(String subscriberId);
}
