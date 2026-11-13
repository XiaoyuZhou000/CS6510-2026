package notification;

import database.StoreFailure;
import database.SubscriptionChangeRules;
import database.SubscriptionStore;
import database.SubscriptionStore.Applied;
import database.SubscriptionStore.ChangeResult;
import database.SubscriptionStore.ChangeRow;
import database.SubscriptionStore.Rejected;
import database.SubscriptionStore.SubscriberState;
import database.SubscriptionStore.Unchanged;
import events.TopicNames;
import metrics.PerfMetrics;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Subscription changes with commit-time effect (007 FR-006–FR-008, contracts/notification-delivery.md "Subscription
 * change transaction").
 *
 * <ol>
 *   <li>Validation in the HTTP contract's precedence: the subscriber ID ({@code ^[A-Za-z0-9_-]{1,40}$}), a non-empty
 *       SKU list, and a loaded catalog; then, under the clock lock, the subscriber's state, then the SKUs against
 *       {@link KnownSkus}.</li>
 *   <li>The store's change transaction; nothing in memory changes before it commits.</li>
 *   <li>After the commit only: the change is applied to {@link SubscriptionIndex} in version order, catching up
 *       from the history on a gap, and the channel topic is provisioned if it is not ready.</li>
 * </ol>
 *
 * <p>A failure before or during the commit, or a commit whose outcome is unknown, is {@code INTERNAL_ERROR} and
 * leaves the index untouched; repeating the identical request is safe, and the next selection's version check
 * reconciles the index either way (FR-008). Register, update, and cancel share the store's change transaction and
 * {@link SubscriptionChangeRules}; the index only moves forward through {@link SubscriptionIndex#apply} of the
 * change just committed or {@link SubscriptionIndex#catchUpTo}, which also covers changes another Notification
 * process committed.</p>
 */
public final class SubscriptionService {
    /** A registration's result: {@code created} for a new or re-registered subscriber (201), else a repeat (200). */
    public record Registration(boolean created, SubscriberState state) { }

    private final SubscriptionStore store;
    private final SubscriptionIndex index;
    private final KnownSkus knownSkus;
    private final Channels channels;
    private final TopicNames topics;
    private final Clock clock;

    public SubscriptionService(SubscriptionStore store, SubscriptionIndex index, KnownSkus knownSkus, Channels channels,
                               TopicNames topics, Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.index = Objects.requireNonNull(index, "index");
        this.knownSkus = Objects.requireNonNull(knownSkus, "knownSkus");
        this.channels = Objects.requireNonNull(channels, "channels");
        this.topics = Objects.requireNonNull(topics, "topics");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Rebuilds the index from the committed subscriptions and the clock, in one consistent read (startup). */
    public void loadIndex() {
        SubscriptionStore.IndexRows rows = store.loadIndex();
        index.rebuild(rows.skusBySubscriber(), rows.version());
        System.out.println("[notification] Subscription index at version " + rows.version() + " ("
                + rows.skusBySubscriber().size() + " active subscribers)");
    }

    public Registration register(String subscriberId, Collection<String> skus) {
        requireSubscriberId(subscriberId);
        if (skus == null || skus.isEmpty()) {
            throw new SubscriptionFailure(SubscriptionFailure.Code.MISSING_SKUS, "skus must list at least one SKU");
        }
        SortedSet<String> requested = new TreeSet<>();
        for (String sku : skus) {
            if (sku == null) throw new SubscriptionFailure(SubscriptionFailure.Code.MISSING_SKUS, "skus must not contain null");
            requested.add(sku);
        }
        if (!knownSkus.loaded()) {
            throw new SubscriptionFailure(SubscriptionFailure.Code.INTERNAL_ERROR,
                    "the catalog is not loaded yet; the request was not applied");
        }
        // The first unknown SKU in order; the store rejects with it only after the state checks (409 before 400).
        String unknown = firstUnknown(requested);
        ChangeResult result = inChangeTransaction(() -> store.register(subscriberId, requested, topics.channel(subscriberId), unknown,
                clock.instant()));
        return switch (result) {
            case Applied applied -> {
                applyCommitted(applied.change());
                yield new Registration(applied.created(), readyState(applied.state()));
            }
            case Unchanged unchanged -> {
                catchUp(unchanged.state().updatedVersion());
                yield new Registration(false, readyState(unchanged.state()));
            }
            case Rejected rejected -> throw failure(subscriberId, rejected);
        };
    }

    /**
     * Adds {@code add} to and removes {@code remove} from an active subscriber's SKUs (T088). The answer is the
     * committed state: at the change's version, or at the current version for a no-op.
     */
    public SubscriberState update(String subscriberId, Collection<String> add, Collection<String> remove) {
        requireSubscriberId(subscriberId);
        SortedSet<String> adding = skuSet(add);
        SortedSet<String> removing = skuSet(remove);
        String problem = SubscriptionChangeRules.updateRequestProblem(adding, removing);
        if (problem != null) throw new SubscriptionFailure(SubscriptionFailure.Code.INVALID_REQUEST, problem);
        if (!knownSkus.loaded()) {
            throw new SubscriptionFailure(SubscriptionFailure.Code.INTERNAL_ERROR,
                    "the catalog is not loaded yet; the request was not applied");
        }
        SortedSet<String> requested = new TreeSet<>(adding);
        requested.addAll(removing);
        String unknown = firstUnknown(requested);
        return settled(subscriberId, inChangeTransaction(() -> store.update(subscriberId, adding, removing, unknown, clock.instant())));
    }

    /**
     * Cancels every subscription of {@code subscriberId} (T088). Notifications already selected for it are still
     * delivered (rule 5); a repeated cancel answers the first one's version.
     */
    public SubscriberState cancel(String subscriberId) {
        requireSubscriberId(subscriberId);
        return settled(subscriberId, inChangeTransaction(() -> store.cancel(subscriberId, clock.instant())));
    }

    private SubscriberState settled(String subscriberId, ChangeResult result) {
        return switch (result) {
            case Applied applied -> {
                applyCommitted(applied.change());
                yield withKnownChannel(applied.state());
            }
            case Unchanged unchanged -> {
                catchUp(unchanged.state().updatedVersion());
                yield withKnownChannel(unchanged.state());
            }
            case Rejected rejected -> throw failure(subscriberId, rejected);
        };
    }

    /** Runs one change transaction; any failure is {@code INTERNAL_ERROR}, and memory is untouched. */
    private static ChangeResult inChangeTransaction(java.util.function.Supplier<ChangeResult> change) {
        try {
            return change.get();
        } catch (StoreFailure failure) {
            PerfMetrics.increment("notification.subscription.failures");
            throw new SubscriptionFailure(SubscriptionFailure.Code.INTERNAL_ERROR, failure.commitOutcomeUnknown()
                    ? "the change's commit outcome is unknown; repeating the request is safe"
                    : "the change could not be stored; it was not applied", failure);
        } catch (RuntimeException failure) {
            PerfMetrics.increment("notification.subscription.failures");
            throw new SubscriptionFailure(SubscriptionFailure.Code.INTERNAL_ERROR,
                    "the change could not be stored; it was not applied", failure);
        }
    }

    private static SubscriptionFailure failure(String subscriberId, Rejected rejected) {
        return switch (rejected.reason()) {
            case INVALID_REQUEST -> new SubscriptionFailure(SubscriptionFailure.Code.INVALID_REQUEST, rejected.detail());
            case SUBSCRIBER_NOT_FOUND -> new SubscriptionFailure(SubscriptionFailure.Code.SUBSCRIBER_NOT_FOUND,
                    "subscriber " + subscriberId + " is not registered");
            case SUBSCRIBER_EXISTS -> new SubscriptionFailure(SubscriptionFailure.Code.SUBSCRIBER_EXISTS,
                    "subscriber " + subscriberId + " is already registered with a different SKU set");
            case SUBSCRIBER_CANCELLED -> new SubscriptionFailure(SubscriptionFailure.Code.SUBSCRIBER_CANCELLED,
                    "subscriber " + subscriberId + " is cancelled; register it again first");
            case LAST_SKU -> new SubscriptionFailure(SubscriptionFailure.Code.LAST_SKU,
                    "subscriber " + subscriberId + " must keep at least one SKU; cancel it instead");
            case UNKNOWN_SKU -> new SubscriptionFailure(SubscriptionFailure.Code.UNKNOWN_SKU,
                    "SKU " + rejected.detail() + " is not in the catalog");
        };
    }

    private String firstUnknown(SortedSet<String> skus) {
        return skus.stream().filter(sku -> !knownSkus.contains(sku)).findFirst().orElse(null);
    }

    private static SortedSet<String> skuSet(Collection<String> skus) {
        SortedSet<String> set = new TreeSet<>();
        if (skus == null) return set;
        for (String sku : skus) {
            if (sku == null) throw new SubscriptionFailure(SubscriptionFailure.Code.INVALID_REQUEST, "SKU lists must not contain null");
            set.add(sku);
        }
        return set;
    }

    /** {@code state}, remembering its channel for the dispatcher; an update or cancel never provisions a topic. */
    private SubscriberState withKnownChannel(SubscriberState state) {
        channels.remember(state.subscriberId(), state.channelTopic(), state.channelReady());
        return state;
    }

    /** The subscriber's committed state; never waits. */
    public SubscriberState find(String subscriberId) {
        requireSubscriberId(subscriberId);
        try {
            return store.find(subscriberId).orElseThrow(() -> new SubscriptionFailure(
                    SubscriptionFailure.Code.SUBSCRIBER_NOT_FOUND, "subscriber " + subscriberId + " is not registered"));
        } catch (StoreFailure failure) {
            throw new SubscriptionFailure(SubscriptionFailure.Code.INTERNAL_ERROR, "the subscriber could not be read", failure);
        }
    }

    /** Applies a committed change to the index; on a gap, the index catches up through its one catch-up path. */
    private void applyCommitted(ChangeRow change) {
        if (index.apply(change) == SubscriptionIndex.Applied.GAP) catchUp(change.version());
    }

    /**
     * Brings the index to at least {@code version} from the committed history ({@link SubscriptionIndex#catchUpTo}).
     * A failure only delays it: the next selection's version check catches the index up anyway (N5).
     */
    private void catchUp(long version) {
        if (index.version() >= version) return;
        try {
            index.catchUpTo(version, store::changesAfter);
        } catch (RuntimeException failure) {
            System.err.println("[notification] Subscription index not caught up to " + version + ": " + failure);
        }
    }

    /** {@code state} with its channel provisioned when possible; a failure is retried before the first delivery. */
    private SubscriberState readyState(SubscriberState state) {
        channels.remember(state.subscriberId(), state.channelTopic(), state.channelReady());
        if (state.channelReady()) return state;
        try {
            channels.ensureReady(state.subscriberId());
            return new SubscriberState(state.subscriberId(), state.status(), state.skus(), state.channelTopic(), true,
                    state.createdVersion(), state.updatedVersion());
        } catch (RuntimeException failure) {
            PerfMetrics.increment("notification.channel.provisionFailures");
            System.err.println("[notification] Channel " + state.channelTopic() + " not created yet; retried before "
                    + "the first delivery: " + failure.getMessage());
            return state;
        }
    }

    private static void requireSubscriberId(String subscriberId) {
        if (subscriberId == null || !TopicNames.SUBSCRIBER_ID.matcher(subscriberId).matches()) {
            throw new SubscriptionFailure(SubscriptionFailure.Code.INVALID_SUBSCRIBER_ID,
                    "subscriberId must be 1 to 40 letters, digits, '_' or '-'");
        }
    }
}
