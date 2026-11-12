package notification;

import database.SubscriptionStore;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The subscribers' channels as the service and the dispatcher see them (007 research N7): each subscriber's stored
 * {@code channel_topic}, cached after its first read, and whether that topic is known to exist. A channel becomes
 * ready only after {@link ChannelProvisioner} created it and {@code subscriber.channel_ready} was set, so a
 * provisioning failure at registration is retried before the first delivery.
 */
public final class Channels {
    /** A subscriber's channel. */
    public record Channel(String subscriberId, String topic, boolean ready) { }

    private final SubscriptionStore store;
    private final ChannelProvisioner provisioner;
    private final Map<String, Channel> known = new ConcurrentHashMap<>();

    public Channels(SubscriptionStore store, ChannelProvisioner provisioner) {
        this.store = Objects.requireNonNull(store, "store");
        this.provisioner = Objects.requireNonNull(provisioner, "provisioner");
    }

    /** The subscriber's channel, read once from the database; empty for an unknown subscriber. */
    public Optional<Channel> channel(String subscriberId) {
        Channel cached = known.get(subscriberId);
        if (cached != null) return Optional.of(cached);
        Optional<SubscriptionStore.Channel> stored = store.channel(subscriberId);
        if (stored.isEmpty()) return Optional.empty();
        Channel channel = new Channel(subscriberId, stored.get().topic(), stored.get().ready());
        Channel raced = known.putIfAbsent(subscriberId, channel);
        return Optional.of(raced == null ? channel : raced);
    }

    /** Remembers a channel read elsewhere, for example from a registration's committed state. */
    public void remember(String subscriberId, String topic, boolean ready) {
        known.merge(subscriberId, new Channel(subscriberId, topic, ready),
                (old, fresh) -> old.ready() ? old : fresh);
    }

    /**
     * Makes sure the subscriber's channel exists: when it is not known to be ready, creates the topic and records
     * {@code channel_ready}. Throws when the subscriber is unknown or the topic could not be created.
     */
    public Channel ensureReady(String subscriberId) {
        Channel channel = channel(subscriberId)
                .orElseThrow(() -> new IllegalStateException("unknown subscriber " + subscriberId));
        if (channel.ready()) return channel;
        provisioner.provision(channel.topic());
        store.markChannelReady(subscriberId);
        Channel ready = new Channel(subscriberId, channel.topic(), true);
        known.put(subscriberId, ready);
        return ready;
    }
}
