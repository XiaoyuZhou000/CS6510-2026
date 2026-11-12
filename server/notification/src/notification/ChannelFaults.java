package notification;

import events.RecordSender;
import faults.FaultHooks;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The test-fault wrapper of the channel sender (007 research N13, T059), applied by {@link NotificationDispatcher}
 * to channel sends only, before a record reaches the producer, so the broker and every other channel stay healthy:
 *
 * <ul>
 *   <li>{@code notification.channel.reject:<subscriberId>}: the send's future fails at once;</li>
 *   <li>{@code notification.channel.stall:<subscriberId>}: the send's future fails as a timeout after 10 s.</li>
 * </ul>
 *
 * <p>The flags are read from {@code FaultHooks}' property on every send, so a test or
 * {@code POST /notifications/ops/faults} can set and clear them at runtime. Without them every send goes straight
 * to the wrapped sender. The record key of a channel send is the subscriber ID.</p>
 */
public final class ChannelFaults implements RecordSender {
    public static final Duration STALL = Duration.ofSeconds(10);

    private final RecordSender channels;
    private final Duration stall;

    public ChannelFaults(RecordSender channels) {
        this(channels, STALL);
    }

    public ChannelFaults(RecordSender channels, Duration stall) {
        this.channels = Objects.requireNonNull(channels, "channels");
        this.stall = Objects.requireNonNull(stall, "stall");
    }

    @Override
    public CompletableFuture<Void> send(String topic, String subscriberId, byte[] value) {
        if (FaultHooks.isSet(FaultHooks.CHANNEL_REJECT + ":" + subscriberId)) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "injected fault: channel of " + subscriberId + " rejected the record"));
        }
        if (FaultHooks.isSet(FaultHooks.CHANNEL_STALL + ":" + subscriberId)) {
            CompletableFuture<Void> held = new CompletableFuture<>();
            CompletableFuture.delayedExecutor(stall.toMillis(), TimeUnit.MILLISECONDS).execute(() ->
                    held.completeExceptionally(new TimeoutException("injected fault: channel of " + subscriberId
                            + " stalled for " + stall.toMillis() + " ms")));
            return held;
        }
        return channels.send(topic, subscriberId, value);
    }

    @Override
    public void close() {
        channels.close();
    }
}
