package notification;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The last confirmed send per channel (007 research N8), which decides only whether a failure is charged to its
 * channel: it is, when some other subscriber's channel had a confirmed send after the failed attempt started;
 * otherwise the failure is broker-wide and uncharged, so a broker outage never parks a notification. Never
 * authoritative for any state.
 */
public final class ChargingTracker {
    private final Map<String, Instant> lastConfirmed = new ConcurrentHashMap<>();
    private volatile Instant latest;

    /** Records a send to {@code subscriberId}'s channel that the broker confirmed at {@code at}. */
    public void confirmed(String subscriberId, Instant at) {
        lastConfirmed.merge(subscriberId, at, (old, fresh) -> fresh.isAfter(old) ? fresh : old);
        Instant current = latest;
        if (current == null || at.isAfter(current)) latest = at;
    }

    /** Whether a failure of an attempt to {@code subscriberId} that started at {@code attemptStartedAt} is charged. */
    public boolean isCharged(Instant attemptStartedAt, String subscriberId) {
        Instant newest = latest;
        if (newest == null || !newest.isAfter(attemptStartedAt)) return false;
        for (Map.Entry<String, Instant> entry : lastConfirmed.entrySet()) {
            if (!entry.getKey().equals(subscriberId) && entry.getValue().isAfter(attemptStartedAt)) return true;
        }
        return false;
    }

    /** The newest confirmed send on any channel. */
    public Optional<Instant> lastConfirmedSend() {
        return Optional.ofNullable(latest);
    }

    public Optional<Instant> lastConfirmedSend(String subscriberId) {
        return Optional.ofNullable(lastConfirmed.get(subscriberId));
    }
}
