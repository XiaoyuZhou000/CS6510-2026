package support;

import events.OutboxSignal;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An {@link OutboxSignal} that only records what business code hands to its dispatcher, for tests that
 * check a service's own local transactions without publishing anything.
 */
public final class RecordingOutboxSignal implements OutboxSignal {
    public final List<String> signalled = new CopyOnWriteArrayList<>();
    public final List<String> resent = new CopyOnWriteArrayList<>();

    @Override
    public void signal(List<String> eventIds) {
        signalled.addAll(eventIds);
    }

    @Override
    public void resend(String eventId) {
        resent.add(eventId);
    }
}
