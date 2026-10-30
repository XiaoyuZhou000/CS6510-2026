package events;

import java.util.List;

/**
 * How business code hands committed outbox rows to its service's dispatcher, without depending on
 * {@link OutboxDispatcher} (D5). Neither method waits for the broker.
 */
public interface OutboxSignal {
    /** Publishes these just-committed rows, in this order. Call only after the inserting unit committed. */
    void signal(List<String> eventIds);

    /** Re-sends one already recorded row, with its stored {@code event_id} and envelope (D5 point 4). */
    void resend(String eventId);
}
