package notification;

import database.NotificationStatusQueries;
import database.NotificationStatusQueries.ParkedEvent;
import database.NotificationStore;
import database.NotificationStore.Parked;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The operator's view of what Notification parked (007 T101, FR-016, FR-017): parked notifications, which an operator
 * retry moves back to {@code PENDING} with their identity kept, and parked {@code SaleCompleted} or catalog records,
 * which are listed for inspection only. Every answer comes from the database; nothing here is cached.
 */
public final class ParkedOperations {
    /** What {@code GET /notifications/ops/parked} lists. */
    public record View(List<Parked> notifications, List<ParkedEvent> events) {
        public View {
            notifications = List.copyOf(notifications);
            events = List.copyOf(events);
        }
    }

    private final NotificationStore store;
    private final NotificationStatusQueries queries;
    private final NotificationDispatcher dispatcher;
    private final Clock clock;

    public ParkedOperations(NotificationStore store, NotificationStatusQueries queries, NotificationDispatcher dispatcher,
                            Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.queries = Objects.requireNonNull(queries, "queries");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Up to {@code limit} parked notifications (all or one subscriber's) and up to {@code limit} parked records. */
    public View list(Optional<String> subscriberId, int limit) {
        return new View(store.listParked(subscriberId, limit), queries.parkedEvents(limit));
    }

    /**
     * Moves the parked rows, all or one subscriber's, back to {@code PENDING}: {@code charged_failures = 0},
     * {@code operator_retries + 1}, due now. The dispatcher's next round sweeps them, so they are sent again with
     * their stored identity and contents.
     *
     * @return the rows moved
     */
    public int retry(Optional<String> subscriberId) {
        int retried = store.retryParked(subscriberId, clock.instant());
        if (retried > 0) {
            dispatcher.requestSweep();
            System.out.println("[notification] Operator retry moved " + retried + " parked notifications back to PENDING"
                    + subscriberId.map(id -> " for " + id).orElse(""));
        }
        return retried;
    }
}
