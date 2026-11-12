package database;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * The {@code notification} table as delivery record and outbox of the subscribers' channels (007 research N8,
 * data-model.md "Notification" state machine). Only {@code PENDING} rows ever change state: a confirmed publish
 * makes a row {@code DELIVERED} for good, and the {@code charged_failures}-th failure parks it.
 */
public interface NotificationStore {
    /**
     * A notification that still has to be sent, with everything its {@code SubscriberNotified} carries:
     * {@code createdAt} is the selection commit time, and {@code subscriptionVersion} the selection's history point.
     */
    record Pending(String notificationId, String saleId, String sku, String subscriberId, int quantity,
                   Instant completedAt, Instant createdAt, long subscriptionVersion, int chargedFailures,
                   int unchargedFailures) { }

    /** One failed send; {@code charged} when the failure is attributed to the channel (N8). */
    record Failure(String notificationId, boolean charged, String error) { }

    /** Rows by state and the failure totals over all rows. */
    record Counts(long pending, long delivered, long parked, long chargedFailures, long unchargedFailures) { }

    /** Marks these {@code PENDING} rows {@code DELIVERED} at {@code at}; returns the rows changed. */
    int markDelivered(Collection<String> notificationIds, Instant at);

    /**
     * Records the failures in one unit: each counts one attempt and one charged or uncharged failure, keeps its
     * error, and is due again after {@code min(200 ms × 2^(charged + uncharged), 5 s)} from {@code at}. A
     * charged row that has reached {@code parkAfterCharged} charged failures is parked in the same unit.
     *
     * @return the rows parked
     */
    int recordFailures(List<Failure> failures, Instant at, int parkAfterCharged);

    /** Parks these {@code PENDING} rows at {@code at}; returns the rows changed. */
    int park(Collection<String> notificationIds, Instant at);

    /**
     * Up to {@code limit} {@code PENDING} rows due at {@code now}, oldest {@code next_attempt_at} first, leaving out
     * the subscribers in {@code skipSubscribers} (lanes that are already full).
     */
    List<Pending> selectDue(Instant now, int limit, Collection<String> skipSubscribers);

    /** Moves parked rows, all or one subscriber's, back to {@code PENDING}, due at {@code now}; returns the count. */
    int retryParked(Optional<String> subscriberId, Instant now);

    Counts counts();
}
