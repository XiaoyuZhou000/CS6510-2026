package events;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * A service's transactional outbox (D5, FR-010). Rows are inserted only inside the local transaction whose
 * state change they announce, and afterwards only {@code published_at} and {@code send_count} change.
 *
 * @param <U> the caller's open unit of work that {@link #insert} joins; the JDBC adapter uses the unit's
 *            {@code Connection}, which keeps this port free of SQL types
 */
public interface OutboxStore<U> {
    /** Adds a row to the caller's open unit of work; it becomes visible only when that unit commits. */
    void insert(U unitOfWork, OutboxRow row);

    /** Sets {@code published_at} on those of {@code eventIds} not yet published, in one statement and commit. */
    void markPublished(Collection<String> eventIds, Instant publishedAt);

    /**
     * Up to {@code limit} rows with {@code published_at IS NULL AND created_at < olderThan}, in
     * {@code outbox_seq} order.
     */
    List<OutboxRow> selectUnpublished(Instant olderThan, int limit);

    Optional<OutboxRow> findByEventId(String eventId);

    /** The rows of {@code eventIds} that exist, in any order, in one query. */
    List<OutboxRow> findByEventIds(Collection<String> eventIds);

    /** The rows recorded for one aggregate, in {@code outbox_seq} order. */
    List<OutboxRow> findByAggregateKey(String aggregateKey);

    /** Adds one to {@code send_count} of each row, for re-sends by the sweep or by {@link OutboxSignal#resend}. */
    void incrementSendCount(Collection<String> eventIds);

    long countUnpublished();
}
