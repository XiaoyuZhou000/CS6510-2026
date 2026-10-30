package support;

import events.OutboxRow;
import events.OutboxStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An {@link OutboxStore} held in memory for dispatcher and Saga tests (D14). An insert is visible at once;
 * the unit of work argument is ignored. Every {@code markPublished} and {@code selectUnpublished} call is
 * recorded, so tests can assert one store call per marking batch and the sweep's arguments.
 */
public final class InMemoryOutboxStore implements OutboxStore<Object> {
    /** One stored row with its bookkeeping columns. */
    public static final class Entry {
        public final long outboxSeq;
        public final OutboxRow row;
        public volatile Instant publishedAt;
        public volatile int sendCount;

        Entry(long outboxSeq, OutboxRow row) {
            this.outboxSeq = outboxSeq;
            this.row = row;
        }
    }

    public record SelectCall(Instant olderThan, int limit, int returned) { }

    private final AtomicLong sequence = new AtomicLong();
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private final List<List<String>> markCalls = new CopyOnWriteArrayList<>();
    private final List<SelectCall> selectCalls = new CopyOnWriteArrayList<>();
    private volatile RuntimeException failure;

    @Override
    public void insert(Object unitOfWork, OutboxRow row) {
        add(row);
    }

    public void add(OutboxRow row) {
        throwIfFailing();
        if (entries.putIfAbsent(row.eventId(), new Entry(sequence.incrementAndGet(), row)) != null) {
            throw new IllegalStateException("duplicate event_id " + row.eventId());
        }
    }

    @Override
    public synchronized void markPublished(Collection<String> eventIds, Instant publishedAt) {
        throwIfFailing();
        markCalls.add(List.copyOf(eventIds));
        for (String eventId : eventIds) {
            Entry entry = entries.get(eventId);
            if (entry != null && entry.publishedAt == null) entry.publishedAt = publishedAt;
        }
    }

    @Override
    public List<OutboxRow> selectUnpublished(Instant olderThan, int limit) {
        throwIfFailing();
        List<OutboxRow> due = ordered().stream()
                .filter(entry -> entry.publishedAt == null && entry.row.createdAt().isBefore(olderThan))
                .limit(limit).map(entry -> entry.row).toList();
        selectCalls.add(new SelectCall(olderThan, limit, due.size()));
        return due;
    }

    @Override
    public Optional<OutboxRow> findByEventId(String eventId) {
        throwIfFailing();
        Entry entry = entries.get(eventId);
        return entry == null ? Optional.empty() : Optional.of(entry.row);
    }

    @Override
    public List<OutboxRow> findByEventIds(Collection<String> eventIds) {
        throwIfFailing();
        List<OutboxRow> rows = new ArrayList<>();
        for (String eventId : eventIds) {
            Entry entry = entries.get(eventId);
            if (entry != null) rows.add(entry.row);
        }
        return rows;
    }

    @Override
    public List<OutboxRow> findByAggregateKey(String aggregateKey) {
        throwIfFailing();
        return ordered().stream().filter(entry -> aggregateKey.equals(entry.row.aggregateKey()))
                .map(entry -> entry.row).toList();
    }

    @Override
    public void incrementSendCount(Collection<String> eventIds) {
        throwIfFailing();
        for (String eventId : eventIds) {
            Entry entry = entries.get(eventId);
            if (entry != null) entry.sendCount++;
        }
    }

    @Override
    public long countUnpublished() {
        throwIfFailing();
        return entries.values().stream().filter(entry -> entry.publishedAt == null).count();
    }

    /** Makes every call throw {@code failure} until it is cleared with {@code null}. */
    public void failWith(RuntimeException failure) {
        this.failure = failure;
    }

    public Entry entry(String eventId) {
        return entries.get(eventId);
    }

    public List<List<String>> markCalls() {
        return List.copyOf(markCalls);
    }

    public List<SelectCall> selectCalls() {
        return List.copyOf(selectCalls);
    }

    public List<Entry> ordered() {
        return entries.values().stream().sorted(Comparator.comparingLong(entry -> entry.outboxSeq)).toList();
    }

    private void throwIfFailing() {
        RuntimeException current = failure;
        if (current != null) throw current;
    }
}
