package support;

import events.DeadLetterStore;
import events.ParkedRecord;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link DeadLetterStore} held in memory with the {@code dead_letter_event} semantics: a non-null
 * {@code event_id} is unique and parking it again increments its count; null-ID rows never collide.
 */
public final class InMemoryDeadLetterStore implements DeadLetterStore {
    /** One parked row. */
    public static final class Row {
        public final ParkedRecord first;
        public volatile ParkedRecord last;
        public volatile int parkCount = 1;

        Row(ParkedRecord record) {
            this.first = record;
            this.last = record;
        }
    }

    private final Map<String, Row> byEventId = new LinkedHashMap<>();
    private final List<Row> rows = new ArrayList<>();
    private volatile RuntimeException failure;

    @Override
    public synchronized void park(ParkedRecord record) {
        RuntimeException current = failure;
        if (current != null) throw current;
        if (record.eventId() != null) {
            Row existing = byEventId.get(record.eventId());
            if (existing != null) {
                existing.parkCount++;
                existing.last = record;
                return;
            }
        }
        Row row = new Row(record);
        rows.add(row);
        if (record.eventId() != null) byEventId.put(record.eventId(), row);
    }

    @Override
    public synchronized long countParked() {
        return rows.size();
    }

    public synchronized List<Row> rows() {
        return List.copyOf(rows);
    }

    public synchronized Row byEventId(String eventId) {
        return byEventId.get(eventId);
    }

    /** Makes {@code park} throw until cleared with {@code null}. */
    public void failWith(RuntimeException failure) {
        this.failure = failure;
    }
}
