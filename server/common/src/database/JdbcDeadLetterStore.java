package database;

import events.DeadLetterStore;
import events.ParkedRecord;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * {@code dead_letter_event} on the owning service's own pool (D6; 006 Implementation Rule 5). The table has
 * the same shape in all three schemas. A park is one INSERT and one commit; parking an {@code event_id} that is
 * already parked increments {@code park_count} and {@code last_parked_at} through the unique key instead of
 * adding a row, while rows with a null {@code event_id} never collide.
 */
public final class JdbcDeadLetterStore implements DeadLetterStore {
    private static final String PARK = "INSERT INTO dead_letter_event (event_id, topic, partition_no, record_offset, "
            + "event_type, schema_version, reason, detail, record_bytes, first_parked_at, last_parked_at, park_count) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1) AS parked "
            + "ON DUPLICATE KEY UPDATE park_count = dead_letter_event.park_count + 1, last_parked_at = parked.last_parked_at";

    private final ConnectionPool pool;
    private final String metricPrefix;

    /** {@code metricPrefix} names the timed units, for example {@code inventory.deadLetter}. */
    public JdbcDeadLetterStore(ConnectionPool pool, String metricPrefix) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.metricPrefix = Objects.requireNonNull(metricPrefix, "metricPrefix");
    }

    @Override
    public void park(ParkedRecord record) {
        JdbcUnitOfWork.inTransaction(pool, metricPrefix + ".park", connection -> {
            try (PreparedStatement insert = connection.prepareStatement(PARK)) {
                Timestamp at = Timestamp.from(record.parkedAt());
                insert.setString(1, record.eventId());
                insert.setString(2, record.topic());
                insert.setInt(3, record.partition());
                insert.setLong(4, record.offset());
                insert.setString(5, record.eventType());
                if (record.schemaVersion() == null) insert.setNull(6, java.sql.Types.INTEGER);
                else insert.setInt(6, record.schemaVersion());
                insert.setString(7, record.reason().name());
                insert.setString(8, record.detail());
                insert.setBytes(9, record.recordBytes());
                insert.setTimestamp(10, at);
                insert.setTimestamp(11, at);
                return insert.executeUpdate();
            }
        });
    }

    @Override
    public long countParked() {
        return JdbcUnitOfWork.read(pool, metricPrefix + ".count", connection -> {
            try (PreparedStatement select = connection.prepareStatement("SELECT COUNT(*) FROM dead_letter_event");
                 ResultSet row = select.executeQuery()) {
                row.next();
                return row.getLong(1);
            }
        });
    }
}
