package events;

import java.time.Instant;
import java.util.Objects;

/**
 * One poison record for a {@code dead_letter_event} row (D6). {@code eventId}, {@code eventType}, and
 * {@code schemaVersion} are null when the envelope could not be read that far; {@code recordBytes} is the
 * record value exactly as consumed.
 */
public record ParkedRecord(String eventId, String topic, int partition, long offset, String eventType,
                           Integer schemaVersion, ParkReason reason, String detail, byte[] recordBytes,
                           Instant parkedAt) {
    /** {@code dead_letter_event.detail} is VARCHAR(500). */
    public static final int MAX_DETAIL = 500;

    public ParkedRecord {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(parkedAt, "parkedAt");
        if (detail != null && detail.length() > MAX_DETAIL) detail = detail.substring(0, MAX_DETAIL);
        if (recordBytes == null) recordBytes = new byte[0];
    }
}
