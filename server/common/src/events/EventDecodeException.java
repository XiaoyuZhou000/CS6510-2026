package events;

/**
 * A record failed an envelope check of {@link EnvelopeCodec#decode}. It is always a permanent failure: the
 * consumer parks the record with {@link #reason()} and whichever envelope fields could be read (D6).
 */
public final class EventDecodeException extends RuntimeException {
    private final ParkReason reason;
    private final String eventId;
    private final String eventType;
    private final Integer schemaVersion;

    public EventDecodeException(ParkReason reason, String message, String eventId, String eventType,
                                Integer schemaVersion, Throwable cause) {
        super(message, cause);
        if (reason == null) throw new IllegalArgumentException("reason is required");
        this.reason = reason;
        this.eventId = blankToNull(eventId);
        this.eventType = blankToNull(eventType);
        this.schemaVersion = schemaVersion;
    }

    public ParkReason reason() { return reason; }

    /** The envelope's {@code event_id}, or null when the envelope did not decode or carried none. */
    public String eventId() { return eventId; }

    public String eventType() { return eventType; }

    public Integer schemaVersion() { return schemaVersion; }

    private static String blankToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }
}
