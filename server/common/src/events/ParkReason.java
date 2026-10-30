package events;

/**
 * The {@code dead_letter_event.reason} values (data-model.md, D6); the schemas' CHECK constraint allows
 * exactly these.
 */
public enum ParkReason {
    /** The record value is not an {@code EventEnvelope}; such a row has a null {@code event_id}. */
    UNDECODABLE,
    UNKNOWN_TYPE,
    UNSUPPORTED_VERSION,
    /** A required envelope field is missing, the producer is not the type's owner, or the payload does not decode. */
    INVALID_PAYLOAD,
    /** A payload identity field differs from {@code routing_key}, or a reused key carries a different payload. */
    PAYLOAD_MISMATCH,
    /** An outcome names an attempt that Transaction never recorded. */
    UNKNOWN_ATTEMPT,
    /** An event contradicts durable state, for example an outcome that conflicts with a settled attempt. */
    INTEGRITY,
    /** The handler threw an exception that is neither transient nor a declared permanent failure. */
    HANDLER_ERROR
}
