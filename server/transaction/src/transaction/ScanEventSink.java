package transaction;

import database.RelayStore.UndeliveredScan;

import java.util.List;

/**
 * Transaction's port for publishing accepted scans (contracts/scan-event-delivery.md section 2, D10). Delivery
 * is at least once: every event carries its identity {@code (transaction_id, scan_sequence)} and the
 * deterministic event ID derived from it, so resending a batch is always safe. {@link #deliver} never throws
 * for a transport fault.
 */
public interface ScanEventSink {

    DeliveryResult deliver(List<UndeliveredScan> events);

    sealed interface DeliveryResult permits Delivered, InvalidBatch, Uncertain { }

    /** The broker confirmed every event of the batch durably ({@code acks=all}). */
    record Delivered(int confirmed) implements DeliveryResult { }

    /** A row cannot be encoded as {@code ScanAccepted}, a programming fault; the rows stay unpublished and the relay alarms. */
    record InvalidBatch(String detail) implements DeliveryResult { }

    /** Some send failed or was not confirmed: the broker may or may not hold the batch, so it is resent after a backoff. */
    record Uncertain(String reason) implements DeliveryResult { }
}
