package transaction;

import database.RelayStore.UndeliveredScan;

import java.util.List;

/**
 * Transaction's port to Analytics' ingestion (contracts/scan-event-delivery.md section 2). Delivery is at
 * least once: every event carries its identity {@code (transaction_id, scan_sequence)}, so resending a
 * batch is always safe. {@link #deliver} never throws for a transport or status fault.
 */
public interface ScanEventSink {

    DeliveryResult deliver(List<UndeliveredScan> events);

    sealed interface DeliveryResult permits Delivered, InvalidBatch, Uncertain { }

    /** OK: Analytics committed every event of the batch durably; {@code stored + duplicates} is the batch size. */
    record Delivered(int stored, int duplicates) implements DeliveryResult { }

    /** INVALID_ARGUMENT: a programming fault; the rows stay undelivered and the relay alarms. */
    record InvalidBatch(String detail) implements DeliveryResult { }

    /** Any other status: Analytics may or may not have stored the batch, so it is resent after a backoff. */
    record Uncertain(String status) implements DeliveryResult { }
}
