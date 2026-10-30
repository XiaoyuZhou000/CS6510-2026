package analytics;

import database.ScanIngestStore;
import database.StoreFailure;
import events.BatchEventHandler;
import events.DecodedEvent;
import events.EventTypes;
import events.ParkReason;
import events.PermanentEventFailure;
import events.RecordStream;
import events.TransientEventFailure;
import faults.FaultHooks;
import selfcheckout.events.v1.ScanAccepted;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Ingests consumed {@code ScanAccepted} batches (contracts/scan-event-delivery.md section 3, D10): validates
 * each event, then hands the batch and the offset after its last record to the ingestion transaction, which
 * commits both together. The consumer commits the broker offset only after this returns (FR-011).
 *
 * <ul>
 *   <li>An event that breaks the contract's validation rules is a {@link PermanentEventFailure}, so it is
 *       parked and never stored.</li>
 *   <li>Any other failure, including a store failure or an unknown commit outcome, is transient: a valid scan is
 *       read again until it is stored, because a parked valid scan would be missing from every window.</li>
 * </ul>
 *
 * <p>Test-only fault hook {@code analytics.ingest.received}, after the poll returned and before the ingestion
 * transaction begins.</p>
 */
public final class ScanIngestionService implements BatchEventHandler {
    private final ScanIngestStore store;

    public ScanIngestionService(ScanIngestStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public void handleBatch(List<DecodedEvent> events) {
        ingest(events);
    }

    /** Ingests consecutive records of one partition and returns after the ingestion commit. */
    public ScanIngestStore.IngestResult ingest(List<DecodedEvent> events) {
        if (events == null || events.isEmpty() || events.size() > ScanIngestStore.MAX_BATCH) {
            throw new IllegalArgumentException("A batch holds 1 to " + ScanIngestStore.MAX_BATCH + " events");
        }
        List<ScanIngestStore.ScanEvent> scans = new ArrayList<>(events.size());
        for (DecodedEvent event : events) scans.add(scanOf(event));
        DecodedEvent last = events.getLast();
        ScanIngestStore.ConsumedOffset next = new ScanIngestStore.ConsumedOffset(last.topic(), last.partition(),
                last.offset() + 1);
        try {
            FaultHooks.hit("analytics.ingest.received");
            return store.ingest(scans, next);
        } catch (StoreFailure failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new TransientEventFailure("Ingesting " + scans.size() + " scans failed; they are read again", failure);
        }
    }

    /** Where the consumer resumes {@code partition}: the stored next offset, or 0 before the first batch. */
    public long nextOffset(RecordStream.Partition partition) {
        return store.readNextOffset(partition.topic(), partition.partition()).orElse(0L);
    }

    static ScanIngestStore.ScanEvent scanOf(DecodedEvent event) {
        if (event.type() != EventTypes.SCAN_ACCEPTED) {
            throw invalid(event, "the scan topic carries only ScanAccepted, not " + event.type().typeName());
        }
        ScanAccepted scan = event.payload(ScanAccepted.class);
        text(event, scan.getTransactionId(), 40, "transaction_id");
        if (scan.getScanSequence() < 1) throw invalid(event, "scan_sequence must be at least 1");
        text(event, scan.getSku(), 20, "sku");
        text(event, scan.getProductName(), 100, "product_name");
        return new ScanIngestStore.ScanEvent(scan.getTransactionId(), scan.getScanSequence(), scan.getSku(),
                scan.getProductName(), Instant.ofEpochMilli(scan.getAcceptedAtMs()));
    }

    private static void text(DecodedEvent event, String value, int maximum, String field) {
        if (value == null || value.isBlank() || value.length() > maximum) {
            throw invalid(event, field + " must be 1 to " + maximum + " characters");
        }
    }

    private static PermanentEventFailure invalid(DecodedEvent event, String detail) {
        return new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, "Event " + event.eventId() + ": " + detail);
    }
}
