package analytics;

import database.ScanIngestStore;
import rpc.FaultHooks;

import java.util.List;
import java.util.Objects;

/**
 * {@code DeliverScanEvents} business rules (contracts/grpc-internal-contracts.md "AnalyticsIngestService"):
 * validates a delivered batch, then hands it to the ingestion transaction. The result is returned only
 * after that transaction committed, so it may be acknowledged as durable.
 */
public final class ScanIngestionService {
    private final ScanIngestStore store;

    public ScanIngestionService(ScanIngestStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /**
     * Ingests one batch. A malformed batch raises {@link InvalidBatch} and stores nothing; a store
     * failure, including an unknown commit outcome, propagates as {@code StoreFailure}.
     */
    public ScanIngestStore.IngestResult ingest(List<ScanIngestStore.ScanEvent> events) {
        validate(events);
        FaultHooks.hit("analytics.ingest.received");
        return store.ingest(events);
    }

    static void validate(List<ScanIngestStore.ScanEvent> events) {
        if (events == null || events.isEmpty() || events.size() > ScanIngestStore.MAX_BATCH) {
            throw new InvalidBatch("A batch must hold 1 to " + ScanIngestStore.MAX_BATCH + " events");
        }
        for (int index = 0; index < events.size(); index++) {
            ScanIngestStore.ScanEvent event = events.get(index);
            text(event.transactionId(), 40, "transaction_id", index);
            if (event.scanSequence() < 1) throw new InvalidBatch("Event " + index + ": scan_sequence must be at least 1");
            text(event.sku(), 20, "sku", index);
            text(event.productName(), 100, "product_name", index);
        }
    }

    private static void text(String value, int maximum, String field, int index) {
        if (value == null || value.isBlank() || value.length() > maximum) {
            throw new InvalidBatch("Event " + index + ": " + field + " must be 1 to " + maximum + " characters");
        }
    }

    /** A batch that breaks the contract's validation rules; it is never retried as is. */
    public static final class InvalidBatch extends RuntimeException {
        public InvalidBatch(String message) {
            super(message);
        }
    }
}
