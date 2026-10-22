package database;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Analytics' ingestion transaction (contracts/scan-event-delivery.md section 3, R11, R12): one local
 * transaction per delivered batch deduplicates by source identity, assigns the new events consecutive
 * positions in batch order, upserts changed names, and advances {@code scan_sequence} in the same commit.
 */
public interface ScanIngestStore {
    int MAX_BATCH = 1_000;

    /**
     * Stores the events of one batch that are not already stored and returns only after that commit
     * succeeded, so a caller may acknowledge the whole batch as durable. A repeated identity, inside the
     * batch or from an earlier batch, counts as a duplicate and never receives a second position. Any
     * failure, including a commit whose outcome is unknown, raises {@link StoreFailure}; a retry of the
     * same batch is always safe.
     */
    IngestResult ingest(List<ScanEvent> events) throws StoreFailure;

    /** Up to {@code limit} stored names by SKU, for the name map at startup. */
    Map<String, String> readProductNames(int limit) throws StoreFailure;

    /** One delivered accepted scan; its identity is {@code (transactionId, scanSequence)}. */
    record ScanEvent(String transactionId, int scanSequence, String sku, String productName, Instant acceptedAt) {
        public ScanEvent {
            Objects.requireNonNull(transactionId, "transactionId");
            Objects.requireNonNull(sku, "sku");
            Objects.requireNonNull(productName, "productName");
            Objects.requireNonNull(acceptedAt, "acceptedAt");
        }

        public Identity identity() {
            return new Identity(transactionId, scanSequence);
        }
    }

    record Identity(String transactionId, int scanSequence) { }

    /**
     * {@code stored + duplicates} equals the batch size. When {@code stored > 0} the new events hold
     * positions {@code firstPosition..lastPosition}; otherwise both are 0.
     */
    record IngestResult(int stored, int duplicates, long firstPosition, long lastPosition) { }
}
