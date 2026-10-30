package database;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Analytics' ingestion transaction (contracts/scan-event-delivery.md section 3, R11, R12, D10): one local
 * transaction per consumed batch deduplicates by source identity, assigns the new events consecutive
 * positions in batch order, upserts changed names, advances {@code scan_sequence}, and stores the next
 * offset to read in {@code consumer_offset}, all in the same commit. Analytics' database, not the broker,
 * is the authority for how far the scan topic has been consumed (FR-049).
 */
public interface ScanIngestStore {
    int MAX_BATCH = 1_000;

    /**
     * Stores the events of one batch that are not already stored, records {@code next} as the partition's
     * next offset to read, and returns only after that commit succeeded, so the caller may then commit the
     * broker offset. A repeated identity, inside the batch or from an earlier batch, counts as a duplicate and
     * never receives a second position; a batch of duplicates still stores {@code next}. Any failure,
     * including a commit whose outcome is unknown, raises {@link StoreFailure}; a retry of the same batch is
     * always safe.
     */
    IngestResult ingest(List<ScanEvent> events, ConsumedOffset next) throws StoreFailure;

    /** The stored next offset of {@code topic}'s {@code partition}, or empty before its first batch. */
    OptionalLong readNextOffset(String topic, int partition) throws StoreFailure;

    /** Up to {@code limit} stored names by SKU, for the name map at startup. */
    Map<String, String> readProductNames(int limit) throws StoreFailure;

    /** One consumed accepted scan; its identity is {@code (transactionId, scanSequence)}. */
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
     * The scan topic's partition a batch came from and the offset after its last record. Parked records before
     * that record are covered by it, so they never block the partition after a restart.
     */
    record ConsumedOffset(String topic, int partition, long nextOffset) {
        public ConsumedOffset {
            if (topic == null || topic.isBlank()) throw new IllegalArgumentException("topic is required");
            if (partition < 0) throw new IllegalArgumentException("partition must not be negative");
            if (nextOffset < 0) throw new IllegalArgumentException("nextOffset must not be negative");
        }
    }

    /**
     * {@code stored + duplicates} equals the batch size. When {@code stored > 0} the new events hold
     * positions {@code firstPosition..lastPosition}; otherwise both are 0.
     */
    record IngestResult(int stored, int duplicates, long firstPosition, long lastPosition) { }
}
