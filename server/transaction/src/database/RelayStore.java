package database;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * The outbox relay's view of {@code accepted_scan} (contracts/scan-event-delivery.md section 2). Rows are
 * never deleted: they are also the basket and audit record. No cursor is kept, so a row that commits late
 * with a lower {@code scan_id} is still selected by the next round.
 */
public interface RelayStore {

    /**
     * Up to {@code limit} rows whose {@code published_at} is NULL, in {@code scan_id} order; one SELECT whose
     * connection is back in the pool when this returns, so the sends that follow hold none.
     */
    List<UndeliveredScan> selectUndelivered(int limit) throws StoreFailure;

    /**
     * Sets {@code published_at} on exactly the given rows that are still undelivered, in one UPDATE and one
     * commit, and returns how many rows changed. A row another relay already marked keeps its first time.
     */
    int markDelivered(List<Long> scanIds, Instant deliveredAt) throws StoreFailure;

    /** The outbox backlog, for the {@code transaction.relay.backlog} gauge. */
    long countUndelivered() throws StoreFailure;

    /** One unpublished accepted scan with the name and unit price captured when it was accepted. */
    record UndeliveredScan(long scanId, String transactionId, int scanSequence, String sku, String productName,
                           BigDecimal unitPrice, Instant acceptedAt) {
        public UndeliveredScan {
            Objects.requireNonNull(transactionId, "transactionId");
            Objects.requireNonNull(sku, "sku");
            Objects.requireNonNull(productName, "productName");
            Objects.requireNonNull(unitPrice, "unitPrice");
            Objects.requireNonNull(acceptedAt, "acceptedAt");
        }
    }
}
