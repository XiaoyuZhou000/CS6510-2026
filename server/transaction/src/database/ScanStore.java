package database;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/** The Scan transaction: a guarded update of the transaction row plus its accepted-scan event (R10, R19). */
public interface ScanStore {

    /**
     * In one local transaction, moves the transaction from {@code expectedItemCount} to
     * {@code scan.scanSequence()} items and {@code newTotal}, and records {@code scan}. The update
     * applies only while the transaction is OPEN, has no pending checkout attempt, and still has
     * {@code expectedItemCount} items; otherwise nothing is written and the result is
     * {@link AppendResult#GUARD_MISS}. A failing commit raises {@link StoreFailure} with
     * {@code commitOutcomeUnknown} and is never retried.
     */
    AppendResult appendScan(String transactionId, int expectedItemCount, BigDecimal newTotal, AcceptedScan scan)
            throws StoreFailure;

    enum AppendResult { OK, GUARD_MISS }

    /** One accepted unit with the line's captured name and price; {@code scanSequence} is the new item count. */
    record AcceptedScan(int scanSequence, String sku, String productName, BigDecimal unitPrice, Instant acceptedAt) {
        public AcceptedScan {
            if (scanSequence < 1) throw new IllegalArgumentException("scanSequence must be at least 1");
            Objects.requireNonNull(sku, "sku");
            Objects.requireNonNull(productName, "productName");
            Objects.requireNonNull(unitPrice, "unitPrice");
            Objects.requireNonNull(acceptedAt, "acceptedAt");
        }
    }
}
