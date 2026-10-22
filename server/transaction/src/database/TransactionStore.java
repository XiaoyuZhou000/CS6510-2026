package database;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface TransactionStore {
    /** Inserts an OPEN transaction with the service's own start time; nothing is read back. */
    void insertOpen(String transactionId, String stationId, Instant startedAt) throws StoreFailure;
    Optional<TransactionRecord> findById(String transactionId) throws StoreFailure;
    /** The cold path: the transaction row plus its lines, grouped from accepted_scan (R19). */
    Optional<TransactionState> loadState(String transactionId) throws StoreFailure;

    enum TransactionStatus { OPEN, COMPLETED, CANCELLED }

    record TransactionRecord(String transactionId, String stationId, TransactionStatus status,
                             BigDecimal totalAmount, Instant startedAt, Instant completedAt,
                             int itemCount) { }

    record TransactionState(TransactionRecord transaction, int attemptCount, Integer activeAttemptNumber,
                            List<TransactionLine> lines) {
        public TransactionState {
            Objects.requireNonNull(transaction, "transaction");
            lines = List.copyOf(Objects.requireNonNull(lines, "lines"));
        }
    }

    /** One line per SKU: quantity is the row count, with the first captured name and price. */
    record TransactionLine(String sku, String name, int quantity, BigDecimal unitPrice) { }
}
