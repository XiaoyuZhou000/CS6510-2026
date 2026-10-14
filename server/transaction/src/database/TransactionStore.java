package database;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface TransactionStore {
    void insertOpen(String transactionId, String stationId) throws StoreFailure;
    Optional<TransactionRecord> findById(String transactionId) throws StoreFailure;
    List<TransactionLine> findLines(String transactionId) throws StoreFailure;

    enum TransactionStatus { OPEN, COMPLETED, CANCELLED }

    record TransactionRecord(String transactionId, String stationId, TransactionStatus status,
                             BigDecimal totalAmount, Instant startedAt, Instant completedAt,
                             int itemCount) { }

    record TransactionLine(String sku, String name, int quantity, BigDecimal unitPrice) { }
}
