package database;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Database boundary for durable transaction creation and lookup. */
public interface TransactionStore {

    /** Durably creates a transaction exactly once in the {@link TransactionStatus#OPEN} state. */
    void insertOpen(String transactionId, String stationId) throws StoreFailure;

    /** Returns durable metadata and completed-line aggregates, or absence when the ID is unknown. */
    Optional<TransactionRecord> findById(String transactionId) throws StoreFailure;

    enum TransactionStatus {
        OPEN,
        COMPLETED,
        CANCELLED
    }

    record TransactionRecord(
            String transactionId,
            String stationId,
            TransactionStatus status,
            BigDecimal totalAmount,
            Instant startedAt,
            Instant completedAt,
            int itemCount) {

        public TransactionRecord {
            transactionId = requiredText(transactionId, 40, "transactionId");
            stationId = requiredText(stationId, 40, "stationId");
            Objects.requireNonNull(status, "status must not be null");
            totalAmount = money(totalAmount, "totalAmount");
            if (totalAmount.signum() < 0) {
                throw new IllegalArgumentException("totalAmount must be non-negative");
            }
            Objects.requireNonNull(startedAt, "startedAt must not be null");
            if (itemCount < 0) {
                throw new IllegalArgumentException("itemCount must be non-negative");
            }
            if (status == TransactionStatus.OPEN && totalAmount.signum() != 0) {
                throw new IllegalArgumentException("an OPEN transaction must have a zero total");
            }
            if (status == TransactionStatus.COMPLETED && completedAt == null) {
                throw new IllegalArgumentException("a COMPLETED transaction requires completedAt");
            }
            if (status != TransactionStatus.COMPLETED && completedAt != null) {
                throw new IllegalArgumentException("completedAt is set only for COMPLETED transactions");
            }
            if (completedAt != null && completedAt.isBefore(startedAt)) {
                throw new IllegalArgumentException("completedAt must not precede startedAt");
            }
        }
    }

    private static String requiredText(String value, int maximumLength, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        if (value.length() > maximumLength) {
            throw new IllegalArgumentException(field + " must be at most " + maximumLength + " characters");
        }
        return value;
    }

    private static BigDecimal money(BigDecimal value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        final BigDecimal scaled;
        try {
            scaled = value.setScale(2, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(field + " must have at most two decimal places", e);
        }
        if (scaled.precision() > 12) {
            throw new IllegalArgumentException(field + " exceeds DECIMAL(12,2)");
        }
        return scaled;
    }
}
