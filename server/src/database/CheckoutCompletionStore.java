package database;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Coarse database boundary for the all-or-nothing checkout commit.
 *
 * <p>The implementation owns connection borrowing, guarded status transition, line insertion,
 * inventory decrements, commit, and rollback. No JDBC type crosses this contract.</p>
 */
public interface CheckoutCompletionStore {

    CompletionResult completeAtomically(CompletionCommand command) throws StoreFailure;

    record CompletionCommand(
            String transactionId,
            BigDecimal totalAmount,
            List<CompletionLine> lines) {

        public CompletionCommand {
            transactionId = requiredText(transactionId, 40, "transactionId");
            totalAmount = decimal(totalAmount, 12, "totalAmount");
            if (totalAmount.signum() < 0) {
                throw new IllegalArgumentException("totalAmount must be non-negative");
            }
            lines = List.copyOf(Objects.requireNonNull(lines, "lines must not be null"));
            if (lines.isEmpty()) {
                throw new IllegalArgumentException("lines must not be empty");
            }

            String previousSku = null;
            BigDecimal calculatedTotal = BigDecimal.ZERO.setScale(2);
            for (CompletionLine line : lines) {
                Objects.requireNonNull(line, "lines must not contain null");
                if (previousSku != null && previousSku.compareTo(line.sku()) >= 0) {
                    throw new IllegalArgumentException("lines must contain distinct SKUs in ascending order");
                }
                previousSku = line.sku();
                calculatedTotal = calculatedTotal.add(
                        line.unitPrice().multiply(BigDecimal.valueOf(line.quantity())));
            }
            if (calculatedTotal.compareTo(totalAmount) != 0) {
                throw new IllegalArgumentException("totalAmount must equal the sum of completion lines");
            }
        }
    }

    record CompletionLine(String sku, int quantity, BigDecimal unitPrice) {
        public CompletionLine {
            sku = requiredText(sku, 20, "sku");
            if (quantity <= 0) {
                throw new IllegalArgumentException("quantity must be positive");
            }
            unitPrice = decimal(unitPrice, 10, "unitPrice");
            if (unitPrice.signum() < 0) {
                throw new IllegalArgumentException("unitPrice must be non-negative");
            }
        }
    }

    enum Outcome {
        COMPLETED,
        NOT_OPEN,
        INSUFFICIENT_STOCK
    }

    sealed interface CompletionResult permits Completed, NotOpen, InsufficientStock {
        Outcome outcome();
    }

    record Completed(Instant completedAt) implements CompletionResult {
        public Completed {
            Objects.requireNonNull(completedAt, "completedAt must not be null");
        }

        @Override
        public Outcome outcome() {
            return Outcome.COMPLETED;
        }
    }

    record NotOpen() implements CompletionResult {
        @Override
        public Outcome outcome() {
            return Outcome.NOT_OPEN;
        }
    }

    record InsufficientStock(String sku) implements CompletionResult {
        public InsufficientStock {
            sku = requiredText(sku, 20, "sku");
        }

        @Override
        public Outcome outcome() {
            return Outcome.INSUFFICIENT_STOCK;
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

    private static BigDecimal decimal(BigDecimal value, int precision, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        final BigDecimal scaled;
        try {
            scaled = value.setScale(2, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(field + " must have at most two decimal places", e);
        }
        if (scaled.precision() > precision) {
            throw new IllegalArgumentException(field + " exceeds DECIMAL(" + precision + ",2)");
        }
        return scaled;
    }
}
