package transaction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** API-facing transaction lifecycle contract with no HTTP or persistence types. */
public interface TransactionOperations {

    TransactionView start(StartCommand command);

    ScanView scan(ScanCommand command);

    ReceiptView complete(String transactionId);

    TransactionView get(String transactionId);

    record StartCommand(String stationId) {
        public StartCommand {
            stationId = requiredText(stationId, 40, "stationId");
        }
    }

    record ScanCommand(String transactionId, String sku) {
        public ScanCommand {
            transactionId = requiredText(transactionId, 40, "transactionId");
            sku = requiredText(sku, 20, "sku");
        }
    }

    enum TransactionStatus {
        OPEN,
        COMPLETED,
        CANCELLED
    }

    record TransactionView(
            String transactionId,
            String stationId,
            TransactionStatus status,
            int itemCount,
            BigDecimal runningTotal,
            Instant startedAt) {

        public TransactionView {
            transactionId = requiredText(transactionId, 40, "transactionId");
            stationId = requiredText(stationId, 40, "stationId");
            Objects.requireNonNull(status, "status must not be null");
            requireNonNegative(itemCount, "itemCount");
            runningTotal = money(runningTotal, "runningTotal");
            if (runningTotal.signum() < 0) {
                throw new IllegalArgumentException("runningTotal must be non-negative");
            }
            Objects.requireNonNull(startedAt, "startedAt must not be null");
        }
    }

    record ScanView(
            String transactionId,
            String sku,
            String name,
            BigDecimal unitPrice,
            int itemCount,
            BigDecimal runningTotal) {

        public ScanView {
            transactionId = requiredText(transactionId, 40, "transactionId");
            sku = requiredText(sku, 20, "sku");
            name = requiredText(name, 100, "name");
            unitPrice = money(unitPrice, "unitPrice");
            if (unitPrice.signum() < 0) {
                throw new IllegalArgumentException("unitPrice must be non-negative");
            }
            requireNonNegative(itemCount, "itemCount");
            runningTotal = money(runningTotal, "runningTotal");
            if (runningTotal.signum() < 0) {
                throw new IllegalArgumentException("runningTotal must be non-negative");
            }
        }
    }

    record ReceiptLineView(String sku, String name, BigDecimal unitPrice, int quantity) {
        public ReceiptLineView {
            sku = requiredText(sku, 20, "sku");
            name = requiredText(name, 100, "name");
            unitPrice = money(unitPrice, "unitPrice");
            if (unitPrice.signum() < 0) {
                throw new IllegalArgumentException("unitPrice must be non-negative");
            }
            if (quantity <= 0) {
                throw new IllegalArgumentException("quantity must be positive");
            }
        }
    }

    record ReceiptView(
            String transactionId,
            String stationId,
            int itemCount,
            BigDecimal totalAmount,
            Instant startedAt,
            Instant completedAt,
            List<ReceiptLineView> lines) {

        public ReceiptView {
            transactionId = requiredText(transactionId, 40, "transactionId");
            stationId = requiredText(stationId, 40, "stationId");
            if (itemCount <= 0) {
                throw new IllegalArgumentException("itemCount must be positive");
            }
            totalAmount = money(totalAmount, "totalAmount");
            if (totalAmount.signum() < 0) {
                throw new IllegalArgumentException("totalAmount must be non-negative");
            }
            Objects.requireNonNull(startedAt, "startedAt must not be null");
            Objects.requireNonNull(completedAt, "completedAt must not be null");
            if (completedAt.isBefore(startedAt)) {
                throw new IllegalArgumentException("completedAt must not precede startedAt");
            }
            lines = List.copyOf(Objects.requireNonNull(lines, "lines must not be null"));
            if (lines.isEmpty()) {
                throw new IllegalArgumentException("lines must not be empty");
            }

            int calculatedCount = 0;
            BigDecimal calculatedTotal = BigDecimal.ZERO.setScale(2);
            Set<String> skus = new HashSet<>();
            for (ReceiptLineView line : lines) {
                Objects.requireNonNull(line, "lines must not contain null");
                if (!skus.add(line.sku())) {
                    throw new IllegalArgumentException("lines must contain one entry per SKU");
                }
                calculatedCount = Math.addExact(calculatedCount, line.quantity());
                calculatedTotal = calculatedTotal.add(
                        line.unitPrice().multiply(BigDecimal.valueOf(line.quantity())));
            }
            if (calculatedCount != itemCount) {
                throw new IllegalArgumentException("itemCount must equal the sum of line quantities");
            }
            if (calculatedTotal.compareTo(totalAmount) != 0) {
                throw new IllegalArgumentException("totalAmount must equal the sum of receipt lines");
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

    private static void requireNonNegative(int value, String field) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
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
