package transaction;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Synchronized in-memory state for one non-durable, open checkout basket. */
public final class Basket {
    public enum Status { OPEN, COMPLETING, COMPLETED }

    public record Line(String sku, String name, BigDecimal unitPrice, int quantity) {
        public Line {
            Objects.requireNonNull(sku, "sku");
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(unitPrice, "unitPrice");
            if (quantity <= 0) throw new IllegalArgumentException("quantity must be positive");
        }
    }

    public record ScanSnapshot(int itemCount, BigDecimal runningTotal) { }

    public record CompletionSnapshot(Map<String, Line> lines, int itemCount, BigDecimal totalAmount) {
        public CompletionSnapshot {
            lines = Collections.unmodifiableMap(new LinkedHashMap<>(lines));
        }

        public boolean isEmpty() { return lines.isEmpty(); }
    }

    private final String transactionId;
    private final String stationId;
    private final Instant startedAt;
    private final Map<String, Line> lines = new LinkedHashMap<>();
    private Status status = Status.OPEN;

    public Basket(String transactionId, String stationId) {
        // MySQL TIMESTAMP(3) stores millisecond precision. Matching that precision prevents a
        // completion written in the same millisecond from appearing earlier than this basket.
        this(transactionId, stationId, Instant.now().truncatedTo(ChronoUnit.MILLIS));
    }

    Basket(String transactionId, String stationId, Instant startedAt) {
        this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
        this.stationId = Objects.requireNonNull(stationId, "stationId");
        this.startedAt = Objects.requireNonNull(startedAt, "startedAt");
    }

    public String transactionId() { return transactionId; }
    public String stationId() { return stationId; }
    public Instant startedAt() { return startedAt; }
    public synchronized Status status() { return status; }

    public synchronized ScanSnapshot addScanIfOpen(String sku, String name, BigDecimal unitPrice) {
        if (status != Status.OPEN) return null;
        Line existing = lines.get(sku);
        if (existing == null) {
            lines.put(sku, new Line(sku, name, unitPrice, 1));
        } else {
            lines.put(sku, new Line(existing.sku(), existing.name(), existing.unitPrice(),
                    Math.addExact(existing.quantity(), 1)));
        }
        return new ScanSnapshot(itemCountInternal(), runningTotalInternal());
    }

    public synchronized CompletionSnapshot beginCompletion() {
        if (status != Status.OPEN) return null;
        status = Status.COMPLETING;
        return new CompletionSnapshot(lines, itemCountInternal(), runningTotalInternal());
    }

    public synchronized void completionFailed() {
        if (status == Status.COMPLETING) status = Status.OPEN;
    }

    public synchronized void completionSucceeded() {
        if (status != Status.COMPLETING) {
            throw new IllegalStateException("No completion is in progress");
        }
        status = Status.COMPLETED;
    }

    public synchronized int itemCount() { return itemCountInternal(); }
    public synchronized BigDecimal runningTotal() { return runningTotalInternal(); }
    public synchronized boolean isEmpty() { return lines.isEmpty(); }

    private int itemCountInternal() {
        int count = 0;
        for (Line line : lines.values()) count = Math.addExact(count, line.quantity());
        return count;
    }

    private BigDecimal runningTotalInternal() {
        BigDecimal total = BigDecimal.ZERO.setScale(2);
        for (Line line : lines.values()) {
            total = total.add(line.unitPrice().multiply(BigDecimal.valueOf(line.quantity())));
        }
        return total;
    }
}
