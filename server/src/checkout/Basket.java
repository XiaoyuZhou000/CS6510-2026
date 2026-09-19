package checkout;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Basket {

    public enum Status { OPEN, COMPLETED }

    public static final class Line {
        public final String sku;
        public final String name;
        public final BigDecimal unitPrice;
        public int quantity;

        Line(String sku, String name, BigDecimal unitPrice) {
            this.sku = sku;
            this.name = name;
            this.unitPrice = unitPrice;
            this.quantity = 1;
        }
    }

    public record ScanSnapshot(int itemCount, BigDecimal runningTotal) {}

    /** Immutable basket contents reserved by a single completion attempt. */
    public record CompletionSnapshot(Map<String, Line> lines, int itemCount, BigDecimal totalAmount) {
        public boolean isEmpty() { return lines.isEmpty(); }
    }

    private final String transactionId;
    private final String stationId;
    private final Instant startedAt;
    private Status status = Status.OPEN;
    private boolean completionInProgress;
    private final Map<String, Line> lines = new LinkedHashMap<>();

    public Basket(String transactionId, String stationId) {
        this.transactionId = transactionId;
        this.stationId = stationId;
        this.startedAt = Instant.now();
    }

    public String transactionId() { return transactionId; }
    public String stationId()     { return stationId; }
    public Instant startedAt()    { return startedAt; }

    public synchronized Status status() { return status; }

    /**
     * Admits and adds one scan atomically. Returns null once a completion has reserved a
     * snapshot (or after completion), so an accepted scan can never fall outside the receipt.
     */
    public synchronized ScanSnapshot addScanIfOpen(String sku, String name, BigDecimal unitPrice) {
        if (status != Status.OPEN || completionInProgress) return null;
        Line existing = lines.get(sku);
        if (existing != null) {
            existing.quantity++;
        } else {
            lines.put(sku, new Line(sku, name, unitPrice));
        }
        return new ScanSnapshot(itemCountInternal(), runningTotalInternal());
    }

    /**
     * Reserves the current contents for completion and closes scan admission. Returns null
     * when the transaction is completed or another completion attempt owns the reservation.
     */
    public synchronized CompletionSnapshot beginCompletion() {
        if (status != Status.OPEN || completionInProgress) return null;
        completionInProgress = true;
        Map<String, Line> snapshot = copyLines();
        return new CompletionSnapshot(
            Collections.unmodifiableMap(snapshot), itemCountInternal(), runningTotalInternal());
    }

    /** Reopens scan admission after a completion attempt rolls back or otherwise fails. */
    public synchronized void completionFailed() {
        if (status == Status.OPEN) completionInProgress = false;
    }

    /** Finalizes a successfully committed completion. */
    public synchronized void completionSucceeded() {
        if (status != Status.OPEN || !completionInProgress) {
            throw new IllegalStateException("No completion is in progress");
        }
        status = Status.COMPLETED;
        completionInProgress = false;
    }

    public synchronized int itemCount()        { return itemCountInternal(); }
    public synchronized BigDecimal runningTotal() { return runningTotalInternal(); }
    public synchronized boolean isEmpty()      { return lines.isEmpty(); }

    /** Returns an immutable deep copy of the current lines, safe to read without the basket lock. */
    public synchronized Map<String, Line> linesSnapshot() {
        return Collections.unmodifiableMap(copyLines());
    }

    private Map<String, Line> copyLines() {
        Map<String, Line> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Line> e : lines.entrySet()) {
            Line orig = e.getValue();
            Line ln = new Line(orig.sku, orig.name, orig.unitPrice);
            ln.quantity = orig.quantity;
            copy.put(e.getKey(), ln);
        }
        return copy;
    }

    private int itemCountInternal() {
        int n = 0;
        for (Line l : lines.values()) n += l.quantity;
        return n;
    }

    private BigDecimal runningTotalInternal() {
        BigDecimal t = BigDecimal.ZERO;
        for (Line l : lines.values()) {
            t = t.add(l.unitPrice.multiply(BigDecimal.valueOf(l.quantity)));
        }
        return t;
    }
}
