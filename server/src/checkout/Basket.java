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

    private final String transactionId;
    private final String stationId;
    private final Instant startedAt;
    private Status status = Status.OPEN;
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

    /** Adds one scan of the given SKU; returns a consistent post-scan snapshot. */
    public synchronized ScanSnapshot addScan(String sku, String name, BigDecimal unitPrice) {
        Line existing = lines.get(sku);
        if (existing != null) {
            existing.quantity++;
        } else {
            lines.put(sku, new Line(sku, name, unitPrice));
        }
        return new ScanSnapshot(itemCountInternal(), runningTotalInternal());
    }

    /** Atomically transitions OPEN → COMPLETED. Returns true on success. */
    public synchronized boolean markCompleted() {
        if (status != Status.OPEN) return false;
        status = Status.COMPLETED;
        return true;
    }

    public synchronized int itemCount()        { return itemCountInternal(); }
    public synchronized BigDecimal runningTotal() { return runningTotalInternal(); }
    public synchronized boolean isEmpty()      { return lines.isEmpty(); }

    /** Returns an immutable deep copy of the current lines, safe to read without the basket lock. */
    public synchronized Map<String, Line> linesSnapshot() {
        Map<String, Line> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Line> e : lines.entrySet()) {
            Line orig = e.getValue();
            Line ln = new Line(orig.sku, orig.name, orig.unitPrice);
            ln.quantity = orig.quantity;
            copy.put(e.getKey(), ln);
        }
        return Collections.unmodifiableMap(copy);
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
