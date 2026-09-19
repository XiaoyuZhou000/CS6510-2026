package checkout;

import analytics.AnalyticsRecorder;
import api.ApiErrors;
import catalog.CatalogCache;
import persistence.ConnectionPool;
import persistence.InventoryDao;
import persistence.TransactionDao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class CheckoutService {

    // ------------------------------------------------------------------ result types

    public record TransactionView(
        String transactionId, String stationId, String status,
        int itemCount, BigDecimal runningTotal, Instant startedAt
    ) {}

    public record ScanView(
        String transactionId, String sku, String name,
        BigDecimal unitPrice, int itemCount, BigDecimal runningTotal
    ) {}

    public record ReceiptLineView(String sku, String name, BigDecimal unitPrice, int quantity) {}

    public record ReceiptView(
        String transactionId, String stationId, int itemCount,
        BigDecimal totalAmount, Instant startedAt, Instant completedAt,
        List<ReceiptLineView> lines
    ) {}

    // ------------------------------------------------------------------ fields

    private final ConcurrentHashMap<String, Basket> baskets = new ConcurrentHashMap<>();

    private final ConnectionPool    pool;
    private final TransactionDao    transactionDao;
    private final InventoryDao      inventoryDao;
    private final CatalogCache      catalog;
    private final AnalyticsRecorder analytics;

    public CheckoutService(ConnectionPool pool, TransactionDao transactionDao,
                           InventoryDao inventoryDao, CatalogCache catalog,
                           AnalyticsRecorder analytics) {
        this.pool           = pool;
        this.transactionDao = transactionDao;
        this.inventoryDao   = inventoryDao;
        this.catalog        = catalog;
        this.analytics      = analytics;
    }

    // ------------------------------------------------------------------ public API

    /** POST /transactions — starts a new transaction. */
    public TransactionView start(String stationId) throws SQLException {
        if (stationId == null || stationId.isBlank()) {
            throw new CheckoutException(400, ApiErrors.MISSING_STATION_ID,
                "stationId is required and must not be blank");
        }
        String transactionId = UUID.randomUUID().toString();
        Basket basket = new Basket(transactionId, stationId);
        transactionDao.insertOpen(transactionId, stationId);
        baskets.put(transactionId, basket);
        return new TransactionView(transactionId, stationId, "OPEN", 0, BigDecimal.ZERO, basket.startedAt());
    }

    /** POST /transactions/{id}/items — scans one unit of sku into the basket. */
    public ScanView scan(String transactionId, String sku) throws SQLException {
        Basket basket = requireBasket(transactionId);
        if (basket.status() != Basket.Status.OPEN) {
            throw new CheckoutException(409, ApiErrors.TRANSACTION_NOT_OPEN,
                "Transaction is not open: " + transactionId);
        }
        CatalogCache.CatalogItem item = catalog.get(sku);
        if (item == null) {
            throw new CheckoutException(404, ApiErrors.SKU_NOT_FOUND,
                "SKU not found: " + sku);
        }
        Basket.ScanSnapshot snap = basket.addScan(sku, item.name(), item.price());
        analytics.recordScan(sku);
        return new ScanView(transactionId, sku, item.name(), item.price(),
            snap.itemCount(), snap.runningTotal());
    }

    /** POST /transactions/{id}/complete — completes the transaction atomically. */
    public ReceiptView complete(String transactionId) throws SQLException {
        Basket basket = requireBasket(transactionId);

        // Check + snapshot under a single synchronized block (status + emptiness + lines are consistent)
        Map<String, Basket.Line> lines;
        synchronized (basket) {
            if (basket.status() != Basket.Status.OPEN) {
                throw new CheckoutException(409, ApiErrors.TRANSACTION_NOT_OPEN,
                    "Transaction is not open: " + transactionId);
            }
            if (basket.isEmpty()) {
                throw new CheckoutException(409, ApiErrors.EMPTY_BASKET,
                    "Basket is empty: " + transactionId);
            }
            lines = basket.linesSnapshot();
        }

        // Ascending SKU order — prevents lock-ordering deadlocks (research.md §1)
        List<String> sortedSkus = new ArrayList<>(lines.keySet());
        Collections.sort(sortedSkus);

        // Compute totals from snapshot (independent of further basket mutations)
        BigDecimal totalAmount = BigDecimal.ZERO;
        int itemCount = 0;
        for (Basket.Line l : lines.values()) {
            totalAmount = totalAmount.add(l.unitPrice.multiply(BigDecimal.valueOf(l.quantity)));
            itemCount += l.quantity;
        }

        Connection conn = pool.borrow();
        try {
            conn.setAutoCommit(false);

            // Step 1: idempotency guard
            Instant completedAt = transactionDao.completeIfOpen(conn, transactionId, totalAmount);
            if (completedAt == null) {
                conn.rollback();
                throw new CheckoutException(409, ApiErrors.TRANSACTION_NOT_OPEN,
                    "Transaction is not open: " + transactionId);
            }

            // Step 2: insert transaction_line rows
            List<TransactionDao.TransactionLine> txLines = new ArrayList<>(sortedSkus.size());
            for (String sku : sortedSkus) {
                Basket.Line l = lines.get(sku);
                txLines.add(new TransactionDao.TransactionLine(sku, l.quantity, l.unitPrice));
            }
            transactionDao.insertLines(conn, transactionId, txLines);

            // Step 3: decrement inventory in ascending SKU order (deadlock avoidance)
            for (String sku : sortedSkus) {
                Basket.Line l = lines.get(sku);
                if (!inventoryDao.decrementIfAvailable(conn, sku, l.quantity)) {
                    conn.rollback();
                    throw new CheckoutException(409, ApiErrors.INSUFFICIENT_STOCK,
                        "Insufficient stock for SKU: " + sku);
                }
            }

            // Step 4: commit
            conn.commit();

            // Mark basket completed and evict from map (basket's durable state now lives in DB)
            basket.markCompleted();
            baskets.remove(transactionId, basket);

            Instant startedAt = basket.startedAt();
            String stationId  = basket.stationId();

            List<ReceiptLineView> receiptLines = new ArrayList<>(sortedSkus.size());
            for (String sku : sortedSkus) {
                Basket.Line l = lines.get(sku);
                receiptLines.add(new ReceiptLineView(sku, l.name, l.unitPrice, l.quantity));
            }

            return new ReceiptView(transactionId, stationId, itemCount,
                totalAmount, startedAt, completedAt, receiptLines);

        } catch (SQLException e) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            throw e;
        } finally {
            try { conn.setAutoCommit(true); } catch (SQLException ignored) {}
            pool.release(conn);
        }
    }

    private Basket requireBasket(String transactionId) throws SQLException {
        Basket basket = baskets.get(transactionId);
        if (basket != null) return basket;

        // Completed baskets are evicted, but their durable status still determines the error.
        TransactionDao.TransactionRow stored = transactionDao.readById(transactionId);
        if (stored != null && !"OPEN".equals(stored.status())) {
            throw new CheckoutException(409, ApiErrors.TRANSACTION_NOT_OPEN,
                "Transaction is not open: " + transactionId);
        }
        throw new CheckoutException(404, ApiErrors.TRANSACTION_NOT_FOUND,
            "Transaction not found: " + transactionId);
    }

    /**
     * GET /transactions/{id} — reads a live basket while OPEN and falls back to
     * durable transaction_line aggregates after the basket has been evicted.
     */
    public TransactionView get(String transactionId) throws SQLException {
        Basket basket = baskets.get(transactionId);
        if (basket != null) {
            synchronized (basket) {
                return new TransactionView(
                    transactionId,
                    basket.stationId(),
                    basket.status().name(),
                    basket.itemCount(),
                    basket.runningTotal(),
                    basket.startedAt()
                );
            }
        }

        TransactionDao.TransactionRow stored = transactionDao.readById(transactionId);
        if (stored == null) {
            throw new CheckoutException(404, ApiErrors.TRANSACTION_NOT_FOUND,
                "Transaction not found: " + transactionId);
        }
        return new TransactionView(
            stored.transactionId(),
            stored.stationId(),
            stored.status(),
            stored.itemCount(),
            stored.runningTotal(),
            stored.startedAt()
        );
    }
}
