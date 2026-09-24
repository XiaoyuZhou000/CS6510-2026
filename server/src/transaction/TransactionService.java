package transaction;

import database.CatalogStore;
import database.CheckoutCompletionStore;
import database.TransactionStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Transaction-layer implementation of basket, scan, completion, and lookup behavior. */
public final class TransactionService implements TransactionOperations {
    private final ConcurrentHashMap<String, Basket> baskets = new ConcurrentHashMap<>();
    private final TransactionStore transactions;
    private final CheckoutCompletionStore completions;
    private final CatalogCache catalog;
    private final AcceptedScanSink acceptedScans;

    public TransactionService(TransactionStore transactions,
                              CheckoutCompletionStore completions,
                              CatalogCache catalog,
                              AcceptedScanSink acceptedScans) {
        this.transactions = Objects.requireNonNull(transactions, "transactions");
        this.completions = Objects.requireNonNull(completions, "completions");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.acceptedScans = Objects.requireNonNull(acceptedScans, "acceptedScans");
    }

    @Override
    public TransactionView start(StartCommand command) {
        Objects.requireNonNull(command, "command");
        String transactionId = UUID.randomUUID().toString();
        Basket basket = new Basket(transactionId, command.stationId());
        transactions.insertOpen(transactionId, command.stationId());
        baskets.put(transactionId, basket);
        return liveView(basket);
    }

    @Override
    public ScanView scan(ScanCommand command) {
        Objects.requireNonNull(command, "command");
        Basket basket = requireLiveBasket(command.transactionId());
        CatalogStore.CatalogItem item = catalog.get(command.sku());
        if (item == null) {
            throw failure(TransactionFailure.Code.UNKNOWN_SKU, "SKU not found: " + command.sku());
        }
        Basket.ScanSnapshot snapshot = basket.addScanIfOpen(item.sku(), item.name(), item.price());
        if (snapshot == null) {
            throw failure(TransactionFailure.Code.NOT_OPEN,
                    "Transaction is not open: " + command.transactionId());
        }

        // Analytics is observational. Once admission succeeds its outage cannot roll back a scan.
        try {
            acceptedScans.recordAcceptedScan(item.sku());
        } catch (RuntimeException sinkFailure) {
            System.err.println("[transaction] Accepted-scan sink failed: " + sinkFailure.getMessage());
        }
        return new ScanView(command.transactionId(), item.sku(), item.name(), item.price(),
                snapshot.itemCount(), snapshot.runningTotal());
    }

    @Override
    public ReceiptView complete(String transactionId) {
        Basket basket = requireLiveBasket(transactionId);
        Basket.CompletionSnapshot snapshot = basket.beginCompletion();
        if (snapshot == null) {
            throw failure(TransactionFailure.Code.NOT_OPEN, "Transaction is not open: " + transactionId);
        }
        if (snapshot.isEmpty()) {
            basket.completionFailed();
            throw failure(TransactionFailure.Code.EMPTY_BASKET, "Basket is empty: " + transactionId);
        }

        List<Basket.Line> lines = snapshot.lines().values().stream()
                .sorted(Comparator.comparing(Basket.Line::sku))
                .toList();
        CheckoutCompletionStore.CompletionCommand command = new CheckoutCompletionStore.CompletionCommand(
                transactionId,
                snapshot.totalAmount(),
                lines.stream().map(line -> new CheckoutCompletionStore.CompletionLine(
                        line.sku(), line.quantity(), line.unitPrice())).toList());

        try {
            CheckoutCompletionStore.CompletionResult result = completions.completeAtomically(command);
            if (result instanceof CheckoutCompletionStore.NotOpen) {
                throw failure(TransactionFailure.Code.NOT_OPEN,
                        "Transaction is not open: " + transactionId);
            }
            if (result instanceof CheckoutCompletionStore.InsufficientStock insufficient) {
                throw failure(TransactionFailure.Code.INSUFFICIENT_STOCK,
                        "Insufficient stock for SKU: " + insufficient.sku());
            }
            Instant completedAt = ((CheckoutCompletionStore.Completed) result).completedAt();

            // The store returns COMPLETED only after commit, so a receipt cannot precede durability.
            basket.completionSucceeded();
            baskets.remove(transactionId, basket);
            List<ReceiptLineView> receiptLines = new ArrayList<>(lines.size());
            for (Basket.Line line : lines) {
                receiptLines.add(new ReceiptLineView(line.sku(), line.name(), line.unitPrice(), line.quantity()));
            }
            // Receipt timestamps must share the database clock/precision. The durable row is
            // authoritative after commit and avoids comparing MySQL TIMESTAMP(3) to a JVM clock.
            Instant receiptStartedAt = transactions.findById(transactionId)
                    .map(TransactionStore.TransactionRecord::startedAt)
                    .orElse(basket.startedAt());
            return new ReceiptView(transactionId, basket.stationId(), snapshot.itemCount(),
                    snapshot.totalAmount(), receiptStartedAt, completedAt, receiptLines);
        } finally {
            if (basket.status() == Basket.Status.COMPLETING) basket.completionFailed();
        }
    }

    @Override
    public TransactionView get(String transactionId) {
        Basket live = baskets.get(transactionId);
        if (live != null) return liveView(live);
        TransactionStore.TransactionRecord stored = transactions.findById(transactionId)
                .orElseThrow(() -> failure(TransactionFailure.Code.NOT_FOUND,
                        "Transaction not found: " + transactionId));
        return new TransactionView(stored.transactionId(), stored.stationId(),
                TransactionStatus.valueOf(stored.status().name()), stored.itemCount(),
                stored.totalAmount(), stored.startedAt());
    }

    private Basket requireLiveBasket(String transactionId) {
        Basket live = baskets.get(transactionId);
        if (live != null) return live;
        TransactionStore.TransactionRecord stored = transactions.findById(transactionId).orElse(null);
        if (stored != null && stored.status() != TransactionStore.TransactionStatus.OPEN) {
            throw failure(TransactionFailure.Code.NOT_OPEN, "Transaction is not open: " + transactionId);
        }
        throw failure(TransactionFailure.Code.NOT_FOUND, "Transaction not found: " + transactionId);
    }

    private static TransactionView liveView(Basket basket) {
        return new TransactionView(basket.transactionId(), basket.stationId(), TransactionStatus.OPEN,
                basket.itemCount(), basket.runningTotal(), basket.startedAt());
    }

    private static TransactionFailure failure(TransactionFailure.Code code, String message) {
        return new TransactionFailure(code, message);
    }
}
