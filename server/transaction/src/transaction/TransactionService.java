package transaction;

import database.AcceptedScanStore;
import database.CheckoutCompletionStore;
import database.TransactionStore;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Checkout orchestration uses only committed store results, including after process restart. */
public final class TransactionService implements TransactionOperations {
    private final TransactionStore transactions;
    private final AcceptedScanStore scans;
    private final CheckoutCompletionStore completions;

    public TransactionService(TransactionStore transactions, AcceptedScanStore scans,
                              CheckoutCompletionStore completions) {
        this.transactions = Objects.requireNonNull(transactions);
        this.scans = Objects.requireNonNull(scans);
        this.completions = Objects.requireNonNull(completions);
    }

    @Override
    public TransactionView start(StartCommand command) {
        Objects.requireNonNull(command);
        String id = UUID.randomUUID().toString();
        transactions.insertOpen(id, command.stationId());
        return view(transactions.findById(id).orElseThrow(
            () -> new IllegalStateException("Committed transaction disappeared")));
    }

    @Override
    public ScanView scan(ScanCommand command) {
        Objects.requireNonNull(command);
        var result = scans.scanAtomically(command.transactionId(), command.sku());
        if (result instanceof AcceptedScanStore.MissingTransaction)
            throw failure(TransactionFailure.Code.NOT_FOUND, "Transaction not found: " + command.transactionId());
        if (result instanceof AcceptedScanStore.NotOpen)
            throw failure(TransactionFailure.Code.NOT_OPEN, "Transaction is not open: " + command.transactionId());
        if (result instanceof AcceptedScanStore.UnknownSku)
            throw failure(TransactionFailure.Code.UNKNOWN_SKU, "SKU not found: " + command.sku());
        var accepted = (AcceptedScanStore.Accepted) result;
        return new ScanView(command.transactionId(),command.sku(),accepted.name(),
            accepted.unitPrice(),accepted.itemCount(),accepted.runningTotal());
    }

    @Override
    public ReceiptView complete(String id) {
        var result = completions.completeAtomically(id);
        if (result instanceof CheckoutCompletionStore.MissingTransaction)
            throw failure(TransactionFailure.Code.NOT_FOUND, "Transaction not found: " + id);
        if (result instanceof CheckoutCompletionStore.NotOpen)
            throw failure(TransactionFailure.Code.NOT_OPEN, "Transaction is not open: " + id);
        if (result instanceof CheckoutCompletionStore.EmptyBasket)
            throw failure(TransactionFailure.Code.EMPTY_BASKET, "Basket is empty: " + id);
        if (result instanceof CheckoutCompletionStore.InsufficientStock stock)
            throw failure(TransactionFailure.Code.INSUFFICIENT_STOCK, "Insufficient stock for SKU: " + stock.sku());
        var committed = (CheckoutCompletionStore.Completed) result;
        var row = transactions.findById(id).orElseThrow(
            () -> new IllegalStateException("Completed transaction disappeared"));
        List<ReceiptLineView> lines = transactions.findLines(id).stream()
            .map(line -> new ReceiptLineView(line.sku(),line.name(),line.unitPrice(),line.quantity()))
            .toList();
        return new ReceiptView(id,row.stationId(),row.itemCount(),row.totalAmount(),
            row.startedAt(),committed.completedAt(),lines);
    }

    @Override
    public TransactionView get(String id) {
        return transactions.findById(id).map(TransactionService::view).orElseThrow(
            () -> failure(TransactionFailure.Code.NOT_FOUND, "Transaction not found: " + id));
    }

    private static TransactionView view(TransactionStore.TransactionRecord row) {
        return new TransactionView(row.transactionId(),row.stationId(),
            TransactionStatus.valueOf(row.status().name()),row.itemCount(),
            row.totalAmount(),row.startedAt());
    }

    private static TransactionFailure failure(TransactionFailure.Code code, String message) {
        return new TransactionFailure(code,message);
    }
}
