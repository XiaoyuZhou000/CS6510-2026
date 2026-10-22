package transaction;

import database.ScanStore;
import database.TransactionStore;
import rpc.ResponseBudget;
import transaction.TransactionStateCache.Line;
import transaction.TransactionStateCache.State;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Checkout lifecycle over committed store results (R19).
 *
 * <p>Scan prices from the in-memory product replica and the transaction state cache, then commits one
 * guarded UPDATE plus one accepted-scan row. The cache never decides an outcome: a stale entry makes the
 * guarded write miss, so the request reloads from the database and re-evaluates, and every 404 or 409
 * is confirmed against the database before it is sent.</p>
 *
 * <p>Complete is the checkout Saga, delegated to {@link CheckoutOrchestrator}, which shares this
 * service's locks and cache. A Scan that finds a checkout pending waits for its settlement within the
 * response budget, holding nothing, then restarts (FR-035).</p>
 */
public final class TransactionService implements TransactionOperations {
    /** Reloads after guarded-write misses before a Scan gives up with a 500. */
    private static final int MAX_GUARD_RELOADS = 3;

    private final TransactionStore transactions;
    private final ScanStore scans;
    private final CheckoutOrchestrator checkout;
    private final ProductCatalogReplica catalog;
    private final TransactionLocks locks;
    private final TransactionStateCache cache;

    public TransactionService(TransactionStore transactions, ScanStore scans, CheckoutOrchestrator checkout,
                              ProductCatalogReplica catalog, TransactionLocks locks, TransactionStateCache cache) {
        this.transactions = Objects.requireNonNull(transactions);
        this.scans = Objects.requireNonNull(scans);
        this.checkout = Objects.requireNonNull(checkout);
        this.catalog = Objects.requireNonNull(catalog);
        this.locks = Objects.requireNonNull(locks);
        this.cache = Objects.requireNonNull(cache);
        if (cache.locks() != locks) throw new IllegalArgumentException("The cache must check this service's locks");
        if (checkout.cache() != cache) throw new IllegalArgumentException("Complete must share this service's cache");
    }

    @Override
    public TransactionView start(StartCommand command) {
        Objects.requireNonNull(command);
        String id = UUID.randomUUID().toString();
        Instant startedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        transactions.insertOpen(id, command.stationId(), startedAt);
        try (var held = locks.acquire(id)) {
            cache.put(State.open(id, command.stationId(), startedAt));
        }
        return new TransactionView(id, command.stationId(), TransactionStatus.OPEN, 0, BigDecimal.ZERO, startedAt);
    }

    @Override
    public ScanView scan(ScanCommand command) {
        Objects.requireNonNull(command);
        ResponseBudget budget = checkout.startBudget();
        while (true) {
            ScanStep step;
            try (var held = locks.acquire(command.transactionId())) {
                step = scanUnderLock(command);
            }
            if (step.view() != null) return step.view();
            checkout.awaitSettlement(step.pending(), budget);
        }
    }

    /** Either the accepted Scan, or a pending checkout to wait for before restarting. */
    private record ScanStep(ScanView view, CheckoutOrchestrator.PendingWait pending) { }

    private ScanStep scanUnderLock(ScanCommand command) {
        String id = command.transactionId();
        String sku = command.sku();
        int guardMisses = 0;
        while (true) {
            boolean[] fresh = {false};
            Optional<State> found = cache.getOrLoad(id, key -> {
                fresh[0] = true;
                return TransactionStates.load(transactions, key);
            });
            RuntimeException rejection = rejection(id, sku, found);
            if (rejection != null) {
                if (!fresh[0]) {
                    cache.invalidate(id);
                    continue;
                }
                if (rejection instanceof CheckoutPendingFailure) {
                    return new ScanStep(null, checkout.waitFor(found.orElseThrow()));
                }
                throw rejection;
            }
            State state = found.orElseThrow();
            Line line = state.lines().get(sku);
            ProductCatalogReplica.Product product = catalog.lookup(sku).orElseThrow();
            String name = line != null ? line.name() : product.name();
            BigDecimal unitPrice = line != null ? line.unitPrice() : product.price();
            State next = state.withScan(sku, name, unitPrice);
            ScanStore.AppendResult result;
            try {
                result = scans.appendScan(id, state.itemCount(), next.totalAmount(), new ScanStore.AcceptedScan(
                        next.itemCount(), sku, name, unitPrice, Instant.now().truncatedTo(ChronoUnit.MILLIS)));
            } catch (RuntimeException failure) {
                // The commit may have happened: reconcile from the database before any further mutation.
                cache.invalidate(id);
                throw failure;
            }
            if (result == ScanStore.AppendResult.OK) {
                cache.put(next);
                return new ScanStep(new ScanView(id, sku, name, unitPrice, next.itemCount(), next.totalAmount()), null);
            }
            cache.recordGuardMiss();
            cache.invalidate(id);
            if (++guardMisses > MAX_GUARD_RELOADS) {
                throw new IllegalStateException("Transaction " + id + " changed concurrently during "
                        + guardMisses + " scan attempts");
            }
        }
    }

    /** The established precedence (FR-020, FR-023, FR-035); null when the Scan may proceed. */
    private RuntimeException rejection(String id, String sku, Optional<State> found) {
        if (found.isEmpty()) return failure(TransactionFailure.Code.NOT_FOUND, "Transaction not found: " + id);
        State state = found.get();
        if (state.status() != TransactionStatus.OPEN) {
            return failure(TransactionFailure.Code.NOT_OPEN, "Transaction is not open: " + id);
        }
        if (state.pending()) return new CheckoutPendingFailure(id);
        if (!catalog.usable()) return new ProductCatalogUnavailableFailure();
        if (catalog.lookup(sku).isEmpty()) return failure(TransactionFailure.Code.UNKNOWN_SKU, "SKU not found: " + sku);
        return null;
    }

    /** The checkout Saga (contracts/checkout-saga.md). */
    @Override
    public ReceiptView complete(String id) {
        return checkout.complete(id);
    }

    /** Lookup reads the database, so it never depends on the cache. */
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
