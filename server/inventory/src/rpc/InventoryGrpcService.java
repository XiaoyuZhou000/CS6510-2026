package rpc;

import contracts.inventory.v1.ApplyCheckoutRequest;
import contracts.inventory.v1.ApplyCheckoutResponse;
import contracts.inventory.v1.CatalogProduct;
import contracts.inventory.v1.CheckoutAttemptKey;
import contracts.inventory.v1.CheckoutLine;
import contracts.inventory.v1.CheckoutOutcome;
import contracts.inventory.v1.GetCatalogRequest;
import contracts.inventory.v1.GetCatalogResponse;
import contracts.inventory.v1.GetCatalogVersionRequest;
import contracts.inventory.v1.GetCatalogVersionResponse;
import contracts.inventory.v1.GetCheckoutOutcomeRequest;
import contracts.inventory.v1.GetCheckoutOutcomeResponse;
import contracts.inventory.v1.InventoryServiceGrpc;
import contracts.inventory.v1.RestoreCheckoutRequest;
import contracts.inventory.v1.RestoreCheckoutResponse;
import contracts.inventory.v1.RestoreResult;
import database.StoreFailure;
import inventory.CatalogCache;
import inventory.CatalogOperations;
import inventory.CheckoutStockService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Translates InventoryService calls to Inventory's business operations (contracts/grpc-internal-contracts.md).
 * Catalog replication is served from {@link CatalogCache}; the checkout calls go to
 * {@link CheckoutStockService}. Business outcomes, including rejections, travel with status OK; error
 * statuses are reserved for validation, integrity, contention, and transport faults, and their
 * descriptions carry no database details.
 */
public final class InventoryGrpcService extends InventoryServiceGrpc.InventoryServiceImplBase {
    private static final LatencyHistogram APPLY = PerfMetrics.timer("inventory.grpc.apply");

    private final CatalogCache catalog;
    private final CheckoutStockService stock;

    public InventoryGrpcService(CatalogCache catalog, CheckoutStockService stock) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.stock = Objects.requireNonNull(stock, "stock");
    }

    @Override
    public void getCatalogVersion(GetCatalogVersionRequest request, StreamObserver<GetCatalogVersionResponse> response) {
        try {
            CatalogCache.Version version = catalog.currentVersion();
            if (version.catalogVersion() == null) {
                response.onError(Status.UNAVAILABLE.withDescription("The catalog is not seeded").asRuntimeException());
                return;
            }
            response.onNext(GetCatalogVersionResponse.newBuilder().setCatalogVersion(version.catalogVersion())
                    .setProductCount(version.productCount()).build());
            response.onCompleted();
        } catch (RuntimeException failure) {
            response.onError(status(failure).asRuntimeException());
        }
    }

    @Override
    public void getCatalog(GetCatalogRequest request, StreamObserver<GetCatalogResponse> response) {
        try {
            CatalogCache.Snapshot snapshot = catalog.snapshot();
            if (snapshot.version() == null) {
                response.onError(Status.UNAVAILABLE.withDescription("The catalog is not seeded").asRuntimeException());
                return;
            }
            GetCatalogResponse.Builder catalogResponse = GetCatalogResponse.newBuilder()
                    .setCatalogVersion(snapshot.version());
            for (CatalogOperations.CatalogItemView product : snapshot.products()) {
                catalogResponse.addProducts(CatalogProduct.newBuilder().setSku(product.sku()).setName(product.name())
                        .setPriceCents(product.price().movePointRight(2).longValueExact()));
            }
            response.onNext(catalogResponse.build());
            response.onCompleted();
        } catch (RuntimeException failure) {
            response.onError(status(failure).asRuntimeException());
        }
    }

    @Override
    public void applyCheckout(ApplyCheckoutRequest request, StreamObserver<ApplyCheckoutResponse> response) {
        long started = System.nanoTime();
        try {
            List<CheckoutStockService.Line> lines = new ArrayList<>(request.getLinesCount());
            for (CheckoutLine line : request.getLinesList()) {
                lines.add(new CheckoutStockService.Line(line.getSku(), line.getQuantity()));
            }
            CheckoutAttemptKey attempt = request.getAttempt();
            CheckoutStockService.ApplyResult result = stock.apply(attempt.getTransactionId(), attempt.getAttemptNumber(),
                    lines);
            response.onNext(ApplyCheckoutResponse.newBuilder().setOutcome(outcome(result.outcome()))
                    .setRejectedSku(result.rejectedSku() == null ? "" : result.rejectedSku())
                    .setReplayed(result.replayed()).build());
            response.onCompleted();
        } catch (RuntimeException failure) {
            response.onError(status(failure).asRuntimeException());
        } finally {
            APPLY.record(System.nanoTime() - started);
        }
    }

    @Override
    public void getCheckoutOutcome(GetCheckoutOutcomeRequest request, StreamObserver<GetCheckoutOutcomeResponse> response) {
        try {
            CheckoutAttemptKey attempt = request.getAttempt();
            CheckoutStockService.RecordedOutcome recorded = stock.outcome(attempt.getTransactionId(),
                    attempt.getAttemptNumber());
            response.onNext(GetCheckoutOutcomeResponse.newBuilder().setOutcome(outcome(recorded.outcome()))
                    .setRejectedSku(recorded.rejectedSku() == null ? "" : recorded.rejectedSku()).build());
            response.onCompleted();
        } catch (RuntimeException failure) {
            response.onError(status(failure).asRuntimeException());
        }
    }

    @Override
    public void restoreCheckout(RestoreCheckoutRequest request, StreamObserver<RestoreCheckoutResponse> response) {
        try {
            CheckoutAttemptKey attempt = request.getAttempt();
            CheckoutStockService.RestoreResult result = stock.restore(attempt.getTransactionId(), attempt.getAttemptNumber());
            RestoreCheckoutResponse.Builder restored = RestoreCheckoutResponse.newBuilder().setResult(switch (result.outcome()) {
                case RESTORED -> RestoreResult.RESTORE_RESULT_RESTORED;
                case ALREADY_RESTORED -> RestoreResult.RESTORE_RESULT_ALREADY_RESTORED;
                case NOTHING_TO_RESTORE -> RestoreResult.RESTORE_RESULT_NOTHING_TO_RESTORE;
            });
            for (CheckoutStockService.Line line : result.lines()) {
                restored.addRestoredLines(CheckoutLine.newBuilder().setSku(line.sku()).setQuantity(line.quantity()));
            }
            response.onNext(restored.build());
            response.onCompleted();
        } catch (RuntimeException failure) {
            response.onError(status(failure).asRuntimeException());
        }
    }

    private static CheckoutOutcome outcome(CheckoutStockService.Outcome outcome) {
        return switch (outcome) {
            case APPLIED -> CheckoutOutcome.CHECKOUT_OUTCOME_APPLIED;
            case INSUFFICIENT_STOCK -> CheckoutOutcome.CHECKOUT_OUTCOME_INSUFFICIENT_STOCK;
            case INVALID_REQUEST -> CheckoutOutcome.CHECKOUT_OUTCOME_INVALID_REQUEST;
            case STOCK_RESTORED -> CheckoutOutcome.CHECKOUT_OUTCOME_STOCK_RESTORED;
            case NOT_PROCESSED -> CheckoutOutcome.CHECKOUT_OUTCOME_NOT_PROCESSED;
        };
    }

    /** The contract's status table; no description carries database details. */
    private static Status status(RuntimeException failure) {
        if (failure instanceof CheckoutStockService.InvalidCheckoutRequest invalid) {
            return Status.INVALID_ARGUMENT.withDescription(invalid.getMessage());
        }
        if (failure instanceof CheckoutStockService.CheckoutConflict) {
            return Status.FAILED_PRECONDITION.withDescription("The checkout attempt conflicts with a recorded operation");
        }
        if (failure instanceof StoreFailure store) {
            return store.contended()
                    ? Status.ABORTED.withDescription("Inventory's stock rows stayed contended")
                    : Status.UNAVAILABLE.withDescription("Inventory's database is unavailable");
        }
        return Status.INTERNAL.withDescription("Inventory failed to answer");
    }
}
