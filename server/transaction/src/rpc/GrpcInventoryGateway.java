package rpc;

import contracts.inventory.v1.ApplyCheckoutRequest;
import contracts.inventory.v1.ApplyCheckoutResponse;
import contracts.inventory.v1.CatalogProduct;
import contracts.inventory.v1.CheckoutAttemptKey;
import contracts.inventory.v1.GetCatalogRequest;
import contracts.inventory.v1.GetCatalogResponse;
import contracts.inventory.v1.GetCatalogVersionRequest;
import contracts.inventory.v1.GetCatalogVersionResponse;
import contracts.inventory.v1.GetCheckoutOutcomeRequest;
import contracts.inventory.v1.GetCheckoutOutcomeResponse;
import contracts.inventory.v1.InventoryServiceGrpc;
import database.CheckoutAttemptStore.AttemptKey;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;
import transaction.DependencyUnavailableFailure;
import transaction.InventoryGateway;
import transaction.ProductCatalogReplica;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * {@link InventoryGateway} over Inventory's gRPC interface, on one reused channel (R2). Deadlines are
 * 2 s for {@code GetCatalogVersion} and {@code GetCheckoutOutcome}, 5 s for {@code GetCatalog}, and the
 * caller's for {@code ApplyCheckout}; statuses are read per contracts/grpc-internal-contracts.md. Every
 * {@code ApplyCheckout} records {@code transaction.rpc.applyCheckout} and a
 * {@code transaction.rpc.applyCheckout.<status>} counter (R21).
 */
public final class GrpcInventoryGateway implements InventoryGateway {
    static final Duration VERSION_DEADLINE = Duration.ofSeconds(2);
    static final Duration CATALOG_DEADLINE = Duration.ofSeconds(5);
    static final Duration OUTCOME_DEADLINE = Duration.ofSeconds(2);
    private static final LatencyHistogram APPLY = PerfMetrics.timer("transaction.rpc.applyCheckout");

    private final InventoryServiceGrpc.InventoryServiceBlockingStub stub;

    public GrpcInventoryGateway(Channel channel) {
        this.stub = InventoryServiceGrpc.newBlockingStub(Objects.requireNonNull(channel, "channel"));
    }

    /** The gateway on the process-wide channel for {@code host:port}. */
    public static GrpcInventoryGateway forTarget(String target) {
        return new GrpcInventoryGateway(GrpcChannels.forTarget(target));
    }

    @Override
    public CatalogVersion catalogVersion() {
        try {
            GetCatalogVersionResponse response = stub
                    .withDeadlineAfter(VERSION_DEADLINE.toMillis(), TimeUnit.MILLISECONDS)
                    .getCatalogVersion(GetCatalogVersionRequest.getDefaultInstance());
            return new CatalogVersion(response.getCatalogVersion(), response.getProductCount());
        } catch (StatusRuntimeException failure) {
            throw translate("GetCatalogVersion", failure);
        }
    }

    @Override
    public Catalog catalog() {
        try {
            GetCatalogResponse response = stub
                    .withDeadlineAfter(CATALOG_DEADLINE.toMillis(), TimeUnit.MILLISECONDS)
                    .getCatalog(GetCatalogRequest.getDefaultInstance());
            List<ProductCatalogReplica.Product> products = new ArrayList<>(response.getProductsCount());
            for (CatalogProduct product : response.getProductsList()) {
                products.add(new ProductCatalogReplica.Product(product.getSku(), product.getName(),
                        BigDecimal.valueOf(product.getPriceCents(), 2)));
            }
            return new Catalog(response.getCatalogVersion(), products);
        } catch (StatusRuntimeException failure) {
            throw translate("GetCatalog", failure);
        }
    }

    @Override
    public ApplyResult applyCheckout(AttemptKey key, List<InventoryGateway.CheckoutLine> lines, Duration deadline) {
        long started = System.nanoTime();
        String status = "OK";
        try {
            if (deadline == null || deadline.isZero() || deadline.isNegative()) {
                status = "DEADLINE_EXCEEDED";
                return new Uncertain(status);
            }
            ApplyCheckoutRequest.Builder request = ApplyCheckoutRequest.newBuilder().setAttempt(attempt(key));
            for (InventoryGateway.CheckoutLine line : lines) {
                // Qualified: the port's own CheckoutLine is the inherited simple name here.
                request.addLines(contracts.inventory.v1.CheckoutLine.newBuilder().setSku(line.sku())
                        .setQuantity(line.quantity()));
            }
            ApplyCheckoutResponse response = stub.withDeadlineAfter(deadline.toNanos(), TimeUnit.NANOSECONDS)
                    .applyCheckout(request.build());
            return switch (response.getOutcome()) {
                case CHECKOUT_OUTCOME_APPLIED -> new Applied(response.getReplayed());
                case CHECKOUT_OUTCOME_INSUFFICIENT_STOCK -> new Rejected(RejectionReason.INSUFFICIENT_STOCK,
                        response.getRejectedSku());
                case CHECKOUT_OUTCOME_INVALID_REQUEST -> new Rejected(RejectionReason.INVALID_REQUEST,
                        response.getRejectedSku());
                case CHECKOUT_OUTCOME_STOCK_RESTORED -> new StockRestored();
                default -> {
                    // A reply the contract never sends proves nothing about the attempt: treat it as uncertain.
                    status = "UNEXPECTED_OUTCOME";
                    yield new Uncertain(status + " " + response.getOutcome());
                }
            };
        } catch (StatusRuntimeException failure) {
            status = failure.getStatus().getCode().name();
            String detail = failure.getStatus().getDescription();
            return switch (RpcStatusClassifier.classify(failure)) {
                case INVALID_ARGUMENT -> new InvalidRequest(detail);
                case FAILED_PRECONDITION -> new PayloadMismatch(detail);
                default -> new Uncertain(status);
            };
        } catch (RuntimeException unexpected) {
            status = "CLIENT_FAILURE";
            return new Uncertain(status + " " + unexpected.getClass().getSimpleName());
        } finally {
            APPLY.record(System.nanoTime() - started);
            PerfMetrics.increment("transaction.rpc.applyCheckout." + status);
        }
    }

    @Override
    public RecordedOutcome checkoutOutcome(AttemptKey key) {
        try {
            GetCheckoutOutcomeResponse response = stub
                    .withDeadlineAfter(OUTCOME_DEADLINE.toMillis(), TimeUnit.MILLISECONDS)
                    .getCheckoutOutcome(GetCheckoutOutcomeRequest.newBuilder().setAttempt(attempt(key)).build());
            String sku = response.getRejectedSku().isEmpty() ? null : response.getRejectedSku();
            return switch (response.getOutcome()) {
                case CHECKOUT_OUTCOME_APPLIED -> new RecordedOutcome(Outcome.APPLIED, null);
                case CHECKOUT_OUTCOME_INSUFFICIENT_STOCK -> new RecordedOutcome(Outcome.INSUFFICIENT_STOCK, sku);
                case CHECKOUT_OUTCOME_INVALID_REQUEST -> new RecordedOutcome(Outcome.INVALID_REQUEST, sku);
                case CHECKOUT_OUTCOME_STOCK_RESTORED -> new RecordedOutcome(Outcome.STOCK_RESTORED, null);
                case CHECKOUT_OUTCOME_NOT_PROCESSED -> new RecordedOutcome(Outcome.NOT_PROCESSED, null);
                default -> throw new DependencyUnavailableFailure("Inventory sent an unexpected outcome "
                        + response.getOutcome());
            };
        } catch (StatusRuntimeException failure) {
            throw translate("GetCheckoutOutcome", failure);
        }
    }

    private static CheckoutAttemptKey attempt(AttemptKey key) {
        return CheckoutAttemptKey.newBuilder().setTransactionId(key.transactionId())
                .setAttemptNumber(key.attemptNumber()).build();
    }

    private static RuntimeException translate(String call, StatusRuntimeException failure) {
        if (RpcStatusClassifier.classify(failure) == RpcStatusClassifier.Kind.UNCERTAIN) {
            return new DependencyUnavailableFailure("Inventory " + call + " failed: " + failure.getStatus().getCode(),
                    failure);
        }
        return new IllegalStateException("Inventory refused " + call + ": " + failure.getStatus().getCode(), failure);
    }
}
