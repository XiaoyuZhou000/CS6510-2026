package transaction;

import database.CheckoutAttemptStore.AttemptKey;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Transaction's port to Inventory Service: catalog replication and the checkout Saga's pivot step. The
 * Scan path never calls it (FR-012). Catalog failures without a definitive answer raise
 * {@link DependencyUnavailableFailure}; {@link #applyCheckout} reports them as {@link Uncertain} instead.
 */
public interface InventoryGateway {

    CatalogVersion catalogVersion();

    /** The complete catalog snapshot with the version it was loaded at, in ascending SKU order. */
    Catalog catalog();

    /**
     * T2, the pivot (R5, R8): applies the attempt's lines atomically and idempotently per {@code key}.
     * It never throws for a transport or status fault; every call under one key carries the same lines.
     */
    ApplyResult applyCheckout(AttemptKey key, List<CheckoutLine> lines, Duration deadline);

    /** Inventory's recorded outcome for an attempt, for diagnostics and validation; it never changes state. */
    RecordedOutcome checkoutOutcome(AttemptKey key);

    record CatalogVersion(String version, int productCount) {
        public CatalogVersion {
            if (version == null || version.isBlank()) throw new IllegalArgumentException("version must not be blank");
        }
    }

    record Catalog(String version, List<ProductCatalogReplica.Product> products) {
        public Catalog {
            if (version == null || version.isBlank()) throw new IllegalArgumentException("version must not be blank");
            products = List.copyOf(Objects.requireNonNull(products, "products"));
        }
    }

    record CheckoutLine(String sku, int quantity) {
        public CheckoutLine {
            Objects.requireNonNull(sku, "sku");
            if (quantity < 1) throw new IllegalArgumentException("quantity must be at least 1");
        }
    }

    enum RejectionReason { INSUFFICIENT_STOCK, INVALID_REQUEST }

    /** What an {@code ApplyCheckout} call established (contracts/grpc-internal-contracts.md "Status code usage"). */
    sealed interface ApplyResult permits Applied, Rejected, StockRestored, Uncertain, PayloadMismatch, InvalidRequest { }

    /** Every line was deducted, by this call or, when {@code replayed}, an earlier one. */
    record Applied(boolean replayed) implements ApplyResult { }

    /** Nothing was deducted; {@code sku} is the first unknown or short SKU in SKU order. */
    record Rejected(RejectionReason reason, String sku) implements ApplyResult {
        public Rejected {
            Objects.requireNonNull(reason, "reason");
        }
    }

    /** The attempt was restored or blocked earlier; nothing was deducted. */
    record StockRestored() implements ApplyResult { }

    /** No definitive answer: Inventory may or may not have applied the attempt, so repeat it under the same key. */
    record Uncertain(String status) implements ApplyResult { }

    /** FAILED_PRECONDITION: an integrity fault, never a rejection; the attempt stays pending with an alarm. */
    record PayloadMismatch(String detail) implements ApplyResult { }

    /** INVALID_ARGUMENT: a programming fault, never retried; the attempt stays pending with an alarm. */
    record InvalidRequest(String detail) implements ApplyResult { }

    enum Outcome { APPLIED, INSUFFICIENT_STOCK, INVALID_REQUEST, STOCK_RESTORED, NOT_PROCESSED }

    record RecordedOutcome(Outcome outcome, String rejectedSku) {
        public RecordedOutcome {
            Objects.requireNonNull(outcome, "outcome");
        }
    }
}
