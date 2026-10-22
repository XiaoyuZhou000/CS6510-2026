package transaction;

/**
 * Transaction Service has never obtained product information, so it cannot decide whether a SKU exists.
 * Scan answers 500 rather than 404 SKU_NOT_FOUND (FR-023).
 */
public final class ProductCatalogUnavailableFailure extends RuntimeException {
    public ProductCatalogUnavailableFailure() {
        super("No product information is available yet");
    }
}
