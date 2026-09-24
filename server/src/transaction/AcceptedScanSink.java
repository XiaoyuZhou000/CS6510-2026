package transaction;

/** Callback emitted exactly once after one physical-unit scan is admitted to an open basket. */
@FunctionalInterface
public interface AcceptedScanSink {

    AcceptedScanSink NO_OP = sku -> { };

    void recordAcceptedScan(String sku);
}
