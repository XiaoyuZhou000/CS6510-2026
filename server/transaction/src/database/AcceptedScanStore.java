package database;

import java.math.BigDecimal;

public interface AcceptedScanStore {
    ScanResult scanAtomically(String transactionId, String sku) throws StoreFailure;
    sealed interface ScanResult permits Accepted, MissingTransaction, NotOpen, UnknownSku { }
    record Accepted(long eventId, String name, BigDecimal unitPrice, int itemCount,
                    BigDecimal runningTotal) implements ScanResult { }
    record MissingTransaction() implements ScanResult { }
    record NotOpen() implements ScanResult { }
    record UnknownSku() implements ScanResult { }
}
