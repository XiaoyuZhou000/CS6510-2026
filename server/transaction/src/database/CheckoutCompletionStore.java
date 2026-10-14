package database;

import java.time.Instant;

public interface CheckoutCompletionStore {
    CompletionResult completeAtomically(String transactionId) throws StoreFailure;
    sealed interface CompletionResult permits Completed, MissingTransaction, NotOpen, EmptyBasket, InsufficientStock { }
    record Completed(Instant completedAt) implements CompletionResult { }
    record MissingTransaction() implements CompletionResult { }
    record NotOpen() implements CompletionResult { }
    record EmptyBasket() implements CompletionResult { }
    record InsufficientStock(String sku) implements CompletionResult { }
}
