package transaction;

import database.TransactionStore;
import transaction.TransactionOperations.TransactionStatus;
import transaction.TransactionStateCache.Line;
import transaction.TransactionStateCache.State;

import java.util.Optional;
import java.util.TreeMap;

/** The cold path shared by Scan and Complete: the transaction row plus its lines grouped from accepted_scan (R19). */
final class TransactionStates {
    private TransactionStates() { }

    static Optional<State> load(TransactionStore transactions, String id) {
        return transactions.loadState(id).map(stored -> {
            var row = stored.transaction();
            TreeMap<String, Line> lines = new TreeMap<>();
            for (TransactionStore.TransactionLine line : stored.lines()) {
                lines.put(line.sku(), new Line(line.sku(), line.name(), line.unitPrice(), line.quantity()));
            }
            try {
                return new State(id, TransactionStatus.valueOf(row.status().name()), row.itemCount(),
                        row.totalAmount(), stored.attemptCount(), stored.activeAttemptNumber(), lines, row.stationId(),
                        row.startedAt());
            } catch (IllegalArgumentException inconsistent) {
                throw new IllegalStateException("The stored state of transaction " + id + " is inconsistent", inconsistent);
            }
        });
    }
}
