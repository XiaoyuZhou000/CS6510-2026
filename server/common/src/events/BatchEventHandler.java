package events;

import java.util.List;

/**
 * A business consumer that handles the records of one partition in batches (D6, D10), for a consumer whose
 * own database holds its consumption progress. {@link #handleBatch} receives consecutive decoded records of
 * one partition, each carrying its position, and returns only after one local transaction committed their
 * effects together with the next offset to read. Its failures follow the same rules as
 * {@link EventHandler}'s: {@link TransientEventFailure} or a {@code database.StoreFailure} to have the
 * records read again, {@link PermanentEventFailure} to have a record parked.
 */
@FunctionalInterface
public interface BatchEventHandler {
    void handleBatch(List<DecodedEvent> events);
}
