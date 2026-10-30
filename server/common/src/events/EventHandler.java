package events;

/**
 * A business consumer of one topic (D6). It returns only after its local transaction committed, or after
 * deciding that the event needs no change. It throws {@link TransientEventFailure} or a
 * {@code database.StoreFailure} to have the same record retried, and {@link PermanentEventFailure} to have
 * it parked; any other exception is parked as {@link ParkReason#HANDLER_ERROR}.
 */
@FunctionalInterface
public interface EventHandler {
    void handle(DecodedEvent event);
}
