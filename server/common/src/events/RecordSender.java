package events;

import java.util.concurrent.CompletableFuture;

/**
 * Sends one record to the broker (D5). Only {@link OutboxDispatcher} and the scan relay's sink may call it
 * (FR-010, Implementation Rule 2). The future completes normally only after the broker confirmed the record
 * with {@code acks=all}, and exceptionally on any failure; {@code send} itself never blocks for longer than
 * the producer's {@code max.block.ms}.
 */
public interface RecordSender extends AutoCloseable {
    CompletableFuture<Void> send(String topic, String key, byte[] value);

    @Override
    default void close() { }
}
