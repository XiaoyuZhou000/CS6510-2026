package messaging;

import events.RecordSender;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * {@link RecordSender} over one Kafka producer (D5). The future completes when the broker acknowledged the
 * record with {@code acks=all}, and fails on any send error; a send that cannot even be queued, for example
 * because the broker's metadata is unavailable for {@code max.block.ms}, fails at once instead of throwing.
 *
 * <p>A {@linkplain #lazy lazy} sender creates its producer on the first send, on the sending thread (a
 * dispatcher or the scan relay), never at startup (FR-015, 006 T137). A producer that cannot be created, for
 * example because the broker address does not resolve, fails that send, and the next send tries again.</p>
 */
public final class KafkaRecordSender implements RecordSender {
    private final Supplier<? extends Producer<String, byte[]>> factory;
    private volatile Producer<String, byte[]> producer;
    private boolean closed;

    public KafkaRecordSender(Producer<String, byte[]> producer) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.factory = null;
    }

    private KafkaRecordSender(Supplier<? extends Producer<String, byte[]>> factory) {
        this.factory = Objects.requireNonNull(factory, "factory");
    }

    /** A sender whose producer {@code factory} builds on the first send. */
    public static KafkaRecordSender lazy(Supplier<? extends Producer<String, byte[]>> factory) {
        return new KafkaRecordSender(factory);
    }

    @Override
    public CompletableFuture<Void> send(String topic, String key, byte[] value) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        try {
            producer().send(new ProducerRecord<>(topic, key, value), (metadata, failure) -> {
                if (failure == null) confirmation.complete(null);
                else confirmation.completeExceptionally(failure);
            });
        } catch (RuntimeException failure) {
            confirmation.completeExceptionally(failure);
        }
        return confirmation;
    }

    /** Sends whatever {@code linger.ms} still holds back. */
    public void flush() {
        Producer<String, byte[]> current = producer;
        if (current != null) current.flush();
    }

    @Override
    public void close() {
        Producer<String, byte[]> current;
        synchronized (this) {
            closed = true;
            current = producer;
        }
        if (current != null) current.close(Duration.ofSeconds(5));
    }

    private Producer<String, byte[]> producer() {
        Producer<String, byte[]> current = producer;
        if (current != null) return current;
        synchronized (this) {
            if (closed) throw new IllegalStateException("The sender is closed");
            if (producer == null) producer = Objects.requireNonNull(factory.get(), "producer");
            return producer;
        }
    }
}
