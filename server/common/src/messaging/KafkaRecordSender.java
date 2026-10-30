package messaging;

import events.RecordSender;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * {@link RecordSender} over one Kafka producer (D5). The future completes when the broker acknowledged the
 * record with {@code acks=all}, and fails on any send error; a send that cannot even be queued, for example
 * because the broker's metadata is unavailable for {@code max.block.ms}, fails at once instead of throwing.
 */
public final class KafkaRecordSender implements RecordSender {
    private final Producer<String, byte[]> producer;

    public KafkaRecordSender(Producer<String, byte[]> producer) {
        this.producer = Objects.requireNonNull(producer, "producer");
    }

    @Override
    public CompletableFuture<Void> send(String topic, String key, byte[] value) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        try {
            producer.send(new ProducerRecord<>(topic, key, value), (metadata, failure) -> {
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
        producer.flush();
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(5));
    }
}
