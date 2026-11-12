import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.SubscriberNotified;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

/**
 * One subscriber: one {@link KafkaConsumer} manually assigned to partition 0 of its channel topic, with no consumer
 * group and no offset commits (research N10). The position comes from the receipt file instead: the thread seeks to
 * {@link ReceiptWriter#resumeOffset()}, which is 0 on a fresh run and one past the highest recorded offset on
 * {@code --resume} or after a reconnect.
 *
 * <p>Every record is decoded as an {@code EventEnvelope} carrying a {@code SubscriberNotified} addressed to this
 * subscriber, and recorded with {@code receivedAtMs = System.currentTimeMillis()}; anything else is recorded as an
 * {@code undecodable} line. Receipts are flushed after every poll. The channel lag (end offset minus position) is
 * refreshed from {@code endOffsets} at most once a second.
 *
 * <p>A consumer failure is logged and the consumer is recreated a second later, so a broker restart is ridden out;
 * after 60 consecutive failures, or on any receipt write failure, the thread stops and reports itself failed.
 */
public final class SubscriberThread implements Runnable {
    static final int MAX_CONSECUTIVE_FAILURES = 60;

    private final String subscriberId;
    private final String channel;
    private final ReceiptWriter writer;
    private final PrintStream log;
    private final TopicPartition partition;

    private final Supplier<Consumer<String, byte[]>> consumers;
    private volatile boolean stopping;
    private volatile Consumer<String, byte[]> consumer;
    private volatile Throwable failure;
    private volatile boolean finished;
    private volatile long lag = -1;
    private volatile long received;
    private volatile long redeliveries;
    private volatile long undecodable;
    private long lagCheckedAtNanos;

    public SubscriberThread(String subscriberId, String channel, String bootstrap, ReceiptWriter writer, PrintStream log) {
        this(subscriberId, channel, writer, log, () -> kafkaConsumer(subscriberId, bootstrap));
    }

    /** With a consumer factory, called again after each failure; tests pass Kafka's {@code MockConsumer}. */
    SubscriberThread(String subscriberId, String channel, ReceiptWriter writer, PrintStream log,
                     Supplier<Consumer<String, byte[]>> consumers) {
        this.subscriberId = subscriberId;
        this.channel = channel;
        this.writer = writer;
        this.log = log;
        this.consumers = consumers;
        this.partition = new TopicPartition(channel, 0);
        publishCounts();
    }

    @Override
    public void run() {
        int consecutiveFailures = 0;
        try {
            while (!stopping) {
                try {
                    if (consumer == null) consumer = open();
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
                    for (ConsumerRecord<String, byte[]> record : records) handle(record);
                    if (!records.isEmpty()) {
                        writer.flush();
                        publishCounts();
                    }
                    refreshLag();
                    consecutiveFailures = 0;
                } catch (WakeupException wakeup) {
                    if (stopping) break;
                } catch (IOException receiptFailure) {
                    failure = receiptFailure;
                    log.println(subscriberId + ": cannot write receipts: " + receiptFailure);
                    break;
                } catch (RuntimeException consumerFailure) {
                    if (stopping) break;
                    consecutiveFailures++;
                    log.println(subscriberId + ": consumer failure " + consecutiveFailures + ": " + consumerFailure);
                    closeConsumer();
                    if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                        failure = consumerFailure;
                        break;
                    }
                    sleepQuietly(1_000);
                }
            }
        } finally {
            try {
                writer.flush();
            } catch (IOException e) {
                if (failure == null) failure = e;
            }
            publishCounts();
            closeConsumer();
            try {
                writer.close();
            } catch (IOException e) {
                if (failure == null) failure = e;
            }
            finished = true;
        }
    }

    private Consumer<String, byte[]> open() {
        Consumer<String, byte[]> opened = consumers.get();
        opened.assign(List.of(partition));
        opened.seek(partition, writer.resumeOffset());
        return opened;
    }

    private static Consumer<String, byte[]> kafkaConsumer(String subscriberId, String bootstrap) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "subscriber-" + subscriberId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    private void handle(ConsumerRecord<String, byte[]> record) throws IOException {
        long receivedAtMs = System.currentTimeMillis();
        SubscriberNotified notified;
        try {
            if (record.value() == null) throw new IllegalArgumentException("empty record value");
            EventEnvelope envelope = EventEnvelope.parseFrom(record.value());
            if (!"SubscriberNotified".equals(envelope.getEventType())) {
                throw new IllegalArgumentException("unexpected event type " + envelope.getEventType());
            }
            if (envelope.getSchemaVersion() != 1) {
                throw new IllegalArgumentException("unsupported schema version " + envelope.getSchemaVersion());
            }
            notified = SubscriberNotified.parseFrom(envelope.getPayload());
            if (!subscriberId.equals(notified.getSubscriberId())) {
                throw new IllegalArgumentException("addressed to " + notified.getSubscriberId());
            }
            if (!envelope.getEventId().equals(notified.getNotificationId())) {
                throw new IllegalArgumentException("event_id " + envelope.getEventId() + " differs from notification_id");
            }
        } catch (IOException | RuntimeException invalid) {
            writer.recordUndecodable(record.offset(), receivedAtMs, invalid.toString());
            return;
        }
        writer.record(notified.getNotificationId(), notified.getSaleId(), notified.getSku(), notified.getQuantity(),
                notified.getCompletedAtMs(), receivedAtMs, record.offset());
    }

    private void refreshLag() {
        long now = System.nanoTime();
        if (now - lagCheckedAtNanos < 1_000_000_000L) return;
        lagCheckedAtNanos = now;
        try {
            Map<TopicPartition, Long> end = consumer.endOffsets(Set.of(partition), Duration.ofSeconds(2));
            Long endOffset = end.get(partition);
            if (endOffset != null) lag = Math.max(0, endOffset - consumer.position(partition, Duration.ofSeconds(2)));
        } catch (WakeupException wakeup) {
            throw wakeup;
        } catch (RuntimeException unknown) {
            lag = -1;
        }
    }

    private void publishCounts() {
        received = writer.received();
        redeliveries = writer.redeliveries();
        undecodable = writer.undecodable();
    }

    private void closeConsumer() {
        Consumer<String, byte[]> current = consumer;
        consumer = null;
        lag = -1;
        if (current == null) return;
        try {
            current.close(CloseOptions.timeout(Duration.ofSeconds(2)));
        } catch (RuntimeException ignored) {
            // Closing a consumer that already failed may fail too; it is replaced either way.
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Asks the thread to stop; it flushes its receipts and closes its consumer. */
    public void stop() {
        stopping = true;
        Consumer<String, byte[]> current = consumer;
        if (current != null) current.wakeup();
    }

    public String subscriberId() { return subscriberId; }

    public String channel() { return channel; }

    public Throwable failure() { return failure; }

    public boolean finished() { return finished; }

    public StatusWriter.Stats stats() {
        return new StatusWriter.Stats(received, redeliveries, undecodable, lag);
    }
}
