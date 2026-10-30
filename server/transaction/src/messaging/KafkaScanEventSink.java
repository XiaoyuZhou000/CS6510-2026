package messaging;

import database.RelayStore.UndeliveredScan;
import events.EnvelopeCodec;
import events.EventTypes;
import events.RecordSender;
import events.TopicNames;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;
import selfcheckout.events.v1.ScanAccepted;
import transaction.ScanEventSink;

import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link ScanEventSink} over the scan producer (contracts/scan-event-delivery.md section 2, D10): every row
 * becomes one {@code ScanAccepted} record on {@code selfcheckout.scan-accepted.v1}, keyed by its transaction
 * ID, with the deterministic event ID {@code scan:<transaction_id>:<scan_sequence>}, the captured name and
 * unit price, and the acceptance time. The producer is Transaction's {@code transaction-scan} producer with
 * {@code linger.ms = SCAN_PRODUCER_LINGER_MS} (5), so a batch leaves in few requests.
 *
 * <p>{@link #deliver} sends the whole batch, then waits for every confirmation, and returns {@link Delivered}
 * only when all of them arrived. Any failed send, or confirmations still missing after {@link #CONFIRM_TIMEOUT}
 * (the producer's 30 s delivery timeout plus a margin), is {@link Uncertain}. A send that fails before it is
 * queued stops the batch at once, so with the broker down a round costs one {@code max.block.ms}, not one per
 * row; the relay re-sends the whole batch after its backoff (006 T138). It is the only caller of
 * {@link RecordSender} besides {@code OutboxDispatcher} (FR-010). Timer {@code transaction.scan.publish}, counter
 * {@code transaction.scan.publishFailures}.</p>
 */
public final class KafkaScanEventSink implements ScanEventSink {
    public static final Duration CONFIRM_TIMEOUT = Duration.ofSeconds(35);
    private static final LatencyHistogram PUBLISH = PerfMetrics.timer("transaction.scan.publish");

    private final RecordSender sender;
    private final String topic;

    public KafkaScanEventSink(RecordSender sender, TopicNames names) {
        this.sender = Objects.requireNonNull(sender, "sender");
        this.topic = names.scanAccepted();
    }

    @Override
    public DeliveryResult deliver(List<UndeliveredScan> events) {
        long started = System.nanoTime();
        try {
            List<byte[]> values = new ArrayList<>(events.size());
            try {
                for (UndeliveredScan scan : events) values.add(encode(scan));
            } catch (RuntimeException invalid) {
                return new InvalidBatch(invalid.getMessage());
            }
            List<CompletableFuture<Void>> confirmations = new ArrayList<>(events.size());
            for (int i = 0; i < events.size(); i++) {
                CompletableFuture<Void> confirmation = sender.send(topic, events.get(i).transactionId(), values.get(i));
                // A send that failed before it was queued, such as one blocked for max.block.ms on missing broker
                // metadata, means the rest would fail the same way, one max.block.ms each.
                if (confirmation.isCompletedExceptionally()) return uncertain("a send failed: " + failureOf(confirmation));
                confirmations.add(confirmation);
            }
            CompletableFuture.allOf(confirmations.toArray(CompletableFuture[]::new))
                    .get(CONFIRM_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return new Delivered(events.size());
        } catch (ExecutionException failed) {
            return uncertain("a send failed: " + failed.getCause());
        } catch (TimeoutException late) {
            return uncertain("confirmations missing after " + CONFIRM_TIMEOUT.toSeconds() + " s");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return uncertain("interrupted while waiting for confirmations");
        } catch (RuntimeException unexpected) {
            return uncertain("sending failed: " + unexpected);
        } finally {
            PUBLISH.recordSince(started);
        }
    }

    /** The record value of {@code scan}: a {@code ScanAccepted} envelope produced by Transaction. */
    public static byte[] encode(UndeliveredScan scan) {
        ScanAccepted payload = ScanAccepted.newBuilder()
                .setTransactionId(scan.transactionId())
                .setScanSequence(scan.scanSequence())
                .setSku(scan.sku())
                .setProductName(scan.productName())
                .setUnitPrice(scan.unitPrice().setScale(2, RoundingMode.UNNECESSARY).toPlainString())
                .setAcceptedAtMs(scan.acceptedAt().toEpochMilli())
                .build();
        return EnvelopeCodec.encode(EventTypes.scanEventId(scan.transactionId(), scan.scanSequence()),
                EventTypes.SCAN_ACCEPTED, scan.transactionId(), scan.transactionId(), EventTypes.TRANSACTION,
                scan.acceptedAt().toEpochMilli(), payload);
    }

    private static Throwable failureOf(CompletableFuture<Void> failed) {
        try {
            failed.join();
            return null;
        } catch (CompletionException failure) {
            return failure.getCause();
        } catch (CancellationException cancelled) {
            return cancelled;
        }
    }

    private static Uncertain uncertain(String reason) {
        PerfMetrics.increment("transaction.scan.publishFailures");
        return new Uncertain(reason);
    }
}
