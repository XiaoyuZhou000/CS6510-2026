package rpc;

import contracts.analytics.v1.AnalyticsIngestServiceGrpc;
import contracts.analytics.v1.DeliverScanEventsRequest;
import contracts.analytics.v1.DeliverScanEventsResponse;
import contracts.analytics.v1.ScanEvent;
import database.RelayStore.UndeliveredScan;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;
import transaction.ScanEventSink;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * {@link ScanEventSink} over Analytics' {@code DeliverScanEvents}, on one reused channel (R2), with a
 * per-call deadline of {@code ANALYTICS_RPC_DEADLINE_MS} (5 s). Only OK proves durable receipt; every
 * status except INVALID_ARGUMENT is uncertain (contracts/grpc-internal-contracts.md "Status code usage").
 * Every call records {@code transaction.rpc.deliverScanEvents} and a
 * {@code transaction.rpc.deliverScanEvents.<status>} counter (R21).
 *
 * <p>After UNAVAILABLE it resets the channel's reconnect backoff, which otherwise grows to minutes during a
 * long Analytics outage, so the relay's own 100 ms to 5 s backoff decides when the next attempt connects.</p>
 */
public final class GrpcAnalyticsIngestClient implements ScanEventSink {
    public static final Duration DEFAULT_DEADLINE = Duration.ofSeconds(5);
    private static final LatencyHistogram DELIVER = PerfMetrics.timer("transaction.rpc.deliverScanEvents");

    private final Channel channel;
    private final AnalyticsIngestServiceGrpc.AnalyticsIngestServiceBlockingStub stub;
    private final Duration deadline;

    public GrpcAnalyticsIngestClient(Channel channel, Duration deadline) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.stub = AnalyticsIngestServiceGrpc.newBlockingStub(channel);
        this.deadline = Objects.requireNonNull(deadline, "deadline");
        if (deadline.isZero() || deadline.isNegative()) throw new IllegalArgumentException("deadline must be positive");
    }

    /** The client on the process-wide channel for {@code host:port}. */
    public static GrpcAnalyticsIngestClient forTarget(String target, Duration deadline) {
        return new GrpcAnalyticsIngestClient(GrpcChannels.forTarget(target), deadline);
    }

    /** {@code ANALYTICS_RPC_DEADLINE_MS}, default 5,000. */
    public static Duration deadline(Map<String, String> environment) {
        String configured = environment.get("ANALYTICS_RPC_DEADLINE_MS");
        if (configured == null || configured.isBlank()) return DEFAULT_DEADLINE;
        long millis = Long.parseLong(configured.trim());
        if (millis <= 0) throw new IllegalArgumentException("ANALYTICS_RPC_DEADLINE_MS must be positive");
        return Duration.ofMillis(millis);
    }

    @Override
    public DeliveryResult deliver(List<UndeliveredScan> events) {
        long started = System.nanoTime();
        String status = "OK";
        try {
            DeliverScanEventsRequest.Builder request = DeliverScanEventsRequest.newBuilder();
            for (UndeliveredScan event : events) {
                request.addEvents(ScanEvent.newBuilder().setTransactionId(event.transactionId())
                        .setScanSequence(event.scanSequence()).setSku(event.sku())
                        .setProductName(event.productName())
                        .setAcceptedAtEpochMillis(event.acceptedAt().toEpochMilli()));
            }
            DeliverScanEventsResponse response = stub.withDeadlineAfter(deadline.toNanos(), TimeUnit.NANOSECONDS)
                    .deliverScanEvents(request.build());
            return new Delivered(response.getStored(), response.getDuplicates());
        } catch (StatusRuntimeException failure) {
            status = failure.getStatus().getCode().name();
            if (failure.getStatus().getCode() == Status.Code.UNAVAILABLE && channel instanceof ManagedChannel managed) {
                managed.resetConnectBackoff();
            }
            return RpcStatusClassifier.classify(failure) == RpcStatusClassifier.Kind.INVALID_ARGUMENT
                    ? new InvalidBatch(failure.getStatus().getDescription())
                    : new Uncertain(status);
        } catch (RuntimeException unexpected) {
            status = "CLIENT_FAILURE";
            return new Uncertain(status + " " + unexpected.getClass().getSimpleName());
        } finally {
            DELIVER.record(System.nanoTime() - started);
            PerfMetrics.increment("transaction.rpc.deliverScanEvents." + status);
        }
    }
}
