package rpc;

import analytics.ScanIngestionService;
import contracts.analytics.v1.AnalyticsIngestServiceGrpc;
import contracts.analytics.v1.DeliverScanEventsRequest;
import contracts.analytics.v1.DeliverScanEventsResponse;
import contracts.analytics.v1.ScanEvent;
import database.ScanIngestStore;
import database.StoreFailure;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import metrics.PerfMetrics;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Translates {@code DeliverScanEvents} to {@link ScanIngestionService} (contracts/grpc-internal-contracts.md).
 * The reply is sent only after the ingestion commit returned, so {@code OK} means every event of the batch
 * is durable with its position and deduplication key. Failures map to INVALID_ARGUMENT, UNAVAILABLE (any
 * store failure, including an unknown commit outcome), or INTERNAL; no description carries database details.
 */
public final class AnalyticsIngestGrpcService extends AnalyticsIngestServiceGrpc.AnalyticsIngestServiceImplBase {
    private final ScanIngestionService ingestion;

    public AnalyticsIngestGrpcService(ScanIngestionService ingestion) {
        this.ingestion = Objects.requireNonNull(ingestion, "ingestion");
    }

    @Override
    public void deliverScanEvents(DeliverScanEventsRequest request, StreamObserver<DeliverScanEventsResponse> response) {
        ScanIngestStore.IngestResult result;
        try {
            List<ScanIngestStore.ScanEvent> events = new ArrayList<>(request.getEventsCount());
            for (ScanEvent event : request.getEventsList()) {
                // uint32 arrives as a signed int; values above Integer.MAX_VALUE fail validation as non-positive.
                events.add(new ScanIngestStore.ScanEvent(event.getTransactionId(), event.getScanSequence(),
                        event.getSku(), event.getProductName(),
                        Instant.ofEpochMilli(event.getAcceptedAtEpochMillis())));
            }
            result = ingestion.ingest(events);
        } catch (RuntimeException failure) {
            Status status = status(failure);
            PerfMetrics.increment("analytics.grpc.deliver." + status.getCode().name());
            response.onError(status.asRuntimeException());
            return;
        }
        PerfMetrics.increment("analytics.grpc.deliver.OK");
        response.onNext(DeliverScanEventsResponse.newBuilder().setStored(result.stored())
                .setDuplicates(result.duplicates()).build());
        response.onCompleted();
    }

    private static Status status(RuntimeException failure) {
        if (failure instanceof ScanIngestionService.InvalidBatch invalid) {
            return Status.INVALID_ARGUMENT.withDescription(invalid.getMessage());
        }
        if (failure instanceof StoreFailure) {
            return Status.UNAVAILABLE.withDescription("Analytics' database is unavailable");
        }
        return Status.INTERNAL.withDescription("Analytics failed to ingest the batch");
    }
}
