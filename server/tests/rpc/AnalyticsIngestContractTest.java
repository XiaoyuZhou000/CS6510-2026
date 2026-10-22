package rpc;

import analytics.ProductNameMap;
import analytics.ScanIngestionService;
import contracts.analytics.v1.AnalyticsIngestServiceGrpc;
import contracts.analytics.v1.DeliverScanEventsRequest;
import contracts.analytics.v1.DeliverScanEventsResponse;
import contracts.analytics.v1.ScanEvent;
import database.ConnectionPool;
import database.JdbcScanIngestStore;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.FaultingConnections;
import support.FaultingConnections.CommitFault;
import support.GrpcTestSupport;
import support.InProcessAnalytics;

import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static support.InProcessAnalytics.event;

/**
 * {@code AnalyticsIngestService.DeliverScanEvents} (contracts/grpc-internal-contracts.md, scan-event-delivery.md
 * section 3) over the real adapters and an {@link AnalyticsTestSchema}: positions, deduplication, validation,
 * outage, and acknowledgement only after the ingestion commit.
 */
class AnalyticsIngestContractTest {

    @Test
    void batchStoresEventsAtConsecutivePositionsInBatchOrder() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            DeliverScanEventsResponse response = analytics.deliver(List.of(
                    event("tx-b", 1, "sku2", "Item 2"), event("tx-a", 1, "sku1", "Item 1"), event("tx-a", 2, "sku3", "Item 3")));
            assertEquals(3, response.getStored());
            assertEquals(0, response.getDuplicates());
            List<AnalyticsTestSchema.StoredEvent> stored = schema.events();
            assertEquals(List.of(new AnalyticsTestSchema.StoredEvent(1, "tx-b", 1, "sku2"),
                    new AnalyticsTestSchema.StoredEvent(2, "tx-a", 1, "sku1"),
                    new AnalyticsTestSchema.StoredEvent(3, "tx-a", 2, "sku3")), stored);
            assertEquals(3, schema.lastPosition());
        }
    }

    @Test
    void redeliveryStoresNothingAndReportsEveryEventAsDuplicate() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            List<ScanEvent> batch = InProcessAnalytics.events("tx-1", 1, List.of("sku1", "sku2", "sku1", "sku4"));
            assertEquals(4, analytics.deliver(batch).getStored());
            List<AnalyticsTestSchema.StoredEvent> first = schema.events();
            DeliverScanEventsResponse again = analytics.deliver(batch);
            assertEquals(0, again.getStored());
            assertEquals(4, again.getDuplicates());
            assertEquals(first, schema.events(), "redelivery must not move or add positions");
            assertEquals(4, schema.lastPosition());
        }
    }

    @Test
    void identityRepeatedInsideOneBatchCountsAsDuplicate() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            DeliverScanEventsResponse response = analytics.deliver(List.of(event("tx-1", 1, "sku1", "Item 1"),
                    event("tx-1", 2, "sku2", "Item 2"), event("tx-1", 1, "sku1", "Item 1")));
            assertEquals(2, response.getStored());
            assertEquals(1, response.getDuplicates());
            assertEquals(2, schema.storedEvents());
            assertEquals(2, schema.lastPosition());
        }
    }

    @Test
    void overlappingBatchesAssignPositionsOnlyToNewIdentities() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            analytics.deliver(InProcessAnalytics.events("tx-1", 1, List.of("sku1", "sku2", "sku3")));
            DeliverScanEventsResponse overlap = analytics.deliver(InProcessAnalytics.events("tx-1", 2,
                    List.of("sku2", "sku3", "sku4", "sku5")));
            assertEquals(2, overlap.getStored());
            assertEquals(2, overlap.getDuplicates());
            List<AnalyticsTestSchema.StoredEvent> stored = schema.events();
            assertEquals(5, stored.size());
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), stored.stream().map(AnalyticsTestSchema.StoredEvent::position).toList());
            assertEquals(List.of(1, 2, 3, 4, 5), stored.stream().map(AnalyticsTestSchema.StoredEvent::sourceScanSequence).toList());
            assertEquals("sku4", stored.get(3).sku());
            assertEquals("sku5", stored.get(4).sku());
        }
    }

    @Test
    void malformedBatchesAreInvalidArgumentAndStoreNothing() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            List<ScanEvent> oversized = new ArrayList<>();
            for (int i = 1; i <= 1_001; i++) oversized.add(event("tx-big", i, "sku1", "Item 1"));
            List<List<ScanEvent>> malformed = List.of(
                    List.of(),
                    oversized,
                    List.of(event("tx-1", 1, "sku1", "Item 1"), event("tx-1", 2, " ", "Blank")),
                    List.of(event("tx-1", 0, "sku1", "Item 1")),
                    List.of(event("tx-1", 1, "sku1", "x".repeat(101))),
                    List.of(event("", 1, "sku1", "Item 1")),
                    List.of(event("t".repeat(41), 1, "sku1", "Item 1")),
                    List.of(event("tx-1", 1, "s".repeat(21), "Item 1")),
                    List.of(event("tx-1", 1, "sku1", "")));
            for (List<ScanEvent> batch : malformed) {
                StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, () -> analytics.deliver(batch));
                assertEquals(Status.Code.INVALID_ARGUMENT, failure.getStatus().getCode());
            }
            assertEquals(0, schema.storedEvents());
            assertEquals(0, schema.lastPosition());
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM product_name"));

            List<ScanEvent> exactlyMax = new ArrayList<>();
            for (int i = 1; i <= 1_000; i++) exactlyMax.add(event("tx-max", i, "sku1", "x".repeat(100)));
            assertEquals(1_000, analytics.deliver(exactlyMax).getStored(), "1,000 events and 100-character names are valid");
        }
    }

    @Test
    void capturedNamesLandInProductName() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            analytics.deliver(List.of(event("tx-1", 1, "sku1", "Apples"), event("tx-1", 2, "sku2", "Bread")));
            assertEquals("Apples", schema.productName("sku1"));
            assertEquals("Bread", schema.productName("sku2"));
            analytics.deliver(List.of(event("tx-2", 1, "sku1", "Green apples")));
            assertEquals("Green apples", schema.productName("sku1"), "a changed captured name replaces the stored one");
            assertEquals("Green apples", analytics.names.name("sku1"));
        }
    }

    @Test
    void databaseOutageIsUnavailableAndStoresNoRows() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); InProcessAnalytics analytics = InProcessAnalytics.over(schema)) {
            schema.setTableUnavailable("accepted_scan", true);
            StatusRuntimeException failure = assertThrows(StatusRuntimeException.class,
                    () -> analytics.deliver(InProcessAnalytics.events("tx-1", 1, List.of("sku1", "sku2"))));
            assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
            assertFalse(failure.getStatus().getDescription().contains("accepted_scan"), "no database details leave Analytics");
            schema.setTableUnavailable("accepted_scan", false);
            assertEquals(0, schema.storedEvents());
            assertEquals(0, schema.lastPosition());
            assertNull(schema.productName("sku1"));
            assertEquals(2, analytics.deliver(InProcessAnalytics.events("tx-1", 1, List.of("sku1", "sku2"))).getStored());
        }
    }

    @Test
    void replyIsSentOnlyAfterTheIngestionCommitReturns() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); OnePool single = OnePool.over(schema, faults)) {
            faults.armForConnection(single.connection(), CommitFault.delay(1_500));
            long started = System.nanoTime();
            CompletableFuture<DeliverScanEventsResponse> reply = CompletableFuture.supplyAsync(
                    () -> single.analytics.deliver(InProcessAnalytics.events("tx-1", 1, List.of("sku1", "sku2"))));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (faults.commitsStarted() == 0 && System.nanoTime() < deadline) Thread.sleep(5);
            assertEquals(1, faults.commitsStarted(), "the ingestion commit must have started");
            Thread.sleep(500);
            assertFalse(reply.isDone(), "no reply may leave Analytics while its commit is still running");
            assertEquals(0, faults.commitsReturned());
            DeliverScanEventsResponse response = reply.get(10, TimeUnit.SECONDS);
            assertEquals(1, faults.commitsReturned(), "the reply follows the returned commit");
            assertTrue(System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(1_500));
            assertEquals(2, response.getStored());
            assertEquals(2, schema.storedEvents());
        }
    }

    @Test
    void faultBeforeCommitIsUnavailableAndLeavesNoRowsAndNoSequenceAdvance() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); OnePool single = OnePool.over(schema, faults)) {
            faults.armForConnection(single.connection(), CommitFault.failBeforeCommit());
            List<ScanEvent> batch = InProcessAnalytics.events("tx-1", 1, List.of("sku1", "sku2", "sku3"));
            StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, () -> single.analytics.deliver(batch));
            assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
            assertEquals(0, schema.storedEvents());
            assertEquals(0, schema.lastPosition());
            DeliverScanEventsResponse retry = single.analytics.deliver(batch);
            assertEquals(3, retry.getStored());
            assertEquals(0, retry.getDuplicates());
            schema.assertPositionsContiguous();
        }
    }

    @Test
    void faultAfterSuccessfulCommitIsUnavailableButDurableAndRetryReportsDuplicates() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create(); OnePool single = OnePool.over(schema, faults)) {
            faults.armForConnection(single.connection(), CommitFault.failAfterCommit());
            List<ScanEvent> batch = InProcessAnalytics.events("tx-1", 1, List.of("sku1", "sku2", "sku3"));
            StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, () -> single.analytics.deliver(batch));
            assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode(), "an unknown commit outcome is UNAVAILABLE");
            List<AnalyticsTestSchema.StoredEvent> durable = schema.events();
            assertEquals(3, durable.size(), "the rows committed before the failure");
            DeliverScanEventsResponse retry = single.analytics.deliver(batch);
            assertEquals(0, retry.getStored());
            assertEquals(3, retry.getDuplicates());
            assertEquals(durable, schema.events(), "a retry never reassigns positions");
            assertEquals(3, schema.lastPosition());
        }
    }

    @Test
    void realPortServesTheSameContract() throws Exception {
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            ProductNameMap names = new ProductNameMap();
            JdbcScanIngestStore store = new JdbcScanIngestStore(schema.pool(), names);
            names.loadFrom(store);
            GrpcServers.Running server = GrpcServers.start(GrpcTestSupport.freePort(),
                    new AnalyticsIngestGrpcService(new ScanIngestionService(store)));
            ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
            try {
                var stub = AnalyticsIngestServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
                DeliverScanEventsRequest request = DeliverScanEventsRequest.newBuilder()
                        .addAllEvents(InProcessAnalytics.events("tx-port", 1, Collections.nCopies(500, "sku1"))).build();
                assertEquals(500, stub.deliverScanEvents(request).getStored());
                DeliverScanEventsResponse again = AnalyticsIngestServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(10, TimeUnit.SECONDS).deliverScanEvents(request);
                assertEquals(0, again.getStored());
                assertEquals(500, again.getDuplicates());
                StatusRuntimeException invalid = assertThrows(StatusRuntimeException.class, () -> AnalyticsIngestServiceGrpc
                        .newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS)
                        .deliverScanEvents(DeliverScanEventsRequest.getDefaultInstance()));
                assertEquals(Status.Code.INVALID_ARGUMENT, invalid.getStatus().getCode());
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                server.stop(Duration.ofSeconds(2));
            }
        }
    }

    /**
     * Ingestion over a one-connection pool decorated with {@link FaultingConnections}, so a commit fault can be
     * armed for the exact connection the handler will use.
     */
    private record OnePool(ConnectionPool pool, InProcessAnalytics analytics) implements AutoCloseable {
        static OnePool over(AnalyticsTestSchema schema, FaultingConnections faults) throws Exception {
            ConnectionPool pool = schema.newPool("test.ingest.single", 1, 1, faults::wrap);
            return new OnePool(pool, InProcessAnalytics.over(pool));
        }

        Connection connection() {
            Connection pooled = pool.borrow();
            pool.release(pooled);
            return pooled;
        }

        @Override
        public void close() throws InterruptedException {
            analytics.close();
        }
    }
}
