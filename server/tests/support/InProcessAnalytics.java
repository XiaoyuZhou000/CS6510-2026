package support;

import analytics.AnalyticsService;
import analytics.ProductNameMap;
import analytics.ScanIngestionService;
import contracts.analytics.v1.AnalyticsIngestServiceGrpc;
import contracts.analytics.v1.DeliverScanEventsRequest;
import contracts.analytics.v1.DeliverScanEventsResponse;
import contracts.analytics.v1.ScanEvent;
import database.ConnectionPool;
import database.JdbcAcceptedScanReader;
import database.JdbcPopularWindowStore;
import database.JdbcScanIngestStore;
import database.PopularWindowStore;
import io.grpc.ManagedChannel;
import rpc.AnalyticsIngestGrpcService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Analytics' real ingestion path in this JVM: {@link AnalyticsIngestGrpcService} over
 * {@link JdbcScanIngestStore}, a loaded {@link ProductNameMap}, and an {@link AnalyticsTestSchema} pool,
 * served on an in-process gRPC channel, wired like {@code AnalyticsMain}. The window pipeline is started
 * separately with {@link #startPipeline}, so a test can stop and restart it.
 */
public final class InProcessAnalytics implements AutoCloseable {
    public final ConnectionPool pool;
    public final ProductNameMap names;
    public final JdbcScanIngestStore store;
    public final ScanIngestionService ingestion;
    public final JdbcPopularWindowStore windows;
    private final GrpcTestSupport.InProcess server;
    private final AnalyticsIngestServiceGrpc.AnalyticsIngestServiceBlockingStub stub;

    private InProcessAnalytics(ConnectionPool pool) throws Exception {
        this.pool = pool;
        names = new ProductNameMap();
        store = new JdbcScanIngestStore(pool, names);
        names.loadFrom(store);
        ingestion = new ScanIngestionService(store);
        windows = new JdbcPopularWindowStore(pool);
        server = GrpcTestSupport.inProcess(new AnalyticsIngestGrpcService(ingestion));
        stub = AnalyticsIngestServiceGrpc.newBlockingStub(server.channel());
    }

    /** Serves the schema's own default pool. */
    public static InProcessAnalytics over(AnalyticsTestSchema schema) throws Exception {
        return new InProcessAnalytics(schema.pool());
    }

    /** Serves a given pool, for example one with a test decorator or for a second instance. */
    public static InProcessAnalytics over(ConnectionPool pool) throws Exception {
        return new InProcessAnalytics(pool);
    }

    public ManagedChannel channel() {
        return server.channel();
    }

    /** One {@code DeliverScanEvents} call with a 10 s deadline. */
    public DeliverScanEventsResponse deliver(List<ScanEvent> events) {
        return stub.withDeadlineAfter(10, TimeUnit.SECONDS)
                .deliverScanEvents(DeliverScanEventsRequest.newBuilder().addAllEvents(events).build());
    }

    /** Starts the durable window pipeline over this pool: prewarm from the checkpoint, then polling. */
    public AnalyticsService startPipeline() {
        return startPipeline(windows);
    }

    public AnalyticsService startPipeline(PopularWindowStore windowStore) {
        return new AnalyticsService(windowStore, new JdbcAcceptedScanReader(pool));
    }

    public static ScanEvent event(String transactionId, int scanSequence, String sku, String name) {
        return ScanEvent.newBuilder().setTransactionId(transactionId).setScanSequence(scanSequence).setSku(sku)
                .setProductName(name).setAcceptedAtEpochMillis(System.currentTimeMillis()).build();
    }

    /** One event per SKU, under transaction {@code transactionId} with sequences {@code first, first + 1, ...}. */
    public static List<ScanEvent> events(String transactionId, int first, List<String> skus) {
        List<ScanEvent> events = new ArrayList<>(skus.size());
        for (int i = 0; i < skus.size(); i++) {
            String sku = skus.get(i);
            events.add(event(transactionId, first + i, sku, nameOf(sku)));
        }
        return events;
    }

    /**
     * Delivers {@code skus} in batches of at most 1,000 under fresh identities of transaction
     * {@code transactionId}, starting at sequence 1, and returns the total stored.
     */
    public long deliverAll(String transactionId, List<String> skus) {
        long stored = 0;
        for (int from = 0; from < skus.size(); from += 1_000) {
            List<String> slice = skus.subList(from, Math.min(skus.size(), from + 1_000));
            stored += deliver(events(transactionId, from + 1, slice)).getStored();
        }
        return stored;
    }

    /** The fixture name for {@code sku}: {@code skuN} is "Item N"; any other SKU is "Name of <sku>". */
    public static String nameOf(String sku) {
        return sku.matches("sku\\d+") ? "Item " + sku.substring(3) : "Name of " + sku;
    }

    @Override
    public void close() throws InterruptedException {
        server.close();
    }
}
