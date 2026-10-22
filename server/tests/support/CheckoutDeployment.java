package support;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An Inventory process and a Transaction process, each over its own disposable schema and user (R3),
 * started from the built artifacts for process-level Saga tests. Transaction's product replica is seeded
 * with Inventory's catalog and version, so no refresh is needed before the first Scan. Either service can
 * be started with fault hooks armed (see {@link FaultInjection}) and restarted without them.
 *
 * <p>{@link #withAnalytics} adds an Analytics schema and ports; the Analytics process starts only on
 * {@link #startAnalytics}, so a test can scan while it is down. Transaction's relay always targets this
 * deployment's own Analytics port, which nobody serves until then.</p>
 */
public final class CheckoutDeployment implements AutoCloseable {
    public final TransactionTestSchema transactionSchema;
    public final InventoryTestSchema inventorySchema;
    /** Null unless the deployment was started {@link #withAnalytics}. */
    public final AnalyticsTestSchema analyticsSchema;
    public final int inventoryHttpPort;
    public final int inventoryGrpcPort;
    public final int transactionHttpPort;
    public final int analyticsHttpPort;
    public final int analyticsGrpcPort;
    public final ServiceHttp transactionHttp;
    public final ServiceHttp inventoryHttp;
    public final ServiceHttp analyticsHttp;
    private final Map<String, String> transactionEnvironment;
    private final List<ServiceProcesses.ServiceProcess> extraTransactions = new ArrayList<>();
    private ServiceProcesses.ServiceProcess inventory;
    private ServiceProcesses.ServiceProcess transaction;
    private ServiceProcesses.ServiceProcess analytics;
    private ManagedChannel channel;

    private CheckoutDeployment(int products, int stock, Map<String, String> transactionEnvironment,
                               String transactionFaults, String inventoryFaults, boolean withAnalytics) throws Exception {
        this.transactionEnvironment = new LinkedHashMap<>(transactionEnvironment);
        transactionSchema = TransactionTestSchema.create(TestSchema.Options.defaults().withOwnUser());
        InventoryTestSchema inventories = null;
        AnalyticsTestSchema analyticsSchemaCreated = null;
        try {
            inventories = InventoryTestSchema.create(TestSchema.Options.defaults().withOwnUser());
            if (withAnalytics) analyticsSchemaCreated = AnalyticsTestSchema.create(TestSchema.Options.defaults().withOwnUser());
        } catch (Exception failure) {
            transactionSchema.close();
            if (inventories != null) inventories.close();
            throw failure;
        }
        inventorySchema = inventories;
        analyticsSchema = analyticsSchemaCreated;
        inventoryHttpPort = GrpcTestSupport.freePort();
        inventoryGrpcPort = GrpcTestSupport.freePort();
        transactionHttpPort = GrpcTestSupport.freePort();
        analyticsHttpPort = GrpcTestSupport.freePort();
        analyticsGrpcPort = GrpcTestSupport.freePort();
        transactionHttp = new ServiceHttp(transactionHttpPort);
        inventoryHttp = new ServiceHttp(inventoryHttpPort);
        analyticsHttp = new ServiceHttp(analyticsHttpPort);
        try {
            transactionSchema.seedReplica(products, inventorySchema.seed(products, stock));
            startInventory(inventoryFaults);
            startTransaction(transactionFaults);
        } catch (Exception | Error failure) {
            close();
            throw failure;
        }
    }

    public static CheckoutDeployment start(int products, int stock) throws Exception {
        return new CheckoutDeployment(products, stock, Map.of(), null, null, false);
    }

    /** With extra Transaction variables, for example {@code CATALOG_REFRESH_SECONDS}. */
    public static CheckoutDeployment start(int products, int stock, Map<String, String> transactionEnvironment)
            throws Exception {
        return new CheckoutDeployment(products, stock, transactionEnvironment, null, null, false);
    }

    /** With fault specifications armed in the first Transaction and Inventory processes; null arms none. */
    public static CheckoutDeployment start(int products, int stock, Map<String, String> transactionEnvironment,
                                           String transactionFaults, String inventoryFaults) throws Exception {
        return new CheckoutDeployment(products, stock, transactionEnvironment, transactionFaults, inventoryFaults, false);
    }

    /**
     * Inventory and Transaction as {@link #start}, plus an Analytics schema whose process starts only on
     * {@link #startAnalytics}. {@code transactionFaults} is armed in the first Transaction process.
     */
    public static CheckoutDeployment withAnalytics(int products, int stock, String transactionFaults) throws Exception {
        return new CheckoutDeployment(products, stock, Map.of(), transactionFaults, null, true);
    }

    public void startInventory() throws Exception {
        startInventory(null);
    }

    /** Starts (or restarts) Inventory, with {@code faults} armed when it is not null. */
    public void startInventory(String faults) throws Exception {
        closeQuietly(inventory);
        inventory = ServiceProcesses.service("inventory").database(inventorySchema).httpPort(inventoryHttpPort)
                .grpcPort(inventoryGrpcPort).faults(faults).start();
        inventory.awaitHttp("/items", 200, Duration.ofSeconds(60));
        inventory.awaitPort(inventoryGrpcPort, Duration.ofSeconds(30));
    }

    public void startTransaction() throws Exception {
        startTransaction(null);
    }

    /** Starts (or restarts) Transaction, with {@code faults} armed when it is not null. */
    public void startTransaction(String faults) throws Exception {
        closeQuietly(transaction);
        transaction = transactionBuilder(transactionHttpPort).faults(faults).start();
        transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(60));
    }

    /**
     * Starts one more Transaction process over the same Transaction schema on {@code httpPort}, with its own
     * relay; it is stopped with the deployment.
     */
    public ServiceProcesses.ServiceProcess startExtraTransaction(int httpPort) throws Exception {
        ServiceProcesses.ServiceProcess extra = transactionBuilder(httpPort).start();
        extraTransactions.add(extra);
        extra.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(60));
        return extra;
    }

    private ServiceProcesses.Builder transactionBuilder(int httpPort) {
        ServiceProcesses.Builder builder = ServiceProcesses.service("transaction").database(transactionSchema)
                .httpPort(httpPort).grpcTarget("inventory", inventoryGrpcPort).grpcTarget("analytics", analyticsGrpcPort);
        transactionEnvironment.forEach(builder::env);
        return builder;
    }

    public void startAnalytics() throws Exception {
        startAnalytics(null);
    }

    /** Starts (or restarts) Analytics with metrics printed every second, and {@code faults} armed when not null. */
    public void startAnalytics(String faults) throws Exception {
        if (analyticsSchema == null) throw new IllegalStateException("Start the deployment withAnalytics");
        closeQuietly(analytics);
        analytics = ServiceProcesses.service("analytics").database(analyticsSchema).httpPort(analyticsHttpPort)
                .grpcPort(analyticsGrpcPort).perfMetrics(true).env("PERF_METRICS_INTERVAL_SECONDS", "1")
                .faults(faults).start();
        analytics.awaitHttp("/analytics/popular-items", 200, Duration.ofSeconds(60));
        analytics.awaitPort(analyticsGrpcPort, Duration.ofSeconds(30));
    }

    /** Kills Inventory without shutdown hooks, like a crash. */
    public void killInventory() throws InterruptedException {
        inventory.kill();
    }

    /** Stops a previous process of the same service, halted or not, before its replacement starts. */
    private static void closeQuietly(ServiceProcesses.ServiceProcess process) throws InterruptedException {
        if (process != null) process.close();
    }

    /** Kills Transaction without shutdown hooks, like a crash. */
    public void killTransaction() throws InterruptedException {
        transaction.kill();
    }

    public void stopInventory() throws InterruptedException {
        inventory.stop();
    }

    public void stopTransaction() throws InterruptedException {
        transaction.stop();
    }

    public void stopAnalytics() throws InterruptedException {
        if (analytics != null) analytics.stop();
    }

    /** Kills Analytics without shutdown hooks, like a crash. */
    public void killAnalytics() throws InterruptedException {
        if (analytics != null) analytics.kill();
    }

    public ServiceProcesses.ServiceProcess transactionProcess() {
        return transaction;
    }

    public ServiceProcesses.ServiceProcess inventoryProcess() {
        return inventory;
    }

    public ServiceProcesses.ServiceProcess analyticsProcess() {
        return analytics;
    }

    /** A channel to Inventory's gRPC port, for diagnostics such as GetCheckoutOutcome. */
    public synchronized ManagedChannel inventoryChannel() {
        if (channel == null) {
            channel = NettyChannelBuilder.forAddress("127.0.0.1", inventoryGrpcPort).usePlaintext().build();
        }
        return channel;
    }

    /**
     * Waits until Transaction's outbox is empty and Analytics stores exactly the identities Transaction
     * accepted, then asserts that positions are contiguous from 1.
     */
    public void awaitAnalyticsHoldsEveryScan(Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (transactionSchema.undeliveredCount() == 0
                    && analyticsSchema.storedEvents() == transactionSchema.scanCount()) break;
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Analytics did not catch up: undelivered=" + transactionSchema.undeliveredCount()
                        + " transaction=" + transactionSchema.scanCount() + " analytics=" + analyticsSchema.storedEvents()
                        + System.lineSeparator() + logs());
            }
            Thread.sleep(100);
        }
        org.junit.jupiter.api.Assertions.assertEquals(transactionSchema.identities(), analyticsSchema.identities(),
                "Analytics must store exactly the accepted scans, each once");
        analyticsSchema.assertPositionsContiguous();
    }

    /** Every service's output, for assertion messages. */
    public String logs() {
        StringBuilder logs = new StringBuilder();
        logs.append("--- transaction ---").append(System.lineSeparator()).append(transaction == null ? "" : transaction.output());
        for (ServiceProcesses.ServiceProcess extra : extraTransactions) {
            logs.append(System.lineSeparator()).append("--- extra transaction ---").append(System.lineSeparator())
                    .append(extra.output());
        }
        logs.append(System.lineSeparator()).append("--- inventory ---").append(System.lineSeparator())
                .append(inventory == null ? "" : inventory.output());
        if (analytics != null) {
            logs.append(System.lineSeparator()).append("--- analytics ---").append(System.lineSeparator())
                    .append(analytics.output());
        }
        return logs.toString();
    }

    @Override
    public void close() throws Exception {
        try (transactionSchema; inventorySchema; AutoCloseable analyticsData = analyticsSchema) {
            if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            for (ServiceProcesses.ServiceProcess extra : extraTransactions) extra.close();
            if (transaction != null) transaction.close();
            if (analytics != null) analytics.close();
            if (inventory != null) inventory.close();
        }
    }
}
