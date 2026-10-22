package support;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An Inventory process and a Transaction process, each over its own disposable schema and user (R3),
 * started from the built artifacts for process-level Saga tests. Transaction's product replica is seeded
 * with Inventory's catalog and version, so no refresh is needed before the first Scan.
 */
public final class CheckoutDeployment implements AutoCloseable {
    public final TransactionTestSchema transactionSchema;
    public final InventoryTestSchema inventorySchema;
    public final int inventoryHttpPort;
    public final int inventoryGrpcPort;
    public final int transactionHttpPort;
    public final ServiceHttp transactionHttp;
    public final ServiceHttp inventoryHttp;
    private final Map<String, String> transactionEnvironment;
    private ServiceProcesses.ServiceProcess inventory;
    private ServiceProcesses.ServiceProcess transaction;
    private ManagedChannel channel;

    private CheckoutDeployment(int products, int stock, Map<String, String> transactionEnvironment) throws Exception {
        this.transactionEnvironment = new LinkedHashMap<>(transactionEnvironment);
        transactionSchema = TransactionTestSchema.create(TestSchema.Options.defaults().withOwnUser());
        InventoryTestSchema inventories;
        try {
            inventories = InventoryTestSchema.create(TestSchema.Options.defaults().withOwnUser());
        } catch (Exception failure) {
            transactionSchema.close();
            throw failure;
        }
        inventorySchema = inventories;
        inventoryHttpPort = GrpcTestSupport.freePort();
        inventoryGrpcPort = GrpcTestSupport.freePort();
        transactionHttpPort = GrpcTestSupport.freePort();
        transactionHttp = new ServiceHttp(transactionHttpPort);
        inventoryHttp = new ServiceHttp(inventoryHttpPort);
        try {
            transactionSchema.seedReplica(products, inventorySchema.seed(products, stock));
            startInventory();
            startTransaction();
        } catch (Exception | Error failure) {
            close();
            throw failure;
        }
    }

    public static CheckoutDeployment start(int products, int stock) throws Exception {
        return new CheckoutDeployment(products, stock, Map.of());
    }

    /** With extra Transaction variables, for example {@code CATALOG_REFRESH_SECONDS}. */
    public static CheckoutDeployment start(int products, int stock, Map<String, String> transactionEnvironment)
            throws Exception {
        return new CheckoutDeployment(products, stock, transactionEnvironment);
    }

    public void startInventory() throws Exception {
        inventory = ServiceProcesses.service("inventory").database(inventorySchema).httpPort(inventoryHttpPort)
                .grpcPort(inventoryGrpcPort).start();
        inventory.awaitHttp("/items", 200, Duration.ofSeconds(60));
        inventory.awaitPort(inventoryGrpcPort, Duration.ofSeconds(30));
    }

    public void startTransaction() throws Exception {
        ServiceProcesses.Builder builder = ServiceProcesses.service("transaction").database(transactionSchema)
                .httpPort(transactionHttpPort).grpcTarget("inventory", inventoryGrpcPort);
        transactionEnvironment.forEach(builder::env);
        transaction = builder.start();
        transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(60));
    }

    /** Kills Transaction without shutdown hooks, like a crash. */
    public void killTransaction() throws InterruptedException {
        transaction.kill();
    }

    public void stopInventory() throws InterruptedException {
        inventory.stop();
    }

    public ServiceProcesses.ServiceProcess transactionProcess() {
        return transaction;
    }

    public ServiceProcesses.ServiceProcess inventoryProcess() {
        return inventory;
    }

    /** A channel to Inventory's gRPC port, for diagnostics such as GetCheckoutOutcome. */
    public synchronized ManagedChannel inventoryChannel() {
        if (channel == null) {
            channel = NettyChannelBuilder.forAddress("127.0.0.1", inventoryGrpcPort).usePlaintext().build();
        }
        return channel;
    }

    /** Both services' output, for assertion messages. */
    public String logs() {
        return "--- transaction ---" + System.lineSeparator() + (transaction == null ? "" : transaction.output())
                + System.lineSeparator() + "--- inventory ---" + System.lineSeparator()
                + (inventory == null ? "" : inventory.output());
    }

    @Override
    public void close() throws Exception {
        try (transactionSchema; inventorySchema) {
            if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            if (transaction != null) transaction.close();
            if (inventory != null) inventory.close();
        }
    }
}
