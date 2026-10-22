package transaction;

import api.TransactionHttpHandler;
import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.DatabaseConfig;
import database.JdbcCheckoutAttemptStore;
import database.JdbcProductReplicaStore;
import database.JdbcScanStore;
import database.JdbcTransactionStore;
import metrics.MetricsReporter;
import rpc.GrpcChannels;
import rpc.GrpcInventoryGateway;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/** Independent Transaction Service entry point. Transaction runs no gRPC server; it is a client only. */
public final class TransactionMain {
    private TransactionMain() { }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : intEnv("TRANSACTION_PORT", 8081);
        DatabaseConfig config = DatabaseConfig.fromEnvironment("TRANSACTION", "cs6510_transaction",
            "cs6510_transaction_svc", 32, 64);
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(config, "transaction");
        } catch (Exception failure) {
            System.err.println("Transaction Service cannot connect to MySQL as " + config + ": " + failure.getMessage());
            throw failure;
        }
        ProductCatalogReplica replica = new ProductCatalogReplica();
        GrpcInventoryGateway inventory = GrpcInventoryGateway.forTarget(env("INVENTORY_GRPC_TARGET", "127.0.0.1:9092"));
        CatalogReplicaRefresher refresher = new CatalogReplicaRefresher(inventory,
            new JdbcProductReplicaStore(pool), replica, CatalogReplicaRefresher.refreshInterval(System.getenv()));
        TransactionLocks locks = new TransactionLocks();
        TransactionStateCache cache = new TransactionStateCache(locks, TransactionStateCache.maxEntries(System.getenv()));
        JdbcTransactionStore transactions = new JdbcTransactionStore(pool);
        CheckoutOrchestrator checkout = new CheckoutOrchestrator(transactions, new JdbcCheckoutAttemptStore(pool),
            inventory, cache, new SettlementRegistry(locks), new RecoverySchedule(),
            CheckoutOrchestrator.Settings.fromEnvironment(System.getenv()));
        var service = new TransactionService(transactions, new JdbcScanStore(pool), checkout, replica, locks, cache);
        HttpServer server = HttpServer.create(new InetSocketAddress(port),0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/transactions",new TransactionHttpHandler(service));
        refresher.start();
        MetricsReporter reporter = MetricsReporter.start("transaction");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(5);
            refresher.stop();
            GrpcChannels.shutdownAll();
            reporter.stop();
            pool.close();
        }));
        server.start();
        System.out.println("Transaction Service listening on port " + port);
    }

    private static String env(String key,String fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : value;
    }
    private static int intEnv(String key,int fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : Integer.parseInt(value);
    }
}
