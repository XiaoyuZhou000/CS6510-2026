package inventory;

import api.CatalogHttpHandler;
import api.InventoryHttpHandler;
import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.DatabaseConfig;
import database.JdbcCatalogMetaStore;
import database.JdbcCatalogStore;
import database.JdbcCheckoutOperationStore;
import database.InventoryStatusQueries;
import database.JdbcInventoryStore;
import metrics.MetricsReporter;
import metrics.StatusReporter;
import rpc.GrpcServers;
import rpc.InventoryGrpcService;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Executors;

/** Independent Inventory Service entry point: catalog and low-stock HTTP, plus InventoryService over gRPC. */
public final class InventoryMain {
    private InventoryMain() { }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : intEnv("INVENTORY_PORT", 8082);
        int grpcPort = intEnv("INVENTORY_GRPC_PORT", 9092);
        DatabaseConfig config = DatabaseConfig.fromEnvironment("INVENTORY", "cs6510_inventory",
                "cs6510_inventory_svc", 16, 48);
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(config, "inventory");
        } catch (Exception failure) {
            System.err.println("Inventory Service cannot connect to MySQL as " + config + ": " + failure.getMessage());
            throw failure;
        }

        GrpcServers.Running grpc = null;
        try {
            CatalogCache catalog = new CatalogCache(new JdbcCatalogStore(pool), new JdbcCatalogMetaStore(pool));
            StoreQueryService queries = new StoreQueryService(catalog, new JdbcInventoryStore(pool));
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/items", new CatalogHttpHandler(queries));
            server.createContext("/inventory/low-stock", new InventoryHttpHandler(queries));
            CheckoutStockService checkouts = new CheckoutStockService(new JdbcCheckoutOperationStore(pool));
            grpc = GrpcServers.start(grpcPort, new InventoryGrpcService(catalog, checkouts));
            MetricsReporter reporter = MetricsReporter.start("inventory");
            InventoryStatusQueries statusQueries = new InventoryStatusQueries(pool);
            StatusReporter status = StatusReporter.start("inventory", () -> statusQueries.read().line());
            GrpcServers.Running grpcServer = grpc;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop(5);
                grpcServer.stop(Duration.ofSeconds(5));
                status.stop();
                reporter.stop();
                pool.close();
            }));
            server.start();
            System.out.println("Inventory Service listening on port " + port + " (gRPC " + grpcPort + ")");
        } catch (Exception failure) {
            if (grpc != null) grpc.stop(Duration.ofSeconds(1));
            pool.close();
            throw failure;
        }
    }

    private static int intEnv(String key, int fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : Integer.parseInt(value);
    }
}
