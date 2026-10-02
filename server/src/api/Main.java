package api;

import analytics.AnalyticsOperations;
import analytics.AnalyticsService;
import com.sun.net.httpserver.HttpServer;
import database.CatalogStore;
import database.ConnectionPool;
import database.JdbcCatalogStore;
import database.JdbcCheckoutCompletionStore;
import database.JdbcInventoryStore;
import database.JdbcPopularWindowStore;
import database.JdbcTransactionStore;
import transaction.CatalogOperations;
import transaction.CatalogCache;
import transaction.InventoryOperations;
import transaction.StoreQueryService;
import transaction.TransactionOperations;
import transaction.TransactionService;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.Executors;

/** Construction-only composition root for the four server layers. */
public final class Main {
    private Main() { }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : intEnv("SERVER_PORT", 8080);
        String host = args.length > 1 ? args[1] : env("DB_HOST", "127.0.0.1");
        int databasePort = args.length > 2 ? Integer.parseInt(args[2]) : intEnv("DB_PORT", 3307);
        String database = args.length > 3 ? args[3] : env("DB_NAME", "cs6510_selfcheckout");
        String user = args.length > 4 ? args[4] : env("DB_USER", "root");
        String password = args.length > 5 ? args[5] : env("DB_PASSWORD", "");
        long analyticsShutdownTimeoutMillis = longEnv(
                "ANALYTICS_SHUTDOWN_TIMEOUT_MS", 5_000L);

        ConnectionPool pool = new ConnectionPool(host, databasePort, database, user, password,
                intEnv("DB_POOL_SIZE", 10));
        CatalogStore catalogStore = new JdbcCatalogStore(pool);
        CatalogCache catalog = CatalogCache.load(catalogStore);
        StoreQueryService storeQueries = new StoreQueryService(catalog, new JdbcInventoryStore(pool));
        CatalogOperations catalogOperations = storeQueries;
        InventoryOperations inventoryOperations = storeQueries;
        AnalyticsOperations analytics = new AnalyticsService(
                new JdbcPopularWindowStore(pool), analyticsShutdownTimeoutMillis);
        TransactionOperations transactions = new TransactionService(
                new JdbcTransactionStore(pool), new JdbcCheckoutCompletionStore(pool),
                catalog, analytics::recordAcceptedScan);

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        registerRoutes(server, transactions, catalogOperations, inventoryOperations, analytics);
        server.start();
        System.out.println("Layered self-checkout server listening on port " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(5);
            analytics.shutdown();
        }));
    }

    public static void registerRoutes(
            HttpServer server,
            TransactionOperations transactions,
            CatalogOperations catalog,
            InventoryOperations inventory,
            AnalyticsOperations analytics) {
        Objects.requireNonNull(server, "server");
        server.createContext("/transactions", new TransactionHttpHandler(transactions));
        server.createContext("/items", new CatalogHttpHandler(catalog));
        server.createContext("/inventory/low-stock", new InventoryHttpHandler(inventory));
        server.createContext("/analytics/popular-items", new AnalyticsHttpHandler(analytics));
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static int intEnv(String name, int fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : Integer.parseInt(value);
    }

    private static long longEnv(String name, long fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : positiveLong(value, name);
    }

    private static long positiveLong(String value, String name) {
        long parsed = Long.parseLong(value);
        if (parsed <= 0) throw new IllegalArgumentException(name + " must be positive");
        return parsed;
    }
}
