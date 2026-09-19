import analytics.AnalyticsRecorder;
import analytics.AnalyticsHandlers;
import catalog.CatalogCache;
import catalog.CatalogHandler;
import checkout.CheckoutHandlers;
import checkout.CheckoutService;
import com.sun.net.httpserver.HttpServer;
import inventory.InventoryHandlers;
import persistence.CatalogDao;
import persistence.ConnectionPool;
import persistence.InventoryDao;
import persistence.PopularWindowDao;
import persistence.TransactionDao;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public final class Main {

    public static void main(String[] args) throws Exception {
        // CLI args take precedence; env vars are the fallback; hardcoded defaults are last.
        int    port       = args.length > 0 ? Integer.parseInt(args[0])  : intEnv("SERVER_PORT", 8080);
        String dbHost     = args.length > 1 ? args[1]                    : env("DB_HOST", "127.0.0.1");
        int    dbPort     = args.length > 2 ? Integer.parseInt(args[2])  : intEnv("DB_PORT", 3307);
        String dbName     = args.length > 3 ? args[3]                    : env("DB_NAME", "cs6510_selfcheckout");
        String dbUser     = args.length > 4 ? args[4]                    : env("DB_USER", "root");
        String dbPassword = args.length > 5 ? args[5]                    : env("DB_PASSWORD", "");
        int    dbPoolSize = intEnv("DB_POOL_SIZE", 10);

        System.out.println("Connecting to MySQL at " + dbHost + ":" + dbPort + "/" + dbName);
        ConnectionPool pool = new ConnectionPool(dbHost, dbPort, dbName, dbUser, dbPassword, dbPoolSize);

        System.out.println("Loading catalog...");
        CatalogDao   catalogDao = new CatalogDao(pool);
        CatalogCache catalog    = CatalogCache.load(catalogDao);
        System.out.println("Catalog loaded: " + catalog.size() + " items");

        PopularWindowDao windowDao = new PopularWindowDao(pool);
        AnalyticsRecorder analytics = new AnalyticsRecorder(catalog, windowDao);

        TransactionDao transactionDao = new TransactionDao(pool);
        InventoryDao   inventoryDao   = new InventoryDao(pool);
        CheckoutService checkoutService = new CheckoutService(
            pool, transactionDao, inventoryDao, catalog, analytics);

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        // T017: /items
        server.createContext("/items", new CatalogHandler(catalog));

        // T024/T041: transaction lifecycle and GET /transactions/{id}
        server.createContext("/transactions", new CheckoutHandlers(checkoutService));

        // T032: /inventory/low-stock
        server.createContext("/inventory/low-stock", new InventoryHandlers(inventoryDao));
        // T036: /analytics/popular-items
        server.createContext("/analytics/popular-items", new AnalyticsHandlers(windowDao));

        server.start();
        System.out.println("Server listening on port " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(5);
            analytics.shutdown();
        }));
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return (v != null && !v.isEmpty()) ? v : fallback;
    }

    private static int intEnv(String name, int fallback) {
        String v = System.getenv(name);
        return (v != null && !v.isEmpty()) ? Integer.parseInt(v) : fallback;
    }
}
