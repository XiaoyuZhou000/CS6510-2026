package inventory;

import api.CatalogHttpHandler;
import api.InventoryHttpHandler;
import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.JdbcCatalogStore;
import database.JdbcInventoryStore;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/** Independent Inventory Service entry point for catalog and low-stock queries. */
public final class InventoryMain {
    private InventoryMain() { }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : intEnv("INVENTORY_PORT", 8082);
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(env("DB_HOST", "127.0.0.1"), intEnv("DB_PORT", 3307),
                    env("DB_NAME", "cs6510_selfcheckout"), env("DB_USER", "root"),
                    env("DB_PASSWORD", ""), intEnv("DB_POOL_SIZE", 10));
        } catch (Exception failure) {
            System.err.println("Inventory Service cannot connect to MySQL: " + failure.getMessage());
            throw failure;
        }

        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            StoreQueryService queries = new StoreQueryService(
                    new JdbcCatalogStore(pool), new JdbcInventoryStore(pool));
            server.createContext("/items", new CatalogHttpHandler(queries));
            server.createContext("/inventory/low-stock", new InventoryHttpHandler(queries));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop(5);
                pool.close();
            }));
            server.start();
            System.out.println("Inventory Service listening on port " + port);
        } catch (Exception failure) {
            pool.close();
            throw failure;
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static int intEnv(String key, int fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : Integer.parseInt(value);
    }
}
