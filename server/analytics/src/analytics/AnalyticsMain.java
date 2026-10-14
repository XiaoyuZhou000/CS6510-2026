package analytics;

import api.AnalyticsHttpHandler;
import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.JdbcAcceptedScanReader;
import database.JdbcPopularWindowStore;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/** Independent Analytics Service entry point on its private port. */
public final class AnalyticsMain {
    private AnalyticsMain() { }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : intEnv("ANALYTICS_PORT", 8083);
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(env("DB_HOST", "127.0.0.1"), intEnv("DB_PORT", 3307),
                    env("DB_NAME", "cs6510_selfcheckout"), env("DB_USER", "root"),
                    env("DB_PASSWORD", ""), intEnv("DB_POOL_SIZE", 10));
        } catch (Exception failure) {
            System.err.println("Analytics Service cannot connect to MySQL: " + failure.getMessage());
            throw failure;
        }
        AnalyticsService service = null;
        try {
            JdbcPopularWindowStore windows = new JdbcPopularWindowStore(pool);
            service = new AnalyticsService(windows, new JdbcAcceptedScanReader(pool));
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/analytics/popular-items", new AnalyticsHttpHandler(service));
            AnalyticsService running = service;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop(5);
                running.shutdown();
                pool.close();
            }));
            server.start();
            System.out.println("Analytics Service listening on port " + port);
        } catch (Exception failure) {
            if (service != null) service.shutdown();
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
