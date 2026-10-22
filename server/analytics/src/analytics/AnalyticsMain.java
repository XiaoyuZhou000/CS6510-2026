package analytics;

import api.AnalyticsHttpHandler;
import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.DatabaseConfig;
import database.JdbcAcceptedScanReader;
import database.JdbcPopularWindowStore;
import database.JdbcScanIngestStore;
import metrics.MetricsReporter;
import rpc.AnalyticsIngestGrpcService;
import rpc.GrpcServers;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Executors;

/**
 * Independent Analytics Service entry point: popular items over HTTP on its private port, and
 * AnalyticsIngestService over gRPC for Transaction's outbox relay. The pipeline is prewarmed and polling
 * before the gRPC server accepts deliveries.
 */
public final class AnalyticsMain {
    private AnalyticsMain() { }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : intEnv("ANALYTICS_PORT", 8083);
        int grpcPort = intEnv("ANALYTICS_GRPC_PORT", 9093);
        DatabaseConfig config = DatabaseConfig.fromEnvironment("ANALYTICS", "cs6510_analytics",
                "cs6510_analytics_svc", 10, 10);
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(config, "analytics");
        } catch (Exception failure) {
            System.err.println("Analytics Service cannot connect to MySQL as " + config + ": " + failure.getMessage());
            throw failure;
        }
        AnalyticsService service = null;
        GrpcServers.Running grpc = null;
        try {
            ProductNameMap names = new ProductNameMap();
            JdbcScanIngestStore ingest = new JdbcScanIngestStore(pool, names);
            names.loadFrom(ingest);
            JdbcPopularWindowStore windows = new JdbcPopularWindowStore(pool);
            service = new AnalyticsService(windows, new JdbcAcceptedScanReader(pool));
            grpc = GrpcServers.start(grpcPort, new AnalyticsIngestGrpcService(new ScanIngestionService(ingest)));
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/analytics/popular-items", new AnalyticsHttpHandler(service));
            MetricsReporter reporter = MetricsReporter.start("analytics");
            AnalyticsService running = service;
            GrpcServers.Running grpcServer = grpc;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop(5);
                grpcServer.stop(Duration.ofSeconds(5));
                running.shutdown();
                reporter.stop();
                pool.close();
            }));
            server.start();
            System.out.println("Analytics Service listening on port " + port + " (gRPC " + grpcPort + ", "
                    + names.size() + " product names loaded)");
        } catch (Exception failure) {
            if (grpc != null) grpc.stop(Duration.ofSeconds(1));
            if (service != null) service.shutdown();
            pool.close();
            throw failure;
        }
    }

    private static int intEnv(String key, int fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : Integer.parseInt(value);
    }
}
