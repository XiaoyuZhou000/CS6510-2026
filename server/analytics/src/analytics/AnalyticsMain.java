package analytics;

import api.AnalyticsHttpHandler;
import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.DatabaseConfig;
import database.AnalyticsStatusQueries;
import database.JdbcAcceptedScanReader;
import database.JdbcDeadLetterStore;
import database.JdbcPopularWindowStore;
import database.JdbcScanIngestStore;
import events.ConsumerLoop;
import events.ParkingPolicy;
import events.TopicNames;
import messaging.KafkaClients;
import messaging.ScanAcceptedConsumer;
import metrics.MetricsReporter;
import metrics.StatusReporter;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Independent Analytics Service entry point: popular items over HTTP on its private port, and the
 * {@code analytics-scan-ingest} consumer of Transaction's {@code ScanAccepted} events (D10). Analytics serves
 * no gRPC and calls no peer. The pipeline is prewarmed and polling before the consumer starts, and the broker
 * is never awaited at startup.
 */
public final class AnalyticsMain {
    private AnalyticsMain() { }

    public static void main(String[] args) throws Exception {
        Map<String, String> environment = System.getenv();
        int port = args.length > 0 ? Integer.parseInt(args[0]) : intEnv("ANALYTICS_PORT", 8083);
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
        try {
            ProductNameMap names = new ProductNameMap();
            JdbcScanIngestStore ingest = new JdbcScanIngestStore(pool, names);
            names.loadFrom(ingest);
            JdbcPopularWindowStore windows = new JdbcPopularWindowStore(pool);
            service = new AnalyticsService(windows, new JdbcAcceptedScanReader(pool));
            TopicNames topics = TopicNames.fromEnvironment(environment);
            ConsumerLoop scans = ScanAcceptedConsumer.create(topics, ScanAcceptedConsumer.kafkaStreams(environment, topics),
                    new ScanIngestionService(ingest), new JdbcDeadLetterStore(pool, "analytics.deadLetter"),
                    ConsumerLoop.Settings.defaults().withPolicy(ParkingPolicy.fromEnvironment(environment)));
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/analytics/popular-items", new AnalyticsHttpHandler(service));
            MetricsReporter reporter = MetricsReporter.start("analytics");
            AnalyticsStatusQueries statusQueries = new AnalyticsStatusQueries(pool);
            StatusReporter status = StatusReporter.start("analytics",
                    () -> statusQueries.read().line() + " scan_consumer_lag=" + scans.lag());
            AnalyticsService running = service;
            // contracts/broker-and-deployment.md "Start, stop, restart": HTTP, then the consumer (final commitSync),
            // then the pipeline and the other background workers, then the pool; Analytics has no producer.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop(5);
                scans.stop(Duration.ofSeconds(5));
                running.shutdown();
                status.stop();
                reporter.stop();
                pool.close();
            }));
            server.start();
            scans.start();
            System.out.println("Analytics Service listening on port " + port + " (broker "
                    + KafkaClients.bootstrap(environment) + ", " + names.size() + " product names loaded)");
        } catch (Exception failure) {
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
