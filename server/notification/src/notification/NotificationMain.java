package notification;

import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.DatabaseConfig;
import database.NotificationStatusQueries;
import messaging.KafkaClients;
import metrics.MetricsReporter;
import metrics.StatusReporter;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Independent Notification Service entry point (007): its own pool on {@code cs6510_notification}, its HTTP
 * server on {@code NOTIFICATION_HTTP_PORT} (8084), and its status line. It calls no peer service and never
 * waits for the broker at startup: every producer it will own is created on its first send.
 *
 * <p>This is the skeleton of phase 2. The subscription routes, the {@code SaleCompleted} and catalog
 * consumers, the delivery dispatcher, and its workers are added by the user stories, each into the shutdown
 * order of 006 (contracts/broker-and-deployment.md "Start, stop, restart"): HTTP, then the consumers (final
 * {@code commitSync}), then the dispatcher (final mark), then the background workers, then the producers,
 * then the pool.</p>
 */
public final class NotificationMain {
    private NotificationMain() { }

    public static void main(String[] args) throws Exception {
        Map<String, String> environment = System.getenv();
        NotificationConfig settings = NotificationConfig.fromEnvironment(environment);
        if (args.length > 0) settings = settings.withHttpPort(Integer.parseInt(args[0]));
        DatabaseConfig config = settings.database();
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(config, "notification");
        } catch (Exception failure) {
            System.err.println("Notification Service cannot connect to MySQL as " + config + ": " + failure.getMessage());
            throw failure;
        }
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(settings.httpPort()), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            MetricsReporter reporter = MetricsReporter.start("notification");
            NotificationStatusQueries statusQueries = new NotificationStatusQueries(pool);
            StatusReporter status = StatusReporter.start("notification", () -> statusQueries.read().line());
            // HTTP, then (once the stories add them) the consumers, the dispatcher, the workers, and the
            // producers, then the pool.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop(5);
                status.stop();
                reporter.stop();
                pool.close();
            }));
            server.start();
            System.out.println("Notification Service listening on port " + settings.httpPort() + " (broker "
                    + KafkaClients.bootstrap(environment) + ", topic prefix '" + settings.topics().prefix() + "', "
                    + settings.database() + ")");
        } catch (Exception failure) {
            pool.close();
            throw failure;
        }
    }
}
