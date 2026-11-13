package notification;

import api.NotificationHttpHandler;
import api.OpsHttpHandler;
import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.DatabaseConfig;
import database.JdbcDeadLetterStore;
import database.JdbcNotificationStore;
import database.JdbcSelectionStore;
import database.JdbcSubscriptionStore;
import database.NotificationStatusQueries;
import events.ConsumerLoop;
import events.ParkingPolicy;
import events.TopicNames;
import messaging.CatalogSkuConsumer;
import messaging.ChannelSender;
import messaging.KafkaChannelProvisioner;
import messaging.KafkaClients;
import messaging.KafkaRecordSender;
import messaging.SaleCompletedConsumer;
import metrics.MetricsReporter;
import metrics.StatusReporter;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Independent Notification Service entry point (007): its own pool on {@code cs6510_notification}, its HTTP
 * server on {@code NOTIFICATION_HTTP_PORT} (8084), and its status line. It calls no peer service and never
 * waits for the broker at startup: the channel producer and the admin client are created on first use, and each
 * consumer opens its stream on its own thread.
 *
 * <ul>
 *   <li>The subscription index is rebuilt from the committed subscriptions before anything consumes (N5).</li>
 *   <li>{@link KnownSkus} is filled by replaying the catalog topic from offset 0 (N6).</li>
 *   <li>The {@code notification-sale-completed} group selects recipients per sale, and the dispatcher publishes
 *       the stored rows to each subscriber's channel through the test-fault wrapper (N8, N13).</li>
 * </ul>
 *
 * <p>Shutdown follows 006 (contracts/broker-and-deployment.md "Start, stop, restart"): HTTP, then the consumers
 * (final {@code commitSync}), then the dispatcher (final mark), then the background workers, then the producers,
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
            Clock clock = Clock.systemUTC();
            TopicNames names = settings.topics();
            JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(pool);
            KnownSkus knownSkus = new KnownSkus();
            SubscriptionIndex index = new SubscriptionIndex();
            KafkaChannelProvisioner provisioner = KafkaChannelProvisioner.fromEnvironment(environment);
            Channels channels = new Channels(subscriptions, provisioner);
            SubscriptionService service = new SubscriptionService(subscriptions, index, knownSkus, channels, names, clock);
            service.loadIndex();

            NotificationStatusQueries statusQueries = new NotificationStatusQueries(pool);
            reportParkedAtStart(statusQueries);

            KafkaRecordSender channelSender = ChannelSender.lazy(environment);
            JdbcNotificationStore notifications = new JdbcNotificationStore(pool);
            NotificationDispatcher dispatcher = new NotificationDispatcher(notifications, channels,
                    new ChannelFaults(channelSender), new ChargingTracker(), NotificationDispatcher.Settings.of(settings), clock);
            JdbcDeadLetterStore deadLetters = new JdbcDeadLetterStore(pool, "notification.deadLetter");
            ConsumerLoop.Settings consumerSettings = ConsumerLoop.Settings.defaults()
                    .withPolicy(ParkingPolicy.fromEnvironment(environment));
            ConsumerLoop catalogConsumer = CatalogSkuConsumer.create(names, CatalogSkuConsumer.kafkaStreams(environment),
                    knownSkus, deadLetters, consumerSettings);
            SaleSelectionHandler selection = new SaleSelectionHandler(new JdbcSelectionStore(pool, clock), index,
                    dispatcher::accept);
            ConsumerLoop sales = SaleCompletedConsumer.create(names, SaleCompletedConsumer.kafkaStreams(environment, names),
                    selection, deadLetters, consumerSettings.withThreads(settings.saleConsumerThreads()));

            HttpServer server = HttpServer.create(new InetSocketAddress(settings.httpPort()), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            NotificationStatus statusView = new NotificationStatus(statusQueries, index, knownSkus, dispatcher, () -> sales);
            ParkedOperations parked = new ParkedOperations(notifications, statusQueries, dispatcher, clock);
            server.createContext(NotificationHttpHandler.CONTEXT, new NotificationHttpHandler(service));
            server.createContext(OpsHttpHandler.CONTEXT, new OpsHttpHandler(statusView::read, parked, settings.testFaults()));
            MetricsReporter reporter = MetricsReporter.start("notification");
            StatusReporter status = StatusReporter.start("notification", () -> statusView.read().line());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop(5);
                sales.stop(Duration.ofSeconds(5));
                catalogConsumer.stop(Duration.ofSeconds(5));
                dispatcher.stop(Duration.ofSeconds(5));
                status.stop();
                reporter.stop();
                channelSender.close();
                provisioner.close();
                pool.close();
            }));
            dispatcher.start();
            catalogConsumer.start();
            sales.start();
            server.start();
            System.out.println("Notification Service listening on port " + settings.httpPort() + " (broker "
                    + KafkaClients.bootstrap(environment) + ", topic prefix '" + names.prefix() + "', "
                    + settings.database() + ")");
        } catch (Exception failure) {
            pool.close();
            throw failure;
        }
    }

    /**
     * T102: what an earlier run parked is still parked, so its alarm is raised again at start from the database
     * counts (the status line and {@code ops/status} keep it until the rows are retried and delivered).
     */
    private static void reportParkedAtStart(NotificationStatusQueries queries) {
        try {
            NotificationStatusQueries.Status durable = queries.read();
            if (durable.parked() > 0 || durable.parkedEvents() > 0) {
                System.err.println("[notification] ALARM at start: " + durable.parked() + " notifications and "
                        + durable.parkedEvents() + " records are parked; list them with GET /notifications/ops/parked "
                        + "and retry notifications with POST /notifications/ops/parked/retry");
            }
        } catch (RuntimeException failure) {
            System.err.println("[notification] Could not read the parked counts at start: " + failure);
        }
    }
}
