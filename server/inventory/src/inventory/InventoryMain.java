package inventory;

import api.CatalogHttpHandler;
import api.InventoryHttpHandler;
import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.DatabaseConfig;
import database.InventoryStatusQueries;
import database.JdbcCatalogMetaStore;
import database.JdbcCatalogStore;
import database.JdbcCheckoutOperationStore;
import database.JdbcDeadLetterStore;
import database.JdbcInventoryStore;
import database.JdbcOutboxStore;
import events.ConsumerLoop;
import events.OutboxDispatcher;
import events.ParkingPolicy;
import events.TopicNames;
import messaging.CheckoutEventConsumer;
import messaging.KafkaClients;
import messaging.KafkaRecordSender;
import metrics.MetricsReporter;
import metrics.StatusReporter;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Independent Inventory Service entry point: catalog and low-stock HTTP; the catalog's publication through the
 * outbox at every startup (D9); and the checkout Saga's Inventory side over events (the
 * {@code inventory-checkout} consumer and the outcome outbox, D8). Inventory runs no gRPC server. The broker is
 * never awaited at startup: the dispatcher and consumer retry on their own.
 */
public final class InventoryMain {
    private InventoryMain() { }

    public static void main(String[] args) throws Exception {
        Map<String, String> environment = System.getenv();
        int port = args.length > 0 ? Integer.parseInt(args[0]) : intEnv("INVENTORY_PORT", 8082);
        DatabaseConfig config = DatabaseConfig.fromEnvironment("INVENTORY", "cs6510_inventory",
                "cs6510_inventory_svc", 16, 48);
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(config, "inventory");
        } catch (Exception failure) {
            System.err.println("Inventory Service cannot connect to MySQL as " + config + ": " + failure.getMessage());
            throw failure;
        }

        KafkaRecordSender outcomeSender = null;
        KafkaRecordSender catalogSender = null;
        try {
            JdbcCatalogMetaStore catalogMeta = new JdbcCatalogMetaStore(pool);
            CatalogCache catalog = new CatalogCache(new JdbcCatalogStore(pool), catalogMeta);
            StoreQueryService queries = new StoreQueryService(catalog, new JdbcInventoryStore(pool));
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/items", new CatalogHttpHandler(queries));
            server.createContext("/inventory/low-stock", new InventoryHttpHandler(queries));

            TopicNames names = TopicNames.fromEnvironment(environment);
            outcomeSender = KafkaClients.sender(environment, "inventory-outcome", KafkaClients.IMMEDIATE_LINGER_MS);
            catalogSender = KafkaClients.sender(environment, "inventory-catalog", KafkaClients.batchLingerMs(environment));
            OutboxDispatcher dispatcher = new OutboxDispatcher("inventory", new JdbcOutboxStore(pool, "inventory.outbox"),
                    Map.of(names.checkoutOutcome(), outcomeSender, names.catalog(), catalogSender),
                    List.of(names.checkoutOutcome(), names.catalog()),
                    OutboxDispatcher.Settings.fromEnvironment(environment), Clock.systemUTC());
            CatalogPublisher publisher = new CatalogPublisher(catalog, catalogMeta, names, dispatcher, Clock.systemUTC());
            CheckoutStockService checkouts = new CheckoutStockService(new JdbcCheckoutOperationStore(pool),
                    new StockOutcomeEvents(names, Clock.systemUTC()), dispatcher);
            ConsumerLoop checkoutConsumer = CheckoutEventConsumer.create(names,
                    CheckoutEventConsumer.kafkaStreams(environment, names), new CheckoutRequestHandler(checkouts),
                    new StockRestorationHandler(checkouts), new JdbcDeadLetterStore(pool, "inventory.deadLetter"),
                    ConsumerLoop.Settings.defaults().withThreads(CheckoutEventConsumer.threads(environment))
                            .withPolicy(ParkingPolicy.fromEnvironment(environment)));

            MetricsReporter reporter = MetricsReporter.start("inventory");
            InventoryStatusQueries statusQueries = new InventoryStatusQueries(pool);
            StatusReporter status = StatusReporter.start("inventory",
                    () -> statusQueries.read().line() + " checkout_consumer_lag=" + checkoutConsumer.lag());
            KafkaRecordSender outcomes = outcomeSender;
            KafkaRecordSender catalogRecords = catalogSender;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop(5);
                checkoutConsumer.stop(Duration.ofSeconds(5));
                dispatcher.stop(Duration.ofSeconds(5));
                outcomes.close();
                catalogRecords.close();
                status.stop();
                reporter.stop();
                pool.close();
            }));
            dispatcher.start();
            publisher.publish();
            checkoutConsumer.start();
            server.start();
            System.out.println("Inventory Service listening on port " + port + " (broker "
                    + KafkaClients.bootstrap(environment) + ")");
        } catch (Exception failure) {
            if (outcomeSender != null) outcomeSender.close();
            if (catalogSender != null) catalogSender.close();
            pool.close();
            throw failure;
        }
    }

    private static int intEnv(String key, int fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : Integer.parseInt(value);
    }
}
