package transaction;

import api.TransactionHttpHandler;
import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.DatabaseConfig;
import database.JdbcCheckoutAttemptStore;
import database.JdbcDeadLetterStore;
import database.JdbcOutboxStore;
import database.JdbcProductReplicaStore;
import database.JdbcRelayStore;
import database.JdbcScanStore;
import database.JdbcTransactionStore;
import database.TransactionStatusQueries;
import events.ConsumerLoop;
import events.OutboxDispatcher;
import events.ParkingPolicy;
import events.TopicNames;
import messaging.CheckoutOutcomeConsumer;
import messaging.KafkaClients;
import messaging.KafkaRecordSender;
import metrics.MetricsReporter;
import metrics.StatusReporter;
import rpc.GrpcAnalyticsIngestClient;
import rpc.GrpcChannels;
import rpc.GrpcInventoryGateway;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Independent Transaction Service entry point. Transaction runs no gRPC server. Checkout runs over events:
 * T1 records {@code CheckoutRequested} in the outbox, and the {@code transaction-checkout-outcome} consumer
 * settles attempts (D7). Until T110 and T126, Transaction is still a gRPC client of Inventory (catalog) and
 * of Analytics (the scan relay). The broker is never awaited at startup.
 */
public final class TransactionMain {
    private TransactionMain() { }

    public static void main(String[] args) throws Exception {
        Map<String, String> environment = System.getenv();
        int port = args.length > 0 ? Integer.parseInt(args[0]) : intEnv("TRANSACTION_PORT", 8081);
        DatabaseConfig config = DatabaseConfig.fromEnvironment("TRANSACTION", "cs6510_transaction",
            "cs6510_transaction_svc", 32, 64);
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(config, "transaction");
        } catch (Exception failure) {
            System.err.println("Transaction Service cannot connect to MySQL as " + config + ": " + failure.getMessage());
            throw failure;
        }
        ProductCatalogReplica replica = new ProductCatalogReplica();
        GrpcInventoryGateway inventory = GrpcInventoryGateway.forTarget(env("INVENTORY_GRPC_TARGET", "127.0.0.1:9092"));
        CatalogReplicaRefresher refresher = new CatalogReplicaRefresher(inventory,
            new JdbcProductReplicaStore(pool), replica, CatalogReplicaRefresher.refreshInterval(environment));
        TransactionLocks locks = new TransactionLocks();
        TransactionStateCache cache = new TransactionStateCache(locks, TransactionStateCache.maxEntries(environment));
        JdbcTransactionStore transactions = new JdbcTransactionStore(pool);
        JdbcCheckoutAttemptStore attempts = new JdbcCheckoutAttemptStore(pool);
        SettlementRegistry registry = new SettlementRegistry(locks);
        RecoverySchedule schedule = new RecoverySchedule();

        TopicNames names = TopicNames.fromEnvironment(environment);
        KafkaRecordSender checkoutSender = KafkaClients.sender(environment, "transaction-checkout",
            KafkaClients.IMMEDIATE_LINGER_MS);
        OutboxDispatcher dispatcher = new OutboxDispatcher("transaction", new JdbcOutboxStore(pool, "transaction.outbox"),
            Map.of(names.checkout(), checkoutSender), List.of(names.checkout()),
            OutboxDispatcher.Settings.fromEnvironment(environment), Clock.systemUTC());
        CheckoutOrchestrator checkout = new CheckoutOrchestrator(transactions, attempts, new CheckoutEvents(names),
            dispatcher, cache, registry, schedule, CheckoutOrchestrator.Settings.fromEnvironment(environment));
        CheckoutRecoveryWorker.Settings recoverySettings = CheckoutRecoveryWorker.Settings.fromEnvironment(environment);
        CheckoutRecoveryWorker recovery = new CheckoutRecoveryWorker(attempts, dispatcher, cache, registry, schedule,
            recoverySettings, Clock.systemUTC());
        ConsumerLoop outcomes = CheckoutOutcomeConsumer.create(names, CheckoutOutcomeConsumer.kafkaStreams(environment, names),
            new CheckoutOutcomeHandler(attempts, cache, registry, CheckoutOrchestrator.Timing.SYSTEM),
            new JdbcDeadLetterStore(pool, "transaction.deadLetter"),
            ConsumerLoop.Settings.defaults().withThreads(CheckoutOutcomeConsumer.threads(environment))
                .withPolicy(ParkingPolicy.fromEnvironment(environment)));
        var service = new TransactionService(transactions, new JdbcScanStore(pool), checkout, replica, locks, cache);
        ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(pool),
            GrpcAnalyticsIngestClient.forTarget(env("ANALYTICS_GRPC_TARGET", "127.0.0.1:9093"),
                GrpcAnalyticsIngestClient.deadline(environment)),
            ScanEventRelay.Settings.fromEnvironment(environment));
        HttpServer server = HttpServer.create(new InetSocketAddress(port),0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/transactions",new TransactionHttpHandler(service));
        refresher.start();
        MetricsReporter reporter = MetricsReporter.start("transaction");
        TransactionStatusQueries statusQueries = new TransactionStatusQueries(pool, Clock.systemUTC(),
            recoverySettings.stuckAfter());
        StatusReporter status = StatusReporter.start("transaction",
            () -> statusQueries.read().line() + " outcome_consumer_lag=" + outcomes.lag());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(5);
            relay.close();
            recovery.close();
            outcomes.stop(Duration.ofSeconds(5));
            dispatcher.stop(Duration.ofSeconds(5));
            checkoutSender.close();
            refresher.stop();
            GrpcChannels.shutdownAll();
            status.stop();
            reporter.stop();
            pool.close();
        }));
        dispatcher.start();
        outcomes.start();
        server.start();
        recovery.start();
        relay.start();
        System.out.println("Transaction Service listening on port " + port + " (broker "
            + KafkaClients.bootstrap(environment) + ")");
    }

    private static String env(String key,String fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : value;
    }
    private static int intEnv(String key,int fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : Integer.parseInt(value);
    }
}
