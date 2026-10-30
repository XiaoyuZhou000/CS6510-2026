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
import messaging.CatalogConsumer;
import messaging.CheckoutOutcomeConsumer;
import messaging.KafkaClients;
import messaging.KafkaRecordSender;
import messaging.KafkaScanEventSink;
import metrics.MetricsReporter;
import metrics.StatusReporter;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Independent Transaction Service entry point. Transaction runs no gRPC server. Checkout runs over events:
 * T1 records {@code CheckoutRequested} in the outbox, and the {@code transaction-checkout-outcome} consumer
 * settles attempts (D7). The product replica is loaded from the database first, so Scan works before the
 * catalog topic is read, and is then kept current by the catalog consumer, which replays the topic from
 * offset 0 (D9); Transaction never calls Inventory. The scan relay publishes accepted scans as {@code ScanAccepted}
 * records for Analytics (D10). Transaction calls no peer, and the broker is never awaited at startup.
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
        CatalogEventHandler catalog = new CatalogEventHandler(new JdbcProductReplicaStore(pool), replica);
        catalog.loadDurable();
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
        ConsumerLoop catalogConsumer = CatalogConsumer.create(names, CatalogConsumer.kafkaStreams(environment), catalog,
            new JdbcDeadLetterStore(pool, "transaction.deadLetter"),
            ConsumerLoop.Settings.defaults().withPolicy(ParkingPolicy.fromEnvironment(environment)));
        var service = new TransactionService(transactions, new JdbcScanStore(pool), checkout, replica, locks, cache);
        KafkaRecordSender scanSender = KafkaClients.sender(environment, "transaction-scan",
            KafkaClients.batchLingerMs(environment));
        ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(pool), new KafkaScanEventSink(scanSender, names),
            ScanEventRelay.Settings.fromEnvironment(environment));
        HttpServer server = HttpServer.create(new InetSocketAddress(port),0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/transactions",new TransactionHttpHandler(service));
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
            catalogConsumer.stop(Duration.ofSeconds(5));
            dispatcher.stop(Duration.ofSeconds(5));
            checkoutSender.close();
            scanSender.close();
            status.stop();
            reporter.stop();
            pool.close();
        }));
        dispatcher.start();
        catalogConsumer.start();
        outcomes.start();
        server.start();
        recovery.start();
        relay.start();
        System.out.println("Transaction Service listening on port " + port + " (broker "
            + KafkaClients.bootstrap(environment) + ")");
    }

    private static int intEnv(String key,int fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : Integer.parseInt(value);
    }
}
