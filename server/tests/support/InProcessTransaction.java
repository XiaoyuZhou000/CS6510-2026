package support;

import database.CheckoutAttemptStore;
import database.ConnectionPool;
import database.JdbcCheckoutAttemptStore;
import database.JdbcDeadLetterStore;
import database.JdbcOutboxStore;
import database.JdbcProductReplicaStore;
import database.JdbcScanStore;
import database.JdbcTransactionStore;
import database.ProductReplicaStore;
import database.ScanStore;
import database.TransactionStore;
import events.ConsumerLoop;
import events.OutboxDispatcher;
import events.OutboxSignal;
import events.RecordSender;
import events.RecordStream;
import events.TopicNames;
import messaging.CatalogConsumer;
import messaging.CheckoutOutcomeConsumer;
import transaction.CatalogEventHandler;
import transaction.CheckoutEvents;
import transaction.CheckoutOrchestrator;
import transaction.CheckoutOutcomeHandler;
import transaction.CheckoutRecoveryWorker;
import transaction.ProductCatalogReplica;
import transaction.RecoverySchedule;
import transaction.SettlementRegistry;
import transaction.TransactionLocks;
import transaction.TransactionOperations;
import transaction.TransactionService;
import transaction.TransactionStateCache;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * One in-process Transaction Service over a {@link TransactionTestSchema}, wired like
 * {@code TransactionMain} but without HTTP, for the performance, cache, and recovery tests (R19, R22; 006
 * T059). The schema's product replica must be seeded first; it is loaded from the durable store, as at startup.
 * With {@link Builder#catalogFromEvents()} it may start empty instead, and a catalog consumer keeps it current
 * from the catalog topic on the same transport, as in {@code TransactionMain} (D9).
 *
 * <p>Checkout runs over events: the orchestrator records {@code CheckoutRequested} in this schema's
 * {@code outbox_event}, an {@link OutboxDispatcher} sends it, and the {@code transaction-checkout-outcome}
 * {@link ConsumerLoop} feeds {@link CheckoutOutcomeHandler}. The transport is an {@link InMemoryEventBus} by
 * default (its own, or one shared with {@link Builder#bus}), or the live broker with a per-run topic prefix
 * ({@link Builder#liveKafka}). Nothing answers checkout requests unless the test starts an Inventory on the
 * same transport, for example a {@link FakeInventoryReactor}. The recovery worker runs only after
 * {@link #startRecovery()}, or when a test drives it directly.</p>
 */
public final class InProcessTransaction implements AutoCloseable {
    public final ConnectionPool pool;
    public final TransactionStore transactions;
    public final ScanStore scans;
    public final CheckoutAttemptStore attempts;
    public final ProductCatalogReplica replica;
    public final TransactionLocks locks;
    public final TransactionStateCache cache;
    public final SettlementRegistry registry;
    public final RecoverySchedule schedule;
    public final TopicNames names;
    /** Null when the transport is the live broker. */
    public final InMemoryEventBus bus;
    public final JdbcOutboxStore outbox;
    public final JdbcDeadLetterStore deadLetters;
    public final OutboxDispatcher dispatcher;
    public final CheckoutOrchestrator checkout;
    public final CheckoutOutcomeHandler outcomes;
    public final ConsumerLoop outcomeConsumer;
    public final TransactionService service;
    public final CheckoutRecoveryWorker recovery;
    /** The catalog handler and its consumer; null unless built {@link Builder#catalogFromEvents()}. */
    public final CatalogEventHandler catalogHandler;
    public final ConsumerLoop catalogConsumer;
    private final RecordSender sender;
    private final boolean ownsSender;
    private boolean messagingStarted;
    private boolean closed;

    private InProcessTransaction(Builder builder) throws SQLException {
        pool = builder.pool != null ? builder.pool : builder.schema.newPool(
                "test.inprocess." + UUID.randomUUID().toString().substring(0, 8), builder.initialPool,
                builder.maxPool, builder.decorator);
        transactions = builder.wrapTransactions.apply(new JdbcTransactionStore(pool));
        scans = builder.wrapScans.apply(new JdbcScanStore(pool));
        attempts = builder.wrapAttempts.apply(new JdbcCheckoutAttemptStore(pool));
        replica = new ProductCatalogReplica();
        if (builder.catalogFromEvents) {
            catalogHandler = new CatalogEventHandler(new JdbcProductReplicaStore(pool), replica);
            catalogHandler.loadDurable();
        } else {
            catalogHandler = null;
            ProductReplicaStore.Replica stored = new JdbcProductReplicaStore(pool).load()
                    .orElseThrow(() -> new IllegalStateException("Seed the product replica before building the service"));
            replica.install(stored.catalogVersion(), stored.products().stream()
                    .map(product -> new ProductCatalogReplica.Product(product.sku(), product.name(), product.price()))
                    .toList());
        }
        locks = builder.stripes == 0 ? new TransactionLocks() : new TransactionLocks(builder.stripes);
        cache = new TransactionStateCache(locks, builder.cacheEntries);
        registry = new SettlementRegistry(locks);
        schedule = new RecoverySchedule();

        Supplier<RecordStream> streams;
        Supplier<RecordStream> catalogStreams;
        if (builder.live) {
            names = builder.names != null ? builder.names : KafkaTestSupport.runTopics();
            KafkaTestSupport.createCatalogTopics(names);
            bus = null;
            sender = builder.wrapSender.apply(KafkaTestSupport.sender("test-transaction-checkout", 0));
            ownsSender = true;
            streams = KafkaTestSupport.streams("test-transaction-outcome",
                    names.group(TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP));
            catalogStreams = KafkaTestSupport.streams("test-transaction-catalog", null);
        } else {
            names = builder.names != null ? builder.names : TopicNames.unprefixed();
            bus = builder.bus != null ? builder.bus : InMemoryEventBus.withCatalogTopics(names, builder.seed);
            sender = builder.wrapSender.apply(bus);
            ownsSender = false;
            streams = bus::stream;
            catalogStreams = bus::stream;
        }
        outbox = new JdbcOutboxStore(pool, "transaction.outbox");
        deadLetters = new JdbcDeadLetterStore(pool, "transaction.deadLetter");
        dispatcher = new OutboxDispatcher("transaction", outbox, Map.of(names.checkout(), sender),
                List.of(names.checkout()), builder.outboxSettings, Clock.systemUTC());
        OutboxSignal signal = builder.signal != null ? builder.signal : dispatcher;
        checkout = new CheckoutOrchestrator(transactions, attempts, new CheckoutEvents(names), signal, cache, registry,
                schedule, builder.settings, builder.timing);
        outcomes = new CheckoutOutcomeHandler(attempts, cache, registry, builder.timing);
        outcomeConsumer = CheckoutOutcomeConsumer.create(names, streams, outcomes, deadLetters, builder.consumerSettings);
        catalogConsumer = catalogHandler == null ? null : CatalogConsumer.create(names, catalogStreams, catalogHandler,
                deadLetters, ConsumerLoop.Settings.defaults());
        service = new TransactionService(transactions, scans, checkout, replica, locks, cache);
        recovery = new CheckoutRecoveryWorker(attempts, signal, cache, registry, schedule, builder.recoverySettings,
                Clock.systemUTC());
        builder.schema.closeWith(this);
        if (builder.startMessaging) startMessaging();
    }

    /** Starts the dispatcher, the catalog consumer if any, and the outcome consumer, as {@code TransactionMain} does. */
    public synchronized InProcessTransaction startMessaging() {
        if (!messagingStarted) {
            dispatcher.start();
            if (catalogConsumer != null) catalogConsumer.start();
            outcomeConsumer.start();
            messagingStarted = true;
        }
        return this;
    }

    /** Starts the recovery worker thread, as {@code TransactionMain} does after its HTTP server. */
    public InProcessTransaction startRecovery() {
        recovery.start();
        return this;
    }

    /** Stops the recovery worker, if it runs. */
    public void stopRecovery() {
        recovery.close();
    }

    /** Stops the recovery worker, the consumer, and the dispatcher; the pool belongs to the schema. */
    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        recovery.close();
        if (messagingStarted) {
            outcomeConsumer.stop(Duration.ofSeconds(5));
            if (catalogConsumer != null) catalogConsumer.stop(Duration.ofSeconds(5));
            dispatcher.stop(Duration.ofSeconds(5));
            messagingStarted = false;
        }
        if (ownsSender) sender.close();
    }

    public static Builder over(TransactionTestSchema schema) {
        return new Builder(schema);
    }

    public String start() {
        return service.start(new TransactionOperations.StartCommand("perf-station")).transactionId();
    }

    public TransactionOperations.ScanView scan(String transactionId, String sku) {
        return service.scan(new TransactionOperations.ScanCommand(transactionId, sku));
    }

    public TransactionOperations.ReceiptView complete(String transactionId) {
        return service.complete(transactionId);
    }

    /** A {@link FakeInventoryReactor} on this instance's bus; the caller starts and closes it. */
    public FakeInventoryReactor reactor() {
        if (bus == null) throw new IllegalStateException("A live-broker instance has no in-memory bus");
        return FakeInventoryReactor.deducting(bus, names);
    }

    public static final class Builder {
        private final TransactionTestSchema schema;
        private ConnectionPool pool;
        private int initialPool = 4;
        private int maxPool = 8;
        private UnaryOperator<Connection> decorator;
        private int cacheEntries = TransactionStateCache.DEFAULT_MAX_ENTRIES;
        private int stripes;
        private UnaryOperator<TransactionStore> wrapTransactions = UnaryOperator.identity();
        private UnaryOperator<ScanStore> wrapScans = UnaryOperator.identity();
        private UnaryOperator<CheckoutAttemptStore> wrapAttempts = UnaryOperator.identity();
        private UnaryOperator<RecordSender> wrapSender = UnaryOperator.identity();
        private CheckoutOrchestrator.Settings settings = CheckoutOrchestrator.Settings.defaults();
        private CheckoutOrchestrator.Timing timing = CheckoutOrchestrator.Timing.SYSTEM;
        private CheckoutRecoveryWorker.Settings recoverySettings = CheckoutRecoveryWorker.Settings.defaults();
        private OutboxDispatcher.Settings outboxSettings = OutboxDispatcher.Settings.defaults();
        private ConsumerLoop.Settings consumerSettings = ConsumerLoop.Settings.defaults().withThreads(2);
        private InMemoryEventBus bus;
        private TopicNames names;
        private long seed = 6510;
        private boolean live;
        private boolean startMessaging = true;
        private OutboxSignal signal;
        private boolean catalogFromEvents;

        private Builder(TransactionTestSchema schema) {
            this.schema = schema;
        }

        /** Shares an existing pool instead of opening one, for example a second service instance. */
        public Builder pool(ConnectionPool shared) { this.pool = shared; return this; }
        public Builder pool(int initial, int max) { this.initialPool = initial; this.maxPool = max; return this; }
        public Builder decorator(UnaryOperator<Connection> wrap) { this.decorator = wrap; return this; }
        public Builder cacheEntries(int entries) { this.cacheEntries = entries; return this; }
        /** A test-only stripe count; 0 keeps the production 4,096. */
        public Builder stripes(int count) { this.stripes = count; return this; }
        public Builder wrapTransactions(UnaryOperator<TransactionStore> wrap) { this.wrapTransactions = wrap; return this; }
        public Builder wrapScans(UnaryOperator<ScanStore> wrap) { this.wrapScans = wrap; return this; }
        public Builder wrapAttempts(UnaryOperator<CheckoutAttemptStore> wrap) { this.wrapAttempts = wrap; return this; }
        /** Wraps the dispatcher's record sender, for example in a {@link CountingRecordSender}. */
        public Builder wrapSender(UnaryOperator<RecordSender> wrap) { this.wrapSender = wrap; return this; }
        public Builder settings(CheckoutOrchestrator.Settings checkoutSettings) { this.settings = checkoutSettings; return this; }
        public Builder timing(CheckoutOrchestrator.Timing checkoutTiming) { this.timing = checkoutTiming; return this; }
        public Builder recovery(CheckoutRecoveryWorker.Settings settings) { this.recoverySettings = settings; return this; }
        public Builder outbox(OutboxDispatcher.Settings settings) { this.outboxSettings = settings; return this; }
        public Builder consumer(ConsumerLoop.Settings settings) { this.consumerSettings = settings; return this; }
        /** Shares a bus, for example with an Inventory or a second Transaction instance (same outcome group). */
        public Builder bus(InMemoryEventBus shared, TopicNames topicNames) {
            this.bus = shared;
            this.names = topicNames;
            return this;
        }
        public Builder seed(long busSeed) { this.seed = busSeed; return this; }
        /** The live broker at {@code KAFKA_TEST_BOOTSTRAP}, with {@code topicNames} or a fresh per-run prefix. */
        public Builder liveKafka(TopicNames topicNames) {
            this.live = true;
            this.names = topicNames;
            return this;
        }
        /** Leaves the dispatcher and consumer stopped until {@link InProcessTransaction#startMessaging()}. */
        public Builder messagingStopped() { this.startMessaging = false; return this; }
        /**
         * Loads the durable replica if there is one, without requiring it, and keeps it current with a catalog
         * consumer on this instance's transport, which replays the catalog topic from offset 0 (D9).
         */
        public Builder catalogFromEvents() { this.catalogFromEvents = true; return this; }
        /** Replaces the dispatcher as the orchestrator's and the worker's signal, for unit-level checks. */
        public Builder signal(OutboxSignal replacement) { this.signal = replacement; return this; }

        public InProcessTransaction build() throws SQLException {
            return new InProcessTransaction(this);
        }
    }
}
