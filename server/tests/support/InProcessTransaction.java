package support;

import database.CheckoutAttemptStore;
import database.ConnectionPool;
import database.JdbcCheckoutAttemptStore;
import database.JdbcProductReplicaStore;
import database.JdbcScanStore;
import database.JdbcTransactionStore;
import database.ProductReplicaStore;
import database.ScanStore;
import database.TransactionStore;
import transaction.CheckoutOrchestrator;
import transaction.CheckoutRecoveryWorker;
import transaction.InventoryGateway;
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
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 * One in-process Transaction Service over a {@link TransactionTestSchema}, wired like
 * {@code TransactionMain} but without HTTP, for the performance, cache, and recovery tests (R19, R22).
 * The schema's product replica must be seeded first; it is loaded from the durable store, as at startup.
 * Complete calls {@link Builder#inventory}, which by default fails the test. The recovery worker is
 * built over the same schedule and registry but runs only after {@link #startRecovery()}, or when a
 * test drives it directly.
 */
public final class InProcessTransaction {
    public final ConnectionPool pool;
    public final TransactionStore transactions;
    public final ScanStore scans;
    public final CheckoutAttemptStore attempts;
    public final ProductCatalogReplica replica;
    public final TransactionLocks locks;
    public final TransactionStateCache cache;
    public final SettlementRegistry registry;
    public final RecoverySchedule schedule;
    public final CheckoutOrchestrator checkout;
    public final TransactionService service;
    public final CheckoutRecoveryWorker recovery;

    private InProcessTransaction(Builder builder) throws SQLException {
        pool = builder.pool != null ? builder.pool : builder.schema.newPool(
                "test.inprocess." + UUID.randomUUID().toString().substring(0, 8), builder.initialPool,
                builder.maxPool, builder.decorator);
        transactions = builder.wrapTransactions.apply(new JdbcTransactionStore(pool));
        scans = builder.wrapScans.apply(new JdbcScanStore(pool));
        attempts = builder.wrapAttempts.apply(new JdbcCheckoutAttemptStore(pool));
        replica = new ProductCatalogReplica();
        ProductReplicaStore.Replica stored = new JdbcProductReplicaStore(pool).load()
                .orElseThrow(() -> new IllegalStateException("Seed the product replica before building the service"));
        replica.install(stored.catalogVersion(), stored.products().stream()
                .map(product -> new ProductCatalogReplica.Product(product.sku(), product.name(), product.price()))
                .toList());
        locks = builder.stripes == 0 ? new TransactionLocks() : new TransactionLocks(builder.stripes);
        cache = new TransactionStateCache(locks, builder.cacheEntries);
        registry = new SettlementRegistry(locks);
        schedule = new RecoverySchedule();
        checkout = new CheckoutOrchestrator(transactions, attempts, builder.inventory, cache, registry, schedule,
                builder.settings, builder.timing);
        service = new TransactionService(transactions, scans, checkout, replica, locks, cache);
        recovery = new CheckoutRecoveryWorker(attempts, builder.inventory, cache, registry, schedule,
                builder.recoverySettings, Clock.systemUTC());
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
        private InventoryGateway inventory = FakeInventoryGateway.failing();
        private CheckoutOrchestrator.Settings settings = CheckoutOrchestrator.Settings.defaults();
        private CheckoutOrchestrator.Timing timing = CheckoutOrchestrator.Timing.SYSTEM;
        private CheckoutRecoveryWorker.Settings recoverySettings = CheckoutRecoveryWorker.Settings.defaults();

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
        /** The Inventory that Complete calls, for example {@link InProcessInventory#gateway()}. */
        public Builder inventory(InventoryGateway gateway) { this.inventory = gateway; return this; }
        public Builder settings(CheckoutOrchestrator.Settings checkoutSettings) { this.settings = checkoutSettings; return this; }
        public Builder timing(CheckoutOrchestrator.Timing checkoutTiming) { this.timing = checkoutTiming; return this; }
        public Builder recovery(CheckoutRecoveryWorker.Settings settings) { this.recoverySettings = settings; return this; }

        public InProcessTransaction build() throws SQLException {
            return new InProcessTransaction(this);
        }
    }
}
