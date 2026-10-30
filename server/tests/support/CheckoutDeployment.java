package support;

import events.TopicNames;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An Inventory process and a Transaction process, each over its own disposable schema and user (R3),
 * started from the built artifacts for process-level Saga tests. They exchange checkout events through the
 * live test broker under a per-deployment topic prefix ({@link #names}), whose topics the deployment
 * creates; without a reachable broker the calling test is skipped, or fails under {@code STRICT_LIVE_GATES=1}.
 * Transaction's product replica is seeded with Inventory's catalog and version, so Scan works before the
 * catalog topic is read, and the catalog Inventory publishes at every startup is a no-op for it (D9). Either
 * service can be started with fault hooks armed (see {@link FaultInjection}) and restarted without them.
 *
 * <p>{@link #withAnalytics} adds an Analytics schema and port; the Analytics process starts only on
 * {@link #startAnalytics}, so a test can scan while it is down. Transaction's relay publishes to this
 * deployment's scan topic whether or not Analytics runs; the records wait there until it consumes them.</p>
 */
public final class CheckoutDeployment implements AutoCloseable {
    /** {@code CHECKOUT_CONSUMER_THREADS} of both services unless a test overrides Transaction's. */
    private static final int CONSUMER_THREADS = 8;
    public final TransactionTestSchema transactionSchema;
    public final InventoryTestSchema inventorySchema;
    /** Null unless the deployment was started {@link #withAnalytics}. */
    public final AnalyticsTestSchema analyticsSchema;
    public final int inventoryHttpPort;
    public final int transactionHttpPort;
    public final int analyticsHttpPort;
    public final ServiceHttp transactionHttp;
    public final ServiceHttp inventoryHttp;
    public final ServiceHttp analyticsHttp;
    /** This deployment's topics and consumer groups on the live broker. */
    public final TopicNames names;
    private final Map<String, String> transactionEnvironment;
    private final List<ServiceProcesses.ServiceProcess> extraTransactions = new ArrayList<>();
    private ServiceProcesses.ServiceProcess inventory;
    private ServiceProcesses.ServiceProcess transaction;
    private ServiceProcesses.ServiceProcess analytics;

    private CheckoutDeployment(int products, int stock, Map<String, String> transactionEnvironment,
                               String transactionFaults, String inventoryFaults, boolean withAnalytics) throws Exception {
        this.transactionEnvironment = new LinkedHashMap<>(transactionEnvironment);
        KafkaTestSupport.requireBroker();
        names = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(names);
        transactionSchema = TransactionTestSchema.create(TestSchema.Options.defaults().withOwnUser());
        InventoryTestSchema inventories = null;
        AnalyticsTestSchema analyticsSchemaCreated = null;
        try {
            inventories = InventoryTestSchema.create(TestSchema.Options.defaults().withOwnUser());
            if (withAnalytics) analyticsSchemaCreated = AnalyticsTestSchema.create(TestSchema.Options.defaults().withOwnUser());
        } catch (Exception failure) {
            transactionSchema.close();
            if (inventories != null) inventories.close();
            throw failure;
        }
        inventorySchema = inventories;
        analyticsSchema = analyticsSchemaCreated;
        inventoryHttpPort = ServiceProcesses.freePort();
        transactionHttpPort = ServiceProcesses.freePort();
        analyticsHttpPort = ServiceProcesses.freePort();
        transactionHttp = new ServiceHttp(transactionHttpPort);
        inventoryHttp = new ServiceHttp(inventoryHttpPort);
        analyticsHttp = new ServiceHttp(analyticsHttpPort);
        try {
            transactionSchema.seedReplica(products, inventorySchema.seed(products, stock));
            startInventory(inventoryFaults);
            startTransaction(transactionFaults);
        } catch (Exception | Error failure) {
            close();
            throw failure;
        }
    }

    public static CheckoutDeployment start(int products, int stock) throws Exception {
        return new CheckoutDeployment(products, stock, Map.of(), null, null, false);
    }

    /** With extra Transaction variables, for example {@code CHECKOUT_CONSUMER_THREADS}. */
    public static CheckoutDeployment start(int products, int stock, Map<String, String> transactionEnvironment)
            throws Exception {
        return new CheckoutDeployment(products, stock, transactionEnvironment, null, null, false);
    }

    /** With fault specifications armed in the first Transaction and Inventory processes; null arms none. */
    public static CheckoutDeployment start(int products, int stock, Map<String, String> transactionEnvironment,
                                           String transactionFaults, String inventoryFaults) throws Exception {
        return new CheckoutDeployment(products, stock, transactionEnvironment, transactionFaults, inventoryFaults, false);
    }

    /**
     * Inventory and Transaction as {@link #start}, plus an Analytics schema whose process starts only on
     * {@link #startAnalytics}. {@code transactionFaults} is armed in the first Transaction process.
     */
    public static CheckoutDeployment withAnalytics(int products, int stock, String transactionFaults) throws Exception {
        return new CheckoutDeployment(products, stock, Map.of(), transactionFaults, null, true);
    }

    public void startInventory() throws Exception {
        startInventory(null);
    }

    /** Starts (or restarts) Inventory, with {@code faults} armed when it is not null. */
    public void startInventory(String faults) throws Exception {
        if (faults != null) awaitQuiet();
        Set<String> previous = KafkaTestSupport.memberIds(names.group(TopicNames.INVENTORY_CHECKOUT_GROUP));
        closeQuietly(inventory);
        inventory = ServiceProcesses.service("inventory").database(inventorySchema).httpPort(inventoryHttpPort)
                .kafka(names).faults(faults).start();
        inventory.awaitHttp("/items", 200, Duration.ofSeconds(60));
        KafkaTestSupport.awaitGroupAssigned(names.group(TopicNames.INVENTORY_CHECKOUT_GROUP), names.checkout(),
                TopicNames.Topic.CHECKOUT.partitions(), CONSUMER_THREADS, previous, Duration.ofSeconds(60));
    }

    public void startTransaction() throws Exception {
        startTransaction(null);
    }

    /**
     * Waits until no checkout event is on its way: both outboxes are published and both consumer groups have
     * committed every record, twice in a row 1 s apart. A process restarted with a fault hook then meets only
     * the events of the test's next checkout, never a redelivery or a late re-send of an earlier one.
     */
    public void awaitQuiet() throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        int quiet = 0;
        while (quiet < 2) {
            if (System.nanoTime() > deadline) throw new AssertionError("The deployment never became quiet" + logs());
            boolean published = transactionSchema.unpublishedOutboxCount() == 0
                    && inventorySchema.unpublishedOutboxCount() == 0;
            if (published) {
                KafkaTestSupport.awaitNoLag(names.group(TopicNames.INVENTORY_CHECKOUT_GROUP), names.checkout(),
                        Duration.ofSeconds(30));
                KafkaTestSupport.awaitNoLag(names.group(TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP),
                        names.checkoutOutcome(), Duration.ofSeconds(30));
                quiet++;
            } else {
                quiet = 0;
            }
            Thread.sleep(1_000);
        }
    }

    /** Starts (or restarts) Transaction, with {@code faults} armed when it is not null. */
    public void startTransaction(String faults) throws Exception {
        if (faults != null) awaitQuiet();
        // Only this process's members are stale when no extra Transaction process holds members of its own.
        boolean alone = extraTransactions.stream().noneMatch(ServiceProcesses.ServiceProcess::isAlive);
        Set<String> previous = alone ? KafkaTestSupport.memberIds(names.group(TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP))
                : Set.of();
        closeQuietly(transaction);
        transaction = transactionBuilder(transactionHttpPort).faults(faults).start();
        transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(60));
        awaitOutcomeGroup(previous);
    }

    /**
     * Waits until Transaction's outcome group consists of exactly the consumers of the live Transaction
     * processes and holds all 16 partitions, so no partition waits on a crashed member.
     */
    public void awaitOutcomeGroup() {
        awaitOutcomeGroup(Set.of());
    }

    /** As {@link #awaitOutcomeGroup()}, with none of {@code stale}, the members of a stopped process, left. */
    private void awaitOutcomeGroup(Set<String> stale) {
        int live = transaction != null && transaction.isAlive() ? 1 : 0;
        for (ServiceProcesses.ServiceProcess extra : extraTransactions) if (extra.isAlive()) live++;
        int threads = Integer.parseInt(transactionEnvironment.getOrDefault("CHECKOUT_CONSUMER_THREADS",
                Integer.toString(CONSUMER_THREADS)));
        KafkaTestSupport.awaitGroupAssigned(names.group(TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP),
                names.checkoutOutcome(), TopicNames.Topic.CHECKOUT_OUTCOME.partitions(), live * threads, stale,
                Duration.ofSeconds(60));
    }

    /**
     * Restarts Transaction and waits only until it answers HTTP, for a test that runs while the broker is stopped:
     * no consumer group can be awaited then.
     */
    public void restartTransactionWithoutBroker() throws Exception {
        closeQuietly(transaction);
        transaction = transactionBuilder(transactionHttpPort).start();
        transaction.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(60));
    }

    /**
     * Starts one more Transaction process over the same Transaction schema on {@code httpPort}, with its own
     * relay; it is stopped with the deployment.
     */
    public ServiceProcesses.ServiceProcess startExtraTransaction(int httpPort) throws Exception {
        ServiceProcesses.ServiceProcess extra = transactionBuilder(httpPort).start();
        extraTransactions.add(extra);
        extra.awaitHttp("/transactions/missing", 404, Duration.ofSeconds(60));
        awaitOutcomeGroup();
        return extra;
    }

    private ServiceProcesses.Builder transactionBuilder(int httpPort) {
        ServiceProcesses.Builder builder = ServiceProcesses.service("transaction").database(transactionSchema)
                .httpPort(httpPort).kafka(names);
        transactionEnvironment.forEach(builder::env);
        return builder;
    }

    public void startAnalytics() throws Exception {
        startAnalytics(null);
    }

    /**
     * Starts (or restarts) Analytics with metrics printed every second. Without {@code faults} it waits until
     * Analytics answers HTTP and its scan consumer holds the scan topic's partition. With {@code faults} armed it
     * returns at once, because a hook may halt the process before either happens; the test awaits the outcome.
     */
    public void startAnalytics(String faults) throws Exception {
        if (analyticsSchema == null) throw new IllegalStateException("Start the deployment withAnalytics");
        String group = names.group(TopicNames.ANALYTICS_SCAN_INGEST_GROUP);
        Set<String> previous = KafkaTestSupport.memberIds(group);
        closeQuietly(analytics);
        analytics = ServiceProcesses.service("analytics").database(analyticsSchema).httpPort(analyticsHttpPort)
                .kafka(names).perfMetrics(true).env("PERF_METRICS_INTERVAL_SECONDS", "1")
                .faults(faults).start();
        if (faults != null) return;
        analytics.awaitHttp("/analytics/popular-items", 200, Duration.ofSeconds(60));
        KafkaTestSupport.awaitGroupAssigned(group, names.scanAccepted(), TopicNames.Topic.SCAN_ACCEPTED.partitions(),
                1, previous, Duration.ofSeconds(60));
    }

    /**
     * Starts Transaction with {@code faults} armed and returns at once, for a hook that halts it before it
     * answers HTTP, such as {@code relay.afterConfirm} with scans already waiting in the outbox.
     */
    public void launchTransaction(String faults) throws Exception {
        awaitQuiet();
        closeQuietly(transaction);
        transaction = transactionBuilder(transactionHttpPort).faults(faults).start();
    }

    /** The records on this deployment's scan topic. */
    public long scanTopicEnd() {
        return KafkaTestSupport.endOffset(names.scanAccepted(), 0);
    }

    /** Waits until Transaction's relay has published every accepted scan to the broker. */
    public void awaitScansPublished(Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (transactionSchema.undeliveredCount() > 0) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Unpublished scans remain: " + transactionSchema.undeliveredCount() + logs());
            }
            Thread.sleep(50);
        }
    }

    /** Kills Inventory without shutdown hooks, like a crash. */
    public void killInventory() throws InterruptedException {
        inventory.kill();
    }

    /** Stops a previous process of the same service, halted or not, before its replacement starts. */
    private static void closeQuietly(ServiceProcesses.ServiceProcess process) throws InterruptedException {
        if (process != null) process.close();
    }

    /** Kills Transaction without shutdown hooks, like a crash. */
    public void killTransaction() throws InterruptedException {
        transaction.kill();
    }

    public void stopInventory() throws InterruptedException {
        inventory.stop();
    }

    public void stopTransaction() throws InterruptedException {
        transaction.stop();
    }

    public void stopAnalytics() throws InterruptedException {
        if (analytics != null) analytics.stop();
    }

    /** Kills Analytics without shutdown hooks, like a crash. */
    public void killAnalytics() throws InterruptedException {
        if (analytics != null) analytics.kill();
    }

    public ServiceProcesses.ServiceProcess transactionProcess() {
        return transaction;
    }

    public ServiceProcesses.ServiceProcess inventoryProcess() {
        return inventory;
    }

    public ServiceProcesses.ServiceProcess analyticsProcess() {
        return analytics;
    }

    /**
     * Waits until Transaction's outbox is empty and Analytics stores exactly the identities Transaction
     * accepted, then asserts that positions are contiguous from 1.
     */
    public void awaitAnalyticsHoldsEveryScan(Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (transactionSchema.undeliveredCount() == 0
                    && analyticsSchema.storedEvents() == transactionSchema.scanCount()) break;
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Analytics did not catch up: undelivered=" + transactionSchema.undeliveredCount()
                        + " transaction=" + transactionSchema.scanCount() + " analytics=" + analyticsSchema.storedEvents()
                        + System.lineSeparator() + logs());
            }
            Thread.sleep(100);
        }
        org.junit.jupiter.api.Assertions.assertEquals(transactionSchema.identities(), analyticsSchema.identities(),
                "Analytics must store exactly the accepted scans, each once");
        analyticsSchema.assertPositionsContiguous();
    }

    /** Every service's output, for assertion messages. */
    public String logs() {
        StringBuilder logs = new StringBuilder();
        logs.append("--- transaction ---").append(System.lineSeparator()).append(transaction == null ? "" : transaction.output());
        for (ServiceProcesses.ServiceProcess extra : extraTransactions) {
            logs.append(System.lineSeparator()).append("--- extra transaction ---").append(System.lineSeparator())
                    .append(extra.output());
        }
        logs.append(System.lineSeparator()).append("--- inventory ---").append(System.lineSeparator())
                .append(inventory == null ? "" : inventory.output());
        if (analytics != null) {
            logs.append(System.lineSeparator()).append("--- analytics ---").append(System.lineSeparator())
                    .append(analytics.output());
        }
        return logs.toString();
    }

    @Override
    public void close() throws Exception {
        try (transactionSchema; inventorySchema; AutoCloseable analyticsData = analyticsSchema) {
            for (ServiceProcesses.ServiceProcess extra : extraTransactions) extra.close();
            if (transaction != null) transaction.close();
            if (analytics != null) analytics.close();
            if (inventory != null) inventory.close();
        }
    }
}
