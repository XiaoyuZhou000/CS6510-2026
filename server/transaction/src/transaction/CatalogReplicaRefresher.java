package transaction;

import database.ProductReplicaStore;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Keeps the product replica in step with Inventory's catalog (R4).
 *
 * <ul>
 *   <li>At startup it installs the durable replica, so Scan works while Inventory is down.</li>
 *   <li>Every {@code CATALOG_REFRESH_SECONDS} (30) it compares Inventory's version with the installed
 *       one; on a change, or while the replica is empty, it fetches the catalog, replaces the durable
 *       replica, and only then swaps the in-memory snapshot.</li>
 *   <li>While never populated it retries every second; otherwise failures back off from 1 s to 30 s.</li>
 * </ul>
 */
public final class CatalogReplicaRefresher {
    public static final Duration DEFAULT_REFRESH = Duration.ofSeconds(30);
    static final Duration UNPOPULATED_RETRY = Duration.ofSeconds(1);
    static final Duration MIN_BACKOFF = Duration.ofSeconds(1);
    static final Duration MAX_BACKOFF = Duration.ofSeconds(30);
    private static final LatencyHistogram REFRESH = PerfMetrics.timer("transaction.replica.refresh");

    private final InventoryGateway inventory;
    private final ProductReplicaStore store;
    private final ProductCatalogReplica replica;
    private final Duration refreshInterval;
    private Duration backoff = MIN_BACKOFF;
    private boolean failing;
    private volatile Thread worker;
    private volatile boolean stopped;

    public CatalogReplicaRefresher(InventoryGateway inventory, ProductReplicaStore store, ProductCatalogReplica replica,
                                   Duration refreshInterval) {
        this.inventory = Objects.requireNonNull(inventory, "inventory");
        this.store = Objects.requireNonNull(store, "store");
        this.replica = Objects.requireNonNull(replica, "replica");
        this.refreshInterval = Objects.requireNonNull(refreshInterval, "refreshInterval");
        if (refreshInterval.isZero() || refreshInterval.isNegative()) {
            throw new IllegalArgumentException("refreshInterval must be positive");
        }
    }

    /** {@code CATALOG_REFRESH_SECONDS}, default 30. */
    public static Duration refreshInterval(Map<String, String> environment) {
        String configured = environment.get("CATALOG_REFRESH_SECONDS");
        if (configured == null || configured.isBlank()) return DEFAULT_REFRESH;
        long seconds = Long.parseLong(configured.trim());
        if (seconds <= 0) throw new IllegalArgumentException("CATALOG_REFRESH_SECONDS must be positive");
        return Duration.ofSeconds(seconds);
    }

    /** Installs the durable replica if there is one; true when the replica is usable afterwards. */
    public boolean loadDurable() {
        try {
            Optional<ProductReplicaStore.Replica> stored = store.load();
            stored.ifPresent(durable -> replica.install(durable.catalogVersion(), products(durable.products())));
        } catch (RuntimeException failure) {
            PerfMetrics.increment("transaction.replica.loadFailures");
            System.err.println("[transaction] Durable product replica not loaded: " + failure.getMessage());
        }
        return replica.usable();
    }

    /** One refresh round; returns the delay before the next one. */
    public Duration runOnce() {
        long started = System.nanoTime();
        try {
            if (!replica.usable()) loadDurable();
            InventoryGateway.CatalogVersion current = inventory.catalogVersion();
            if (!replica.usable() || !current.version().equals(replica.version())) {
                InventoryGateway.Catalog catalog = inventory.catalog();
                ProductCatalogReplica.Snapshot snapshot = ProductCatalogReplica.snapshotOf(catalog.version(),
                        catalog.products());
                store.replace(catalog.version(), stored(catalog.products()));
                replica.install(snapshot);
                REFRESH.recordSince(started);
                PerfMetrics.increment("transaction.replica.swaps");
                System.out.println("[transaction] Product replica now at catalog version " + catalog.version()
                        + " (" + catalog.products().size() + " products)");
            }
            if (failing) System.out.println("[transaction] Product replica refresh recovered");
            failing = false;
            backoff = MIN_BACKOFF;
            return refreshInterval;
        } catch (RuntimeException failure) {
            PerfMetrics.increment("transaction.replica.refreshFailures");
            if (!failing) {
                System.err.println("[transaction] Product replica refresh failed; retrying: " + failure.getMessage());
            }
            failing = true;
            if (!replica.usable()) return UNPOPULATED_RETRY;
            Duration delay = backoff;
            backoff = backoff.multipliedBy(2).compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff.multipliedBy(2);
            return delay;
        }
    }

    /** Installs the durable replica now, then refreshes on a daemon thread. */
    public synchronized void start() {
        if (worker != null) throw new IllegalStateException("The refresher is already started");
        loadDurable();
        Thread thread = Thread.ofPlatform().daemon().name("catalog-replica-refresher").unstarted(this::loop);
        worker = thread;
        thread.start();
    }

    public void stop() {
        stopped = true;
        Thread thread = worker;
        if (thread == null) return;
        thread.interrupt();
        try {
            thread.join(2000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void loop() {
        while (!stopped) {
            Duration delay = runOnce();
            try {
                Thread.sleep(delay);
            } catch (InterruptedException interrupted) {
                return;
            }
        }
    }

    private static List<ProductCatalogReplica.Product> products(List<ProductReplicaStore.ReplicaProduct> stored) {
        return stored.stream()
                .map(product -> new ProductCatalogReplica.Product(product.sku(), product.name(), product.price()))
                .toList();
    }

    private static List<ProductReplicaStore.ReplicaProduct> stored(List<ProductCatalogReplica.Product> products) {
        return products.stream()
                .map(product -> new ProductReplicaStore.ReplicaProduct(product.sku(), product.name(), product.price()))
                .toList();
    }
}
