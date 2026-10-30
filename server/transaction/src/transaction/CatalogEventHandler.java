package transaction;

import database.ProductReplicaStore;
import events.DecodedEvent;
import events.EventHandler;
import events.EventTypes;
import events.ParkReason;
import events.PermanentEventFailure;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;
import selfcheckout.events.v1.CatalogItemPublished;
import selfcheckout.events.v1.CatalogSnapshotPublished;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Keeps the product replica in step with Inventory's catalog events (D9, FR-017, FR-018). It is the only
 * writer of {@code product_replica}, and it runs on the catalog consumer's single thread.
 *
 * <ul>
 *   <li><b>Version buffer</b> (data-model "Catalog version buffer"): each {@code CatalogItemPublished} is held
 *       under its {@code catalog_version} and SKU, so an item repeated by several Inventory startups replaces
 *       the buffered one and is counted once. The buffer is never authoritative: it is discarded at restart,
 *       when the topic is replayed from offset 0.</li>
 *   <li><b>Replacement</b>: a {@code CatalogSnapshotPublished} for a version other than the installed one,
 *       whose buffered distinct SKUs number {@code product_count}, replaces the durable replica in one local
 *       transaction and only then swaps the in-memory snapshot; the whole buffer is dropped afterwards, so
 *       records of older versions read earlier can never complete. A marker with fewer SKUs, or for the
 *       installed version, changes nothing.</li>
 *   <li><b>Failures</b>: an item whose name or price breaks the replica's column rules is a permanent
 *       {@code INVALID_PAYLOAD} and is parked; a store failure propagates as transient, leaving the buffer
 *       intact, so the consumer retries the marker.</li>
 * </ul>
 *
 * <p>At startup {@link #loadDurable} installs the stored replica, so Scan works before the topic is read and
 * while Inventory or the broker is down. If that read failed, it is retried before the next event.</p>
 */
public final class CatalogEventHandler implements EventHandler {
    private static final LatencyHistogram REPLACE = PerfMetrics.timer("transaction.replica.replace");

    private final ProductReplicaStore store;
    private final ProductCatalogReplica replica;
    private final Map<String, Map<String, ProductCatalogReplica.Product>> buffered = new HashMap<>();
    private volatile long nextOffset;

    public CatalogEventHandler(ProductReplicaStore store, ProductCatalogReplica replica) {
        this.store = Objects.requireNonNull(store, "store");
        this.replica = Objects.requireNonNull(replica, "replica");
    }

    /** Installs the durable replica if there is one; true when the replica is usable afterwards. */
    public boolean loadDurable() {
        try {
            Optional<ProductReplicaStore.Replica> stored = store.load();
            stored.ifPresent(durable -> replica.install(durable.catalogVersion(), durable.products().stream()
                    .map(product -> new ProductCatalogReplica.Product(product.sku(), product.name(), product.price()))
                    .toList()));
        } catch (RuntimeException failure) {
            PerfMetrics.increment("transaction.replica.loadFailures");
            System.err.println("[transaction] Durable product replica not loaded: " + failure.getMessage());
        }
        return replica.usable();
    }

    /** The offset after the last catalog record handled, for tests and status: the consumer's progress. */
    public long nextOffset() {
        return nextOffset;
    }

    @Override
    public void handle(DecodedEvent event) {
        if (!replica.usable()) loadDurable();
        switch (event.type()) {
            case CATALOG_ITEM_PUBLISHED -> buffer(event.payload(CatalogItemPublished.class));
            case CATALOG_SNAPSHOT_PUBLISHED -> complete(event.payload(CatalogSnapshotPublished.class));
            default -> throw new PermanentEventFailure(ParkReason.UNKNOWN_TYPE,
                    event.type().typeName() + " does not belong on the catalog topic");
        }
        if (event.offset() >= 0) nextOffset = event.offset() + 1;
    }

    private void buffer(CatalogItemPublished item) {
        ProductCatalogReplica.Product product = product(item);
        buffered.computeIfAbsent(item.getCatalogVersion(), ignored -> new HashMap<>()).put(product.sku(), product);
    }

    private void complete(CatalogSnapshotPublished marker) {
        String version = marker.getCatalogVersion();
        if (version.equals(replica.version())) {
            buffered.remove(version);
            return;
        }
        Map<String, ProductCatalogReplica.Product> items = buffered.getOrDefault(version, Map.of());
        if (items.size() != marker.getProductCount()) {
            PerfMetrics.increment("transaction.replica.incompleteSnapshots");
            return;
        }
        List<ProductCatalogReplica.Product> products = items.values().stream()
                .sorted(Comparator.comparing(ProductCatalogReplica.Product::sku)).toList();
        ProductCatalogReplica.Snapshot snapshot = ProductCatalogReplica.snapshotOf(version, products);
        long started = System.nanoTime();
        store.replace(version, products.stream()
                .map(product -> new ProductReplicaStore.ReplicaProduct(product.sku(), product.name(), product.price()))
                .toList());
        replica.install(snapshot);
        REPLACE.recordSince(started);
        buffered.clear();
        PerfMetrics.increment("transaction.replica.swaps");
        System.out.println("[transaction] Product replica now at catalog version " + version + " ("
                + products.size() + " products)");
    }

    private static ProductCatalogReplica.Product product(CatalogItemPublished item) {
        try {
            return new ProductCatalogReplica.Product(item.getSku(), item.getName(), new BigDecimal(item.getPrice()));
        } catch (IllegalArgumentException invalid) {
            // NumberFormatException is an IllegalArgumentException too.
            throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD,
                    "CatalogItemPublished " + item.getSku() + " of " + item.getCatalogVersion() + ": " + invalid.getMessage(),
                    invalid);
        }
    }
}
