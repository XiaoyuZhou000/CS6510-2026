package notification;

import events.DecodedEvent;
import events.EventHandler;
import events.ParkReason;
import events.PermanentEventFailure;
import metrics.PerfMetrics;
import selfcheckout.events.v1.CatalogItemPublished;
import selfcheckout.events.v1.CatalogSnapshotPublished;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The catalog's SKUs, for validating subscription requests only (007 research N6, FR-006). It is filled by
 * replaying the compacted {@code selfcheckout.catalog.v1} topic from offset 0 at every start, on the catalog
 * reader's single thread, and is never authoritative: selections never consult it.
 *
 * <ul>
 *   <li>Each {@code CatalogItemPublished} is buffered under its {@code catalog_version}, so an item repeated by
 *       several Inventory startups is counted once.</li>
 *   <li>A {@code CatalogSnapshotPublished} for a version other than the installed one, whose buffered SKUs number
 *       {@code product_count}, installs that version's set and drops every buffer, so items of an older version
 *       read afterwards can never complete or change the installed set. A marker with fewer buffered SKUs changes
 *       nothing.</li>
 *   <li>Until the first complete snapshot, the set is empty and {@link #loaded()} is false; registration then
 *       answers {@code 500} with no change.</li>
 * </ul>
 */
public final class KnownSkus implements EventHandler {
    private record Installed(String version, Set<String> skus) { }

    private final Map<String, Set<String>> buffered = new HashMap<>();
    private volatile Installed installed;
    private volatile long nextOffset;

    @Override
    public void handle(DecodedEvent event) {
        switch (event.type()) {
            case CATALOG_ITEM_PUBLISHED -> buffer(event.payload(CatalogItemPublished.class));
            case CATALOG_SNAPSHOT_PUBLISHED -> complete(event.payload(CatalogSnapshotPublished.class));
            default -> throw new PermanentEventFailure(ParkReason.UNKNOWN_TYPE,
                    event.type().typeName() + " does not belong on the catalog topic");
        }
        if (event.offset() >= 0) nextOffset = event.offset() + 1;
    }

    /** Whether a complete catalog snapshot has been installed. */
    public boolean loaded() {
        return installed != null;
    }

    public boolean contains(String sku) {
        Installed current = installed;
        return current != null && current.skus().contains(sku);
    }

    /** The installed catalog version, if any. */
    public Optional<String> version() {
        Installed current = installed;
        return current == null ? Optional.empty() : Optional.of(current.version());
    }

    public int size() {
        Installed current = installed;
        return current == null ? 0 : current.skus().size();
    }

    /** The offset after the last catalog record handled: where a reopened stream resumes. */
    public long nextOffset() {
        return nextOffset;
    }

    private void buffer(CatalogItemPublished item) {
        if (item.getSku().isEmpty()) {
            throw new PermanentEventFailure(ParkReason.INVALID_PAYLOAD, "CatalogItemPublished has no SKU");
        }
        Installed current = installed;
        if (current != null && current.version().equals(item.getCatalogVersion())) return;
        buffered.computeIfAbsent(item.getCatalogVersion(), ignored -> new HashSet<>()).add(item.getSku());
    }

    private void complete(CatalogSnapshotPublished marker) {
        String version = marker.getCatalogVersion();
        Installed current = installed;
        if (current != null && current.version().equals(version)) {
            buffered.remove(version);
            return;
        }
        Set<String> skus = buffered.getOrDefault(version, Set.of());
        if (skus.size() != marker.getProductCount()) {
            PerfMetrics.increment("notification.catalog.incompleteSnapshots");
            return;
        }
        installed = new Installed(version, Set.copyOf(skus));
        buffered.clear();
        PerfMetrics.increment("notification.catalog.swaps");
        System.out.println("[notification] Known SKUs now at catalog version " + version + " (" + skus.size() + " SKUs)");
    }
}
