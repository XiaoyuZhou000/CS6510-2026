package inventory;

import com.google.protobuf.Message;
import database.CatalogMetaStore;
import events.EnvelopeCodec;
import events.EventTypes;
import events.OutboxRow;
import events.OutboxSignal;
import events.TopicNames;
import metrics.PerfMetrics;
import selfcheckout.events.v1.CatalogItemPublished;
import selfcheckout.events.v1.CatalogSnapshotPublished;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Publishes Inventory's whole catalog at every startup (D9, FR-018). One local transaction records a
 * {@code CatalogItemPublished} outbox row per product, in SKU order (key SKU; SKU, name, price as a decimal
 * string with two fraction digits, {@code catalog_version}), followed by one {@code CatalogSnapshotPublished}
 * (key {@code __snapshot__}; the version and {@code product_count}), and sets {@code catalog_meta.published_at}.
 * After the commit the dispatcher is signalled with the IDs in {@code outbox_seq} order, so the marker reaches
 * the single-partition topic after its items.
 *
 * <p>The catalog changes only through reset and reseed, while Inventory is stopped, so publishing at startup
 * covers every change and heals a recreated topic. Every row's correlation ID is the version, and its
 * aggregate key is null.</p>
 */
public final class CatalogPublisher {
    private final CatalogCache catalog;
    private final CatalogMetaStore meta;
    private final TopicNames names;
    private final OutboxSignal signal;
    private final Clock clock;

    public CatalogPublisher(CatalogCache catalog, CatalogMetaStore meta, TopicNames names, OutboxSignal signal,
                            Clock clock) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.meta = Objects.requireNonNull(meta, "meta");
        this.names = Objects.requireNonNull(names, "names");
        this.signal = Objects.requireNonNull(signal, "signal");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** One publication: its version, product count, and the recorded event IDs, the marker last. */
    public record Publication(String catalogVersion, int productCount, List<String> eventIds) {
        public Publication {
            eventIds = List.copyOf(eventIds);
        }
    }

    /**
     * Records and signals the cached catalog; empty, with nothing recorded, when the catalog was never seeded.
     * A database failure propagates and fails the caller's startup.
     */
    public Optional<Publication> publish() {
        CatalogCache.Snapshot snapshot = catalog.snapshot();
        String version = snapshot.version();
        if (version == null) {
            System.err.println("[inventory] The catalog is not seeded; nothing to publish");
            return Optional.empty();
        }
        Instant now = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        List<OutboxRow> rows = new ArrayList<>(snapshot.products().size() + 1);
        for (CatalogOperations.CatalogItemView product : snapshot.products()) {
            rows.add(row(EventTypes.CATALOG_ITEM_PUBLISHED, product.sku(), version, now,
                    CatalogItemPublished.newBuilder().setSku(product.sku()).setName(product.name())
                            .setPrice(product.price().setScale(2).toPlainString()).setCatalogVersion(version).build()));
        }
        rows.add(row(EventTypes.CATALOG_SNAPSHOT_PUBLISHED, EventTypes.SNAPSHOT_KEY, version, now,
                CatalogSnapshotPublished.newBuilder().setCatalogVersion(version)
                        .setProductCount(snapshot.products().size()).build()));
        meta.publish(rows, now);
        List<String> eventIds = rows.stream().map(OutboxRow::eventId).toList();
        signal.signal(eventIds);
        PerfMetrics.increment("inventory.catalog.publications");
        System.out.println("[inventory] Published catalog version " + version + " (" + snapshot.products().size()
                + " products) through the outbox");
        return Optional.of(new Publication(version, snapshot.products().size(), eventIds));
    }

    private OutboxRow row(EventTypes type, String key, String version, Instant now, Message payload) {
        String eventId = UUID.randomUUID().toString();
        byte[] envelope = EnvelopeCodec.encode(eventId, type, key, version, EventTypes.INVENTORY, now.toEpochMilli(),
                payload);
        return new OutboxRow(eventId, names.name(type.topic()), key, type.typeName(), null, envelope, now);
    }
}
