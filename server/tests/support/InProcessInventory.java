package support;

import database.ConnectionPool;
import database.JdbcCatalogMetaStore;
import database.JdbcCatalogStore;
import database.JdbcCheckoutOperationStore;
import database.JdbcDeadLetterStore;
import database.JdbcOutboxStore;
import events.ConsumerLoop;
import events.OutboxDispatcher;
import events.RecordSender;
import events.RecordStream;
import events.TopicNames;
import inventory.CatalogCache;
import inventory.CatalogPublisher;
import inventory.CheckoutRequestHandler;
import inventory.CheckoutStockService;
import inventory.StockOutcomeEvents;
import inventory.StockRestorationHandler;
import messaging.CheckoutEventConsumer;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The real Inventory event paths in this JVM (006 T059, T113): {@link CheckoutStockService} over the real
 * adapters and an {@link InventoryTestSchema}, consuming {@code CheckoutRequested} as group
 * {@code inventory-checkout} and publishing its outcome events through its own {@code outbox_event} and an
 * {@link OutboxDispatcher}, on an {@link InMemoryEventBus} (by default) or the live broker. Several instances
 * over one schema and one transport behave like several Inventory processes, each with its own pool and
 * group member. The catalog is published, as {@code InventoryMain} does at startup, only when a test calls
 * {@link #publishCatalog()}.
 */
public final class InProcessInventory implements AutoCloseable {
    public final ConnectionPool pool;
    public final CatalogCache catalog;
    public final TopicNames names;
    /** Null when the transport is the live broker. */
    public final InMemoryEventBus bus;
    public final JdbcOutboxStore outbox;
    public final JdbcDeadLetterStore deadLetters;
    public final OutboxDispatcher dispatcher;
    public final CheckoutStockService stock;
    public final ConsumerLoop checkoutConsumer;
    public final CatalogPublisher catalogPublisher;
    private final RecordSender sender;
    private final boolean ownsSender;
    private boolean closed;

    private InProcessInventory(InventoryTestSchema schema, ConnectionPool pool, InMemoryEventBus sharedBus,
                               TopicNames topicNames, boolean live, int consumerThreads) throws Exception {
        this.pool = pool;
        schema.closeWith(this);
        JdbcCatalogMetaStore meta = new JdbcCatalogMetaStore(pool);
        catalog = new CatalogCache(new JdbcCatalogStore(pool), meta);
        Supplier<RecordStream> streams;
        if (live) {
            names = topicNames != null ? topicNames : KafkaTestSupport.runTopics();
            KafkaTestSupport.createCatalogTopics(names);
            bus = null;
            sender = KafkaTestSupport.sender("test-inventory-outcome", 0);
            ownsSender = true;
            streams = KafkaTestSupport.streams("test-inventory-checkout", names.group(TopicNames.INVENTORY_CHECKOUT_GROUP));
        } else {
            names = topicNames != null ? topicNames : TopicNames.unprefixed();
            bus = sharedBus != null ? sharedBus : InMemoryEventBus.withCatalogTopics(names, 6510);
            sender = bus;
            ownsSender = false;
            streams = bus::stream;
        }
        outbox = new JdbcOutboxStore(pool, "inventory.outbox");
        deadLetters = new JdbcDeadLetterStore(pool, "inventory.deadLetter");
        dispatcher = new OutboxDispatcher("inventory", outbox, Map.of(names.checkoutOutcome(), sender, names.catalog(), sender),
                List.of(names.checkoutOutcome(), names.catalog()), OutboxDispatcher.Settings.defaults(), Clock.systemUTC());
        catalogPublisher = new CatalogPublisher(catalog, meta, names, dispatcher, Clock.systemUTC());
        stock = new CheckoutStockService(new JdbcCheckoutOperationStore(pool), new StockOutcomeEvents(names, Clock.systemUTC()),
                dispatcher);
        checkoutConsumer = CheckoutEventConsumer.create(names, streams, new CheckoutRequestHandler(stock),
                new StockRestorationHandler(stock), deadLetters, ConsumerLoop.Settings.defaults().withThreads(consumerThreads));
        dispatcher.start();
        checkoutConsumer.start();
    }

    /** Serves the schema's own default pool on a bus of its own. */
    public static InProcessInventory over(InventoryTestSchema schema) throws Exception {
        return new InProcessInventory(schema, schema.pool(), null, null, false, 2);
    }

    /** Serves the schema's own default pool on a shared bus. */
    public static InProcessInventory over(InventoryTestSchema schema, InMemoryEventBus bus, TopicNames names)
            throws Exception {
        return new InProcessInventory(schema, schema.pool(), bus, names, false, 2);
    }

    /** Serves a pool of its own on a shared bus, for a second independent instance. */
    public static InProcessInventory overNewPool(InventoryTestSchema schema, String metricsName, int initial, int max,
                                                 InMemoryEventBus bus, TopicNames names) throws Exception {
        return new InProcessInventory(schema, schema.newPool(metricsName, initial, max, null), bus, names, false, 2);
    }

    /** Serves the schema's default pool on the live broker, with {@code names} or a fresh per-run prefix. */
    public static InProcessInventory overLiveKafka(InventoryTestSchema schema, TopicNames names) throws Exception {
        return new InProcessInventory(schema, schema.pool(), null, names, true, 2);
    }

    /** Records and signals the catalog as {@code InventoryMain} does at startup; empty when never seeded. */
    public Optional<CatalogPublisher.Publication> publishCatalog() {
        return catalogPublisher.publish();
    }

    @Override
    public synchronized void close() throws InterruptedException {
        if (closed) return;
        closed = true;
        checkoutConsumer.stop(Duration.ofSeconds(5));
        dispatcher.stop(Duration.ofSeconds(5));
        if (ownsSender) sender.close();
    }
}
