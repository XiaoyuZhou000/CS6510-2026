package support;

import api.NotificationHttpHandler;
import api.OpsHttpHandler;
import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.JdbcDeadLetterStore;
import database.JdbcNotificationStore;
import database.JdbcSelectionStore;
import database.JdbcSubscriptionStore;
import database.NotificationStatusQueries;
import events.ConsumerLoop;
import events.EventTypes;
import events.TopicNames;
import messaging.CatalogSkuConsumer;
import messaging.SaleCompletedConsumer;
import notification.ChannelFaults;
import notification.ChannelProvisioner;
import notification.Channels;
import notification.ChargingTracker;
import notification.KnownSkus;
import notification.NotificationConfig;
import notification.NotificationDispatcher;
import notification.NotificationStatus;
import notification.SaleSelectionHandler;
import notification.SubscriptionIndex;
import notification.SubscriptionService;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Notification Service's components in this JVM, wired like {@code NotificationMain} but over a disposable
 * {@link NotificationTestSchema} pool and an {@link InMemoryEventBus} instead of MySQL's service schema and the
 * broker (007 T017, T044).
 *
 * <p>An instance starts its components when it is created, in {@code NotificationMain}'s order: the index rebuild,
 * the dispatcher, the catalog reader, the {@code SaleCompleted} consumer, and an HTTP server on an ephemeral port
 * with the subscription and ops routes. {@link #restart()} stops them in the shutdown order and starts a fresh set
 * over the same pool and bus, as a restarted service would; {@link #crash()} stops the dispatcher as a killed
 * process would, without its final mark, and then starts a fresh set. Channel topics are created on the bus by the
 * provisioner, and every channel send goes through {@link ChannelFaults} to {@link #channelSends}.</p>
 */
public final class InProcessNotification implements AutoCloseable {
    /** One running component, stopped in reverse start order. */
    public interface Component {
        /** A graceful stop: final commit, final mark, and so on. */
        void stop();

        /** A stop as a killed process would leave it; by default the same as {@link #stop()}. */
        default void crash() { stop(); }
    }

    /** The components of one start. */
    public static final class Running {
        public final KnownSkus knownSkus = new KnownSkus();
        public final SubscriptionIndex index = new SubscriptionIndex();
        public final ChargingTracker tracker = new ChargingTracker();
        public SubscriptionService service;
        public Channels channels;
        public NotificationDispatcher dispatcher;
        public ConsumerLoop catalogConsumer;
        public ConsumerLoop saleConsumer;
        public NotificationStatus status;
        public HttpServer server;
    }

    public final ConnectionPool pool;
    public final InMemoryEventBus bus;
    public final TopicNames topics;
    public final NotificationConfig config;
    /** Every channel send, after the fault wrapper, on its way to the bus. */
    public final CountingRecordSender channelSends;
    private final AtomicBoolean provisioningFails = new AtomicBoolean();
    private final List<Component> running = new ArrayList<>();
    private volatile Running current;
    private int starts;
    private boolean closed;

    private InProcessNotification(ConnectionPool pool, InMemoryEventBus bus, TopicNames topics,
                                  Map<String, String> environment) {
        this.pool = pool;
        this.bus = bus;
        this.topics = topics;
        this.config = NotificationConfig.fromEnvironment(environment);
        this.channelSends = new CountingRecordSender(bus);
        startComponents();
    }

    /** Over the schema's default pool and a new bus with the five fixed topics, unprefixed. */
    public static InProcessNotification over(NotificationTestSchema schema) throws Exception {
        return over(schema, Map.of());
    }

    /** As {@link #over(NotificationTestSchema)}, with {@code NOTIFICATION_*} settings such as {@code NOTIFICATION_TEST_FAULTS}. */
    public static InProcessNotification over(NotificationTestSchema schema, Map<String, String> settings) throws Exception {
        TopicNames topics = TopicNames.unprefixed();
        return new InProcessNotification(schema.pool(), InMemoryEventBus.withCatalogTopics(topics, 6510), topics, settings);
    }

    /** Over a given pool and a bus another component shares; {@code topics} names that bus's topics. */
    public static InProcessNotification over(ConnectionPool pool, InMemoryEventBus bus, TopicNames topics) {
        return new InProcessNotification(pool, bus, topics, Map.of("KAFKA_TOPIC_PREFIX", topics.prefix()));
    }

    /** As {@link #over(ConnectionPool, InMemoryEventBus, TopicNames)}, with {@code NOTIFICATION_*} settings. */
    public static InProcessNotification over(ConnectionPool pool, InMemoryEventBus bus, TopicNames topics,
                                             Map<String, String> settings) {
        Map<String, String> environment = new HashMap<>(settings);
        environment.put("KAFKA_TOPIC_PREFIX", topics.prefix());
        return new InProcessNotification(pool, bus, topics, environment);
    }

    /** Graceful stop and start over the same pool and bus. */
    public synchronized void restart() {
        ensureOpen();
        stopAll(false);
        startComponents();
    }

    /** Stop as a crash would, then start over the same pool and bus. */
    public synchronized void crash() {
        ensureOpen();
        stopAll(true);
        startComponents();
    }

    /** How many times the components were started: 1 after creation, plus one per restart or crash. */
    public synchronized int starts() { return starts; }

    /** The running components, in start order. */
    public synchronized List<Component> components() { return Collections.unmodifiableList(new ArrayList<>(running)); }

    /** The components of the current start. */
    public Running current() { return current; }

    public SubscriptionService service() { return current.service; }

    public NotificationDispatcher dispatcher() { return current.dispatcher; }

    public KnownSkus knownSkus() { return current.knownSkus; }

    public SubscriptionIndex index() { return current.index; }

    /** The ephemeral port of the current HTTP server. */
    public int httpPort() { return current.server.getAddress().getPort(); }

    /** Makes the channel provisioner fail (as a broker that refuses admin calls would) or work again. */
    public void failProvisioning(boolean fail) { provisioningFails.set(fail); }

    /** Publishes a complete catalog version of {@code skus} on the bus's catalog topic, as Inventory would. */
    public void publishCatalog(String version, List<String> skus) {
        for (String sku : skus) bus.send(topics.catalog(), sku, TestEvents.catalogItem(sku, "Item " + sku, "1.00", version)).join();
        bus.send(topics.catalog(), EventTypes.SNAPSHOT_KEY, TestEvents.catalogSnapshot(version, skus.size())).join();
    }

    /** Waits until the catalog reader installed a complete catalog. */
    public void awaitCatalog(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!current.knownSkus.loaded()) {
            if (System.nanoTime() > deadline) throw new AssertionError("the catalog was never loaded");
            Thread.sleep(10);
        }
    }

    /** Stops the components gracefully; the pool and the bus stay open for the caller. */
    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        stopAll(false);
    }

    private void startComponents() {
        starts++;
        Running components = new Running();
        Clock clock = Clock.systemUTC();
        JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(pool);
        ChannelProvisioner provisioner = topic -> {
            if (provisioningFails.get()) throw new IllegalStateException("channel provisioning refused");
            bus.createTopicIfAbsent(topic, 1);
        };
        components.channels = new Channels(subscriptions, provisioner);
        components.service = new SubscriptionService(subscriptions, components.index, components.knownSkus,
                components.channels, topics, clock);
        components.service.loadIndex();
        components.dispatcher = new NotificationDispatcher(new JdbcNotificationStore(pool), components.channels,
                new ChannelFaults(channelSends, Duration.ofSeconds(2)), components.tracker,
                NotificationDispatcher.Settings.of(config), clock);
        NotificationDispatcher dispatcher = components.dispatcher;
        dispatcher.start();
        running.add(new Component() {
            @Override public void stop() { dispatcher.stop(Duration.ofSeconds(5)); }
            @Override public void crash() { dispatcher.crash(); }
        });
        JdbcDeadLetterStore deadLetters = new JdbcDeadLetterStore(pool, "notification.deadLetter");
        components.catalogConsumer = CatalogSkuConsumer.create(topics, bus::stream, components.knownSkus, deadLetters,
                ConsumerLoop.Settings.defaults());
        ConsumerLoop catalog = components.catalogConsumer;
        catalog.start();
        running.add(() -> catalog.stop(Duration.ofSeconds(5)));
        SaleSelectionHandler selection = new SaleSelectionHandler(new JdbcSelectionStore(pool, clock), components.index,
                dispatcher::accept);
        components.saleConsumer = SaleCompletedConsumer.create(topics, bus::stream, selection, deadLetters,
                ConsumerLoop.Settings.defaults().withThreads(config.saleConsumerThreads()));
        ConsumerLoop sales = components.saleConsumer;
        sales.start();
        running.add(() -> sales.stop(Duration.ofSeconds(5)));
        components.status = new NotificationStatus(new NotificationStatusQueries(pool), components.index,
                components.knownSkus, dispatcher, () -> sales);
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext(NotificationHttpHandler.CONTEXT, new NotificationHttpHandler(components.service));
            NotificationStatus status = components.status;
            server.createContext(OpsHttpHandler.CONTEXT, new OpsHttpHandler(status::read, config.testFaults()));
            server.start();
            components.server = server;
            running.add(() -> server.stop(0));
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("cannot start Notification's HTTP server", failure);
        }
        current = components;
    }

    private void stopAll(boolean crash) {
        List<Component> reversed = new ArrayList<>(running);
        Collections.reverse(reversed);
        running.clear();
        RuntimeException first = null;
        for (Component component : reversed) {
            try {
                if (crash) component.crash();
                else component.stop();
            } catch (RuntimeException failure) {
                if (first == null) first = failure;
                else first.addSuppressed(failure);
            }
        }
        if (first != null) throw first;
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("InProcessNotification is closed");
    }
}
