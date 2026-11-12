package support;

import database.ConnectionPool;
import events.TopicNames;
import notification.NotificationConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Notification Service's components in this JVM, wired like {@code NotificationMain} but over a disposable
 * {@link NotificationTestSchema} pool and an {@link InMemoryEventBus} instead of MySQL's service schema and the
 * broker (007 T017).
 *
 * <p>An instance starts its components when it is created. {@link #restart()} stops them in the shutdown order
 * of {@code NotificationMain} and starts a fresh set over the same pool and bus, as a restarted service would;
 * {@link #crash()} stops them as a killed process would, without their final commit or mark, and then starts a
 * fresh set. The user stories add each component to {@link #startComponents()}: the subscription service, the
 * {@code SaleCompleted} and catalog consumers, and the delivery dispatcher and its workers.</p>
 */
public final class InProcessNotification implements AutoCloseable {
    /** One running component, stopped in reverse start order. */
    public interface Component {
        /** A graceful stop: final commit, final mark, and so on. */
        void stop();

        /** A stop as a killed process would leave it; by default the same as {@link #stop()}. */
        default void crash() { stop(); }
    }

    public final ConnectionPool pool;
    public final InMemoryEventBus bus;
    public final TopicNames topics;
    public final NotificationConfig config;
    private final List<Component> running = new ArrayList<>();
    private int starts;
    private boolean closed;

    private InProcessNotification(ConnectionPool pool, InMemoryEventBus bus, TopicNames topics,
                                  Map<String, String> environment) {
        this.pool = pool;
        this.bus = bus;
        this.topics = topics;
        this.config = NotificationConfig.fromEnvironment(environment);
        startComponents();
    }

    /** Over the schema's default pool and a new bus with the five fixed topics, unprefixed. */
    public static InProcessNotification over(NotificationTestSchema schema) throws Exception {
        TopicNames topics = TopicNames.unprefixed();
        return new InProcessNotification(schema.pool(), InMemoryEventBus.withCatalogTopics(topics, 6510), topics, Map.of());
    }

    /** Over a given pool and a bus another component shares; {@code topics} names that bus's topics. */
    public static InProcessNotification over(ConnectionPool pool, InMemoryEventBus bus, TopicNames topics) {
        return new InProcessNotification(pool, bus, topics, Map.of("KAFKA_TOPIC_PREFIX", topics.prefix()));
    }

    /** As {@link #over(ConnectionPool, InMemoryEventBus, TopicNames)}, with {@code NOTIFICATION_*} settings. */
    public static InProcessNotification over(ConnectionPool pool, InMemoryEventBus bus, TopicNames topics,
                                             Map<String, String> settings) {
        Map<String, String> environment = new java.util.HashMap<>(settings);
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

    /** Stops the components gracefully; the pool and the bus stay open for the caller. */
    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        stopAll(false);
    }

    private void startComponents() {
        starts++;
        // The user stories add their components here, in NotificationMain's start order.
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
