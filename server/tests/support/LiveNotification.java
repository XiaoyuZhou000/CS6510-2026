package support;

import database.ConnectionPool;
import database.JdbcDeadLetterStore;
import database.JdbcNotificationStore;
import database.JdbcSelectionStore;
import database.JdbcSubscriptionStore;
import database.NotificationStatusQueries;
import events.ConsumerLoop;
import events.TopicNames;
import messaging.ChannelSender;
import messaging.KafkaChannelProvisioner;
import messaging.KafkaRecordSender;
import messaging.SaleCompletedConsumer;
import notification.ChannelFaults;
import notification.Channels;
import notification.ChargingTracker;
import notification.KnownSkus;
import notification.NotificationDispatcher;
import notification.NotificationStatus;
import notification.SaleSelectionHandler;
import notification.SubscriptionIndex;
import notification.SubscriptionService;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

/**
 * Notification's components in this JVM over the live broker and a disposable schema, wired as
 * {@code NotificationMain} wires them (007 T097, T098): the subscription service with a catalog of the given SKUs,
 * the dispatcher behind {@link ChannelFaults} and the shared channel producer, and the {@code SaleCompleted}
 * consumer group of {@code names}. Started on creation; {@link #close()} stops them in the shutdown order.
 */
public final class LiveNotification implements AutoCloseable {
    public final TopicNames names;
    public final SubscriptionService service;
    public final NotificationDispatcher dispatcher;
    public final ConsumerLoop sales;
    public final NotificationStatus status;
    public final String group;
    private final int saleThreads;
    private final KafkaChannelProvisioner provisioner;
    private final KafkaRecordSender channelSender;

    private LiveNotification(ConnectionPool pool, TopicNames names, List<String> skus, String clientId,
                             int saleThreads) {
        this.names = names;
        this.saleThreads = saleThreads;
        Clock clock = Clock.systemUTC();
        provisioner = new KafkaChannelProvisioner(KafkaTestSupport.bootstrap());
        channelSender = ChannelSender.lazy(KafkaTestSupport.clientEnvironment());
        try {
            JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(pool);
            KnownSkus knownSkus = new KnownSkus();
            for (String sku : skus) knownSkus.handle(TestEvents.item(sku, "Item " + sku, "1.00", "v1"));
            knownSkus.handle(TestEvents.snapshot("v1", skus.size()));
            SubscriptionIndex index = new SubscriptionIndex();
            Channels channels = new Channels(subscriptions, provisioner);
            service = new SubscriptionService(subscriptions, index, knownSkus, channels, names, clock);
            service.loadIndex();
            dispatcher = new NotificationDispatcher(new JdbcNotificationStore(pool), channels,
                    new ChannelFaults(channelSender), new ChargingTracker(), NotificationDispatcher.Settings.defaults(), clock);
            group = names.group(TopicNames.NOTIFICATION_SALE_COMPLETED_GROUP);
            sales = SaleCompletedConsumer.create(names, KafkaTestSupport.streams(clientId, group),
                    new SaleSelectionHandler(new JdbcSelectionStore(pool, clock), index, dispatcher::accept),
                    new JdbcDeadLetterStore(pool, "notification.deadLetter"),
                    ConsumerLoop.Settings.defaults().withThreads(saleThreads));
            status = new NotificationStatus(new NotificationStatusQueries(pool), index, knownSkus, dispatcher,
                    () -> sales);
            dispatcher.start();
            sales.start();
        } catch (RuntimeException failure) {
            channelSender.close();
            provisioner.close();
            throw failure;
        }
    }

    /** Over {@code names}' topics, which must exist, with {@code skus} as the catalog. */
    public static LiveNotification start(NotificationTestSchema schema, TopicNames names, List<String> skus,
                                         String clientId, int saleThreads) throws SQLException {
        return new LiveNotification(schema.pool(), names, skus, clientId, saleThreads);
    }

    /** Waits until the sale consumer group, one member per thread, owns every partition of the sale topic. */
    public void awaitAssigned(int partitions, Duration timeout) {
        KafkaTestSupport.awaitGroupAssigned(group, names.saleCompleted(), partitions, saleThreads, timeout);
    }

    @Override
    public void close() {
        try {
            sales.stop(Duration.ofSeconds(5));
            dispatcher.stop(Duration.ofSeconds(5));
        } finally {
            channelSender.close();
            provisioner.close();
        }
    }
}
