package notification;

import database.NotificationStatusQueries;
import events.ConsumerLoop;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Notification's operational status (FR-039, contracts/notification-http.md "Operational endpoints"): the durable
 * counts from {@link NotificationStatusQueries} together with what only the running components know, the index
 * version, the catalog state, the sale consumer's lag and stalls, the dispatcher's alarms, and the last confirmed
 * send. {@code GET /notifications/ops/status} and the status line show the same fields.
 */
public final class NotificationStatus {
    public record Snapshot(long subscriptionVersion, long indexVersion, long activeSubscribers, boolean catalogLoaded,
                           long salesSelected, long pending, long delivered, long parked, long chargedFailures,
                           long unchargedFailures, long parkedEvents, List<String> alarms, long saleConsumerLag,
                           Optional<Instant> lastConfirmedSendAt) {
        public Snapshot {
            alarms = List.copyOf(alarms);
        }

        public String line() {
            return "status service=notification pending_work=" + (pending > 0 ? "yes" : "no")
                    + " subscription_version=" + subscriptionVersion + " index_version=" + indexVersion
                    + " active_subscribers=" + activeSubscribers + " catalog_loaded=" + catalogLoaded
                    + " sales_selected=" + salesSelected + " notifications_pending=" + pending
                    + " notifications_delivered=" + delivered + " notifications_parked=" + parked
                    + " charged_failures=" + chargedFailures + " uncharged_failures=" + unchargedFailures
                    + " parked_events=" + parkedEvents + " sale_consumer_lag=" + saleConsumerLag
                    + " last_confirmed_send=" + lastConfirmedSendAt.map(Instant::toString).orElse("none")
                    + " alarms=" + (alarms.isEmpty() ? "none" : String.join("; ", alarms));
        }
    }

    private final NotificationStatusQueries queries;
    private final SubscriptionIndex index;
    private final KnownSkus knownSkus;
    private final NotificationDispatcher dispatcher;
    private final Supplier<ConsumerLoop> saleConsumer;

    public NotificationStatus(NotificationStatusQueries queries, SubscriptionIndex index, KnownSkus knownSkus,
                              NotificationDispatcher dispatcher, Supplier<ConsumerLoop> saleConsumer) {
        this.queries = Objects.requireNonNull(queries, "queries");
        this.index = Objects.requireNonNull(index, "index");
        this.knownSkus = Objects.requireNonNull(knownSkus, "knownSkus");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.saleConsumer = Objects.requireNonNull(saleConsumer, "saleConsumer");
    }

    public Snapshot read() {
        NotificationStatusQueries.Status durable = queries.read();
        List<String> alarms = new ArrayList<>(dispatcher.alarms());
        ConsumerLoop consumer = saleConsumer.get();
        long lag = 0;
        if (consumer != null) {
            lag = consumer.lag();
            for (ConsumerLoop.StalledPartition stalled : consumer.stalledPartitions()) {
                alarms.add("SALE_CONSUMER_STALLED: " + stalled.topic() + "-" + stalled.partition() + "@" + stalled.offset()
                        + " for " + stalled.age().toSeconds() + " s");
            }
        }
        if (durable.parked() > 0) alarms.add("PARKED_NOTIFICATIONS: " + durable.parked() + " notifications are parked");
        if (durable.parkedEvents() > 0) alarms.add("PARKED_EVENTS: " + durable.parkedEvents() + " records are parked");
        return new Snapshot(durable.subscriptionVersion(), index.version(), durable.activeSubscribers(), knownSkus.loaded(),
                durable.saleSelections(), durable.pending(), durable.delivered(), durable.parked(),
                durable.chargedFailures(), durable.unchargedFailures(), durable.parkedEvents(), alarms, lag,
                dispatcher.lastConfirmedSend());
    }
}
