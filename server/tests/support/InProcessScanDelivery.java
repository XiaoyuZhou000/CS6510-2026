package support;

import database.ConnectionPool;
import database.JdbcRelayStore;
import events.TopicNames;
import transaction.ScanEventRelay;
import transaction.ScanEventSink;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Scan delivery for an in-process Transaction (006 US2): Transaction's real {@link ScanEventRelay} over a
 * Transaction pool publishes through a given sink, by default {@link InProcessAnalytics#sink()} on a shared
 * {@link InMemoryEventBus}, and an {@link InProcessAnalytics} over its own disposable schema ingests from that
 * bus. {@link #startRelay} replaces the running relay, as a restarted Transaction process would.
 */
public final class InProcessScanDelivery implements AutoCloseable {
    public final AnalyticsTestSchema analyticsSchema;
    public final InProcessAnalytics analytics;
    private ScanEventRelay relay;

    private InProcessScanDelivery(AnalyticsTestSchema analyticsSchema, InProcessAnalytics analytics) {
        this.analyticsSchema = analyticsSchema;
        this.analytics = analytics;
    }

    /** Analytics on {@code bus} under {@code names}; no relay runs until {@link #startRelay}. */
    public static InProcessScanDelivery over(InMemoryEventBus bus, TopicNames names) throws Exception {
        AnalyticsTestSchema schema = AnalyticsTestSchema.create();
        try {
            return new InProcessScanDelivery(schema, InProcessAnalytics.over(schema.pool(), bus, names));
        } catch (RuntimeException | Error failure) {
            schema.close();
            throw failure;
        }
    }

    /** Analytics on {@code saga}'s bus, with a relay over its Transaction's pool already running. */
    public static InProcessScanDelivery over(InProcessCheckout saga) throws Exception {
        InProcessScanDelivery delivery = over(saga.bus, saga.names);
        delivery.startRelay(saga.transaction.pool);
        return delivery;
    }

    /** (Re)starts the relay over {@code pool}, publishing onto Analytics' bus. */
    public void startRelay(ConnectionPool pool) {
        startRelay(pool, analytics.sink());
    }

    /** (Re)starts the relay over {@code pool}, publishing through {@code sink}. */
    public synchronized void startRelay(ConnectionPool pool, ScanEventSink sink) {
        stopRelay();
        relay = new ScanEventRelay(new JdbcRelayStore(pool), sink, ScanEventRelay.Settings.defaults());
        relay.start();
    }

    public synchronized void stopRelay() {
        if (relay != null) relay.close();
        relay = null;
    }

    /**
     * Waits until every accepted scan of {@code transactions} is published and stored by Analytics, then asserts
     * that Analytics holds exactly those identities, each once, at contiguous positions from 1.
     */
    public void awaitEveryScanCountedOnce(TransactionTestSchema transactions, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (transactions.undeliveredCount() > 0 || analyticsSchema.storedEvents() < transactions.scanCount()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Analytics did not catch up: unpublished=" + transactions.undeliveredCount()
                        + " accepted=" + transactions.scanCount() + " stored=" + analyticsSchema.storedEvents());
            }
            Thread.sleep(20);
        }
        assertEquals(transactions.identities(), analyticsSchema.identities(),
                "Analytics stores exactly the accepted scans, each once");
        analyticsSchema.assertPositionsContiguous();
    }

    @Override
    public void close() throws Exception {
        try (analyticsSchema) {
            stopRelay();
            analytics.close();
        }
    }
}
