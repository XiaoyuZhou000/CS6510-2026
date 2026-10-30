package messaging;

import analytics.AnalyticsService;
import analytics.ProductNameMap;
import analytics.ScanIngestionService;
import database.JdbcAcceptedScanReader;
import database.JdbcDeadLetterStore;
import database.JdbcPopularWindowStore;
import database.JdbcRelayStore;
import database.JdbcScanIngestStore;
import events.ConsumerLoop;
import events.TopicNames;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.KafkaTestSupport;
import support.TransactionTestSchema;
import transaction.ScanEventRelay;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Scan delivery across broker restarts mid-stream (SC-005, SC-007; contracts/scan-event-delivery.md section 7),
 * with {@code KAFKA_TEST_BROKER_CONTROL=1}: a steady stream of 20,000 accepted scans is committed into
 * {@code accepted_scan}, and Transaction's real relay publishes it through the live broker to Analytics' real
 * consumer and window pipeline while the broker restarts at once, and while it stays down for 10 s, both during
 * the stream. Confirmed records survive in the log and unconfirmed ones are re-sent from
 * {@code accepted_scan}, so no scan is lost or counted twice, and every window equals an independent calculation.
 */
class BrokerRestartScanTest {
    private static final int SCANS = 20_000;
    private static final int PER_TRANSACTION = 500;
    private static final int ROWS_PER_INSERT = 10;
    private static final Duration DRAIN = Duration.ofSeconds(240);

    @Test
    void anImmediateBrokerRestartMidStreamLosesAndDoubleCountsNoScan() throws Exception {
        runAcrossBrokerOutage(Duration.ZERO);
    }

    @Test
    void aBrokerStoppedForTenSecondsMidStreamLosesAndDoubleCountsNoScan() throws Exception {
        runAcrossBrokerOutage(Duration.ofSeconds(10));
    }

    private static void runAcrossBrokerOutage(Duration down) throws Exception {
        KafkaTestSupport.requireBrokerControl();
        TopicNames names = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(names);
        try (TransactionTestSchema transactions = TransactionTestSchema.create();
             AnalyticsTestSchema analytics = AnalyticsTestSchema.create(AnalyticsTestSchema.Options.defaults().pool(4, 8))) {
            ProductNameMap productNames = new ProductNameMap();
            JdbcScanIngestStore store = new JdbcScanIngestStore(analytics.pool(), productNames);
            productNames.loadFrom(store);
            String group = names.group(TopicNames.ANALYTICS_SCAN_INGEST_GROUP);
            ConsumerLoop consumer = ScanAcceptedConsumer.create(names, KafkaTestSupport.streams("test-restart-ingest", group),
                    new ScanIngestionService(store), new JdbcDeadLetterStore(analytics.pool(), "analytics.deadLetter"),
                    ConsumerLoop.Settings.defaults());
            AnalyticsService pipeline = new AnalyticsService(new JdbcPopularWindowStore(analytics.pool()),
                    new JdbcAcceptedScanReader(analytics.pool()));
            try (KafkaRecordSender sender = KafkaTestSupport.sender("test-restart-scan", KafkaClients.DEFAULT_BATCH_LINGER_MS);
                 ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(transactions.pool()),
                         new KafkaScanEventSink(sender, names), ScanEventRelay.Settings.defaults())) {
                consumer.start();
                relay.start();
                AtomicLong inserted = new AtomicLong();
                CompletableFuture<Void> stream = CompletableFuture.runAsync(() -> produce(transactions, inserted));
                awaitAtLeast(inserted::get, 3_000, "the stream to start");
                // On Windows every stop is a forced kill, so an immediate restart is a stop followed at once by a start.
                KafkaTestSupport.stopBroker();
                assertTrue(inserted.get() < SCANS, "the broker stopped mid-stream, at scan " + inserted.get());
                // Nothing is confirmed while the broker is down, so the scans the stream commits stay unpublished.
                JdbcRelayStore outbox = new JdbcRelayStore(transactions.pool());
                awaitAtLeast(outbox::countUndelivered, 100, "unpublished scans to accumulate during the outage");
                if (!down.isZero()) Thread.sleep(down.toMillis());
                KafkaTestSupport.startBroker();
                stream.get(DRAIN.toSeconds(), TimeUnit.SECONDS);

                awaitAtLeast(() -> SCANS - transactions.undeliveredCount(), SCANS, "the relay to publish every scan");
                awaitAtLeast(analytics::storedEvents, SCANS, "Analytics to ingest every scan");
                awaitAtLeast(analytics::checkpoint, SCANS, "the window ending at 20,000 to be published");
            } finally {
                consumer.stop(Duration.ofSeconds(10));
                pipeline.shutdown();
            }
            assertEquals(transactions.identities(), analytics.identities(), "every accepted scan is counted exactly once");
            assertEquals(SCANS, analytics.lastPosition(), "no scan holds a second position");
            analytics.assertPositionsContiguous();
            analytics.assertWindowsMatchPositions(SCANS);
            assertEquals(0, analytics.queryLong("SELECT COUNT(*) FROM dead_letter_event"), "nothing was parked");
        }
    }

    /**
     * Commits the 20,000 scans as a steady stream of 1,000 per second, ten per INSERT, as Scan requests would;
     * a scan never waits for the broker, so the stream keeps its pace through the outage.
     */
    private static void produce(TransactionTestSchema transactions, AtomicLong inserted) {
        try {
            long started = System.nanoTime();
            for (int first = 0; first < SCANS; first += ROWS_PER_INSERT) {
                long wait = started + TimeUnit.MILLISECONDS.toNanos(first / ROWS_PER_INSERT * 10L) - System.nanoTime();
                if (wait > 0) TimeUnit.NANOSECONDS.sleep(wait);
                String id = "tx-restart-" + first / PER_TRANSACTION;
                int sequence = first % PER_TRANSACTION + 1;
                if (sequence == 1) transactions.insertOpenTransaction(id);
                transactions.insertScans(id, sequence, ROWS_PER_INSERT, n -> "sku" + (1 + ((sequence + n) * 7) % 12));
                inserted.addAndGet(ROWS_PER_INSERT);
            }
        } catch (Exception failure) {
            throw new IllegalStateException("The scan stream failed", failure);
        }
    }

    @FunctionalInterface
    private interface Measure {
        long get() throws Exception;
    }

    private static void awaitAtLeast(Measure measure, long target, String what) throws Exception {
        long deadline = System.nanoTime() + DRAIN.toNanos();
        long value;
        while ((value = measure.get()) < target) {
            if (System.nanoTime() > deadline) fail("Timed out waiting for " + what + ": " + value + " of " + target);
            Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
        }
    }
}
