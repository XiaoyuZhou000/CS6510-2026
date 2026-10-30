package support;

import analytics.AnalyticsService;
import analytics.ProductNameMap;
import analytics.ScanIngestionService;
import database.ConnectionPool;
import database.JdbcAcceptedScanReader;
import database.JdbcDeadLetterStore;
import database.JdbcPopularWindowStore;
import database.JdbcScanIngestStore;
import database.PopularWindowStore;
import database.RelayStore.UndeliveredScan;
import database.ScanIngestStore;
import events.BatchEventHandler;
import events.ConsumerLoop;
import events.TopicNames;
import messaging.KafkaScanEventSink;
import messaging.ScanAcceptedConsumer;
import transaction.ScanEventSink;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Analytics' real ingestion path in this JVM, wired like {@code AnalyticsMain}: a {@code ScanAcceptedConsumer}
 * on an {@link InMemoryEventBus} feeds {@link ScanIngestionService} over {@link JdbcScanIngestStore}, a loaded
 * {@link ProductNameMap}, and a given pool, parking into that schema's {@code dead_letter_event}. Scans reach it
 * only as {@code ScanAccepted} records published through Transaction's {@link KafkaScanEventSink}, by
 * {@link #publish} and {@link #deliver} or by a relay given {@link #sink()}.
 *
 * <p>The consumer starts with the instance and stops on {@link #close}; the window pipeline is started separately
 * with {@link #startPipeline}, so a test can stop and restart it. A second instance over the same pool and bus is
 * a restarted Analytics: it joins the same consumer group and resumes from {@code consumer_offset}.</p>
 */
public final class InProcessAnalytics implements AutoCloseable {
    private static final AtomicLong SCAN_IDS = new AtomicLong();
    private static final Duration CONSUME_TIMEOUT = Duration.ofSeconds(30);

    public final ConnectionPool pool;
    public final ProductNameMap names;
    public final JdbcScanIngestStore store;
    public final ScanIngestionService ingestion;
    public final JdbcPopularWindowStore windows;
    public final InMemoryEventBus bus;
    public final TopicNames topics;
    public final ConsumerLoop consumer;
    private final KafkaScanEventSink sink;
    private final long resumedFrom;
    private final AtomicLong stored = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong consumedNext = new AtomicLong();

    /** What ingestion reported while one {@link #deliver} call waited. */
    public record Delivery(long stored, long duplicates) { }

    private InProcessAnalytics(ConnectionPool pool, InMemoryEventBus bus, TopicNames topics) {
        this.pool = pool;
        this.bus = bus;
        this.topics = topics;
        names = new ProductNameMap();
        store = new JdbcScanIngestStore(pool, names);
        names.loadFrom(store);
        ingestion = new ScanIngestionService(store);
        windows = new JdbcPopularWindowStore(pool);
        sink = new KafkaScanEventSink(bus, topics);
        resumedFrom = store.readNextOffset(topics.scanAccepted(), 0).orElse(0L);
        BatchEventHandler counting = events -> {
            try {
                ScanIngestStore.IngestResult result = ingestion.ingest(events);
                stored.addAndGet(result.stored());
                duplicates.addAndGet(result.duplicates());
                consumedNext.accumulateAndGet(events.getLast().offset() + 1, Math::max);
            } catch (RuntimeException failure) {
                failures.incrementAndGet();
                throw failure;
            }
        };
        consumer = ScanAcceptedConsumer.create(topics, bus::stream, counting, ingestion::nextOffset,
                new JdbcDeadLetterStore(pool, "analytics.deadLetter"), ConsumerLoop.Settings.defaults());
        consumer.start();
    }

    /** Ingests from its own bus into the schema's default pool. */
    public static InProcessAnalytics over(AnalyticsTestSchema schema) throws Exception {
        return over(schema.pool());
    }

    /** Ingests from its own bus into a given pool, for example one with a test decorator. */
    public static InProcessAnalytics over(ConnectionPool pool) throws Exception {
        TopicNames topics = TopicNames.unprefixed();
        return new InProcessAnalytics(pool, InMemoryEventBus.withCatalogTopics(topics, 6510), topics);
    }

    /** Ingests from {@code bus}, which a relay or an earlier instance shares; its topics are {@code topics}'. */
    public static InProcessAnalytics over(ConnectionPool pool, InMemoryEventBus bus, TopicNames topics) {
        return new InProcessAnalytics(pool, bus, topics);
    }

    /** Transaction's sink onto this instance's bus, for a relay. */
    public ScanEventSink sink() {
        return sink;
    }

    /** Publishes {@code events} through {@link KafkaScanEventSink} in batches of 500, without waiting for ingestion. */
    public void publish(List<UndeliveredScan> events) {
        for (int from = 0; from < events.size(); from += 500) {
            ScanEventSink.DeliveryResult result = sink.deliver(events.subList(from, Math.min(events.size(), from + 500)));
            if (!(result instanceof ScanEventSink.Delivered)) throw new AssertionError("Publishing failed: " + result);
        }
    }

    /** Publishes {@code events} and waits until they are consumed; returns what ingestion reported meanwhile. */
    public Delivery deliver(List<UndeliveredScan> events) throws InterruptedException {
        long storedBefore = stored.get();
        long duplicatesBefore = duplicates.get();
        publish(events);
        awaitConsumed(CONSUME_TIMEOUT);
        return new Delivery(stored.get() - storedBefore, duplicates.get() - duplicatesBefore);
    }

    /**
     * Waits until every record now on the scan topic was consumed: ingested by this instance, committed by the
     * group (which also covers a parked record), or covered by the offset this instance resumed from.
     */
    public void awaitConsumed(Duration timeout) throws InterruptedException {
        String topic = topics.scanAccepted();
        String group = topics.group(TopicNames.ANALYTICS_SCAN_INGEST_GROUP);
        long end = bus.endOffset(topic, 0);
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            Long committed = bus.committedOffset(group, topic, 0);
            if (consumedNext.get() >= end || resumedFrom >= end || (committed != null && committed >= end)) return;
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Analytics consumed " + consumedNext.get() + " of " + end + " scan records");
            }
            Thread.sleep(5);
        }
    }

    /** Waits until ingestion failed at least {@code count} times, for example at an armed fault hook. */
    public void awaitFailures(long count, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (failures.get() < count) {
            if (System.nanoTime() > deadline) throw new AssertionError("Ingestion failed only " + failures.get() + " times");
            Thread.sleep(5);
        }
    }

    /** Scans this instance's ingestion stored, and records it found already stored, since it started. */
    public long stored() { return stored.get(); }

    public long duplicates() { return duplicates.get(); }

    public long failures() { return failures.get(); }

    /** Starts the durable window pipeline over this pool: prewarm from the checkpoint, then polling. */
    public AnalyticsService startPipeline() {
        return startPipeline(windows);
    }

    public AnalyticsService startPipeline(PopularWindowStore windowStore) {
        return new AnalyticsService(windowStore, new JdbcAcceptedScanReader(pool));
    }

    /** One accepted scan as Transaction's relay selects it, at price 1.00, accepted now. */
    public static UndeliveredScan event(String transactionId, int scanSequence, String sku, String name) {
        return new UndeliveredScan(SCAN_IDS.incrementAndGet(), transactionId, scanSequence, sku, name,
                new BigDecimal("1.00"), Instant.now());
    }

    /** One event per SKU, under transaction {@code transactionId} with sequences {@code first, first + 1, ...}. */
    public static List<UndeliveredScan> events(String transactionId, int first, List<String> skus) {
        List<UndeliveredScan> events = new ArrayList<>(skus.size());
        for (int i = 0; i < skus.size(); i++) {
            String sku = skus.get(i);
            events.add(event(transactionId, first + i, sku, nameOf(sku)));
        }
        return events;
    }

    /**
     * Delivers {@code skus} under fresh identities of transaction {@code transactionId}, starting at sequence 1,
     * and returns the total stored.
     */
    public long deliverAll(String transactionId, List<String> skus) throws InterruptedException {
        return deliver(events(transactionId, 1, skus)).stored();
    }

    /** The fixture name for {@code sku}: {@code skuN} is "Item N"; any other SKU is "Name of <sku>". */
    public static String nameOf(String sku) {
        return sku.matches("sku\\d+") ? "Item " + sku.substring(3) : "Name of " + sku;
    }

    /** Stops the consumer, as an Analytics shutdown would; the pool stays open. */
    @Override
    public void close() {
        consumer.stop(Duration.ofSeconds(5));
    }
}
