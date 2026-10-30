package messaging;

import analytics.ProductNameMap;
import analytics.ScanIngestionService;
import database.JdbcDeadLetterStore;
import database.JdbcScanIngestStore;
import database.RelayStore.UndeliveredScan;
import events.ConsumerLoop;
import events.DecodedEvent;
import events.EnvelopeCodec;
import events.RecordSender;
import events.RecordStream;
import events.TopicNames;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.ScanAccepted;
import support.AnalyticsTestSchema;
import support.KafkaTestSupport;
import support.ServiceProcesses;
import transaction.ScanEventSink;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The scan event contract over the live broker (FR-014, FR-019, FR-048, FR-050; replaces
 * rpc/AnalyticsIngestContractTest#realPortServesTheSameContract): Transaction's real {@link KafkaScanEventSink}
 * publishes {@code ScanAccepted} records with every field of contracts/scan-event-delivery.md, and Analytics'
 * real {@link ScanAcceptedConsumer} stores them at consecutive positions in record order, under a per-run topic
 * prefix. {@code Delivered} means every record was confirmed by the broker.
 */
class ScanEventContractTest {
    private static final Instant ACCEPTED = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    @Test
    void aRealSinkAndARealConsumerExchangeScanAcceptedWithEveryField() throws Exception {
        KafkaTestSupport.requireBroker();
        TopicNames names = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(names);
        List<UndeliveredScan> scans = List.of(
                scan(11, "tx-contract-a", 1, "SKU-000001", "Item 1", "1.25"),
                scan(12, "tx-contract-b", 1, "SKU-000002", "Item 2", "0.50"),
                scan(13, "tx-contract-a", 2, "SKU-000003", "Item 3", "12.00"));
        try (RecordSender sender = KafkaTestSupport.sender("test-scan-contract", 5)) {
            assertEquals(new ScanEventSink.Delivered(3), new KafkaScanEventSink(sender, names).deliver(scans));
        }

        List<RecordStream.Record> records = read(names.scanAccepted(), scans.size());
        for (int i = 0; i < scans.size(); i++) {
            UndeliveredScan expected = scans.get(i);
            RecordStream.Record record = records.get(i);
            assertEquals(i, record.offset(), "records keep the batch's scan_id order on the single partition");
            assertEquals(expected.transactionId(), record.key(), "the record key is the transaction ID");
            DecodedEvent event = EnvelopeCodec.decode(record.value(), record.key());
            assertEquals("scan:" + expected.transactionId() + ":" + expected.scanSequence(), event.eventId());
            assertEquals("ScanAccepted", event.type().typeName());
            assertEquals(1, event.schemaVersion());
            assertEquals("transaction", event.producer());
            assertEquals(expected.transactionId(), event.routingKey());
            assertEquals(expected.transactionId(), event.correlationId());
            assertEquals(ACCEPTED.toEpochMilli(), event.occurredAtMs());
            ScanAccepted payload = event.payload(ScanAccepted.class);
            assertEquals(expected.transactionId(), payload.getTransactionId());
            assertEquals(expected.scanSequence(), payload.getScanSequence());
            assertEquals(expected.sku(), payload.getSku());
            assertEquals(expected.productName(), payload.getProductName(), "the name captured at scan time");
            assertEquals(expected.unitPrice().toPlainString(), payload.getUnitPrice(),
                    "the captured price is a decimal string with two fraction digits");
            assertEquals(ACCEPTED.toEpochMilli(), payload.getAcceptedAtMs(), "the acceptance time");
        }

        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            ProductNameMap productNames = new ProductNameMap();
            JdbcScanIngestStore store = new JdbcScanIngestStore(schema.pool(), productNames);
            productNames.loadFrom(store);
            String group = names.group(TopicNames.ANALYTICS_SCAN_INGEST_GROUP);
            ConsumerLoop consumer = ScanAcceptedConsumer.create(names, KafkaTestSupport.streams("test-scan-ingest", group),
                    new ScanIngestionService(store), new JdbcDeadLetterStore(schema.pool(), "analytics.deadLetter"),
                    ConsumerLoop.Settings.defaults());
            consumer.start();
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (store.readNextOffset(names.scanAccepted(), 0).orElse(0) < 3) {
                    if (System.nanoTime() > deadline) fail("Analytics did not ingest the three records");
                    Thread.sleep(50);
                }
                assertEquals(List.of(new AnalyticsTestSchema.StoredEvent(1, "tx-contract-a", 1, "SKU-000001"),
                                new AnalyticsTestSchema.StoredEvent(2, "tx-contract-b", 1, "SKU-000002"),
                                new AnalyticsTestSchema.StoredEvent(3, "tx-contract-a", 2, "SKU-000003")),
                        schema.events(), "consecutive positions in record order");
                assertEquals("Item 2", schema.productName("SKU-000002"), "captured names land in product_name");
                KafkaTestSupport.awaitNoLag(group, names.scanAccepted(), Duration.ofSeconds(30));
                assertEquals(3, KafkaTestSupport.committedOffset(group, names.scanAccepted(), 0),
                        "the broker offset is committed after the ingestion commit");
                assertEquals(0, consumer.parked());
            } finally {
                consumer.stop(Duration.ofSeconds(10));
            }
        }
    }

    @Test
    void deliveredIsReturnedOnlyAfterEveryConfirmation() throws Exception {
        KafkaTestSupport.requireBroker();
        TopicNames names = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(names);
        List<UndeliveredScan> scans = new ArrayList<>();
        for (int i = 1; i <= 50; i++) scans.add(scan(i, "tx-confirm", i, "SKU-000001", "Item 1", "1.00"));
        try (RecordSender real = KafkaTestSupport.sender("test-scan-confirm", 5)) {
            AtomicInteger confirmed = new AtomicInteger();
            RecordSender slow = (topic, key, value) -> real.send(topic, key, value)
                    .thenCompose(ignored -> CompletableFuture.runAsync(confirmed::incrementAndGet,
                            CompletableFuture.delayedExecutor(200, TimeUnit.MILLISECONDS)));
            assertEquals(new ScanEventSink.Delivered(50), new KafkaScanEventSink(slow, names).deliver(scans));
            assertEquals(50, confirmed.get(), "Delivered waits for the last confirmation");

            AtomicInteger sends = new AtomicInteger();
            RecordSender oneLost = (topic, key, value) -> sends.incrementAndGet() == 25
                    ? CompletableFuture.failedFuture(new IllegalStateException("record lost in transit"))
                    : real.send(topic, key, value);
            assertInstanceOf(ScanEventSink.Uncertain.class, new KafkaScanEventSink(oneLost, names).deliver(scans),
                    "one unconfirmed record makes the whole batch uncertain");
        }
        Map<String, String> unreachable = Map.of(KafkaClients.BOOTSTRAP_VARIABLE, "127.0.0.1:" + ServiceProcesses.freePort());
        try (KafkaRecordSender down = KafkaClients.sender(unreachable, "test-scan-down", 5)) {
            long started = System.nanoTime();
            ScanEventSink.DeliveryResult result = new KafkaScanEventSink(down, names).deliver(scans.subList(0, 2));
            assertInstanceOf(ScanEventSink.Uncertain.class, result, "an unreachable broker confirms nothing");
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(40), "the sink never waits indefinitely");
        }
    }

    private static UndeliveredScan scan(long scanId, String transactionId, int sequence, String sku, String name,
                                        String price) {
        return new UndeliveredScan(scanId, transactionId, sequence, sku, name, new BigDecimal(price), ACCEPTED);
    }

    private static List<RecordStream.Record> read(String topic, int count) {
        List<RecordStream.Record> records = new ArrayList<>();
        try (RecordStream stream = KafkaTestSupport.streams("test-scan-reader", null).get()) {
            RecordStream.Partition partition = new RecordStream.Partition(topic, 0);
            stream.assign(List.of(partition));
            stream.seek(partition, 0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (records.size() < count && System.nanoTime() < deadline) {
                records.addAll(stream.poll(Duration.ofMillis(200)));
            }
        }
        assertEquals(count, records.size(), "records on " + topic);
        return records;
    }
}
