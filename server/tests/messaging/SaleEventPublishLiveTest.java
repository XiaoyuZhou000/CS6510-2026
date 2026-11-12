package messaging;

import events.RecordStream;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.SaleCompleted;
import support.CheckoutDeployment;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.ServiceHttp;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 007 US1 scenario 6 (T023), on the live broker under a per-run prefix: the broker stops right after T3 commits a
 * completion, before its {@code SaleCompleted} is sent. Complete still answers 200, the row waits in Transaction's
 * outbox, and after the broker starts again the record appears exactly once on
 * {@code selfcheckout.sale-completed.v1}, keyed by the transaction ID. The fault hook
 * {@code transaction.sale.afterT3Commit} holds the handler for 4 s after the commit so the broker can be stopped
 * first. Needs {@code KAFKA_TEST_BROKER_CONTROL=1}.
 */
class SaleEventPublishLiveTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);

    @Test
    void aCompletionCommittedWhileTheBrokerIsDownIsPublishedExactlyOnceAfterItReturns() throws Exception {
        KafkaTestSupport.requireBrokerControl();
        try (CheckoutDeployment deployment = CheckoutDeployment.start(5, 50, Map.of(),
                "transaction.sale.afterT3Commit:delay:4000", null)) {
            ServiceHttp http = deployment.transactionHttp;
            String id = http.start("sale-live-station");
            assertEquals(200, http.scan(id, A).statusCode());
            assertEquals(200, http.scan(id, A).statusCode());
            assertEquals(200, http.scan(id, B).statusCode());

            CompletableFuture<Integer> complete = CompletableFuture.supplyAsync(() -> {
                try {
                    return http.complete(id).statusCode();
                } catch (Exception failure) {
                    throw new IllegalStateException(failure);
                }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!"COMPLETED".equals(deployment.transactionSchema.transaction(id).orElseThrow().status())) {
                assertTrue(System.nanoTime() < deadline, "T3 never committed" + deployment.logs());
                Thread.sleep(20);
            }
            boolean brokerStopped = false;
            try {
                KafkaTestSupport.stopBroker();
                brokerStopped = true;
                assertEquals(200, complete.get(30, TimeUnit.SECONDS), "the committed completion answers 200" + deployment.logs());
                assertEquals(1, saleRows(deployment, id));
                assertEquals(1, deployment.transactionSchema.queryLong("SELECT COUNT(*) FROM outbox_event "
                        + "WHERE aggregate_key = ? AND published_at IS NULL", "sale:" + id), "the row waits in the outbox");
            } finally {
                if (brokerStopped) KafkaTestSupport.startBroker();
            }

            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (deployment.transactionSchema.queryLong("SELECT COUNT(*) FROM outbox_event WHERE aggregate_key = ? "
                    + "AND published_at IS NULL", "sale:" + id) > 0) {
                assertTrue(System.nanoTime() < deadline, "the SaleCompleted row was never confirmed" + deployment.logs());
                Thread.sleep(250);
            }
            Thread.sleep(1_000);
            List<SaleCompleted> records = saleRecords(deployment, id);
            assertEquals(1, records.size(), "exactly one SaleCompleted record for the sale");
            assertEquals(id, records.get(0).getSaleId());
            assertEquals(2, records.get(0).getLines(0).getQuantity());
            assertEquals(1, saleRows(deployment, id), "and exactly one outbox row");
        }
    }

    private static long saleRows(CheckoutDeployment deployment, String id) throws Exception {
        return deployment.transactionSchema.queryLong("SELECT COUNT(*) FROM outbox_event WHERE event_type = 'SaleCompleted' "
                + "AND aggregate_key = ?", "sale:" + id);
    }

    /** Every record on the run's sale-completed topic keyed by {@code id}, read from offset 0 of each partition. */
    private static List<SaleCompleted> saleRecords(CheckoutDeployment deployment, String id) throws Exception {
        String topic = deployment.names.saleCompleted();
        List<RecordStream.Partition> partitions = new ArrayList<>();
        long remaining = 0;
        for (int p = 0; p < 16; p++) {
            partitions.add(new RecordStream.Partition(topic, p));
            remaining += KafkaTestSupport.endOffset(topic, p);
        }
        List<SaleCompleted> found = new ArrayList<>();
        try (RecordStream stream = KafkaTestSupport.streams("test-sale-reader", null).get()) {
            stream.assign(partitions);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (remaining > 0) {
                assertTrue(System.nanoTime() < deadline, remaining + " records of " + topic + " were never read");
                for (RecordStream.Record record : stream.poll(Duration.ofMillis(200))) {
                    remaining--;
                    if (!id.equals(record.key())) continue;
                    EventEnvelope envelope = EventEnvelope.parseFrom(record.value());
                    assertEquals("SaleCompleted", envelope.getEventType());
                    found.add(SaleCompleted.parseFrom(envelope.getPayload()));
                }
            }
        }
        return found;
    }
}
