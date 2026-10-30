package messaging;

import events.DecodedEvent;
import events.EnvelopeCodec;
import events.RecordStream;
import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.FaultInjection;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.ServiceHttp;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A consumer that crashes after its handler's commit and before its offset commit (quickstart scenario F; D6,
 * D8): the event is redelivered after the restart and has no second effect. Inventory re-sends the outcome it
 * recorded, with the same {@code event_id}, and Transaction's guarded write makes the redelivered outcome a
 * no-op; the attempt settles once. Live broker and service processes.
 */
class ConsumerCrashAfterCommitTest {
    private static final String HOOK = "consumer.afterHandlerCommit";

    @Test
    void aRedeliveredRequestOrOutcomeAfterAConsumerCrashHasNoSecondEffect() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(5, 20)) {
            ServiceHttp http = deployment.transactionHttp;

            // Inventory halts after applying the request, before committing its offset.
            String sku = InventoryTestSchema.sku(1);
            deployment.startInventory(FaultInjection.haltAt(HOOK));
            String first = http.start("crash-inventory");
            assertEquals(200, http.scan(first, sku).statusCode());
            int status = http.complete(first).statusCode();
            assertTrue(status == 200 || status == 500, "settled or handed over, never cancelled: " + status);
            assertEquals(137, deployment.inventoryProcess().awaitExit(Duration.ofSeconds(60)));
            deployment.startInventory();
            awaitCompleted(deployment, first);
            assertEquals(19, deployment.inventorySchema.stock(sku), "the redelivered request deducted nothing");
            assertEquals(1, deployment.inventorySchema.queryLong(
                    "SELECT COUNT(*) FROM checkout_operation WHERE transaction_id = ?", first));
            List<DecodedEvent> outcomes = outcomes(deployment, first, 1);
            assertEquals(1, outcomes.stream().map(DecodedEvent::eventId).distinct().count(),
                    "Inventory re-sent the outcome it recorded, under the same event_id");
            assertEquals(deployment.inventorySchema.queryString(
                    "SELECT outcome_event_id FROM checkout_operation WHERE transaction_id = ?", first),
                    outcomes.get(0).eventId());

            // Transaction halts after T3, before committing the outcome's offset.
            String second = http.start("crash-transaction");
            assertEquals(200, http.scan(second, sku).statusCode());
            deployment.startTransaction(FaultInjection.haltAt(HOOK));
            try {
                HttpResponse<String> response = http.complete(second);
                fail("Transaction should have halted, but answered " + response.statusCode());
            } catch (IOException dropped) {
                // The halt drops the waiting request's connection.
            }
            assertEquals(137, deployment.transactionProcess().awaitExit(Duration.ofSeconds(30)));
            assertEquals("COMPLETED", deployment.transactionSchema.attempts(second).get(0).state(),
                    "T3 committed before the halt");
            deployment.startTransaction();
            HttpResponse<String> repeated = http.complete(second);
            assertEquals(409, repeated.statusCode(), "the redelivered StockDeducted was a no-op: " + repeated.body());
            assertEquals(1, deployment.transactionSchema.attempts(second).size());
            assertEquals(18, deployment.inventorySchema.stock(sku), "one deduction per transaction");
            assertEquals(0, deployment.transactionSchema.parkedCount() + deployment.inventorySchema.parkedCount(),
                    "a redelivery is never parked");
        }
    }

    private static void awaitCompleted(CheckoutDeployment deployment, String id) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (!deployment.transactionSchema.attempts(id).get(0).state().equals("COMPLETED")) {
            assertTrue(System.nanoTime() < deadline, "the attempt never completed" + deployment.logs());
            Thread.sleep(100);
        }
    }

    /** At least {@code atLeast} outcome records for {@code id}, read from offset 0 of the run's outcome topic. */
    private static List<DecodedEvent> outcomes(CheckoutDeployment deployment, String id, int atLeast) {
        String topic = deployment.names.checkoutOutcome();
        List<DecodedEvent> events = new ArrayList<>();
        try (RecordStream stream = KafkaTestSupport.streams("test-crash-reader", null).get()) {
            List<RecordStream.Partition> partitions = new ArrayList<>();
            for (int p = 0; p < 16; p++) partitions.add(new RecordStream.Partition(topic, p));
            stream.assign(partitions);
            for (RecordStream.Partition partition : partitions) stream.seek(partition, 0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (events.size() < atLeast && System.nanoTime() < deadline) {
                for (RecordStream.Record record : stream.poll(Duration.ofMillis(200))) {
                    if (record.key().equals(id)) events.add(EnvelopeCodec.decode(record.value(), record.key()));
                }
            }
        }
        assertTrue(events.size() >= atLeast, "outcome records for " + id + ": " + events.size());
        return events;
    }
}
