package messaging;

import events.DecodedEvent;
import events.EnvelopeCodec;
import events.RecordStream;
import events.TopicNames;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.CheckoutLine;
import selfcheckout.events.v1.CheckoutRequested;
import selfcheckout.events.v1.RejectionReason;
import selfcheckout.events.v1.StockDeducted;
import selfcheckout.events.v1.StockRejected;
import support.InProcessInventory;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.TestSchema;
import support.TransactionTestSchema;
import transaction.TransactionFailure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The checkout event contract over the live broker (FR-014; replaces rpc/InventoryCheckoutContractTest's
 * boundary checks): a real Transaction producer and a real Inventory consumer, each built from the same
 * generated schema, exchange {@code CheckoutRequested} → {@code StockDeducted} and → {@code StockRejected}
 * under a per-run topic prefix. Every field is asserted; record keys equal the transaction ID, and each
 * envelope names its type's owner as {@code producer}.
 */
class CheckoutEventContractTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);

    @Test
    void checkoutRequestedIsAnsweredByStockDeductedAndByStockRejectedWithEveryField() throws Exception {
        KafkaTestSupport.requireBroker();
        TopicNames names = KafkaTestSupport.runTopics();
        try (TransactionTestSchema transactions = TransactionTestSchema.create(TestSchema.Options.defaults().pool(4, 16));
             InventoryTestSchema stock = InventoryTestSchema.create(TestSchema.Options.defaults().pool(4, 16))) {
            transactions.seedReplica(5, stock.seed(5, 1));
            InProcessInventory inventory = InProcessInventory.overLiveKafka(stock, names);
            InProcessTransaction transaction = InProcessTransaction.over(transactions).pool(4, 16).liveKafka(names).build();
            KafkaTestSupport.awaitGroupAssigned(names.group(TopicNames.INVENTORY_CHECKOUT_GROUP), names.checkout(), 16, 2,
                    Duration.ofSeconds(60));
            KafkaTestSupport.awaitGroupAssigned(names.group(TopicNames.TRANSACTION_CHECKOUT_OUTCOME_GROUP),
                    names.checkoutOutcome(), 16, 2, Duration.ofSeconds(60));

            String paid = transaction.start();
            transaction.scan(paid, B);
            transaction.scan(paid, A);
            assertEquals(2, transaction.complete(paid).itemCount());
            String refused = transaction.start();
            transaction.scan(refused, A);
            assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK,
                    assertThrows(TransactionFailure.class, () -> transaction.complete(refused)).code());

            List<DecodedEvent> requests = read(names.checkout(), 2);
            DecodedEvent paidRequest = byKey(requests, paid);
            assertEquals("CheckoutRequested", paidRequest.type().typeName());
            assertEquals("transaction", paidRequest.producer());
            assertEquals(1, paidRequest.schemaVersion());
            assertEquals(paid, paidRequest.routingKey());
            assertEquals(paid, paidRequest.correlationId());
            assertTrue(paidRequest.occurredAtMs() > 0);
            CheckoutRequested request = paidRequest.payload(CheckoutRequested.class);
            assertEquals(paid, request.getAttempt().getTransactionId());
            assertEquals(1, request.getAttempt().getAttemptNumber());
            assertEquals(List.of(A, B), request.getLinesList().stream().map(CheckoutLine::getSku).toList(),
                    "lines sorted by SKU");
            assertEquals(List.of(1, 1), request.getLinesList().stream().map(CheckoutLine::getQuantity).toList());
            assertEquals(transactions.queryString("SELECT request_event_id FROM checkout_attempt WHERE transaction_id = ?",
                    paid), paidRequest.eventId());

            List<DecodedEvent> outcomes = read(names.checkoutOutcome(), 2);
            DecodedEvent deducted = byKey(outcomes, paid);
            assertEquals("StockDeducted", deducted.type().typeName());
            assertEquals("inventory", deducted.producer());
            assertEquals(1, deducted.schemaVersion());
            assertEquals(paid, deducted.correlationId());
            StockDeducted deduction = deducted.payload(StockDeducted.class);
            assertEquals(paid, deduction.getAttempt().getTransactionId());
            assertEquals(1, deduction.getAttempt().getAttemptNumber());
            assertEquals(stock.queryString("SELECT outcome_event_id FROM checkout_operation WHERE transaction_id = ?", paid),
                    deducted.eventId());

            DecodedEvent rejected = byKey(outcomes, refused);
            assertEquals("StockRejected", rejected.type().typeName());
            assertEquals("inventory", rejected.producer());
            StockRejected rejection = rejected.payload(StockRejected.class);
            assertEquals(refused, rejection.getAttempt().getTransactionId());
            assertEquals(1, rejection.getAttempt().getAttemptNumber());
            assertEquals(RejectionReason.INSUFFICIENT_STOCK, rejection.getReason());
            assertEquals(A, rejection.getSku());
            assertEquals(0, stock.stock(A));
            assertEquals(0, stock.stock(B));
        }
    }

    /** Waits for {@code count} records of {@code topic} and decodes them, read from offset 0 outside any group. */
    private static List<DecodedEvent> read(String topic, int count) {
        List<DecodedEvent> events = new ArrayList<>();
        try (RecordStream stream = KafkaTestSupport.streams("test-contract-reader", null).get()) {
            List<RecordStream.Partition> partitions = new ArrayList<>();
            for (int p = 0; p < 16; p++) partitions.add(new RecordStream.Partition(topic, p));
            stream.assign(partitions);
            for (RecordStream.Partition partition : partitions) stream.seek(partition, 0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (events.size() < count && System.nanoTime() < deadline) {
                for (RecordStream.Record record : stream.poll(Duration.ofMillis(200))) {
                    DecodedEvent event = EnvelopeCodec.decode(record.value(), record.key());
                    assertEquals(event.routingKey(), record.key(), "the record key is the transaction ID");
                    events.add(event);
                }
            }
        }
        assertEquals(count, events.size(), "records on " + topic);
        return events;
    }

    private static DecodedEvent byKey(List<DecodedEvent> events, String key) {
        return events.stream().filter(event -> event.routingKey().equals(key)).findFirst()
                .orElseThrow(() -> new AssertionError("no event for " + key));
    }
}
