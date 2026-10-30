package integration;

import events.EnvelopeCodec;
import events.RecordStream;
import org.junit.jupiter.api.Test;
import support.InProcessCheckout;
import support.InventoryTestSchema;
import support.ServiceProcesses;
import support.TestSchema;
import transaction.TransactionFailure;
import transaction.TransactionOperations;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Every Transaction–Inventory interaction of checkout is a recorded domain event (US1 scenario 7; FR-010,
 * D5): each record on the checkout and outcome topics has a committed {@code outbox_event} row in its
 * producer's own schema, created before the record was sent and linked from
 * {@code checkout_attempt.request_event_id} or {@code checkout_operation.outcome_event_id}; and no class of
 * Transaction Service calls Inventory's {@code ApplyCheckout}.
 */
class CheckoutMessagesOnlyTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);

    @Test
    void everyCheckoutRecordHasACommittedOutboxRowOfItsProducerCreatedBeforeTheSend() throws Exception {
        try (InProcessCheckout saga = InProcessCheckout.start(5, 1)) {
            TransactionOperations service = saga.checkout();
            String paid = service.start(new TransactionOperations.StartCommand("messages")).transactionId();
            service.scan(new TransactionOperations.ScanCommand(paid, A));
            service.complete(paid);
            String refused = service.start(new TransactionOperations.StartCommand("messages")).transactionId();
            service.scan(new TransactionOperations.ScanCommand(refused, B));
            service.scan(new TransactionOperations.ScanCommand(refused, B));
            assertEquals(TransactionFailure.Code.INSUFFICIENT_STOCK,
                    assertThrows(TransactionFailure.class, () -> service.complete(refused)).code());

            List<RecordStream.Record> requests = records(saga, saga.names.checkout());
            List<RecordStream.Record> outcomes = records(saga, saga.names.checkoutOutcome());
            assertEquals(2, requests.size(), "one CheckoutRequested per attempt");
            assertEquals(2, outcomes.size(), "one outcome per attempt: StockDeducted and StockRejected");
            for (RecordStream.Record record : requests) {
                var event = EnvelopeCodec.decode(record.value(), record.key());
                assertEquals("transaction", event.producer());
                assertRecordedBeforeSend(saga.transactionSchema, event.eventId(), record);
                assertEquals(event.eventId(), saga.transactionSchema.queryString(
                        "SELECT request_event_id FROM checkout_attempt WHERE transaction_id = ? AND attempt_number = 1",
                        record.key()), "linked from checkout_attempt.request_event_id");
            }
            for (RecordStream.Record record : outcomes) {
                var event = EnvelopeCodec.decode(record.value(), record.key());
                assertEquals("inventory", event.producer());
                assertRecordedBeforeSend(saga.inventorySchema, event.eventId(), record);
                assertEquals(event.eventId(), saga.inventorySchema.queryString(
                        "SELECT outcome_event_id FROM checkout_operation WHERE transaction_id = ? AND attempt_number = 1",
                        record.key()), "linked from checkout_operation.outcome_event_id");
            }
            assertEquals(0, saga.transactionSchema.queryLong("SELECT COUNT(*) FROM outbox_event WHERE event_id NOT IN "
                    + "(SELECT request_event_id FROM checkout_attempt)"), "Transaction recorded no other checkout event");
        }
    }

    @Test
    void noTransactionServiceClassCallsInventorysApplyCheckout() throws Exception {
        List<String> callers = new ArrayList<>();
        try (JarFile jar = new JarFile(ServiceProcesses.serverRoot().resolve("out/transaction.jar").toFile())) {
            for (JarEntry entry : java.util.Collections.list(jar.entries())) {
                if (!entry.getName().endsWith(".class")) continue;
                try (InputStream in = jar.getInputStream(entry)) {
                    String constants = new String(in.readAllBytes(), StandardCharsets.ISO_8859_1);
                    if (constants.contains("ApplyCheckoutRequest") || constants.contains("applyCheckout")
                            || constants.contains("GetCheckoutOutcomeRequest")) {
                        callers.add(entry.getName());
                    }
                }
            }
        }
        assertEquals(List.of(), callers, "checkout reaches Inventory only through events");
    }

    /** Every record of {@code topic} on the saga's bus, read from offset 0 outside any group. */
    private static List<RecordStream.Record> records(InProcessCheckout saga, String topic) {
        List<RecordStream.Record> records = new ArrayList<>();
        try (RecordStream stream = saga.bus.stream()) {
            List<RecordStream.Partition> partitions = new ArrayList<>();
            for (int p = 0; p < saga.bus.partitions(topic); p++) partitions.add(new RecordStream.Partition(topic, p));
            stream.assign(partitions);
            for (RecordStream.Partition partition : partitions) stream.seek(partition, 0);
            for (int round = 0; round < 5; round++) records.addAll(stream.poll(Duration.ofMillis(50)));
        }
        return records;
    }

    private static void assertRecordedBeforeSend(TestSchema schema, String eventId, RecordStream.Record record)
            throws Exception {
        try (PreparedStatement select = schema.admin().prepareStatement(
                "SELECT created_at, topic, routing_key FROM outbox_event WHERE event_id = ?")) {
            select.setString(1, eventId);
            try (ResultSet row = select.executeQuery()) {
                assertTrue(row.next(), "event " + eventId + " has a committed outbox row in " + schema.name());
                Timestamp created = row.getTimestamp(1);
                assertTrue(created.toInstant().toEpochMilli() <= record.timestampMs(),
                        "the row was created before the record was sent");
                assertEquals(record.topic(), row.getString(2));
                assertEquals(record.key(), row.getString(3), "the record key is the row's routing key");
            }
        }
    }
}
