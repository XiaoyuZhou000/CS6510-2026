package messaging;

import database.JdbcOutboxStore;
import database.JdbcUnitOfWork;
import events.DecodedEvent;
import events.EnvelopeCodec;
import events.EventTypes;
import events.OutboxDispatcher;
import events.OutboxRow;
import events.RecordStream;
import events.TopicNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.RestorationNotNeeded;
import selfcheckout.events.v1.RestorationResult;
import support.FaultInjection;
import support.InventoryTestSchema;
import support.KafkaTestSupport;
import support.ServiceProcesses;
import support.TestEvents;
import support.TestSchema;
import support.TransactionTestSchema;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;
import static support.TestEvents.line;

/**
 * The reserved restoration, exercised directly against Inventory Service over the live broker (FR-039,
 * SC-016; US3 scenario 7; D8; replaces rpc/RestoreCheckoutDirectTest).
 *
 * <p>This is a direct Inventory test, <b>not</b> an end-to-end Saga compensation test: no production code
 * path produces {@code StockRestorationRequested}. The test records each request in a Transaction schema's
 * {@code outbox_event} and sends it with a Transaction-side {@link OutboxDispatcher}, as the owning producer
 * would (FR-010). {@code run-tests.sh} reports it in its separate "Direct restoration (SC-016)" section. Units
 * are restored at most once and only if they were deducted; a redelivered request re-sends the event it
 * recorded; a restoration delivered before its delayed {@code CheckoutRequested} blocks it.</p>
 */
class RestorationEventDirectTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static TopicNames names;
    private static InventoryTestSchema stock;
    private static TransactionTestSchema producer;
    private static ServiceProcesses.ServiceProcess inventory;
    private static OutboxDispatcher dispatcher;
    private static messaging.KafkaRecordSender sender;
    private static Outcomes outcomes;

    @BeforeAll
    static void startInventory() throws Exception {
        KafkaTestSupport.requireBroker();
        names = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(names);
        stock = InventoryTestSchema.create(TestSchema.Options.defaults().withOwnUser());
        stock.seed(3, 100);
        producer = TransactionTestSchema.create();
        inventory = start(stock, names, null);
        sender = KafkaTestSupport.sender("test-restoration-producer", 0);
        dispatcher = new OutboxDispatcher("transaction", new JdbcOutboxStore(producer.pool(), "test.restoration.outbox"),
                Map.of(names.checkout(), sender), List.of(names.checkout()), OutboxDispatcher.Settings.defaults(),
                Clock.systemUTC());
        dispatcher.start();
        outcomes = new Outcomes(names.checkoutOutcome());
    }

    @AfterAll
    static void stop() throws Exception {
        if (outcomes != null) outcomes.close();
        if (dispatcher != null) dispatcher.stop(Duration.ofSeconds(5));
        if (sender != null) sender.close();
        if (inventory != null) inventory.close();
        try (TestSchema closing = producer; TestSchema closingStock = stock) {
            // closed in order
        }
    }

    @Test
    void anAppliedAttemptIsRestoredExactlyOnceAndEveryRepeatIsAlreadyRestored() throws Exception {
        String tx = tx();
        int before = stock.stock(A);
        request(tx, line(A, 2), line(B, 1));
        outcomes.await(tx, "StockDeducted");
        assertEquals(before - 2, stock.stock(A));

        String first = restore(tx);
        DecodedEvent restored = outcomes.await(tx, "StockRestored");
        assertEquals(before, stock.stock(A), "the deducted units came back");
        dispatcher.resend(first);
        outcomes.awaitCount(tx, "StockRestored", 2);
        assertEquals(1, outcomes.of(tx, "StockRestored").stream().map(DecodedEvent::eventId).distinct().count(),
                "a redelivered request re-sends the recorded StockRestored");
        assertEquals(restored.eventId(), stock.queryString(
                "SELECT restoration_event_id FROM checkout_operation WHERE transaction_id = ?", tx));
        restore(tx);
        DecodedEvent already = outcomes.await(tx, "RestorationNotNeeded");
        assertEquals(RestorationResult.ALREADY_RESTORED, already.payload(RestorationNotNeeded.class).getResult());
        assertEquals(before, stock.stock(A), "a repeated restoration changes nothing");
        assertEquals("RESTORED", stock.queryString("SELECT state FROM checkout_operation WHERE transaction_id = ?", tx));
    }

    @Test
    void tenConcurrentFirstRestoresRestoreOnce() throws Exception {
        String tx = tx();
        int before = stock.stock(A);
        request(tx, line(A, 3));
        outcomes.await(tx, "StockDeducted");
        for (int i = 0; i < 10; i++) restore(tx);
        outcomes.awaitCount(tx, "RestorationNotNeeded", 9);
        assertEquals(1, outcomes.of(tx, "StockRestored").size(), "one restoration wins");
        assertTrue(outcomes.of(tx, "RestorationNotNeeded").stream().allMatch(event ->
                event.payload(RestorationNotNeeded.class).getResult() == RestorationResult.ALREADY_RESTORED),
                "the others find it already restored");
        assertEquals(before, stock.stock(A), "the units came back once");
    }

    @Test
    void aRejectedAttemptHasNothingToRestore() throws Exception {
        String tx = tx();
        int before = stock.stock(B);
        request(tx, line(B, before + 1));
        outcomes.await(tx, "StockRejected");
        restore(tx);
        DecodedEvent nothing = outcomes.await(tx, "RestorationNotNeeded");
        assertEquals(RestorationResult.NOTHING_TO_RESTORE, nothing.payload(RestorationNotNeeded.class).getResult());
        assertEquals(before, stock.stock(B), "nothing was deducted, so nothing is added");
    }

    @Test
    void anAbsentAttemptIsBlockedAndALaterDelayedApplyDeductsNothing() throws Exception {
        String tx = tx();
        int before = stock.stock(A);
        restore(tx);
        DecodedEvent blocked = outcomes.await(tx, "RestorationNotNeeded");
        assertEquals(RestorationResult.NOTHING_TO_RESTORE, blocked.payload(RestorationNotNeeded.class).getResult(),
                "the restoration arrived before its stock request");
        assertEquals("BLOCKED", stock.queryString("SELECT state FROM checkout_operation WHERE transaction_id = ?", tx),
                "a tombstone blocks the attempt");
        request(tx, line(A, 2));
        outcomes.await(tx, "StockDeductionRefused");
        assertEquals(before, stock.stock(A), "the delayed request has no effect");
        restore(tx);
        outcomes.awaitCount(tx, "RestorationNotNeeded", 2);
        assertEquals(before, stock.stock(A), "a repeat stays a no-op");
    }

    @Test
    void aRestoreInterruptedByAnInventoryCrashAfterItsCommitIsRedeliveredAndRestoresExactlyOnce() throws Exception {
        TopicNames own = KafkaTestSupport.runTopics();
        KafkaTestSupport.createCatalogTopics(own);
        try (InventoryTestSchema crashing = InventoryTestSchema.create(TestSchema.Options.defaults().withOwnUser());
             Outcomes seen = new Outcomes(own.checkoutOutcome())) {
            crashing.seed(3, 5);
            OutboxDispatcher ownDispatcher = new OutboxDispatcher("transaction", new JdbcOutboxStore(producer.pool(),
                    "test.restoration.crash"), Map.of(own.checkout(), sender), List.of(own.checkout()),
                    OutboxDispatcher.Settings.defaults(), Clock.systemUTC());
            ownDispatcher.start();
            ServiceProcesses.ServiceProcess halting = start(crashing, own, FaultInjection.haltAt("inventory.restore.afterCommit"));
            try {
                String tx = tx();
                send(ownDispatcher, own, tx, EventTypes.CHECKOUT_REQUESTED, TestEvents.requested(id(), tx, 1, line(A, 2)));
                seen.await(tx, "StockDeducted");
                assertEquals(3, crashing.stock(A));
                send(ownDispatcher, own, tx, EventTypes.STOCK_RESTORATION_REQUESTED, null);
                assertEquals(137, halting.awaitExit(Duration.ofSeconds(60)), "Inventory halts after the restore commit");
                assertEquals(5, crashing.stock(A), "the restoration committed before the halt");
                halting.close();
                halting = start(crashing, own, null);
                DecodedEvent restored = seen.await(tx, "StockRestored");
                assertEquals(5, crashing.stock(A), "the redelivered request restored nothing twice");
                assertEquals(restored.eventId(), crashing.queryString(
                        "SELECT restoration_event_id FROM checkout_operation WHERE transaction_id = ?", tx),
                        "the outcome sent is the one recorded before the halt");
                assertEquals(0, crashing.parkedCount());
            } finally {
                halting.close();
                ownDispatcher.stop(Duration.ofSeconds(5));
            }
        }
    }

    private static ServiceProcesses.ServiceProcess start(InventoryTestSchema schema, TopicNames topics, String faults)
            throws Exception {
        ServiceProcesses.ServiceProcess process = ServiceProcesses.service("inventory").database(schema)
                .httpPort(freePort()).grpcPort(freePort()).kafka(topics).faults(faults)
                .start();
        process.awaitHttp("/items", 200, Duration.ofSeconds(60));
        KafkaTestSupport.awaitGroupAssigned(topics.group(TopicNames.INVENTORY_CHECKOUT_GROUP), topics.checkout(), 16, 8,
                Duration.ofSeconds(60));
        return process;
    }

    private static void request(String tx, TestEvents.Line... lines) {
        send(dispatcher, names, tx, EventTypes.CHECKOUT_REQUESTED, TestEvents.requested(id(), tx, 1, lines));
    }

    private static String restore(String tx) {
        return send(dispatcher, names, tx, EventTypes.STOCK_RESTORATION_REQUESTED, null);
    }

    /** Records the event in the Transaction schema's outbox, then signals the dispatcher; returns its ID. */
    private static String send(OutboxDispatcher target, TopicNames topics, String tx, EventTypes type, byte[] envelope) {
        String eventId = envelope == null ? id() : EnvelopeCodec.decode(envelope, tx).eventId();
        byte[] value = envelope != null ? envelope : TestEvents.restorationRequested(eventId, tx, 1);
        OutboxRow row = new OutboxRow(eventId, topics.checkout(), tx, type.typeName(), EventTypes.attemptKey(tx, 1), value,
                Instant.now().truncatedTo(ChronoUnit.MILLIS));
        JdbcUnitOfWork.inTransaction(rowPool(), "test.restoration.insert", connection -> {
            JdbcOutboxStore.insertRow(connection, row);
            return null;
        });
        target.signal(List.of(eventId));
        return eventId;
    }

    private static database.ConnectionPool rowPool() {
        try {
            return producer.pool();
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static int freePort() throws java.io.IOException {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String tx() {
        return "tx-restore-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static String id() {
        return UUID.randomUUID().toString();
    }

    /** Reads an outcome topic from offset 0 on its own thread and keeps every decoded event. */
    private static final class Outcomes implements AutoCloseable {
        private final List<DecodedEvent> events = new CopyOnWriteArrayList<>();
        private final Thread reader;
        private volatile boolean running = true;

        Outcomes(String topic) {
            reader = Thread.ofPlatform().daemon(true).start(() -> {
                try (RecordStream stream = KafkaTestSupport.streams("test-restoration-reader", null).get()) {
                    List<RecordStream.Partition> partitions = new ArrayList<>();
                    for (int p = 0; p < 16; p++) partitions.add(new RecordStream.Partition(topic, p));
                    stream.assign(partitions);
                    for (RecordStream.Partition partition : partitions) stream.seek(partition, 0);
                    while (running) {
                        for (RecordStream.Record record : stream.poll(Duration.ofMillis(100))) {
                            events.add(EnvelopeCodec.decode(record.value(), record.key()));
                        }
                    }
                }
            });
        }

        List<DecodedEvent> of(String tx, String type) {
            return events.stream().filter(matching(tx, type)).toList();
        }

        DecodedEvent await(String tx, String type) throws InterruptedException {
            return awaitCount(tx, type, 1).get(0);
        }

        List<DecodedEvent> awaitCount(String tx, String type, int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (of(tx, type).size() < count) {
                assertTrue(System.nanoTime() < deadline, "no " + count + " " + type + " for " + tx + ": " + events.stream()
                        .filter(event -> event.routingKey().equals(tx)).map(event -> event.type().typeName()).toList());
                Thread.sleep(50);
            }
            return of(tx, type);
        }

        private static Predicate<DecodedEvent> matching(String tx, String type) {
            return event -> event.routingKey().equals(tx) && event.type().typeName().equals(type);
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            reader.join(5_000);
        }
    }
}
