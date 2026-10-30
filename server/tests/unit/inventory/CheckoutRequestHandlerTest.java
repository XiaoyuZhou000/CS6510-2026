package unit.inventory;

import database.CheckoutOperationStore;
import database.JdbcCheckoutOperationStore;
import database.StoreFailure;
import events.EnvelopeCodec;
import events.ParkReason;
import events.PermanentEventFailure;
import events.TopicNames;
import events.TransientEventFailure;
import inventory.CheckoutRequestHandler;
import inventory.CheckoutStockService;
import inventory.StockOutcomeEvents;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.RejectionReason;
import selfcheckout.events.v1.StockDeducted;
import selfcheckout.events.v1.StockRejected;
import support.InventoryTestSchema;
import support.RecordingOutboxSignal;
import support.TestEvents;
import support.TestEvents.Line;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static support.TestEvents.line;

/**
 * Inventory's handling of {@code CheckoutRequested} over the real adapters and a disposable schema (006 D8;
 * FR-033, FR-035, FR-038). Migrates the assertions of the gRPC {@code InventoryCheckoutContractTest}: every
 * outcome is now an outcome event recorded with the operation, a redelivery re-sends the recorded event, and
 * faults are parked or retried instead of answered with a status.
 */
class CheckoutRequestHandlerTest {
    private static final String A = InventoryTestSchema.sku(1);
    private static final String B = InventoryTestSchema.sku(2);
    private static final String C = InventoryTestSchema.sku(3);
    private static final TopicNames NAMES = TopicNames.unprefixed();

    @Test
    void aNewRequestAppliesAllLinesAtomicallyAndARedeliveryResendsTheRecordedOutcome() throws Exception {
        try (InventoryTestSchema schema = seeded(5)) {
            RecordingOutboxSignal signal = new RecordingOutboxSignal();
            CheckoutRequestHandler handler = handler(schema, signal);
            String tx = tx();
            handler.handle(TestEvents.request(tx, 1, line(A, 2), line(B, 1)));
            assertEquals(3, schema.stock(A));
            assertEquals(4, schema.stock(B));
            assertEquals(1, signal.signalled.size(), "one outcome event, signalled after the commit");
            String outcomeId = signal.signalled.get(0);
            var recorded = outcome(schema, outcomeId);
            assertEquals("StockDeducted", recorded.getEventType());
            assertEquals("inventory", recorded.getProducer());
            assertEquals(tx, recorded.getRoutingKey());
            StockDeducted deducted = StockDeducted.parseFrom(recorded.getPayload());
            assertEquals(tx, deducted.getAttempt().getTransactionId());
            assertEquals(1, deducted.getAttempt().getAttemptNumber());
            assertEquals(outcomeId, schema.queryString(
                    "SELECT outcome_event_id FROM checkout_operation WHERE transaction_id = ?", tx),
                    "recorded in the same transaction as the operation");

            handler.handle(TestEvents.request(tx, 1, line(A, 2), line(B, 1)));
            assertEquals(3, schema.stock(A), "no stock change on a redelivery");
            assertEquals(4, schema.stock(B));
            assertEquals(List.of(outcomeId), signal.resent, "the recorded outcome is re-sent by outcome_event_id");
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM outbox_event"), "and nothing new is written");
            assertEquals(CheckoutStockService.Outcome.APPLIED, service(schema, signal).outcome(tx, 1).outcome());
        }
    }

    @Test
    void aShortSkuIsRejectedNamingTheFirstShortSkuInSkuOrderAndDeductsNothing() throws Exception {
        try (InventoryTestSchema schema = seeded(1)) {
            RecordingOutboxSignal signal = new RecordingOutboxSignal();
            String tx = tx();
            handler(schema, signal).handle(TestEvents.request(tx, 1, line(A, 1), line(B, 2), line(C, 2)));
            StockRejected rejected = StockRejected.parseFrom(outcome(schema, signal.signalled.get(0)).getPayload());
            assertEquals(RejectionReason.INSUFFICIENT_STOCK, rejected.getReason());
            assertEquals(B, rejected.getSku(), "B is the first short SKU in SKU order");
            assertEquals(1, schema.stock(A), "nothing deducted, not even the line that had stock");
            assertEquals(1, schema.stock(B));
            assertEquals(1, schema.stock(C));
            var recorded = service(schema, signal).outcome(tx, 1);
            assertEquals(CheckoutStockService.Outcome.INSUFFICIENT_STOCK, recorded.outcome());
            assertEquals(B, recorded.rejectedSku());
        }
    }

    @Test
    void anUnknownSkuIsRejectedAsAnInvalidRequestNamingTheFirstUnknownSku() throws Exception {
        try (InventoryTestSchema schema = seeded(5)) {
            RecordingOutboxSignal signal = new RecordingOutboxSignal();
            handler(schema, signal).handle(TestEvents.request(tx(), 1, line("NO-SUCH-SKU", 1), line(A, 1),
                    line("ZZ-UNKNOWN", 1)));
            StockRejected rejected = StockRejected.parseFrom(outcome(schema, signal.signalled.get(0)).getPayload());
            assertEquals(RejectionReason.INVALID_REQUEST, rejected.getReason());
            assertEquals("NO-SUCH-SKU", rejected.getSku());
            assertEquals(5, schema.stock(A));
        }
    }

    @Test
    void aReusedAttemptKeyWithOtherLinesIsAPayloadMismatchAndChangesNothing() throws Exception {
        try (InventoryTestSchema schema = seeded(5)) {
            RecordingOutboxSignal signal = new RecordingOutboxSignal();
            CheckoutRequestHandler handler = handler(schema, signal);
            String tx = tx();
            handler.handle(TestEvents.request(tx, 1, line(A, 1)));
            for (Line[] other : List.of(new Line[] {line(A, 2)}, new Line[] {line(A, 1), line(B, 1)})) {
                PermanentEventFailure mismatch = assertThrows(PermanentEventFailure.class,
                        () -> handler.handle(TestEvents.request(tx, 1, other)));
                assertEquals(ParkReason.PAYLOAD_MISMATCH, mismatch.reason(), "parked, never answered");
            }
            assertEquals(4, schema.stock(A));
            assertEquals(5, schema.stock(B));
            assertTrue(signal.resent.isEmpty(), "a mismatch re-sends nothing");
            assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM outbox_event"));
        }
    }

    @Test
    void twentyConcurrentDeliveriesOfOneRequestDeductOnce() throws Exception {
        try (InventoryTestSchema schema = seeded(50)) {
            RecordingOutboxSignal signal = new RecordingOutboxSignal();
            CheckoutRequestHandler handler = handler(schema, signal);
            String tx = tx();
            race(20, n -> handler.handle(TestEvents.request(tx, 1, line(A, 3), line(B, 2))));
            assertEquals(47, schema.stock(A), "deducted once");
            assertEquals(48, schema.stock(B));
            assertEquals(1, schema.operationCount("APPLIED"));
            assertEquals(1, signal.signalled.size(), "one delivery applied it");
            assertEquals(19, signal.resent.size(), "the others re-sent its recorded outcome");
            assertTrue(signal.resent.stream().allMatch(signal.signalled.get(0)::equals));
        }
    }

    @Test
    void contentionNeverYieldsInsufficientStockWhenStockSuffices() throws Exception {
        try (InventoryTestSchema schema = seeded(30)) {
            RecordingOutboxSignal signal = new RecordingOutboxSignal();
            CheckoutRequestHandler handler = handler(schema, signal);
            // Overlapping baskets; locking in SKU order serializes them without deadlock.
            race(20, n -> handler.handle(n % 2 == 0 ? TestEvents.request(tx(), 1, line(B, 1), line(C, 1))
                    : TestEvents.request(tx(), 1, line(A, 1), line(B, 1), line(C, 1))));
            assertEquals(20, schema.operationCount("APPLIED"), "every request had stock, so every request applied");
            assertEquals(10, schema.stock(B));
            assertEquals(10, schema.stock(C));
            assertEquals(20, schema.stock(A));
        }
    }

    @Test
    void aDeadlockAfterTheStoresRetriesIsTransientNeverInsufficientStock() throws Exception {
        CheckoutOperationStore contended = new CheckoutOperationStore() {
            @Override
            public ApplyResult apply(Attempt attempt, List<Line> lines, String canonicalLines, OutcomeEvents outcomes) {
                throw StoreFailure.contended("Database unit of work met contention 3 times", null);
            }

            @Override
            public boolean recordRefusal(events.OutboxRow refusal) { throw new AssertionError(); }

            @Override
            public java.util.Optional<Recorded> find(Attempt attempt) { return java.util.Optional.empty(); }

            @Override
            public RestoreResult restore(Attempt attempt, RestorationEvents events) { throw new AssertionError(); }
        };
        RecordingOutboxSignal signal = new RecordingOutboxSignal();
        CheckoutRequestHandler handler = new CheckoutRequestHandler(new CheckoutStockService(contended,
                new StockOutcomeEvents(NAMES, Clock.systemUTC()), signal));
        TransientEventFailure failure = assertThrows(TransientEventFailure.class,
                () -> handler.handle(TestEvents.request(tx(), 1, line(A, 1))));
        assertInstanceOf(StoreFailure.class, failure.getCause());
        assertTrue(signal.signalled.isEmpty(), "no outcome, so no StockRejected for contention (FR-033)");
    }

    @Test
    void malformedRequestsAreParkedAndRecordNothing() throws Exception {
        try (InventoryTestSchema schema = seeded(5)) {
            CheckoutRequestHandler handler = handler(schema, new RecordingOutboxSignal());
            List<Line[]> malformed = List.of(new Line[0], new Line[] {line(A, 1), line(A, 1)}, new Line[] {line(A, 0)},
                    new Line[] {line(B, 1), line(A, 1)}, new Line[] {line("", 1)}, new Line[] {line("S".repeat(21), 1)});
            for (Line[] lines : malformed) {
                PermanentEventFailure parked = assertThrows(PermanentEventFailure.class,
                        () -> handler.handle(TestEvents.request(tx(), 1, lines)));
                assertEquals(ParkReason.INVALID_PAYLOAD, parked.reason());
            }
            PermanentEventFailure longId = assertThrows(PermanentEventFailure.class,
                    () -> handler.handle(TestEvents.request("t".repeat(41), 1, line(A, 1))));
            assertEquals(ParkReason.INVALID_PAYLOAD, longId.reason());
            Line[] tooMany = new Line[2001];
            for (int n = 0; n < tooMany.length; n++) tooMany[n] = line(String.format("X-%05d", n), 1);
            assertEquals(ParkReason.INVALID_PAYLOAD, assertThrows(PermanentEventFailure.class,
                    () -> handler.handle(TestEvents.request(tx(), 1, tooMany))).reason());
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM checkout_operation"), "nothing is recorded");
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM outbox_event"));
            assertEquals(5, schema.stock(A));
        }
    }

    @Test
    void aSecondAppliedAttemptForOneTransactionIsAnIntegrityFault() throws Exception {
        try (InventoryTestSchema schema = seeded(5)) {
            RecordingOutboxSignal signal = new RecordingOutboxSignal();
            CheckoutRequestHandler handler = handler(schema, signal);
            String tx = tx();
            handler.handle(TestEvents.request(tx, 1, line(A, 1)));
            PermanentEventFailure second = assertThrows(PermanentEventFailure.class,
                    () -> handler.handle(TestEvents.request(tx, 2, line(A, 1))));
            assertEquals(ParkReason.INTEGRITY, second.reason());
            assertEquals(4, schema.stock(A), "one effective deduction per transaction (FR-029)");
            assertEquals(CheckoutStockService.Outcome.NOT_PROCESSED, service(schema, signal).outcome(tx, 2).outcome());
        }
    }

    @Test
    void theRecordedOutcomeOfAnUnknownAttemptIsNotProcessedAndAQueryChangesNothing() throws Exception {
        try (InventoryTestSchema schema = seeded(5)) {
            RecordingOutboxSignal signal = new RecordingOutboxSignal();
            CheckoutStockService stock = service(schema, signal);
            String tx = tx();
            var unknown = stock.outcome(tx, 1);
            assertEquals(CheckoutStockService.Outcome.NOT_PROCESSED, unknown.outcome());
            assertNull(unknown.rejectedSku());
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM checkout_operation"), "a query records nothing");
            new CheckoutRequestHandler(stock).handle(TestEvents.request(tx, 1, line(A, 1)));
            assertEquals(1, signal.signalled.size(), "a queried attempt is still processed fresh");
            assertTrue(signal.resent.isEmpty());
        }
    }

    @Test
    void twoInventoryInstancesOverOneSchemaApplyEachAttemptOnceAndNeverOversell() throws Exception {
        try (InventoryTestSchema schema = seeded(50)) {
            RecordingOutboxSignal signal = new RecordingOutboxSignal();
            CheckoutRequestHandler one = new CheckoutRequestHandler(new CheckoutStockService(new JdbcCheckoutOperationStore(
                    schema.newPool("test.inv.one." + suffix(), 4, 8, null)), events(), signal));
            CheckoutRequestHandler two = new CheckoutRequestHandler(new CheckoutStockService(new JdbcCheckoutOperationStore(
                    schema.newPool("test.inv.two." + suffix(), 4, 8, null)), events(), signal));
            String duplicate = tx();
            race(20, n -> (n % 2 == 0 ? one : two).handle(TestEvents.request(duplicate, 1, line(A, 2))));
            assertEquals(48, schema.stock(A), "one deduction across both instances");
            assertEquals(1, signal.signalled.size());

            for (int round = 0; round < 5; round++) {
                schema.setStock(B, 1);
                List<String> ids = List.of(tx(), tx());
                race(2, n -> (n == 0 ? one : two).handle(TestEvents.request(ids.get(n), 1, line(B, 1))));
                assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM checkout_operation WHERE state = 'APPLIED' "
                        + "AND transaction_id IN (?, ?)", ids.get(0), ids.get(1)), "round " + round);
                assertEquals(1, schema.queryLong("SELECT COUNT(*) FROM checkout_operation WHERE rejection_reason = "
                        + "'INSUFFICIENT_STOCK' AND transaction_id IN (?, ?)", ids.get(0), ids.get(1)));
                assertEquals(0, schema.stock(B), "never oversold across instances");
            }
        }
    }

    private static selfcheckout.events.v1.EventEnvelope outcome(InventoryTestSchema schema, String eventId) throws Exception {
        try (var select = schema.admin().prepareStatement("SELECT envelope FROM outbox_event WHERE event_id = ?")) {
            select.setString(1, eventId);
            try (var row = select.executeQuery()) {
                assertTrue(row.next(), "the outcome event " + eventId + " is recorded");
                byte[] envelope = row.getBytes(1);
                EnvelopeCodec.decode(envelope, selfcheckout.events.v1.EventEnvelope.parseFrom(envelope).getRoutingKey());
                return selfcheckout.events.v1.EventEnvelope.parseFrom(envelope);
            }
        }
    }

    private static CheckoutRequestHandler handler(InventoryTestSchema schema, RecordingOutboxSignal signal) throws Exception {
        return new CheckoutRequestHandler(service(schema, signal));
    }

    private static CheckoutStockService service(InventoryTestSchema schema, RecordingOutboxSignal signal) throws Exception {
        return new CheckoutStockService(new JdbcCheckoutOperationStore(schema.pool()), events(), signal);
    }

    private static StockOutcomeEvents events() {
        return new StockOutcomeEvents(NAMES, Clock.systemUTC());
    }

    @FunctionalInterface
    private interface Delivery {
        void run(int index) throws Exception;
    }

    /** Starts {@code count} deliveries together and waits for all of them. */
    private static void race(int count, Delivery delivery) throws Exception {
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try (ExecutorService workers = Executors.newFixedThreadPool(count)) {
            for (int i = 0; i < count; i++) {
                int index = i;
                futures.add(workers.submit(() -> {
                    go.await();
                    delivery.run(index);
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> future : futures) future.get(60, TimeUnit.SECONDS);
        }
    }

    private static InventoryTestSchema seeded(int stock) throws Exception {
        InventoryTestSchema schema = InventoryTestSchema.create();
        schema.seed(3, stock);
        return schema;
    }

    private static String tx() {
        return "tx-" + suffix();
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
