package unit.analytics;

import analytics.ScanIngestionService;
import database.ScanIngestStore;
import database.StoreFailure;
import events.DecodedEvent;
import events.EventTypes;
import events.ParkReason;
import events.PermanentEventFailure;
import events.RecordStream;
import events.TransientEventFailure;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.ScanAccepted;
import support.FaultInjection;
import support.TestEvents;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link ScanIngestionService} (contracts/scan-event-delivery.md section 3, D10): valid {@code ScanAccepted}
 * events go to the ingestion transaction with the offset after the batch's last record; an event that breaks the
 * validation rules is a permanent failure that stores nothing; every other failure is transient, so a valid scan
 * is read again rather than parked (FR-013, FR-049).
 */
class ScanIngestionServiceTest {
    private static final String TOPIC = "selfcheckout.scan-accepted.v1";

    @Test
    void validEventsAreStoredWithTheOffsetAfterTheLastRecord() {
        RecordingStore store = new RecordingStore();
        ScanIngestionService service = new ScanIngestionService(store);
        service.ingest(List.of(scan("tx-1", 1, "sku1", "Item 1", 10), scan("tx-1", 2, "sku2", "Item 2", 11),
                scan("tx-2", 1, "sku1", "Item 1", 12)));
        assertEquals(1, store.calls.size());
        assertEquals(new ScanIngestStore.ConsumedOffset(TOPIC, 0, 13), store.offsets.getFirst());
        assertEquals(List.of(new ScanIngestStore.ScanEvent("tx-1", 1, "sku1", "Item 1", Instant.ofEpochMilli(1_000)),
                new ScanIngestStore.ScanEvent("tx-1", 2, "sku2", "Item 2", Instant.ofEpochMilli(1_000)),
                new ScanIngestStore.ScanEvent("tx-2", 1, "sku1", "Item 1", Instant.ofEpochMilli(1_000))),
                store.calls.getFirst(), "batch order, identities, captured names, and acceptance times are kept");
    }

    @Test
    void anInvalidEventIsAPermanentFailureAndTheBatchStoresNothing() {
        RecordingStore store = new RecordingStore();
        ScanIngestionService service = new ScanIngestionService(store);
        List<DecodedEvent> invalid = List.of(scan("tx-1", 0, "sku1", "Item 1", 1), scan("tx-1", 1, " ", "Item 1", 1),
                scan("tx-1", 1, "s".repeat(21), "Item 1", 1), scan("tx-1", 1, "sku1", "n".repeat(101), 1),
                scan("t".repeat(41), 1, "sku1", "Item 1", 1), scan("tx-1", 1, "sku1", "", 1));
        for (DecodedEvent event : invalid) {
            PermanentEventFailure failure = assertThrows(PermanentEventFailure.class,
                    () -> service.ingest(List.of(scan("tx-ok", 1, "sku1", "Item 1", 0), event)));
            assertEquals(ParkReason.INVALID_PAYLOAD, failure.reason());
        }
        PermanentEventFailure wrongType = assertThrows(PermanentEventFailure.class,
                () -> service.ingest(List.of(TestEvents.request("tx-1", 1, TestEvents.line("sku1", 1)).at(TOPIC, 0, 3))));
        assertEquals(ParkReason.INVALID_PAYLOAD, wrongType.reason(), "the scan topic carries only ScanAccepted");
        assertTrue(store.calls.isEmpty(), "nothing reaches the ingestion transaction");
    }

    @Test
    void storeFailuresStayTransientAndAnyOtherFailureBecomesTransient() {
        RecordingStore store = new RecordingStore();
        ScanIngestionService service = new ScanIngestionService(store);
        List<DecodedEvent> batch = List.of(scan("tx-1", 1, "sku1", "Item 1", 0));
        StoreFailure outage = new StoreFailure("database unavailable");
        store.failure = outage;
        assertSame(outage, assertThrows(StoreFailure.class, () -> service.ingest(batch)), "a store failure is retried as is");
        store.failure = new IllegalStateException("unexpected");
        assertThrows(TransientEventFailure.class, () -> service.ingest(batch), "a valid scan is never parked");
        store.failure = null;
        try (FaultInjection.Armed armed = FaultInjection.spec().fail("analytics.ingest.received").armInThisJvm()) {
            assertThrows(TransientEventFailure.class, () -> service.ingest(batch), "a crash before the transaction is retried");
        }
        assertEquals(2, store.calls.size(), "the received hook fires before the ingestion transaction begins");
    }

    @Test
    void theConsumerResumesFromTheStoredOffsetOrFromZero() {
        RecordingStore store = new RecordingStore();
        ScanIngestionService service = new ScanIngestionService(store);
        RecordStream.Partition partition = new RecordStream.Partition(TOPIC, 0);
        assertEquals(0, service.nextOffset(partition), "offset 0 before the first batch");
        store.stored = OptionalLong.of(1_501);
        assertEquals(1_501, service.nextOffset(partition));
    }

    private static DecodedEvent scan(String transactionId, int sequence, String sku, String name, long offset) {
        ScanAccepted payload = ScanAccepted.newBuilder().setTransactionId(transactionId).setScanSequence(sequence)
                .setSku(sku).setProductName(name).setUnitPrice("1.00").setAcceptedAtMs(1_000).build();
        return new DecodedEvent(EventTypes.scanEventId(transactionId, sequence), EventTypes.SCAN_ACCEPTED, 1,
                transactionId, 1_000, transactionId, EventTypes.TRANSACTION, payload, TOPIC, 0, offset);
    }

    private static final class RecordingStore implements ScanIngestStore {
        final List<List<ScanEvent>> calls = new ArrayList<>();
        final List<ConsumedOffset> offsets = new ArrayList<>();
        RuntimeException failure;
        OptionalLong stored = OptionalLong.empty();

        @Override
        public IngestResult ingest(List<ScanEvent> events, ConsumedOffset next) {
            calls.add(List.copyOf(events));
            offsets.add(next);
            if (failure != null) throw failure;
            return new IngestResult(events.size(), 0, 1, events.size());
        }

        @Override
        public OptionalLong readNextOffset(String topic, int partition) {
            return stored;
        }

        @Override
        public Map<String, String> readProductNames(int limit) {
            return Map.of();
        }
    }
}
