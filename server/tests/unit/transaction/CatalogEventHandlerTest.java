package unit.transaction;

import database.ProductReplicaStore;
import database.StoreFailure;
import events.ParkReason;
import events.PermanentEventFailure;
import org.junit.jupiter.api.Test;
import support.TestEvents;
import transaction.CatalogEventHandler;
import transaction.ProductCatalogReplica;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Transaction's catalog handler (D9): items are buffered per {@code catalog_version}, and a snapshot marker
 * replaces the durable replica, then the in-memory snapshot, only when it completes a new version.
 */
class CatalogEventHandlerTest {

    @Test
    void aCompleteSnapshotOfANewVersionReplacesTheReplicaInOneStoreCall() {
        FakeStore store = new FakeStore(null);
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogEventHandler handler = new CatalogEventHandler(store, replica);

        handler.handle(TestEvents.item("SKU-000002", "Bread", "2.50", "v1"));
        handler.handle(TestEvents.item("SKU-000001", "Apple", "1.25", "v1"));
        assertEquals(0, store.replacements, "items alone change nothing");
        assertFalse(replica.usable());

        handler.handle(TestEvents.snapshot("v1", 2));
        assertEquals(1, store.replacements, "one local transaction replaces the whole replica");
        assertEquals("v1", store.replica.catalogVersion());
        assertEquals(List.of("SKU-000001", "SKU-000002"),
                store.replica.products().stream().map(ProductReplicaStore.ReplicaProduct::sku).toList(),
                "the replica is written in SKU order");
        assertEquals("v1", replica.version());
        assertEquals("Bread", replica.lookup("SKU-000002").orElseThrow().name());
        assertEquals(new BigDecimal("1.25"), replica.lookup("SKU-000001").orElseThrow().price());
    }

    @Test
    void aRepeatedItemOfTheSameVersionReplacesTheBufferedOneAndIsCountedOnce() {
        FakeStore store = new FakeStore(null);
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogEventHandler handler = new CatalogEventHandler(store, replica);

        // An earlier Inventory startup left the same SKU and version on the topic, not yet compacted away.
        handler.handle(TestEvents.item("SKU-000001", "Apple", "1.25", "v1"));
        handler.handle(TestEvents.item("SKU-000001", "Apple", "1.25", "v1"));
        handler.handle(TestEvents.snapshot("v1", 2));
        assertEquals(0, store.replacements, "two records of one SKU are one distinct SKU, not two");
        assertFalse(replica.usable());

        handler.handle(TestEvents.item("SKU-000002", "Bread", "2.50", "v1"));
        handler.handle(TestEvents.item("SKU-000001", "Apple, renamed", "1.25", "v1"));
        handler.handle(TestEvents.snapshot("v1", 2));
        assertEquals(1, store.replacements);
        assertEquals("Apple, renamed", replica.lookup("SKU-000001").orElseThrow().name(),
                "the later record of a SKU and version replaces the buffered one");
        assertEquals(2, replica.size());
    }

    @Test
    void aMarkerWithFewerDistinctSkusThanItsCountDoesNothing() {
        FakeStore store = new FakeStore(new ProductReplicaStore.Replica("v0", List.of(stored("SKU-000009", "Old", "9.00"))));
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogEventHandler handler = new CatalogEventHandler(store, replica);
        assertTrue(handler.loadDurable());

        handler.handle(TestEvents.item("SKU-000001", "Apple", "1.25", "v1"));
        handler.handle(TestEvents.snapshot("v1", 3));
        assertEquals(0, store.replacements);
        assertEquals("v0", replica.version(), "an incomplete snapshot never reaches the replica");
        assertEquals("v0", store.replica.catalogVersion());
    }

    @Test
    void aMarkerForTheCurrentVersionIsANoOp() {
        FakeStore store = new FakeStore(new ProductReplicaStore.Replica("v1", List.of(stored("SKU-000001", "Apple", "1.25"))));
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogEventHandler handler = new CatalogEventHandler(store, replica);
        handler.loadDurable();

        // Every Inventory restart republishes the same version.
        handler.handle(TestEvents.item("SKU-000001", "Apple", "1.25", "v1"));
        handler.handle(TestEvents.snapshot("v1", 1));
        assertEquals(0, store.replacements, "the installed version is not written again");
        assertEquals("v1", replica.version());
    }

    @Test
    void recordsOfOtherVersionsAreIgnoredIncludingLeftoversUnderRemovedSkus() {
        FakeStore store = new FakeStore(null);
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogEventHandler handler = new CatalogEventHandler(store, replica);

        // v1 had SKU 3; v2 removed it, but compaction keeps v1's last record under that key.
        handler.handle(TestEvents.item("SKU-000003", "Gone", "3.00", "v1"));
        handler.handle(TestEvents.item("SKU-000001", "Apple", "1.25", "v2"));
        handler.handle(TestEvents.item("SKU-000002", "Bread", "2.50", "v2"));
        handler.handle(TestEvents.snapshot("v2", 2));
        assertEquals("v2", replica.version());
        assertEquals(2, replica.size());
        assertTrue(replica.lookup("SKU-000003").isEmpty(), "an item of another version never enters the replica");

        // A late record of the older version, and even a marker naming it, never replace the newer replica.
        handler.handle(TestEvents.item("SKU-000001", "Stale", "0.01", "v1"));
        handler.handle(TestEvents.snapshot("v1", 2));
        assertEquals(1, store.replacements);
        assertEquals("v2", replica.version());
        assertEquals("Apple", replica.lookup("SKU-000001").orElseThrow().name());
    }

    @Test
    void pricesParseAsTwoFractionDigitDecimals() {
        FakeStore store = new FakeStore(null);
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogEventHandler handler = new CatalogEventHandler(store, replica);

        handler.handle(TestEvents.item("SKU-000001", "Apple", "12.99", "v1"));
        handler.handle(TestEvents.item("SKU-000002", "Bread", "0.50", "v1"));
        handler.handle(TestEvents.snapshot("v1", 2));
        assertEquals(new BigDecimal("12.99"), replica.lookup("SKU-000001").orElseThrow().price());
        assertEquals(new BigDecimal("0.50"), store.replica.products().get(1).price());
        assertEquals(2, replica.lookup("SKU-000002").orElseThrow().price().scale());

        for (String bad : List.of("1.234", "-1.00", "abc", "")) {
            PermanentEventFailure invalid = assertThrows(PermanentEventFailure.class,
                    () -> handler.handle(TestEvents.item("SKU-000003", "Bad", bad, "v2")), bad);
            assertEquals(ParkReason.INVALID_PAYLOAD, invalid.reason());
        }
        assertThrows(PermanentEventFailure.class, () -> handler.handle(TestEvents.item("SKU-000003", " ", "1.00", "v2")),
                "a blank name is parked too");
    }

    @Test
    void theInMemorySnapshotSwapsOnlyAfterTheStoreCommitAndAFailedReplaceIsRetried() {
        FakeStore store = new FakeStore(new ProductReplicaStore.Replica("v1", List.of(stored("SKU-000001", "Apple", "1.25"))));
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogEventHandler handler = new CatalogEventHandler(store, replica);
        handler.loadDurable();
        handler.handle(TestEvents.item("SKU-000001", "Apple", "9.99", "v2"));

        List<String> seenDuringReplace = new ArrayList<>();
        store.onReplace = () -> seenDuringReplace.add(replica.version());
        store.failure = new StoreFailure("replica write failed");
        assertThrows(StoreFailure.class, () -> handler.handle(TestEvents.snapshot("v2", 1)),
                "a store failure is transient: the consumer retries the marker");
        assertEquals("v1", replica.version(), "a failed store write never reaches memory");

        store.failure = null;
        handler.handle(TestEvents.snapshot("v2", 1));
        assertEquals(List.of("v1", "v1"), seenDuringReplace, "memory still held the old version while the store wrote");
        assertEquals("v2", replica.version(), "the buffer survived the failure, so the retry completes");
        assertEquals(new BigDecimal("9.99"), replica.lookup("SKU-000001").orElseThrow().price());
    }

    @Test
    void aDurableReplicaThatFailedToLoadAtStartupIsRetriedOnTheNextEvent() {
        FakeStore store = new FakeStore(new ProductReplicaStore.Replica("v1", List.of(stored("SKU-000001", "Apple", "1.25"))));
        store.loadFailure = new StoreFailure("database down at startup");
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogEventHandler handler = new CatalogEventHandler(store, replica);
        assertFalse(handler.loadDurable(), "a never-loaded replica is unusable: Scan answers 500 (FR-030)");
        assertFalse(replica.usable());

        store.loadFailure = null;
        handler.handle(TestEvents.item("SKU-000001", "Apple", "1.25", "v1"));
        assertTrue(replica.usable(), "the durable replica is picked up once the database answers");
        handler.handle(TestEvents.snapshot("v1", 1));
        assertEquals(0, store.replacements, "so the replay of the stored version writes nothing");
    }

    @Test
    void onlyCatalogEventsAreAccepted() {
        CatalogEventHandler handler = new CatalogEventHandler(new FakeStore(null), new ProductCatalogReplica());
        PermanentEventFailure wrong = assertThrows(PermanentEventFailure.class,
                () -> handler.handle(TestEvents.request("tx-1", 1, TestEvents.line("SKU-000001", 1))));
        assertEquals(ParkReason.UNKNOWN_TYPE, wrong.reason());
    }

    private static ProductReplicaStore.ReplicaProduct stored(String sku, String name, String price) {
        return new ProductReplicaStore.ReplicaProduct(sku, name, new BigDecimal(price));
    }

    static final class FakeStore implements ProductReplicaStore {
        Replica replica;
        StoreFailure failure;
        StoreFailure loadFailure;
        Runnable onReplace = () -> { };
        int replacements;

        FakeStore(Replica replica) {
            this.replica = replica;
        }

        @Override
        public Optional<Replica> load() {
            if (loadFailure != null) throw loadFailure;
            return Optional.ofNullable(replica);
        }

        @Override
        public void replace(String catalogVersion, List<ReplicaProduct> products) {
            onReplace.run();
            if (failure != null) throw failure;
            replacements++;
            replica = new Replica(catalogVersion, List.copyOf(products));
        }
    }
}
