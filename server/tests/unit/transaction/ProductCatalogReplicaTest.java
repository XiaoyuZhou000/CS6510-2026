package unit.transaction;

import database.ProductReplicaStore;
import database.StoreFailure;
import org.junit.jupiter.api.Test;
import support.TestEvents;
import transaction.CatalogEventHandler;
import transaction.ProductCatalogReplica;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The in-memory catalog snapshot, fed from the durable replica at startup and by catalog events afterwards
 * (R4, 006 D9), with a fake replica store. No gRPC refresh path remains.
 */
class ProductCatalogReplicaTest {

    @Test
    void startupServesTheDurableReplicaWhileInventoryIsDown() {
        CatalogEventHandlerTest.FakeStore store = new CatalogEventHandlerTest.FakeStore(
                new ProductReplicaStore.Replica("v1", List.of(stored("A", "Apple", "1.25"))));
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogEventHandler handler = new CatalogEventHandler(store, replica);

        assertFalse(replica.usable());
        assertTrue(handler.loadDurable(), "no event and no call to Inventory is needed to serve Scan");
        assertTrue(replica.usable());
        assertEquals("v1", replica.version());
        assertEquals("Apple", replica.lookup("A").orElseThrow().name());
        assertEquals(new BigDecimal("1.25"), replica.lookup("A").orElseThrow().price());
        assertTrue(replica.lookup("missing").isEmpty());
        assertEquals(0, store.replacements, "and the durable snapshot keeps serving unchanged");
    }

    @Test
    void theInMemorySnapshotSwapsOnlyAfterTheStoreCommit() {
        CatalogEventHandlerTest.FakeStore store = new CatalogEventHandlerTest.FakeStore(
                new ProductReplicaStore.Replica("v1", List.of(stored("A", "Apple", "1.25"))));
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogEventHandler handler = new CatalogEventHandler(store, replica);
        handler.loadDurable();
        handler.handle(TestEvents.item("A", "Apple", "9.99", "v2"));

        List<String> seenDuringReplace = new ArrayList<>();
        store.onReplace = () -> seenDuringReplace.add(replica.version());
        store.failure = new StoreFailure("replica write failed");
        assertThrows(StoreFailure.class, () -> handler.handle(TestEvents.snapshot("v2", 1)));
        assertEquals("v1", replica.version(), "a failed store write never reaches memory");
        assertEquals(new BigDecimal("1.25"), replica.lookup("A").orElseThrow().price());

        store.failure = null;
        handler.handle(TestEvents.snapshot("v2", 1));
        assertEquals(List.of("v1", "v1"), seenDuringReplace, "memory still held the old version while the store wrote");
        assertEquals("v2", replica.version());
        assertEquals(new BigDecimal("9.99"), replica.lookup("A").orElseThrow().price());
    }

    @Test
    void theSnapshotIsImmutableAndRejectsDuplicateSkus() {
        ProductCatalogReplica replica = new ProductCatalogReplica();
        List<ProductCatalogReplica.Product> products = new ArrayList<>(List.of(product("A", "Apple", "1.25")));
        replica.install("v1", products);
        products.add(product("B", "Bread", "2.50"));
        assertTrue(replica.lookup("B").isEmpty(), "installing copies the product list");
        assertEquals(1, replica.size());
        assertThrows(IllegalArgumentException.class,
                () -> replica.install("v2", List.of(product("A", "Apple", "1.25"), product("A", "Again", "2.00"))));
        assertEquals("v1", replica.version(), "a rejected snapshot leaves the current one");
    }

    private static ProductReplicaStore.ReplicaProduct stored(String sku, String name, String price) {
        return new ProductReplicaStore.ReplicaProduct(sku, name, new BigDecimal(price));
    }

    private static ProductCatalogReplica.Product product(String sku, String name, String price) {
        return new ProductCatalogReplica.Product(sku, name, new BigDecimal(price));
    }
}
