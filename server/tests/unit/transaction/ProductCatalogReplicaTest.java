package unit.transaction;

import database.ProductReplicaStore;
import database.StoreFailure;
import org.junit.jupiter.api.Test;
import transaction.CatalogReplicaRefresher;
import transaction.DependencyUnavailableFailure;
import transaction.InventoryGateway;
import transaction.ProductCatalogReplica;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** The in-memory catalog snapshot and its refresher, with fake Inventory and replica store (R4). */
class ProductCatalogReplicaTest {
    private static final Duration REFRESH = Duration.ofSeconds(30);

    @Test
    void startupServesTheDurableReplicaWhileInventoryIsDown() {
        FakeStore store = new FakeStore(new ProductReplicaStore.Replica("v1", List.of(stored("A", "Apple", "1.25"))));
        FakeInventory inventory = new FakeInventory();
        inventory.down = true;
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogReplicaRefresher refresher = new CatalogReplicaRefresher(inventory, store, replica, REFRESH);

        assertFalse(replica.usable());
        assertTrue(refresher.loadDurable());
        assertTrue(replica.usable());
        assertEquals("v1", replica.version());
        assertEquals("Apple", replica.lookup("A").orElseThrow().name());
        assertEquals(new BigDecimal("1.25"), replica.lookup("A").orElseThrow().price());
        assertTrue(replica.lookup("missing").isEmpty());

        assertEquals(Duration.ofSeconds(1), refresher.runOnce(), "an unreachable Inventory backs off");
        assertEquals("v1", replica.version(), "and the durable snapshot keeps serving");
        assertEquals(0, inventory.catalogCalls);
    }

    @Test
    void aRefreshFetchesTheCatalogOnlyWhenTheVersionDiffers() {
        FakeStore store = new FakeStore(new ProductReplicaStore.Replica("v1", List.of(stored("A", "Apple", "1.25"))));
        FakeInventory inventory = new FakeInventory();
        inventory.version = "v1";
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogReplicaRefresher refresher = new CatalogReplicaRefresher(inventory, store, replica, REFRESH);
        refresher.loadDurable();

        assertEquals(REFRESH, refresher.runOnce());
        assertEquals(1, inventory.versionCalls);
        assertEquals(0, inventory.catalogCalls, "an unchanged version fetches nothing");
        assertEquals(0, store.replacements);

        inventory.version = "v2";
        inventory.products = List.of(product("A", "Apple", "1.30"), product("B", "Bread", "2.50"));
        assertEquals(REFRESH, refresher.runOnce());
        assertEquals(1, inventory.catalogCalls);
        assertEquals(1, store.replacements);
        assertEquals("v2", store.replica.catalogVersion());
        assertEquals("v2", replica.version());
        assertEquals(new BigDecimal("1.30"), replica.lookup("A").orElseThrow().price());
        assertEquals("Bread", replica.lookup("B").orElseThrow().name());

        assertEquals(REFRESH, refresher.runOnce());
        assertEquals(1, inventory.catalogCalls, "the new version is now the stored one");
    }

    @Test
    void theInMemorySnapshotSwapsOnlyAfterTheStoreCommit() {
        FakeStore store = new FakeStore(new ProductReplicaStore.Replica("v1", List.of(stored("A", "Apple", "1.25"))));
        FakeInventory inventory = new FakeInventory();
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogReplicaRefresher refresher = new CatalogReplicaRefresher(inventory, store, replica, REFRESH);
        refresher.loadDurable();
        inventory.version = "v2";
        inventory.products = List.of(product("A", "Apple", "9.99"));

        List<String> seenDuringReplace = new ArrayList<>();
        store.onReplace = () -> seenDuringReplace.add(replica.version());
        store.failure = new StoreFailure("replica write failed");
        assertEquals(Duration.ofSeconds(1), refresher.runOnce());
        assertEquals("v1", replica.version(), "a failed store write never reaches memory");
        assertEquals(new BigDecimal("1.25"), replica.lookup("A").orElseThrow().price());

        store.failure = null;
        assertEquals(REFRESH, refresher.runOnce());
        assertEquals(List.of("v1", "v1"), seenDuringReplace, "memory still held the old version while the store wrote");
        assertEquals("v2", replica.version());
        assertEquals(new BigDecimal("9.99"), replica.lookup("A").orElseThrow().price());
    }

    @Test
    void aNeverPopulatedReplicaIsUnusableAndRetriesEverySecond() {
        FakeStore store = new FakeStore(null);
        FakeInventory inventory = new FakeInventory();
        inventory.down = true;
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogReplicaRefresher refresher = new CatalogReplicaRefresher(inventory, store, replica, REFRESH);

        assertFalse(refresher.loadDurable());
        for (int i = 0; i < 5; i++) {
            assertEquals(Duration.ofSeconds(1), refresher.runOnce(), "no growth while never populated");
            assertFalse(replica.usable());
            assertNull(replica.version());
            assertTrue(replica.lookup("A").isEmpty());
        }

        inventory.down = false;
        inventory.version = "v1";
        inventory.products = List.of(product("A", "Apple", "1.25"));
        assertEquals(REFRESH, refresher.runOnce());
        assertTrue(replica.usable());
        assertEquals("v1", store.replica.catalogVersion(), "the first load is made durable before use");
    }

    @Test
    void aReplicaThatAppearsDurablyIsPickedUpBeforeAskingInventory() {
        FakeStore store = new FakeStore(null);
        FakeInventory inventory = new FakeInventory();
        inventory.down = true;
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogReplicaRefresher refresher = new CatalogReplicaRefresher(inventory, store, replica, REFRESH);
        store.loadFailure = new StoreFailure("database down at startup");
        assertFalse(refresher.loadDurable());

        store.loadFailure = null;
        store.replica = new ProductReplicaStore.Replica("v1", List.of(stored("A", "Apple", "1.25")));
        refresher.runOnce();
        assertTrue(replica.usable(), "the durable replica is retried once the database answers");
        assertEquals("v1", replica.version());
    }

    @Test
    void failuresBackOffFromOneToThirtySeconds() {
        FakeStore store = new FakeStore(new ProductReplicaStore.Replica("v1", List.of(stored("A", "Apple", "1.25"))));
        FakeInventory inventory = new FakeInventory();
        inventory.down = true;
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogReplicaRefresher refresher = new CatalogReplicaRefresher(inventory, store, replica, REFRESH);
        refresher.loadDurable();

        List<Long> delays = new ArrayList<>();
        for (int i = 0; i < 8; i++) delays.add(refresher.runOnce().toSeconds());
        assertEquals(List.of(1L, 2L, 4L, 8L, 16L, 30L, 30L, 30L), delays);

        inventory.down = false;
        inventory.version = "v1";
        assertEquals(REFRESH, refresher.runOnce(), "a success resets the backoff");
        inventory.down = true;
        assertEquals(Duration.ofSeconds(1), refresher.runOnce());
    }

    @Test
    void catalogRefreshSecondsIsHonored() {
        assertEquals(Duration.ofSeconds(30), CatalogReplicaRefresher.refreshInterval(Map.of()));
        assertEquals(Duration.ofSeconds(7), CatalogReplicaRefresher.refreshInterval(Map.of("CATALOG_REFRESH_SECONDS", "7")));
        assertThrows(IllegalArgumentException.class,
                () -> CatalogReplicaRefresher.refreshInterval(Map.of("CATALOG_REFRESH_SECONDS", "0")));

        FakeStore store = new FakeStore(new ProductReplicaStore.Replica("v1", List.of(stored("A", "Apple", "1.25"))));
        FakeInventory inventory = new FakeInventory();
        inventory.version = "v1";
        ProductCatalogReplica replica = new ProductCatalogReplica();
        CatalogReplicaRefresher refresher = new CatalogReplicaRefresher(inventory, store, replica,
                CatalogReplicaRefresher.refreshInterval(Map.of("CATALOG_REFRESH_SECONDS", "7")));
        refresher.loadDurable();
        assertEquals(Duration.ofSeconds(7), refresher.runOnce());
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

    private static final class FakeInventory implements InventoryGateway {
        boolean down;
        String version = "v1";
        List<ProductCatalogReplica.Product> products = List.of();
        int versionCalls;
        int catalogCalls;

        @Override
        public CatalogVersion catalogVersion() {
            versionCalls++;
            if (down) throw new DependencyUnavailableFailure("Inventory unavailable");
            return new CatalogVersion(version, products.size());
        }

        @Override
        public Catalog catalog() {
            catalogCalls++;
            if (down) throw new DependencyUnavailableFailure("Inventory unavailable");
            return new Catalog(version, products);
        }

        @Override
        public ApplyResult applyCheckout(database.CheckoutAttemptStore.AttemptKey key, java.util.List<CheckoutLine> lines,
                                         java.time.Duration deadline) {
            throw new AssertionError("The replica never checks out");
        }

        @Override
        public RecordedOutcome checkoutOutcome(database.CheckoutAttemptStore.AttemptKey key) {
            throw new AssertionError("The replica never checks out");
        }
    }

    private static final class FakeStore implements ProductReplicaStore {
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
