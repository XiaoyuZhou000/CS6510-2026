package database;

import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.FaultingConnections;
import support.FaultingConnections.CommitFault;
import support.InventoryTestSchema;
import support.TransactionTestSchema;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** The durable product replica: one-commit replacement and a usable-or-empty load (R4). */
class JdbcProductReplicaStoreTest {

    @Test
    void replaceWritesEveryProductAndTheMetaRowInOneCommit() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcProductReplicaStore store = new JdbcProductReplicaStore(
                    schema.newPool("test.replica." + suffix(), 1, 2, counting::wrap));
            String version = UUID.randomUUID().toString();
            List<ProductReplicaStore.ReplicaProduct> products = products(1200, "");

            try (CountingConnections.Scope scope = counting.scope()) {
                store.replace(version, products);
                assertEquals(1, scope.counts().commits(), "products and meta share one commit");
                assertEquals(1, scope.counts().deletes());
                assertEquals(1, scope.counts().updates(), "one meta UPDATE");
                assertTrue(scope.counts().inserts() >= 1, "one batched multi-row INSERT");
                assertEquals(0, scope.counts().setAutoCommitCalls());
            }
            assertEquals(1200, schema.queryLong("SELECT COUNT(*) FROM product_replica"));
            assertEquals(1200, schema.queryLong("SELECT COUNT(*) FROM product_replica WHERE catalog_version = ?", version));
            assertEquals(version, schema.queryString("SELECT catalog_version FROM product_replica_meta WHERE meta_key = 1"));
            assertEquals(1200, schema.queryLong("SELECT product_count FROM product_replica_meta WHERE meta_key = 1"));
            assertNotNull(schema.queryString("SELECT refreshed_at FROM product_replica_meta WHERE meta_key = 1"));
        }
    }

    @Test
    void loadReturnsTheSnapshotAndANullMetaVersionMeansUnusable() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcProductReplicaStore store = new JdbcProductReplicaStore(schema.pool());
            assertTrue(store.load().isEmpty(), "a never-loaded replica (NULL version) is unusable");

            String version = UUID.randomUUID().toString();
            store.replace(version, products(3, ""));
            ProductReplicaStore.Replica replica = store.load().orElseThrow();
            assertEquals(version, replica.catalogVersion());
            assertEquals(List.of(InventoryTestSchema.sku(1), InventoryTestSchema.sku(2), InventoryTestSchema.sku(3)),
                    replica.products().stream().map(ProductReplicaStore.ReplicaProduct::sku).toList());
            assertEquals(new BigDecimal("0.85"), replica.products().get(0).price());
            assertEquals("Item 2", replica.products().get(1).name());

            String next = UUID.randomUUID().toString();
            store.replace(next, products(2, "v2 "));
            ProductReplicaStore.Replica replaced = store.load().orElseThrow();
            assertEquals(next, replaced.catalogVersion());
            assertEquals(2, replaced.products().size(), "a replacement removes products the new catalog lacks");
            assertEquals("v2 Item 1", replaced.products().get(0).name());

            schema.update("UPDATE product_replica_meta SET catalog_version = NULL WHERE meta_key = 1");
            assertTrue(store.load().isEmpty());
        }
    }

    @Test
    void aFailedReplaceLeavesThePreviousReplicaIntact() throws Exception {
        FaultingConnections faults = new FaultingConnections();
        try (TransactionTestSchema schema = TransactionTestSchema.create()) {
            JdbcProductReplicaStore store = new JdbcProductReplicaStore(
                    schema.newPool("test.replica.fault." + suffix(), 1, 2, faults::wrap));
            String version = UUID.randomUUID().toString();
            store.replace(version, products(10, ""));

            faults.armForCurrentThread(CommitFault.failBeforeCommit());
            StoreFailure lost = assertThrows(StoreFailure.class,
                    () -> store.replace(UUID.randomUUID().toString(), products(4, "lost ")));
            assertTrue(lost.commitOutcomeUnknown());
            assertIntact(schema, store, version);

            List<ProductReplicaStore.ReplicaProduct> tooLong = new ArrayList<>(products(5, ""));
            tooLong.add(new ProductReplicaStore.ReplicaProduct("SKU-999999", "x".repeat(101), new BigDecimal("1.00")));
            assertThrows(StoreFailure.class, () -> store.replace(UUID.randomUUID().toString(), tooLong),
                    "a statement failure part-way through rolls back the delete and every insert");
            assertIntact(schema, store, version);
        }
    }

    private static void assertIntact(TransactionTestSchema schema, JdbcProductReplicaStore store, String version)
            throws Exception {
        ProductReplicaStore.Replica replica = store.load().orElseThrow();
        assertEquals(version, replica.catalogVersion());
        assertEquals(10, replica.products().size());
        assertEquals("Item 1", replica.products().get(0).name());
        assertEquals(10, schema.queryLong("SELECT COUNT(*) FROM product_replica"));
    }

    private static List<ProductReplicaStore.ReplicaProduct> products(int count, String namePrefix) {
        List<ProductReplicaStore.ReplicaProduct> products = new ArrayList<>();
        for (int n = 1; n <= count; n++) {
            products.add(new ProductReplicaStore.ReplicaProduct(InventoryTestSchema.sku(n), namePrefix + "Item " + n,
                    InventoryTestSchema.price(n)));
        }
        return products;
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
