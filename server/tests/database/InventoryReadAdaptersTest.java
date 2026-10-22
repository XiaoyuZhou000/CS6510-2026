package database;

import org.junit.jupiter.api.Test;
import support.InventoryTestSchema;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Inventory's catalog, catalog-meta, and low-stock readers over Inventory's own schema (R8).
 * Migrates the catalog and inventory parts of JdbcStoreAdaptersTest.
 */
class InventoryReadAdaptersTest {

    @Test
    void catalogAndLowStockAdaptersMapRowsThroughTypedContracts() throws Exception {
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            schema.addProduct("B", "Bread", new BigDecimal("2.50"), 4, 3);
            String version = schema.addProduct("A", "Apple", new BigDecimal("1.25"), 1, 2);
            JdbcCatalogStore catalog = new JdbcCatalogStore(schema.pool());
            JdbcInventoryStore inventory = new JdbcInventoryStore(schema.pool());

            assertEquals(List.of("A", "B"), catalog.loadAll().stream().map(CatalogStore.CatalogItem::sku).toList(),
                    "the catalog comes back in SKU order");
            assertEquals(new BigDecimal("1.25"), catalog.loadAll().getFirst().price());
            assertEquals("Apple", catalog.loadAll().getFirst().name());

            List<InventoryStore.LowStockRecord> defaults = inventory.findLowStock(null);
            assertEquals(List.of("A"), defaults.stream().map(InventoryStore.LowStockRecord::sku).toList());
            assertEquals(2, defaults.getFirst().threshold(), "each row keeps its stored threshold");
            assertEquals(1, defaults.getFirst().currentStock());
            assertEquals("Apple", defaults.getFirst().name());

            List<InventoryStore.LowStockRecord> override = inventory.findLowStock(5);
            assertEquals(List.of("A", "B"), override.stream().map(InventoryStore.LowStockRecord::sku).toList());
            assertTrue(override.stream().allMatch(row -> row.threshold() == 5));
            assertEquals(List.of("A"), inventory.findLowStock(null).stream().map(InventoryStore.LowStockRecord::sku).toList(),
                    "an override never changes the stored thresholds");

            CatalogMetaStore.CatalogMeta meta = new JdbcCatalogMetaStore(schema.pool()).read().orElseThrow();
            assertEquals(version, meta.catalogVersion());
            assertEquals(2, meta.productCount());
        }
    }

    @Test
    void theDefaultThresholdOfFiftyIsInclusive() throws Exception {
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            schema.seed(3, 10_000);
            schema.setStock(InventoryTestSchema.sku(1), InventoryTestSchema.DEFAULT_THRESHOLD);
            schema.setStock(InventoryTestSchema.sku(2), InventoryTestSchema.DEFAULT_THRESHOLD + 1);
            schema.setStock(InventoryTestSchema.sku(3), 0);
            JdbcInventoryStore inventory = new JdbcInventoryStore(schema.pool());

            List<InventoryStore.LowStockRecord> defaults = inventory.findLowStock(null);
            assertEquals(List.of(InventoryTestSchema.sku(1), InventoryTestSchema.sku(3)),
                    defaults.stream().map(InventoryStore.LowStockRecord::sku).toList(), "at the threshold is low; above is not");
            assertTrue(defaults.stream().allMatch(row -> row.threshold() == 50));
            assertEquals(List.of(InventoryTestSchema.sku(3)),
                    inventory.findLowStock(0).stream().map(InventoryStore.LowStockRecord::sku).toList());
            assertThrows(IllegalArgumentException.class, () -> inventory.findLowStock(-1));
        }
    }

    @Test
    void sqlFailuresAreWrappedAsTransportNeutralStoreFailures() throws Exception {
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            schema.seed(2, 10);
            JdbcCatalogStore catalog = new JdbcCatalogStore(schema.pool());
            JdbcInventoryStore inventory = new JdbcInventoryStore(schema.pool());
            JdbcCatalogMetaStore meta = new JdbcCatalogMetaStore(schema.pool());
            schema.update("DROP TABLE inventory");
            schema.update("DROP TABLE catalog_item");
            schema.update("DROP TABLE catalog_meta");

            StoreFailure catalogFailure = assertThrows(StoreFailure.class, catalog::loadAll);
            StoreFailure inventoryFailure = assertThrows(StoreFailure.class, () -> inventory.findLowStock(null));
            StoreFailure metaFailure = assertThrows(StoreFailure.class, meta::read);

            assertInstanceOf(SQLException.class, catalogFailure.getCause());
            assertEquals("Failed to load catalog", catalogFailure.getMessage());
            assertInstanceOf(SQLException.class, inventoryFailure.getCause());
            assertEquals("Failed to read low-stock inventory", inventoryFailure.getMessage());
            assertInstanceOf(SQLException.class, metaFailure.getCause());
            assertEquals("Failed to read catalog version", metaFailure.getMessage());
        }
    }
}
