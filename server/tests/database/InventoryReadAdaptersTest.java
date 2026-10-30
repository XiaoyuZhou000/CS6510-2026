package database;

import events.OutboxRow;
import org.junit.jupiter.api.Test;
import support.InventoryTestSchema;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Inventory's catalog, catalog-meta, and low-stock readers over Inventory's own schema (R8), and the catalog
 * publication's outbox rows and {@code catalog_meta.published_at} (006 D9). Migrates the catalog and inventory
 * parts of JdbcStoreAdaptersTest.
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

            JdbcCatalogMetaStore metaStore = new JdbcCatalogMetaStore(schema.pool());
            CatalogMetaStore.CatalogMeta meta = metaStore.read().orElseThrow();
            assertEquals(version, meta.catalogVersion());
            assertEquals(2, meta.productCount());
            assertNull(meta.publishedAt(), "a fresh seed was never published (006 D9)");

            Instant at = Instant.parse("2026-10-30T12:00:00.123Z");
            metaStore.publish(List.of(outboxRow("e-1", "A"), outboxRow("e-2", "__snapshot__")), at);
            assertEquals(at, metaStore.read().orElseThrow().publishedAt(), "published_at is set by the publication");
            assertEquals(2, schema.queryLong("SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL"));
            assertEquals("__snapshot__", schema.queryString("SELECT routing_key FROM outbox_event ORDER BY outbox_seq DESC LIMIT 1"),
                    "rows are recorded in the order given");
            assertEquals(version, metaStore.read().orElseThrow().catalogVersion(), "the version is untouched");
        }
    }

    private static OutboxRow outboxRow(String eventId, String key) {
        return new OutboxRow(eventId, "selfcheckout.catalog.v1", key, "CatalogItemPublished", null,
                new byte[] {1, 2, 3}, Instant.parse("2026-10-30T12:00:00.100Z"));
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

            StoreFailure publishFailure = assertThrows(StoreFailure.class,
                    () -> meta.publish(List.of(outboxRow("e-1", "A")), Instant.now()));
            assertInstanceOf(SQLException.class, publishFailure.getCause());
            assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM outbox_event"),
                    "a failed published_at UPDATE rolls the outbox rows back with it");
        }
    }
}
