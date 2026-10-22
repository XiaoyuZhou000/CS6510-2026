-- =============================================================================
-- Inventory Service deterministic baseline (FR-052, R13). Run after schema.sql.
--
--     catalog size .......... 2000 products, SKU-000001 .. SKU-002000
--     price ................. round(0.5 + (n mod 47) * 0.35, 2)
--     stock per product ..... 10000 units
--     low-stock threshold ... 50 units
--     catalog_version ....... a new UUID on every reseed
-- =============================================================================

-- The recursive CTE below recurses once per product; this is a session setting only.
SET SESSION cte_max_recursion_depth = 2000;

INSERT INTO catalog_item (sku, name, price)
WITH RECURSIVE seq (n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 2000
)
SELECT
    CONCAT('SKU-', LPAD(n, 6, '0'))   AS sku,
    CONCAT('Item ', n)                AS name,
    ROUND(0.5 + MOD(n, 47) * 0.35, 2) AS price
FROM seq;

INSERT INTO inventory (sku, stock_quantity, low_stock_threshold)
SELECT sku, 10000, 50 FROM catalog_item;

INSERT INTO catalog_meta (meta_key, catalog_version, product_count, seeded_at)
SELECT 1, UUID(), COUNT(*), NOW(3) FROM catalog_item;

-- Sanity report.
SELECT
    (SELECT COUNT(*) FROM catalog_item)          AS catalog_items,
    (SELECT COUNT(*) FROM inventory)             AS inventory_rows,
    (SELECT SUM(stock_quantity) FROM inventory)  AS total_units,
    (SELECT catalog_version FROM catalog_meta)   AS catalog_version;
