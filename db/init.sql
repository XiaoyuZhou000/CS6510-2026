-- =============================================================================
-- Self-Checkout Monolith — MySQL initialization / reset script
--
-- Running this file DROPS and fully recreates the `selfcheckout` database:
-- schema + seed data. It is the single source of truth for the DB and is
-- designed to be re-run before every load-test to guarantee a clean start
-- (see the assignment: "a database that you can easily reinitialize after
-- each test run").
--
-- Usage (native MySQL client):
--     mysql -u root -p < init.sql
--
-- Requires MySQL 8.0+ (uses recursive CTEs for fast seeding).
--
-- Tunable seed parameters (defaults match the assignment / mock server):
--     catalog size .......... 2000 items
--     stock per item ........ 10000 units
--     low-stock threshold ... 50 units
--     popularity window ..... 1000 scans, slide interval 500
-- Change the @vars in the "SEED PARAMETERS" section below to adjust.
-- =============================================================================

-- ----------------------------------------------------------------------------
-- 0. Fresh database
-- ----------------------------------------------------------------------------
DROP DATABASE IF EXISTS cs6510_selfcheckout;
CREATE DATABASE cs6510_selfcheckout
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;
USE cs6510_selfcheckout;

-- ----------------------------------------------------------------------------
-- 1. Schema
-- ----------------------------------------------------------------------------

-- Product catalog: SKU, display name, unit price. Immutable during a test run.
CREATE TABLE catalog_item (
    sku        VARCHAR(20)    NOT NULL,
    name       VARCHAR(100)   NOT NULL,
    price      DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (sku)
) ENGINE = InnoDB;

-- Inventory: current stock per SKU. The check-then-decrement "gotcha" is
-- avoided by the application issuing a single atomic, row-locked statement:
--     UPDATE inventory SET stock_quantity = stock_quantity - 1
--     WHERE sku = ? AND stock_quantity > 0;
-- InnoDB row locking makes this safe under concurrent stations.
CREATE TABLE inventory (
    sku                 VARCHAR(20) NOT NULL,
    stock_quantity      INT         NOT NULL,
    low_stock_threshold INT         NOT NULL,
    PRIMARY KEY (sku),
    CONSTRAINT fk_inventory_sku
        FOREIGN KEY (sku) REFERENCES catalog_item (sku)
        ON DELETE CASCADE,
    CONSTRAINT chk_stock_nonnegative CHECK (stock_quantity >= 0),
    INDEX idx_inventory_lowstock (stock_quantity)
) ENGINE = InnoDB;

-- Transactions: one row per checkout session (start -> scan* -> complete).
CREATE TABLE transaction (
    transaction_id VARCHAR(40)    NOT NULL,
    station_id     VARCHAR(40)    NOT NULL,
    status         ENUM('OPEN', 'COMPLETED', 'CANCELLED') NOT NULL DEFAULT 'OPEN',
    total_amount   DECIMAL(12, 2) NOT NULL DEFAULT 0.00,
    started_at     TIMESTAMP(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    completed_at   TIMESTAMP(3)   NULL,
    PRIMARY KEY (transaction_id),
    INDEX idx_transaction_station (station_id),
    INDEX idx_transaction_status (status)
) ENGINE = InnoDB;

-- Basket lines are committed on each accepted scan, while the transaction is
-- OPEN. They remain after completion and supply the receipt and stock changes.
-- (Same SKU scanned twice -> quantity 2, enforced by the unique key + upsert.)
CREATE TABLE transaction_line (
    transaction_id VARCHAR(40)    NOT NULL,
    sku            VARCHAR(20)    NOT NULL,
    quantity       INT            NOT NULL DEFAULT 1,
    unit_price     DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (transaction_id, sku),
    CONSTRAINT fk_line_transaction
        FOREIGN KEY (transaction_id) REFERENCES transaction (transaction_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_line_sku
        FOREIGN KEY (sku) REFERENCES catalog_item (sku),
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0)
) ENGINE = InnoDB;

-- Only Analytics Materializer locks this singleton, once per bounded batch.
-- Transaction Service never touches it. Assignment and checkpoint commit together.
CREATE TABLE scan_sequence (
    sequence_key  TINYINT NOT NULL,
    last_position BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (sequence_key),
    CONSTRAINT chk_scan_sequence_singleton CHECK (sequence_key = 1),
    CONSTRAINT chk_scan_sequence_nonnegative CHECK (last_position >= 0)
) ENGINE = InnoDB;

INSERT INTO scan_sequence (sequence_key, last_position) VALUES (1, 0);

-- Scan commits an immutable event identity with no analytics position.
-- Materializer later assigns the nullable position once in a bounded batch.
CREATE TABLE accepted_scan (
    event_id       BIGINT         NOT NULL AUTO_INCREMENT,
    position       BIGINT         NULL,
    transaction_id VARCHAR(40)   NOT NULL,
    sku            VARCHAR(20)   NOT NULL,
    unit_price     DECIMAL(10, 2) NOT NULL,
    accepted_at    TIMESTAMP(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (event_id),
    UNIQUE KEY uq_scan_position (position),
    KEY idx_scan_pending (position, event_id),
    CONSTRAINT fk_scan_transaction
        FOREIGN KEY (transaction_id) REFERENCES transaction (transaction_id),
    CONSTRAINT fk_scan_sku
        FOREIGN KEY (sku) REFERENCES catalog_item (sku),
    CONSTRAINT chk_scan_position_positive CHECK (position IS NULL OR position > 0),
    INDEX idx_scan_transaction (transaction_id)
) ENGINE = InnoDB;

-- A complete popular interval contains exactly 1,000 accepted positions.
-- Ends are 1,000, 1,500, 2,000, ...; its start is end - 999.
CREATE TABLE popular_window (
    window_id    BIGINT       NOT NULL AUTO_INCREMENT,
    window_start BIGINT       NOT NULL,
    window_end   BIGINT       NOT NULL,
    computed_at  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (window_id),
    UNIQUE KEY uq_window_end (window_end),
    CONSTRAINT chk_window_bounds CHECK (
        window_end >= 1000 AND MOD(window_end - 1000, 500) = 0
        AND window_start = window_end - 999
    ),
    INDEX idx_window_computed (computed_at)
) ENGINE = InnoDB;

CREATE TABLE popular_item (
    window_id  BIGINT      NOT NULL,
    rank_pos   INT         NOT NULL,       -- 1 = most popular
    sku        VARCHAR(20) NOT NULL,
    scan_count BIGINT      NOT NULL,
    PRIMARY KEY (window_id, rank_pos),
    CONSTRAINT fk_popular_window
        FOREIGN KEY (window_id) REFERENCES popular_window (window_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_popular_sku
        FOREIGN KEY (sku) REFERENCES catalog_item (sku),
    CONSTRAINT chk_popular_rank CHECK (rank_pos BETWEEN 1 AND 10),
    CONSTRAINT chk_popular_count CHECK (scan_count > 0)
) ENGINE = InnoDB;

-- The Analytics writer locks this singleton and commits a complete header,
-- zero to ten contiguous ranks (count descending, SKU ascending on ties),
-- and the next checkpoint in one transaction. The next end is 1,000 after
-- zero, then advances by 500. Normal queries read only committed windows.
CREATE TABLE analytics_checkpoint (
    checkpoint_key TINYINT NOT NULL,
    last_window_end BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (checkpoint_key),
    CONSTRAINT chk_checkpoint_singleton CHECK (checkpoint_key = 1),
    CONSTRAINT chk_checkpoint_boundary CHECK (
        last_window_end = 0 OR
        (last_window_end >= 1000 AND MOD(last_window_end - 1000, 500) = 0)
    )
) ENGINE = InnoDB;

INSERT INTO analytics_checkpoint (checkpoint_key, last_window_end) VALUES (1, 0);

-- ----------------------------------------------------------------------------
-- 2. Seed parameters
-- ----------------------------------------------------------------------------
SET @catalog_size       = 2000;   -- number of distinct products
SET @stock_per_item     = 10000;  -- starting stock for every product
SET @low_stock_threshold = 50;    -- alert threshold for /inventory/low-stock

-- Recursive CTE below can recurse up to @catalog_size levels.
SET SESSION cte_max_recursion_depth = 1000000;

-- ----------------------------------------------------------------------------
-- 3. Seed catalog + inventory
--    Price formula matches the mock server so rankings stay comparable:
--        price = round(0.5 + (i % 47) * 0.35, 2)
-- ----------------------------------------------------------------------------
INSERT INTO catalog_item (sku, name, price)
WITH RECURSIVE seq (n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < @catalog_size
)
SELECT
    CONCAT('SKU-', LPAD(n, 6, '0'))          AS sku,
    CONCAT('Item ', n)                       AS name,
    ROUND(0.5 + MOD(n, 47) * 0.35, 2)        AS price
FROM seq;

INSERT INTO inventory (sku, stock_quantity, low_stock_threshold)
SELECT sku, @stock_per_item, @low_stock_threshold
FROM catalog_item;

-- ----------------------------------------------------------------------------
-- 4. Sanity report
-- ----------------------------------------------------------------------------
SELECT
    (SELECT COUNT(*) FROM catalog_item) AS catalog_items,
    (SELECT COUNT(*) FROM inventory)    AS inventory_rows,
    (SELECT SUM(stock_quantity) FROM inventory) AS total_units,
    (SELECT MIN(price) FROM catalog_item) AS min_price,
    (SELECT MAX(price) FROM catalog_item) AS max_price;
