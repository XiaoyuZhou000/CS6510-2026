-- =============================================================================
-- Inventory Service schema (cs6510_inventory), per data-model.md.
--
-- Drops and recreates only the Inventory Service's own tables, empty. seed.sql
-- then loads the deterministic baseline. Both run with the Inventory user, for
-- example through
--     db/reset-service.ps1 -Service inventory
-- It names no schema and references no other service's data (R3).
-- =============================================================================

DROP TABLE IF EXISTS checkout_operation;
DROP TABLE IF EXISTS catalog_meta;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS catalog_item;

-- Product catalog: SKU, display name, unit price.
CREATE TABLE catalog_item (
    sku   VARCHAR(20)    NOT NULL,
    name  VARCHAR(100)   NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (sku),
    CONSTRAINT chk_catalog_item_price CHECK (price >= 0)
) ENGINE = InnoDB;

-- Stock per SKU. Changed only by checkout operations, and never cached (R8).
-- ApplyCheckout locks the referenced rows in SKU order and deducts them in one
-- UPDATE; the CHECK constraint guards against faults.
CREATE TABLE inventory (
    sku                 VARCHAR(20) NOT NULL,
    stock_quantity      INT         NOT NULL,
    low_stock_threshold INT         NOT NULL,
    PRIMARY KEY (sku),
    CONSTRAINT fk_inventory_sku
        FOREIGN KEY (sku) REFERENCES catalog_item (sku)
        ON DELETE CASCADE,
    CONSTRAINT chk_stock_nonnegative CHECK (stock_quantity >= 0),
    CONSTRAINT chk_low_stock_threshold_nonnegative CHECK (low_stock_threshold >= 0),
    INDEX idx_inventory_lowstock (stock_quantity)
) ENGINE = InnoDB;

-- A new catalog_version UUID at every reset or reseed (R4); Transaction replaces
-- its product replica when the version it reads differs from its stored one.
CREATE TABLE catalog_meta (
    meta_key        TINYINT      NOT NULL,
    catalog_version CHAR(36)     NOT NULL,
    product_count   INT          NOT NULL,
    seeded_at       TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (meta_key),
    CONSTRAINT chk_catalog_meta_singleton CHECK (meta_key = 1),
    CONSTRAINT chk_catalog_meta_count CHECK (product_count >= 0)
) ENGINE = InnoDB;

-- One row per checkout attempt Inventory has processed, keyed by the attempt
-- identifier. The row and any stock UPDATE share one commit. `lines` holds the
-- canonical payload a replay must match; it is NULL only for a BLOCKED tombstone.
CREATE TABLE checkout_operation (
    transaction_id         VARCHAR(40)  NOT NULL,
    attempt_number         INT          NOT NULL,
    state                  ENUM('APPLIED', 'REJECTED', 'RESTORED', 'BLOCKED') NOT NULL,
    rejection_reason       ENUM('INSUFFICIENT_STOCK', 'INVALID_REQUEST') NULL,
    rejected_sku           VARCHAR(20)  NULL,
    `lines`                TEXT         NULL,
    -- At most one effective deduction per transaction (FR-029).
    applied_transaction_id VARCHAR(40)
        GENERATED ALWAYS AS (IF(state = 'APPLIED', transaction_id, NULL)) STORED,
    processed_at           TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    restored_at            TIMESTAMP(3) NULL,
    PRIMARY KEY (transaction_id, attempt_number),
    UNIQUE KEY uq_checkout_operation_applied (applied_transaction_id),
    CONSTRAINT chk_checkout_operation_attempt CHECK (attempt_number >= 1),
    CONSTRAINT chk_checkout_operation_rejection CHECK ((state = 'REJECTED') = (rejection_reason IS NOT NULL)),
    CONSTRAINT chk_checkout_operation_lines CHECK ((state = 'BLOCKED') = (`lines` IS NULL)),
    CONSTRAINT chk_checkout_operation_restored CHECK ((state = 'RESTORED') = (restored_at IS NOT NULL))
) ENGINE = InnoDB;
