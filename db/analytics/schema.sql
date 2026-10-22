-- =============================================================================
-- Analytics Service schema (cs6510_analytics), per data-model.md.
--
-- Drops and recreates only the Analytics Service's own tables, with both
-- singletons at 0. Run it with the Analytics user, for example through
--     db/reset-service.ps1 -Service analytics
-- It names no schema and references no other service's data (R3).
-- =============================================================================

DROP TABLE IF EXISTS popular_item;
DROP TABLE IF EXISTS popular_window;
DROP TABLE IF EXISTS analytics_checkpoint;
DROP TABLE IF EXISTS scan_sequence;
DROP TABLE IF EXISTS accepted_scan;
DROP TABLE IF EXISTS product_name;

-- Names captured at scan time and carried by each event (R12). Upserted only
-- when the in-memory name map lacks or differs from a name.
CREATE TABLE product_name (
    sku        VARCHAR(20)  NOT NULL,
    name       VARCHAR(100) NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (sku)
) ENGINE = InnoDB;

-- Every event durably accepted from Transaction's relay. The ingestion
-- transaction assigns the position and it never changes (FR-043); the source
-- identity deduplicates redelivery (FR-042). sku is a plain value.
CREATE TABLE accepted_scan (
    position              BIGINT       NOT NULL,
    source_transaction_id VARCHAR(40)  NOT NULL,
    source_scan_sequence  INT          NOT NULL,
    sku                   VARCHAR(20)  NOT NULL,
    received_at           TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (position),
    UNIQUE KEY uq_analytics_scan_source (source_transaction_id, source_scan_sequence),
    CONSTRAINT chk_analytics_scan_position CHECK (position > 0),
    CONSTRAINT chk_analytics_scan_sequence CHECK (source_scan_sequence >= 1)
) ENGINE = InnoDB;

-- The ingestion lock and position checkpoint: equals the highest position.
CREATE TABLE scan_sequence (
    sequence_key  TINYINT NOT NULL,
    last_position BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (sequence_key),
    CONSTRAINT chk_scan_sequence_singleton CHECK (sequence_key = 1),
    CONSTRAINT chk_scan_sequence_nonnegative CHECK (last_position >= 0)
) ENGINE = InnoDB;

INSERT INTO scan_sequence (sequence_key, last_position) VALUES (1, 0);

-- A complete popular interval contains exactly 1,000 positions. Ends are 1,000,
-- 1,500, 2,000, ...; its start is end - 999 (FR-044).
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

-- Top ten per window, by count descending then SKU ascending (FR-045).
CREATE TABLE popular_item (
    window_id  BIGINT      NOT NULL,
    rank_pos   INT         NOT NULL,
    sku        VARCHAR(20) NOT NULL,
    scan_count BIGINT      NOT NULL,
    PRIMARY KEY (window_id, rank_pos),
    CONSTRAINT fk_popular_window
        FOREIGN KEY (window_id) REFERENCES popular_window (window_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_popular_product_name
        FOREIGN KEY (sku) REFERENCES product_name (sku),
    CONSTRAINT chk_popular_rank CHECK (rank_pos BETWEEN 1 AND 10),
    CONSTRAINT chk_popular_count CHECK (scan_count > 0)
) ENGINE = InnoDB;

-- The writer locks this singleton and commits a window header, its ranks, and
-- the next checkpoint together (FR-047). 0 or a valid boundary.
CREATE TABLE analytics_checkpoint (
    checkpoint_key  TINYINT NOT NULL,
    last_window_end BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (checkpoint_key),
    CONSTRAINT chk_checkpoint_singleton CHECK (checkpoint_key = 1),
    CONSTRAINT chk_checkpoint_boundary CHECK (
        last_window_end = 0 OR
        (last_window_end >= 1000 AND MOD(last_window_end - 1000, 500) = 0)
    )
) ENGINE = InnoDB;

INSERT INTO analytics_checkpoint (checkpoint_key, last_window_end) VALUES (1, 0);
