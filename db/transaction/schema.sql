-- =============================================================================
-- Transaction Service schema (cs6510_transaction), per data-model.md.
--
-- Drops and recreates only the Transaction Service's own tables, empty. Run it
-- against the Transaction schema with the Transaction user, for example through
--     db/reset-service.ps1 -Service transaction
-- It names no schema, so the caller's default database decides where it runs,
-- and it references no other service's data (R3).
-- =============================================================================

DROP TABLE IF EXISTS dead_letter_event;
DROP TABLE IF EXISTS outbox_event;
DROP TABLE IF EXISTS checkout_attempt;
DROP TABLE IF EXISTS accepted_scan;
DROP TABLE IF EXISTS product_replica_meta;
DROP TABLE IF EXISTS product_replica;
DROP TABLE IF EXISTS `transaction`;

-- One row per checkout session. Every write is a guarded conditional UPDATE
-- (R19); item_count doubles as the Scan guard's expected version.
CREATE TABLE `transaction` (
    transaction_id        VARCHAR(40)    NOT NULL,
    station_id            VARCHAR(40)    NOT NULL,
    status                ENUM('OPEN', 'COMPLETED', 'CANCELLED') NOT NULL DEFAULT 'OPEN',
    item_count            INT            NOT NULL DEFAULT 0,
    total_amount          DECIMAL(12, 2) NOT NULL DEFAULT 0.00,
    attempt_count         INT            NOT NULL DEFAULT 0,
    active_attempt_number INT            NULL,
    started_at            TIMESTAMP(3)   NOT NULL,
    completed_at          TIMESTAMP(3)   NULL,
    PRIMARY KEY (transaction_id),
    INDEX idx_transaction_active_attempt (active_attempt_number),
    CONSTRAINT chk_transaction_item_count CHECK (item_count >= 0),
    CONSTRAINT chk_transaction_attempt_count CHECK (attempt_count >= 0),
    CONSTRAINT chk_transaction_completed_at CHECK ((status = 'COMPLETED') = (completed_at IS NOT NULL)),
    CONSTRAINT chk_transaction_completed_settled CHECK (status <> 'COMPLETED' OR active_attempt_number IS NULL)
) ENGINE = InnoDB;

-- The scan log, the basket record, and the scan outbox in one table (R10, 006 D10).
-- Each row is written in the same local transaction as the Scan's guarded UPDATE,
-- and published_at is set only after the broker confirms the ScanAccepted record.
CREATE TABLE accepted_scan (
    scan_id        BIGINT         NOT NULL AUTO_INCREMENT,
    transaction_id VARCHAR(40)    NOT NULL,
    scan_sequence  INT            NOT NULL,
    sku            VARCHAR(20)    NOT NULL,
    product_name   VARCHAR(100)   NOT NULL,
    unit_price     DECIMAL(10, 2) NOT NULL,
    accepted_at    TIMESTAMP(3)   NOT NULL,
    published_at   TIMESTAMP(3)   NULL,
    PRIMARY KEY (scan_id),
    UNIQUE KEY uq_accepted_scan_identity (transaction_id, scan_sequence),
    INDEX idx_accepted_scan_unpublished (published_at, scan_id),
    CONSTRAINT fk_accepted_scan_transaction
        FOREIGN KEY (transaction_id) REFERENCES `transaction` (transaction_id),
    CONSTRAINT chk_accepted_scan_sequence CHECK (scan_sequence >= 1),
    CONSTRAINT chk_accepted_scan_price CHECK (unit_price >= 0)
) ENGINE = InnoDB;

-- Durable copy of Inventory's catalog, replaced in one local transaction when
-- Inventory's catalog_version changes (R4).
CREATE TABLE product_replica (
    sku             VARCHAR(20)    NOT NULL,
    name            VARCHAR(100)   NOT NULL,
    price           DECIMAL(10, 2) NOT NULL,
    catalog_version CHAR(36)       NOT NULL,
    PRIMARY KEY (sku),
    CONSTRAINT chk_product_replica_price CHECK (price >= 0)
) ENGINE = InnoDB;

-- A NULL catalog_version means the replica was never loaded, so Scan has no
-- usable product information and answers 500 (FR-023).
CREATE TABLE product_replica_meta (
    meta_key        TINYINT      NOT NULL,
    catalog_version CHAR(36)     NULL,
    product_count   INT          NOT NULL DEFAULT 0,
    refreshed_at    TIMESTAMP(3) NULL,
    PRIMARY KEY (meta_key),
    CONSTRAINT chk_product_replica_meta_singleton CHECK (meta_key = 1),
    CONSTRAINT chk_product_replica_meta_count CHECK (product_count >= 0)
) ENGINE = InnoDB;

INSERT INTO product_replica_meta (meta_key, catalog_version, product_count, refreshed_at)
VALUES (1, NULL, 0, NULL);

-- One row per checkout attempt, identified by (transaction_id, attempt_number).
-- `lines` is the immutable canonical snapshot of the basket the attempt froze: a JSON array of
-- {sku, quantity} sorted by SKU, with no whitespace. request_event_id names the CheckoutRequested
-- outbox row written in the same T1 commit; recovery re-sends that row (006 D7). next_retry_at is
-- the republish schedule, and last_error records REPUBLISHED, PUBLISH_FAILED, STUCK, or
-- INTEGRITY:<reason>.
CREATE TABLE checkout_attempt (
    transaction_id           VARCHAR(40)    NOT NULL,
    attempt_number           INT            NOT NULL,
    state                    ENUM('PENDING', 'COMPLETED', 'REJECTED') NOT NULL,
    rejection_reason         ENUM('INSUFFICIENT_STOCK', 'INVALID_REQUEST', 'STOCK_RESTORED') NULL,
    rejected_sku             VARCHAR(20)    NULL,
    `lines`                  TEXT           NOT NULL,
    item_count               INT            NOT NULL,
    total_amount             DECIMAL(12, 2) NOT NULL,
    client_outcome           ENUM('NONE', 'SUCCESS', 'REJECTED', 'PENDING_ERROR') NOT NULL DEFAULT 'NONE',
    created_at               TIMESTAMP(3)   NOT NULL,
    settled_at               TIMESTAMP(3)   NULL,
    retry_count              INT            NOT NULL DEFAULT 0,
    next_retry_at            TIMESTAMP(3)   NULL,
    last_error               VARCHAR(255)   NULL,
    request_event_id         CHAR(36)       NOT NULL,
    -- Defense in depth (FR-029): at most one PENDING and one COMPLETED attempt per transaction.
    pending_transaction_id   VARCHAR(40)
        GENERATED ALWAYS AS (IF(state = 'PENDING', transaction_id, NULL)) STORED,
    completed_transaction_id VARCHAR(40)
        GENERATED ALWAYS AS (IF(state = 'COMPLETED', transaction_id, NULL)) STORED,
    PRIMARY KEY (transaction_id, attempt_number),
    UNIQUE KEY uq_checkout_attempt_pending (pending_transaction_id),
    UNIQUE KEY uq_checkout_attempt_completed (completed_transaction_id),
    INDEX idx_checkout_attempt_due (state, next_retry_at),
    CONSTRAINT fk_checkout_attempt_transaction
        FOREIGN KEY (transaction_id) REFERENCES `transaction` (transaction_id),
    CONSTRAINT chk_checkout_attempt_number CHECK (attempt_number >= 1),
    CONSTRAINT chk_checkout_attempt_rejection CHECK ((state = 'REJECTED') = (rejection_reason IS NOT NULL)),
    CONSTRAINT chk_checkout_attempt_settled CHECK ((state = 'PENDING') = (settled_at IS NULL)),
    CONSTRAINT chk_checkout_attempt_item_count CHECK (item_count >= 0),
    CONSTRAINT chk_checkout_attempt_retry_count CHECK (retry_count >= 0)
) ENGINE = InnoDB;

-- The transactional outbox for this service's events (D5, FR-010). A row is written only inside
-- the local transaction whose state change it announces, and afterwards only published_at (set
-- after the broker confirms) and send_count (re-sends) change.
CREATE TABLE outbox_event (
    outbox_seq    BIGINT       NOT NULL AUTO_INCREMENT,
    event_id      CHAR(36)     NOT NULL,
    topic         VARCHAR(64)  NOT NULL,
    routing_key   VARCHAR(64)  NOT NULL,
    event_type    VARCHAR(48)  NOT NULL,
    -- <transaction_id>:<attempt_number> for checkout events, so recovery and duplicate handling
    -- find the recorded event; NULL for catalog events.
    aggregate_key VARCHAR(64)  NULL,
    envelope      MEDIUMBLOB   NOT NULL,
    created_at    TIMESTAMP(3) NOT NULL,
    published_at  TIMESTAMP(3) NULL,
    send_count    INT          NOT NULL DEFAULT 0,
    PRIMARY KEY (outbox_seq),
    UNIQUE KEY uq_outbox_event_id (event_id),
    INDEX idx_outbox_event_unpublished (published_at, outbox_seq),
    INDEX idx_outbox_event_aggregate (aggregate_key),
    CONSTRAINT chk_outbox_event_send_count CHECK (send_count >= 0)
) ENGINE = InnoDB;

-- Records this service's consumers could not process, parked after their retries (D6, FR-013).
-- A parked event_id that arrives again increments park_count instead of adding a row.
CREATE TABLE dead_letter_event (
    dead_letter_id  BIGINT       NOT NULL AUTO_INCREMENT,
    -- NULL only when the envelope could not be decoded.
    event_id        VARCHAR(80)  NULL,
    topic           VARCHAR(64)  NOT NULL,
    partition_no    INT          NOT NULL,
    record_offset   BIGINT       NOT NULL,
    event_type      VARCHAR(48)  NULL,
    schema_version  INT          NULL,
    reason          VARCHAR(64)  NOT NULL,
    detail          VARCHAR(500) NULL,
    record_bytes    MEDIUMBLOB   NOT NULL,
    first_parked_at TIMESTAMP(3) NOT NULL,
    last_parked_at  TIMESTAMP(3) NOT NULL,
    park_count      INT          NOT NULL DEFAULT 1,
    PRIMARY KEY (dead_letter_id),
    UNIQUE KEY uq_dead_letter_event_id (event_id),
    CONSTRAINT chk_dead_letter_reason CHECK (reason IN ('UNDECODABLE', 'UNKNOWN_TYPE', 'UNSUPPORTED_VERSION',
        'INVALID_PAYLOAD', 'PAYLOAD_MISMATCH', 'UNKNOWN_ATTEMPT', 'INTEGRITY', 'HANDLER_ERROR')),
    CONSTRAINT chk_dead_letter_park_count CHECK (park_count >= 1)
) ENGINE = InnoDB;
