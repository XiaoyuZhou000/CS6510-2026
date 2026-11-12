-- =============================================================================
-- Notification Service schema (cs6510_notification), per
-- specs/007-notification-service/data-model.md section 2.
--
-- Drops and recreates only the Notification Service's own tables, empty except
-- for the subscription clock at version 0. Run it against the Notification
-- schema with the admin credentials, for example through
--     db/reset-service.ps1 -Service notification
-- It names no schema, so the caller's default database decides where it runs,
-- and it references no other service's data (Principle II).
-- =============================================================================

DROP TABLE IF EXISTS dead_letter_event;
DROP TABLE IF EXISTS notification;
DROP TABLE IF EXISTS sale_selection;
DROP TABLE IF EXISTS subscription_change;
DROP TABLE IF EXISTS subscription;
DROP TABLE IF EXISTS subscriber;
DROP TABLE IF EXISTS subscription_clock;

-- The single history clock (N4, N5). A subscription change holds it FOR UPDATE
-- and advances it by one; a sale selection holds it FOR SHARE, so every
-- selection sees one committed history point.
CREATE TABLE subscription_clock (
    id      TINYINT NOT NULL,
    version BIGINT  NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT chk_subscription_clock_singleton CHECK (id = 1)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

INSERT INTO subscription_clock (id, version) VALUES (1, 0);

-- One row per subscriber ever registered. channel_topic is
-- <prefix>selfcheckout.notify.<subscriber_id>.v1, and channel_ready is set once
-- that topic has been created (N7).
CREATE TABLE subscriber (
    subscriber_id   VARCHAR(40)  NOT NULL,
    status          ENUM('ACTIVE', 'CANCELLED') NOT NULL,
    channel_topic   VARCHAR(200) NOT NULL,
    channel_ready   BOOLEAN      NOT NULL DEFAULT FALSE,
    created_version BIGINT       NOT NULL,
    updated_version BIGINT       NOT NULL,
    created_at      TIMESTAMP(3) NOT NULL,
    updated_at      TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (subscriber_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- The current SKUs of active subscribers only.
CREATE TABLE subscription (
    subscriber_id VARCHAR(40) NOT NULL,
    sku           VARCHAR(20) NOT NULL,
    added_version BIGINT      NOT NULL,
    PRIMARY KEY (subscriber_id, sku),
    INDEX idx_subscription_sku (sku, subscriber_id),
    CONSTRAINT fk_subscription_subscriber
        FOREIGN KEY (subscriber_id) REFERENCES subscriber (subscriber_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- The ordered, immutable subscription history (FR-007). version is contiguous
-- from 1 and equals the clock after the change commits. The SKU columns are
-- canonical sorted JSON arrays; resulting_skus is the subscriber's complete set
-- after the change (empty after CANCEL), so replay to version V needs only the
-- latest row per subscriber with version <= V.
CREATE TABLE subscription_change (
    version        BIGINT       NOT NULL,
    subscriber_id  VARCHAR(40)  NOT NULL,
    change_type    ENUM('REGISTER', 'UPDATE', 'CANCEL') NOT NULL,
    added_skus     TEXT         NOT NULL,
    removed_skus   TEXT         NOT NULL,
    resulting_skus TEXT         NOT NULL,
    committed_at   TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (version),
    INDEX idx_subscription_change_subscriber (subscriber_id, version)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- One row per selected sale, including sales with no recipients (FR-011). The
-- primary key makes a redelivered SaleCompleted a no-op (FR-012). `lines` is
-- the canonical sorted [{"sku","quantity"}] of the event, and
-- subscription_version the history point the selection used.
CREATE TABLE sale_selection (
    sale_id              VARCHAR(40)  NOT NULL,
    sale_event_id        CHAR(36)     NOT NULL,
    completed_at         TIMESTAMP(3) NOT NULL,
    `lines`              TEXT         NOT NULL,
    subscription_version BIGINT       NOT NULL,
    recipient_count      INT          NOT NULL,
    notification_count   INT          NOT NULL,
    selected_at          TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (sale_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- The selection result per SKU and subscriber, its delivery record, and the
-- outbox for the subscriber's channel (N8). notification_id is UUID v3 of
-- notification:<sale_id>:<sku>:<subscriber_id> (FR-014). delivered_at is set
-- only by a confirmed publish.
CREATE TABLE notification (
    notification_id    CHAR(36)     NOT NULL,
    sale_id            VARCHAR(40)  NOT NULL,
    sku                VARCHAR(20)  NOT NULL,
    subscriber_id      VARCHAR(40)  NOT NULL,
    quantity           INT          NOT NULL,
    completed_at       TIMESTAMP(3) NOT NULL,
    state              ENUM('PENDING', 'DELIVERED', 'PARKED') NOT NULL,
    attempt_count      INT          NOT NULL DEFAULT 0,
    charged_failures   INT          NOT NULL DEFAULT 0,
    uncharged_failures INT          NOT NULL DEFAULT 0,
    operator_retries   INT          NOT NULL DEFAULT 0,
    next_attempt_at    TIMESTAMP(3) NOT NULL,
    last_error         VARCHAR(255) NULL,
    created_at         TIMESTAMP(3) NOT NULL,
    delivered_at       TIMESTAMP(3) NULL,
    parked_at          TIMESTAMP(3) NULL,
    PRIMARY KEY (notification_id),
    UNIQUE KEY uq_notification_key (sale_id, sku, subscriber_id),
    INDEX idx_notification_due (state, next_attempt_at),
    INDEX idx_notification_subscriber (subscriber_id, state),
    CONSTRAINT fk_notification_sale
        FOREIGN KEY (sale_id) REFERENCES sale_selection (sale_id),
    CONSTRAINT chk_notification_quantity CHECK (quantity > 0),
    CONSTRAINT chk_notification_delivered CHECK ((state = 'DELIVERED') = (delivered_at IS NOT NULL)),
    CONSTRAINT chk_notification_parked CHECK ((state = 'PARKED') = (parked_at IS NOT NULL))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- Records this service's consumers could not process (invalid SaleCompleted or
-- catalog records), parked after their retries (FR-018). Identical to
-- db/transaction/schema.sql's, so the shared JdbcDeadLetterStore works unchanged.
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
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
