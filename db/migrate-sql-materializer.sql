-- One-time in-place migration from commit-time Scan positions.
-- Stop Transaction and Analytics Service before applying. Back up the database first.
-- Existing positioned events and scan_sequence.last_position remain unchanged.
USE cs6510_selfcheckout;

ALTER TABLE accepted_scan
    DROP PRIMARY KEY,
    ADD COLUMN event_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST,
    MODIFY COLUMN position BIGINT NULL,
    ADD UNIQUE KEY uq_scan_position (position),
    ADD KEY idx_scan_pending (position, event_id);

-- These checks must both return the same count before services restart.
SELECT COUNT(*) AS accepted_events, COUNT(position) AS positioned_events
FROM accepted_scan;
SELECT last_position AS materialized_checkpoint
FROM scan_sequence WHERE sequence_key = 1;
