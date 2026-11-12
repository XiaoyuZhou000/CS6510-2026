-- =============================================================================
-- Notification Service evidence for the 007 reconciliation (research N11,
-- FR-024), run with Notification's own credentials by
-- db/export-notification-evidence.ps1. Each "-- @file <name>" line starts one
-- statement whose result becomes exports/<name>. Times are epoch milliseconds.
-- Free-text columns (last_error, detail) and record bytes are left out, so no
-- value contains a tab or a newline.
-- =============================================================================

-- @file selections.tsv
SELECT sale_id,
       sale_event_id,
       TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', completed_at) DIV 1000 AS completed_at_ms,
       `lines`,
       subscription_version,
       recipient_count,
       notification_count,
       TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', selected_at) DIV 1000 AS selected_at_ms
FROM sale_selection
ORDER BY sale_id;

-- @file notifications.tsv
SELECT notification_id,
       sale_id,
       sku,
       subscriber_id,
       quantity,
       TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', completed_at) DIV 1000 AS completed_at_ms,
       state,
       attempt_count,
       charged_failures,
       uncharged_failures,
       operator_retries,
       TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', created_at) DIV 1000 AS created_at_ms,
       TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', delivered_at) DIV 1000 AS delivered_at_ms,
       TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', parked_at) DIV 1000 AS parked_at_ms
FROM notification
ORDER BY sale_id, sku, subscriber_id;

-- @file history.tsv
SELECT version,
       subscriber_id,
       change_type,
       added_skus,
       removed_skus,
       resulting_skus,
       TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', committed_at) DIV 1000 AS committed_at_ms
FROM subscription_change
ORDER BY version;

-- @file parked-events.tsv
SELECT dead_letter_id,
       event_id,
       topic,
       partition_no,
       record_offset,
       event_type,
       reason,
       park_count,
       TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', first_parked_at) DIV 1000 AS first_parked_at_ms
FROM dead_letter_event
ORDER BY dead_letter_id;
