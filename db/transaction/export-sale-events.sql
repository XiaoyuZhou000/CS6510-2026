-- =============================================================================
-- SaleCompleted outbox rows for the 007 reconciliation (SC-001), run with
-- Transaction's own credentials by db/export-notification-evidence.ps1. The
-- envelope is exported as hex and decoded by subscriber-client DecodeSaleEvents
-- into sale-events.tsv. Times are epoch milliseconds; NULL while unpublished.
-- =============================================================================
SELECT event_id,
       aggregate_key,
       TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', published_at) DIV 1000 AS published_at_ms,
       HEX(envelope) AS envelope_hex
FROM outbox_event
WHERE event_type = 'SaleCompleted'
ORDER BY outbox_seq;
