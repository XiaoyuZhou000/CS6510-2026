-- =============================================================================
-- SaleCompleted outbox rows for the 007 reconciliation (SC-001), run with
-- Transaction's own credentials by db/export-notification-evidence.ps1. The
-- envelope is exported as hex and decoded by subscriber-client DecodeSaleEvents
-- into sale-events.tsv. Times are epoch milliseconds; NULL while unpublished.
-- =============================================================================
SELECT event_id,
       aggregate_key,
       CAST(ROUND(UNIX_TIMESTAMP(published_at) * 1000) AS SIGNED) AS published_at_ms,
       HEX(envelope) AS envelope_hex
FROM outbox_event
WHERE event_type = 'SaleCompleted'
ORDER BY outbox_seq;
