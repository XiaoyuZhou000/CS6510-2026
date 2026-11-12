-- =============================================================================
-- Completed sales for the 007 reconciliation (research N11, FR-024), run with
-- Transaction's own credentials by db/export-notification-evidence.ps1.
-- One row per COMPLETED transaction and SKU, with the units counted from
-- accepted_scan, so the owed set never depends on the SaleCompleted event under
-- test. Times are epoch milliseconds.
-- =============================================================================
SELECT t.transaction_id,
       TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', t.completed_at) DIV 1000 AS completed_at_ms,
       s.sku,
       COUNT(*) AS quantity
FROM `transaction` t
JOIN accepted_scan s ON s.transaction_id = t.transaction_id
WHERE t.status = 'COMPLETED'
GROUP BY t.transaction_id, t.completed_at, s.sku
ORDER BY t.transaction_id, s.sku;
