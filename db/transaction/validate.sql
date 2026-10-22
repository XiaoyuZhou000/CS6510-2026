-- =============================================================================
-- Transaction Service invariants (R13). Run after pending work drains, with the
-- Transaction user against its own schema; db/validate-invariants.ps1 does this
-- and then reconciles the services' aggregates. Any violation raises SQLSTATE
-- 45000 and the client exits non-zero.
-- =============================================================================

DELIMITER $$
DROP PROCEDURE IF EXISTS assert_zero$$
CREATE PROCEDURE assert_zero(IN assertion_name VARCHAR(128), IN violation_count BIGINT)
BEGIN
    DECLARE failure_message VARCHAR(255);
    IF violation_count IS NULL OR violation_count <> 0 THEN
        SET failure_message = CONCAT(assertion_name, ': ', COALESCE(violation_count, 'NULL'), ' violation(s)');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = failure_message;
    END IF;
END$$
DELIMITER ;

CALL assert_zero('transaction item counts and totals match accepted scans',
    (SELECT COUNT(*) FROM `transaction` t LEFT JOIN (
        SELECT transaction_id, COUNT(*) units, SUM(unit_price) total
        FROM accepted_scan GROUP BY transaction_id
    ) s ON s.transaction_id = t.transaction_id
    WHERE t.item_count <> COALESCE(s.units, 0) OR t.total_amount <> COALESCE(s.total, 0)));
CALL assert_zero('scan sequences are contiguous from 1 per transaction',
    (SELECT COUNT(*) FROM (
        SELECT transaction_id FROM accepted_scan GROUP BY transaction_id
        HAVING MIN(scan_sequence) <> 1 OR MAX(scan_sequence) <> COUNT(*)
    ) gaps));
CALL assert_zero('one captured price and name per transaction and SKU',
    (SELECT COUNT(*) FROM (
        SELECT transaction_id, sku FROM accepted_scan GROUP BY transaction_id, sku
        HAVING COUNT(DISTINCT unit_price) > 1 OR COUNT(DISTINCT product_name) > 1
    ) repriced));
CALL assert_zero('completed transactions have a completion time and at least one scan',
    (SELECT COUNT(*) FROM `transaction` t WHERE t.status = 'COMPLETED'
        AND (t.completed_at IS NULL OR NOT EXISTS (
            SELECT 1 FROM accepted_scan s WHERE s.transaction_id = t.transaction_id))));
CALL assert_zero('open transactions have no completion time',
    (SELECT COUNT(*) FROM `transaction` WHERE status = 'OPEN' AND completed_at IS NOT NULL));
CALL assert_zero('no checkout attempt is pending',
    (SELECT COUNT(*) FROM checkout_attempt WHERE state = 'PENDING'));
CALL assert_zero('no transaction has an active attempt',
    (SELECT COUNT(*) FROM `transaction` WHERE active_attempt_number IS NOT NULL));
CALL assert_zero('each completed transaction has exactly one completed attempt',
    (SELECT COUNT(*) FROM `transaction` t LEFT JOIN (
        SELECT transaction_id, COUNT(*) completed FROM checkout_attempt
        WHERE state = 'COMPLETED' GROUP BY transaction_id
    ) a ON a.transaction_id = t.transaction_id
    WHERE (t.status = 'COMPLETED') <> (COALESCE(a.completed, 0) = 1)
       OR COALESCE(a.completed, 0) > 1));

-- The completed attempt's line snapshot equals the transaction's grouped scans.
CREATE TEMPORARY TABLE validate_attempt_line AS
SELECT a.transaction_id, j.sku, j.quantity
FROM checkout_attempt a,
     JSON_TABLE(a.`lines`, '$[*]' COLUMNS (
         sku VARCHAR(20) PATH '$.sku' ERROR ON EMPTY,
         quantity INT PATH '$.quantity' ERROR ON EMPTY)) j
WHERE a.state = 'COMPLETED';
CREATE TEMPORARY TABLE validate_scan_line AS
SELECT s.transaction_id, s.sku, COUNT(*) quantity
FROM accepted_scan s JOIN `transaction` t ON t.transaction_id = s.transaction_id
WHERE t.status = 'COMPLETED'
GROUP BY s.transaction_id, s.sku;
SELECT COUNT(*) INTO @attempt_lines_unmatched
FROM validate_attempt_line a LEFT JOIN validate_scan_line s
  ON s.transaction_id = a.transaction_id AND s.sku = a.sku
WHERE s.sku IS NULL OR s.quantity <> a.quantity;
CALL assert_zero('completed attempt lines appear in the grouped scans', @attempt_lines_unmatched);
SELECT COUNT(*) INTO @scan_lines_unmatched
FROM validate_scan_line s LEFT JOIN validate_attempt_line a
  ON a.transaction_id = s.transaction_id AND a.sku = s.sku
WHERE a.sku IS NULL;
CALL assert_zero('grouped scans appear in the completed attempt lines', @scan_lines_unmatched);
DROP TEMPORARY TABLE validate_attempt_line;
DROP TEMPORARY TABLE validate_scan_line;

SELECT 'PASS' invariant_status,
       (SELECT COUNT(*) FROM `transaction`) transactions,
       (SELECT COUNT(*) FROM `transaction` WHERE status = 'COMPLETED') completed_transactions,
       (SELECT COUNT(*) FROM accepted_scan) accepted_scans,
       (SELECT COUNT(*) FROM accepted_scan WHERE delivered_at IS NULL) undelivered_scans,
       (SELECT COUNT(*) FROM checkout_attempt) checkout_attempts,
       (SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'SUCCESS') completed_success,
       (SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'PENDING_ERROR') background_completions,
       (SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'NONE') completions_without_response,
       (SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'REJECTED') completed_reported_rejected;
DROP PROCEDURE assert_zero;
