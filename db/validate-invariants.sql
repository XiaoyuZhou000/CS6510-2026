-- Run after Analytics catches up. Any violation raises SQLSTATE 45000.
-- mysql -h 127.0.0.1 -P 3307 -u root -p cs6510_selfcheckout < db/validate-invariants.sql
USE cs6510_selfcheckout;

DELIMITER $$
DROP PROCEDURE IF EXISTS assert_zero$$
CREATE PROCEDURE assert_zero(IN assertion_name VARCHAR(128), IN violation_count BIGINT)
BEGIN
    DECLARE failure_message VARCHAR(255);
    IF violation_count <> 0 THEN
        SET failure_message = CONCAT(assertion_name, ': ', violation_count, ' violation(s)');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = failure_message;
    END IF;
END$$
DELIMITER ;

CALL assert_zero('baseline has exactly 2000 catalog and inventory SKUs',
    (SELECT ABS(COUNT(*) - 2000) FROM catalog_item) +
    (SELECT ABS(COUNT(*) - 2000) FROM inventory));
CALL assert_zero('inventory stock is nonnegative',
    (SELECT COUNT(*) FROM inventory WHERE stock_quantity < 0));
CALL assert_zero('every SKU reconciles to completed sales',
    (SELECT COUNT(*) FROM inventory i LEFT JOIN (
        SELECT tl.sku, SUM(tl.quantity) sold FROM transaction_line tl
        JOIN `transaction` t ON t.transaction_id = tl.transaction_id
        WHERE t.status = 'COMPLETED' GROUP BY tl.sku
    ) s ON s.sku = i.sku WHERE 10000 - i.stock_quantity <> COALESCE(s.sold, 0)));

-- OPEN baskets are durable and have lines, so check all transaction statuses.
CALL assert_zero('transaction totals match committed lines',
    (SELECT COUNT(*) FROM (
        SELECT t.transaction_id FROM `transaction` t
        LEFT JOIN transaction_line tl ON tl.transaction_id = t.transaction_id
        GROUP BY t.transaction_id, t.total_amount
        HAVING t.total_amount <> COALESCE(SUM(tl.quantity * tl.unit_price), 0)
    ) bad_totals));
CALL assert_zero('completed transactions have lines and timestamp',
    (SELECT COUNT(*) FROM `transaction` t WHERE t.status = 'COMPLETED'
        AND (t.completed_at IS NULL OR NOT EXISTS (
            SELECT 1 FROM transaction_line tl WHERE tl.transaction_id = t.transaction_id))));
CALL assert_zero('open transactions have no completion timestamp',
    (SELECT COUNT(*) FROM `transaction` WHERE status = 'OPEN' AND completed_at IS NOT NULL));
CALL assert_zero('line quantities and prices match accepted scans',
    (SELECT COUNT(*) FROM transaction_line tl LEFT JOIN (
        SELECT transaction_id, sku, COUNT(*) units, MIN(unit_price) min_price,
               MAX(unit_price) max_price FROM accepted_scan GROUP BY transaction_id, sku
    ) s ON s.transaction_id = tl.transaction_id AND s.sku = tl.sku
    WHERE tl.quantity <> COALESCE(s.units, 0) OR tl.unit_price <> s.min_price
       OR s.min_price <> s.max_price));
CALL assert_zero('accepted scans have matching lines',
    (SELECT COUNT(*) FROM accepted_scan s LEFT JOIN transaction_line tl
        ON tl.transaction_id = s.transaction_id AND tl.sku = s.sku WHERE tl.sku IS NULL));

CALL assert_zero('scan sequence singleton exists',
    (SELECT ABS(COUNT(*) - 1) FROM scan_sequence));
CALL assert_zero('all accepted scans materialized',
    (SELECT COUNT(*) FROM accepted_scan WHERE position IS NULL));
CALL assert_zero('materialized positions equal checkpoint and have no gaps',
    (SELECT CASE WHEN ss.last_position = stats.units
                  AND (stats.units = 0 OR (stats.first_position = 1
                       AND stats.last_position = stats.units)) THEN 0 ELSE 1 END
     FROM scan_sequence ss CROSS JOIN (
        SELECT COUNT(position) units, MIN(position) first_position,
               MAX(position) last_position FROM accepted_scan
     ) stats WHERE ss.sequence_key = 1));

CALL assert_zero('window boundaries are valid',
    (SELECT COUNT(*) FROM popular_window WHERE window_end < 1000
        OR MOD(window_end - 1000, 500) <> 0
        OR window_start <> window_end - 999));
CALL assert_zero('checkpoint singleton exists',
    (SELECT ABS(COUNT(*) - 1) FROM analytics_checkpoint));
CALL assert_zero('checkpoint equals highest eligible scan boundary',
    (SELECT CASE WHEN ac.last_window_end =
        CASE WHEN ss.last_position < 1000 THEN 0
             ELSE 1000 + 500 * FLOOR((ss.last_position - 1000) / 500) END
        THEN 0 ELSE 1 END
     FROM analytics_checkpoint ac CROSS JOIN scan_sequence ss
     WHERE ac.checkpoint_key = 1 AND ss.sequence_key = 1));
CALL assert_zero('all eligible windows were published exactly once',
    (SELECT CASE WHEN COUNT(*) =
        (SELECT CASE WHEN last_position < 1000 THEN 0
                     ELSE 1 + FLOOR((last_position - 1000) / 500) END
         FROM scan_sequence WHERE sequence_key = 1)
        AND COALESCE(MAX(window_end), 0) =
        (SELECT last_window_end FROM analytics_checkpoint WHERE checkpoint_key = 1)
        THEN 0 ELSE 1 END FROM popular_window));

-- Recompute every top ten from the immutable event table, including ties.
CREATE TEMPORARY TABLE expected_popular_rank AS
WITH counts AS (
    SELECT pw.window_id, s.sku, COUNT(*) scan_count
    FROM popular_window pw JOIN accepted_scan s
      ON s.position BETWEEN pw.window_start AND pw.window_end
    GROUP BY pw.window_id, s.sku
), ranked AS (
    SELECT window_id, sku, scan_count,
           ROW_NUMBER() OVER (PARTITION BY window_id
                              ORDER BY scan_count DESC, sku ASC) rank_pos
    FROM counts
)
SELECT window_id, sku, scan_count, rank_pos FROM ranked WHERE rank_pos <= 10;
CALL assert_zero('persisted ranks differ from accepted-scan counts',
    (SELECT COUNT(*) FROM expected_popular_rank e LEFT JOIN popular_item p
      ON p.window_id = e.window_id AND p.rank_pos = e.rank_pos
      WHERE p.sku IS NULL OR p.sku <> e.sku OR p.scan_count <> e.scan_count));
CALL assert_zero('persisted ranks contain no unexpected rows',
    (SELECT COUNT(*) FROM popular_item p LEFT JOIN expected_popular_rank e
      ON e.window_id = p.window_id AND e.rank_pos = p.rank_pos
      WHERE e.sku IS NULL));
DROP TEMPORARY TABLE expected_popular_rank;

SELECT 'PASS' invariant_status,
       (SELECT COUNT(*) FROM `transaction` WHERE status = 'COMPLETED') completed_transactions,
       (SELECT COALESCE(SUM(quantity), 0) FROM transaction_line) accepted_units,
       (SELECT last_position FROM scan_sequence WHERE sequence_key = 1) materialized_scan_position,
       (SELECT MIN(stock_quantity) FROM inventory) minimum_stock,
       (SELECT COUNT(*) FROM popular_window) popular_windows,
       (SELECT last_window_end FROM analytics_checkpoint WHERE checkpoint_key = 1) latest_window_end;
DROP PROCEDURE assert_zero;
