-- Validate the correctness invariants after a load-client run.
-- Usage:
--   mysql -h 127.0.0.1 -P 3307 -u root -p cs6510_selfcheckout < db/validate-invariants.sql
--
-- The script stops with SQLSTATE 45000 on the first violation and prints a
-- compact evidence summary only when every assertion passes.

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

CALL assert_zero(
    'inventory stock is non-negative',
    (SELECT COUNT(*) FROM inventory WHERE stock_quantity < 0)
);

CALL assert_zero(
    'stock reduction equals completed quantity for every SKU',
    (
        SELECT COUNT(*)
        FROM (
            SELECT c.sku
            FROM catalog_item c
            JOIN inventory i ON i.sku = c.sku
            LEFT JOIN (
                SELECT tl.sku, SUM(tl.quantity) AS sold_quantity
                FROM transaction_line tl
                JOIN `transaction` t ON t.transaction_id = tl.transaction_id
                WHERE t.status = 'COMPLETED'
                GROUP BY tl.sku
            ) sold ON sold.sku = c.sku
            WHERE 10000 - i.stock_quantity <> COALESCE(sold.sold_quantity, 0)
        ) reconciliation_violations
    )
);

CALL assert_zero(
    'non-completed transactions have no durable lines',
    (
        SELECT COUNT(*)
        FROM `transaction` t
        JOIN transaction_line tl ON tl.transaction_id = t.transaction_id
        WHERE t.status <> 'COMPLETED'
    )
);

CALL assert_zero(
    'completed transactions have lines and coherent totals/timestamps',
    (
        SELECT COUNT(*)
        FROM (
            SELECT t.transaction_id
            FROM `transaction` t
            LEFT JOIN transaction_line tl ON tl.transaction_id = t.transaction_id
            WHERE t.status = 'COMPLETED'
            GROUP BY t.transaction_id, t.total_amount, t.completed_at
            HAVING COUNT(tl.sku) = 0
                OR t.completed_at IS NULL
                OR t.total_amount <> COALESCE(SUM(tl.quantity * tl.unit_price), 0)
        ) partial_completion_violations
    )
);

CALL assert_zero(
    'non-completed transactions have no completion timestamp or total',
    (
        SELECT COUNT(*)
        FROM `transaction`
        WHERE status <> 'COMPLETED'
          AND (completed_at IS NOT NULL OR total_amount <> 0.00)
    )
);

CALL assert_zero(
    'popular-window bounds are coherent 1000-scan checkpoints',
    (
        SELECT COUNT(*)
        FROM popular_window
        WHERE window_start < 1
           OR window_end < window_start
           OR window_end - window_start + 1 <> 1000
           OR MOD(window_end, 500) <> 0
    )
);

CALL assert_zero(
    'popular ranks are bounded, positive, and contiguous',
    (
        SELECT COUNT(*)
        FROM (
            SELECT pw.window_id
            FROM popular_window pw
            LEFT JOIN popular_item pi ON pi.window_id = pw.window_id
            GROUP BY pw.window_id
            HAVING COUNT(pi.rank_pos) > 10
                OR (COUNT(pi.rank_pos) > 0 AND MIN(pi.rank_pos) <> 1)
                OR (COUNT(pi.rank_pos) > 0 AND MAX(pi.rank_pos) <> COUNT(pi.rank_pos))
                OR COALESCE(SUM(pi.scan_count), 0) > 1000
                OR COALESCE(MIN(pi.scan_count), 1) <= 0
        ) rank_violations
    )
);

CALL assert_zero(
    'popular ranks use count-descending and SKU-ascending tie order',
    (
        SELECT COUNT(*)
        FROM popular_item current_rank
        JOIN popular_item previous_rank
          ON previous_rank.window_id = current_rank.window_id
         AND previous_rank.rank_pos = current_rank.rank_pos - 1
        WHERE previous_rank.scan_count < current_rank.scan_count
           OR (previous_rank.scan_count = current_rank.scan_count
               AND previous_rank.sku > current_rank.sku)
    )
);

SELECT
    'PASS' AS invariant_status,
    (SELECT COUNT(*) FROM `transaction` WHERE status = 'COMPLETED') AS completed_transactions,
    (SELECT COALESCE(SUM(quantity), 0) FROM transaction_line) AS completed_units,
    (SELECT MIN(stock_quantity) FROM inventory) AS minimum_stock,
    (SELECT COUNT(*) FROM popular_window) AS popular_windows,
    (SELECT COALESCE(MAX(window_end), 0) FROM popular_window) AS latest_window_end;

DROP PROCEDURE assert_zero;
