-- =============================================================================
-- Analytics Service invariants (R13). Run after Analytics catches up, with the
-- Analytics user against its own schema. Any violation raises SQLSTATE 45000.
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

CALL assert_zero('scan sequence singleton exists',
    (SELECT ABS(COUNT(*) - 1) FROM scan_sequence));
CALL assert_zero('positions are contiguous from 1 to last_position',
    (SELECT CASE WHEN stats.units = ss.last_position
                  AND (stats.units = 0 OR (stats.first_position = 1
                       AND stats.max_position = stats.units)) THEN 0 ELSE 1 END
     FROM scan_sequence ss CROSS JOIN (
        SELECT COUNT(*) units, MIN(position) first_position, MAX(position) max_position
        FROM accepted_scan
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

-- Recompute every top ten from the immutable event table, including ties
-- (the window-rank SQL of the retired db/validate-invariants.sql).
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

CALL assert_zero('every popular item SKU has a name',
    (SELECT COUNT(*) FROM popular_item p LEFT JOIN product_name n ON n.sku = p.sku
     WHERE n.sku IS NULL));

SELECT 'PASS' invariant_status,
       (SELECT COUNT(*) FROM accepted_scan) stored_events,
       (SELECT last_position FROM scan_sequence WHERE sequence_key = 1) last_position,
       (SELECT COUNT(*) FROM product_name) product_names,
       (SELECT COUNT(*) FROM popular_window) popular_windows,
       (SELECT last_window_end FROM analytics_checkpoint WHERE checkpoint_key = 1) latest_window_end;
DROP PROCEDURE assert_zero;
