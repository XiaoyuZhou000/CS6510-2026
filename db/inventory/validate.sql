-- =============================================================================
-- Inventory Service invariants (R13). Run after pending work drains, with the
-- Inventory user against its own schema. Any violation raises SQLSTATE 45000.
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

CALL assert_zero('baseline has exactly 2000 catalog and inventory SKUs',
    (SELECT ABS(COUNT(*) - 2000) FROM catalog_item) +
    (SELECT ABS(COUNT(*) - 2000) FROM inventory));
CALL assert_zero('inventory stock is nonnegative',
    (SELECT COUNT(*) FROM inventory WHERE stock_quantity < 0));
CALL assert_zero('catalog_meta singleton exists',
    (SELECT ABS(COUNT(*) - 1) FROM catalog_meta));
CALL assert_zero('applied and restored operations record their lines',
    (SELECT COUNT(*) FROM checkout_operation
     WHERE state IN ('APPLIED', 'RESTORED') AND `lines` IS NULL));
CALL assert_zero('rejected operations record a reason',
    (SELECT COUNT(*) FROM checkout_operation WHERE state = 'REJECTED' AND rejection_reason IS NULL));

-- Every unit sold is accounted for by exactly the APPLIED operations' lines.
CREATE TEMPORARY TABLE validate_applied_units AS
SELECT j.sku, SUM(j.quantity) units
FROM checkout_operation o,
     JSON_TABLE(o.`lines`, '$[*]' COLUMNS (
         sku VARCHAR(20) PATH '$.sku' ERROR ON EMPTY,
         quantity INT PATH '$.quantity' ERROR ON EMPTY)) j
WHERE o.state = 'APPLIED'
GROUP BY j.sku;
SELECT COUNT(*) INTO @unreconciled_skus
FROM inventory i LEFT JOIN validate_applied_units a ON a.sku = i.sku
WHERE 10000 - i.stock_quantity <> COALESCE(a.units, 0);
CALL assert_zero('per SKU, units sold equal the APPLIED operations'' quantities', @unreconciled_skus);
SELECT COUNT(*) INTO @unknown_applied_skus
FROM validate_applied_units a LEFT JOIN inventory i ON i.sku = a.sku
WHERE i.sku IS NULL;
CALL assert_zero('APPLIED operations name only stocked SKUs', @unknown_applied_skus);
DROP TEMPORARY TABLE validate_applied_units;

SELECT 'PASS' invariant_status,
       (SELECT COUNT(*) FROM catalog_item) catalog_items,
       (SELECT COUNT(*) FROM inventory) inventory_rows,
       (SELECT SUM(stock_quantity) FROM inventory) total_units,
       (SELECT MIN(stock_quantity) FROM inventory) minimum_stock,
       (SELECT COUNT(*) FROM checkout_operation WHERE state = 'APPLIED') applied_operations,
       (SELECT COUNT(*) FROM checkout_operation WHERE state = 'REJECTED') rejected_operations,
       (SELECT COUNT(*) FROM checkout_operation WHERE state = 'RESTORED') restored_operations,
       (SELECT COUNT(*) FROM checkout_operation WHERE state = 'BLOCKED') blocked_operations;
DROP PROCEDURE assert_zero;
