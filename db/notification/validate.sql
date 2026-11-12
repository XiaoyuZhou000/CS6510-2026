-- =============================================================================
-- Notification Service invariants (specs/007-notification-service/data-model.md).
-- Run after pending work drains, against cs6510_notification. The Notification
-- user may only read and write rows, so run it with the admin credentials
-- (DB_USER, DB_PASSWORD), which may create the helper procedure. Any violation
-- raises SQLSTATE 45000 and the client exits non-zero.
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

CALL assert_zero('the subscription clock has exactly one row',
    (SELECT ABS(COUNT(*) - 1) FROM subscription_clock WHERE id = 1));
-- Contiguous from 1 to the clock: as many rows as the clock says, numbered 1..clock.
CALL assert_zero('subscription_change.version is contiguous from 1 to the clock',
    (SELECT CASE
        WHEN COUNT(*) = 0 THEN (SELECT version FROM subscription_clock WHERE id = 1)
        WHEN MIN(version) <> 1 OR MAX(version) <> COUNT(*)
             OR MAX(version) <> (SELECT version FROM subscription_clock WHERE id = 1) THEN 1
        ELSE 0 END
     FROM subscription_change));
CALL assert_zero('every notification has its sale_selection',
    (SELECT COUNT(*) FROM notification n
     LEFT JOIN sale_selection s ON s.sale_id = n.sale_id
     WHERE s.sale_id IS NULL));
CALL assert_zero('sale_selection.notification_count equals its notification rows',
    (SELECT COUNT(*) FROM sale_selection s
     LEFT JOIN (SELECT sale_id, COUNT(*) rows_selected, COUNT(DISTINCT subscriber_id) recipients
                FROM notification GROUP BY sale_id) n ON n.sale_id = s.sale_id
     WHERE s.notification_count <> COALESCE(n.rows_selected, 0)
        OR s.recipient_count <> COALESCE(n.recipients, 0)));
CALL assert_zero('no sale_selection was taken at a version beyond the clock',
    (SELECT COUNT(*) FROM sale_selection
     WHERE subscription_version > (SELECT version FROM subscription_clock WHERE id = 1)));
CALL assert_zero('no DELIVERED notification lacks delivered_at',
    (SELECT COUNT(*) FROM notification WHERE state = 'DELIVERED' AND delivered_at IS NULL));
CALL assert_zero('no PARKED notification lacks parked_at',
    (SELECT COUNT(*) FROM notification WHERE state = 'PARKED' AND parked_at IS NULL));
CALL assert_zero('only active subscribers have current subscriptions',
    (SELECT COUNT(*) FROM subscription c JOIN subscriber s ON s.subscriber_id = c.subscriber_id
     WHERE s.status <> 'ACTIVE'));

SELECT 'PASS' invariant_status,
       (SELECT version FROM subscription_clock WHERE id = 1) subscription_version,
       (SELECT COUNT(*) FROM subscriber WHERE status = 'ACTIVE') active_subscribers,
       (SELECT COUNT(*) FROM subscriber WHERE status = 'CANCELLED') cancelled_subscribers,
       (SELECT COUNT(*) FROM subscription) subscriptions,
       (SELECT COUNT(*) FROM subscription_change) subscription_changes,
       (SELECT COUNT(*) FROM sale_selection) sale_selections,
       (SELECT COUNT(*) FROM notification) notifications,
       (SELECT COUNT(*) FROM notification WHERE state = 'PENDING') pending_notifications,
       (SELECT COUNT(*) FROM notification WHERE state = 'DELIVERED') delivered_notifications,
       (SELECT COUNT(*) FROM notification WHERE state = 'PARKED') parked_notifications,
       (SELECT COUNT(*) FROM dead_letter_event) parked_events;
DROP PROCEDURE assert_zero;
