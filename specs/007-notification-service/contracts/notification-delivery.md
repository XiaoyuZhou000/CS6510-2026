# Contract: Recipient Selection and Notification Delivery

Implements the spec's *Recipient Selection Rule* and FR-010–FR-018. See research N4, N5, N7, N8, and N13.

## Subscription change transaction

The flow is the same for register, update, and cancel (`SubscriptionService`, `JdbcSubscriptionStore`):

1. Validate the request shape and SKUs against `KnownSkus`. A catalog that is not loaded gives 500.
2. Begin. `SELECT version FROM subscription_clock WHERE id = 1 FOR UPDATE` gives `V`.
3. `SELECT … FROM subscriber WHERE subscriber_id = ? FOR UPDATE`, then read its `subscription` rows.
4. Decide: reject (rollback, 4xx), no-op (rollback, 200 with the current view), or apply.
5. Apply:
   - upsert `subscriber` (status, `updated_version = V + 1`, `channel_topic`);
   - insert or delete `subscription` rows;
   - insert `subscription_change` at `V + 1`;
   - `UPDATE subscription_clock SET version = V + 1`.
6. `FaultHooks.hit("notification.subscription.beforeCommit")`, then commit.
7. After the commit only: apply the change to `SubscriptionIndex` (in version order, catching up from the database if a gap exists); for a first registration, provision the channel topic and set `channel_ready`; answer 200 or 201 with `V + 1`.

A failure before or during commit rolls back, and the index is untouched (FR-007, FR-008). An unknown commit outcome answers 500 and leaves the index untouched. The next selection's version check reconciles either way.

## Selection transaction

The selection transaction runs once per `SaleCompleted` (`SaleSelectionHandler`, `JdbcSelectionStore`):

1. Begin. `SELECT version FROM subscription_clock WHERE id = 1 FOR SHARE` gives `V`. No change can commit until this transaction ends.
2. If `index.version < V`: `SELECT … FROM subscription_change WHERE version > ? AND version <= ? ORDER BY version`, apply the rows, and publish the new index snapshot.
3. For each line (SKU, quantity), recipients = `index[sku]`. Build one notification per (SKU, subscriber).
4. `INSERT INTO sale_selection (…, subscription_version = V, …)`. On a duplicate key, roll back: the sale was already selected, and the event is acknowledged as a no-op.
5. `INSERT INTO notification …`, in multi-row statements of at most 500 rows, with state `PENDING` and `next_attempt_at = now`.
6. `FaultHooks.hit("notification.selection.beforeCommit")`, commit, then `FaultHooks.hit("notification.selection.afterCommit")`.
7. Hand the inserted IDs, with their contents, to `NotificationDispatcher`. `ConsumerLoop` then commits the offset.

Budget: 3 + ⌈n/500⌉ statements and 1 commit per sale, enforced in `statement-budgets.properties`.

**Recorded selection**: (`sale_selection.subscription_version`, the `notification` rows of the sale). It is final (rule 5): rows are never deleted when subscriptions change later.

## Dispatch

`NotificationDispatcher` is one thread plus send callbacks, behind `RecordSender`.

| Step | Rule |
|---|---|
| Intake | IDs from selection commits (immediate path) and from the sweep: `state = 'PENDING' AND next_attempt_at <= now`, ordered by `next_attempt_at`, at most 1,000 rows, every 1 s |
| Lanes | One FIFO per subscriber; a send is started only while that lane has < 64 in flight; lanes are served round-robin |
| Channel provisioning | Before the first send to a channel not marked ready, create the topic (idempotent); a failure is an uncharged failure for that notification |
| Send | `RecordSender.send(channel_topic, subscriber_id, envelope)`, first through the test-fault wrapper (`notification.channel.reject:<id>`, `notification.channel.stall:<id>`) |
| Success | Record the channel's last confirmed send time; add to the marking batch: `UPDATE notification SET state = 'DELIVERED', delivered_at = ?, attempt_count = attempt_count + 1 WHERE notification_id IN (…) AND state = 'PENDING'`, flushed every 20 ms or 200 IDs; `FaultHooks.hit("notification.delivery.afterSend")` runs before the batch is written |
| Failure | Charged if another channel had a confirmed send after this attempt started; otherwise uncharged. Batched `UPDATE` of `attempt_count`, the charged or uncharged count, `last_error`, and `next_attempt_at = now + min(200 ms × 2^(charged + uncharged), 5 s)` |
| Parking | On the 8th charged failure: `state = 'PARKED'`, `parked_at`, and an alarm in the status line and `ops/status` |
| Stall alarm | No confirmed send on any channel for 60 s while rows are pending: an alarm (broker-wide outage), and no parking |
| Operator retry | `POST /notifications/ops/parked/retry`: `state = 'PENDING'`, `charged_failures = 0`, `operator_retries + 1`, `next_attempt_at = now` |

Producer settings for channels: `acks=all`, idempotence enabled, `linger.ms=5`, `delivery.timeout.ms=10000`, `max.block.ms=1000`, one producer shared by all channels. Per-lane caps bound the producer buffer that any one channel can occupy.

## Fault and recovery matrix

| Fault point | Effect after restart or redelivery | Effective notifications |
|---|---|---|
| Crash before T3 commit (`transaction.sale.beforeT3Commit`) | No completion and no event; the attempt stays pending and settles through 006 recovery, and T3 then records the event once | One set, after T3 |
| Crash after T3 commit, before the outbox send | The sweep publishes the `SaleCompleted` row | One set |
| Broker accepted the event but the confirmation was lost | Re-sent; Notification sees a duplicate `sale_id`, which is a no-op | One set |
| Crash before the selection commit | Offset not committed; the event is redelivered and selected once | One set |
| Crash after the selection commit, before the offset commit | Redelivered; duplicate `sale_id`, no-op | One set |
| Crash before the first send | The sweep sends the `PENDING` rows | One each |
| Crash after a confirmed send, before marking (`notification.delivery.afterSend`) | Re-sent with the same identity; the subscriber records a redelivery | One each (plus a redelivery) |
| Channel rejects or stalls (one subscriber) | Charged retries, then parked; other lanes unaffected | Delivered after the fault is removed and an operator retry |
| Broker down or restarting | Uncharged failures, retried until the broker returns; Transaction's events wait in its outbox | One each, after recovery |
| Subscriber test application down | Delivery unaffected; the channel retains records; the subscriber resumes from its recorded offset | Received after restart |
| Notification database down | Selection and changes fail transiently; consumption pauses without acknowledgement | One set, after recovery |
| Changes committed during selection | Impossible: the clock's shared lock blocks them until the selection commits | Rule 3 holds |
