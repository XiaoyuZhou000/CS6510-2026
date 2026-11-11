# Contract: Completed-Sale Event and Subscriber Notifications

This contract extends the 006 event catalog (`specs/006-event-based-architecture/contracts/event-catalog.md`). The envelope, the validation rules, the parking rules, and the four existing topics are unchanged.

## Topics (additions)

| Topic | Partitions | Cleanup | Record key | Producer | Consumer (group or mode) | Threads | Parked in |
|---|---:|---|---|---|---|---:|---|
| `selfcheckout.sale-completed.v1` | 16 | `delete`, 006 Windows pins | Transaction ID | Transaction | Notification (`notification-sale-completed`) | 4 | Notification `dead_letter_event` |
| `selfcheckout.notify.<subscriberId>.v1` (one per subscriber) | 1 | `delete`, 006 Windows pins, `segment.index.bytes=1048576` | Subscriber ID | Notification | That subscriber's thread in the subscriber test application (manual assignment, no group) | 1 | Not parked; the receipt records keep everything |
| `selfcheckout.catalog.v1` (existing) | 1 | unchanged | unchanged | Inventory | **added**: Notification (manual assignment, replay from 0, for `KnownSkus`, N6) | 1 | Notification `dead_letter_event` |

The sale topic is created by `deploy/kafka/create-topics.ps1`. Notification Service creates channel topics on registration through the Kafka `Admin` client (N7). All names carry `KAFKA_TOPIC_PREFIX`.

## Event types (additions)

| Event type | Topic | Producer | Consumer | Meaning (a recorded fact) | Payload | Idempotency key at the consumer |
|---|---|---|---|---|---|---|
| `SaleCompleted` | sale-completed | Transaction | Notification | Transaction recorded this transaction as COMPLETED with these SKU quantities | [proto/sale_events.proto](proto/sale_events.proto) | `sale_selection.sale_id` |
| `SubscriberNotified` | the subscriber's channel | Notification | The subscriber | Notification selected this subscriber for this SKU of this sale | [proto/notification_events.proto](proto/notification_events.proto) | `notification_id` at the receiver |

Both are `schema_version` 1. `EventTypes` adds `NOTIFICATION = "notification"` as a producer value. The envelope's `producer` must equal the type's owner, and `routing_key` must equal `transaction_id` or `subscriber_id` respectively. A failed check parks the record (006 D6).

## Envelope values

| Field | `SaleCompleted` | `SubscriberNotified` |
|---|---|---|
| `event_id` | UUID v3 of `sale:<transaction_id>` | `notification_id` = UUID v3 of `notification:<sale_id>:<sku>:<subscriber_id>` |
| `routing_key` | Transaction ID | Subscriber ID |
| `occurred_at_ms` | T3's `completed_at` | The selection commit time |
| `correlation_id` | Transaction ID | Sale ID |
| `producer` | `transaction` | `notification` |

## Production rules

- **`SaleCompleted`**: inserted only by T3, in the same local transaction as `status = 'COMPLETED'` (FR-002). It is published through Transaction's `OutboxDispatcher`: the immediate path, the sweep, and `acks=all` (006 D5). No other code path may create it, which `OutboxOnlyPublishTest` and `SaleEventOnlyFromT3Test` enforce.
- **`SubscriberNotified`**: sent only by `NotificationDispatcher` from an existing `notification` row, always with that row's stored identity and contents (N8). A re-send never creates a new identity.

## Consumption rules

- **Notification, `SaleCompleted`**: validate the envelope and payload: lines non-empty, unique, ascending, quantities ≥ 1, and `sale_id = transaction_id = routing_key`. Then run the selection transaction ([notification-delivery.md](notification-delivery.md#selection-transaction)). A duplicate `sale_id` is a no-op. The offset is committed only after the commit. Invalid records are retried 5 times and then parked with an alarm (FR-018).
- **Subscriber, `SubscriberNotified`**: decode, append a receipt line, and flag it as a redelivery when `notification_id` was already recorded. Records that cannot be decoded are recorded as `undecodable` receipts and reported as unexpected (FR-026).

## Ordering

No ordering is guaranteed across sales, or among one subscriber's notifications (spec Assumptions). Per-key ordering of one sale's single event is trivially preserved. Correctness depends on identities, never on order.

## Compatibility

`sale_events.proto` and `notification_events.proto` join the 006 schema baselines in `tests/messaging/baseline/`. `SchemaCompatibilityTest` checks that fields are never renumbered or retyped. Existing consumers ignore the new topic, because Kafka delivers it only to subscribed groups. Adding the consumer changes no existing consumer (Principle III).
