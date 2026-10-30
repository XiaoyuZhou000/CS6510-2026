# Data Model: Event-Based Self-Checkout Microservices

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md) | **Decisions**: [research.md](research.md)

This feature keeps feature 005's schemas, users, session settings, and guarded-write rules ([005 data model](../005-microservices-architecture/data-model.md)). The tables below are only the **changes**. Any 005 table not mentioned here is unchanged. Exact DDL lives in `db/<service>/schema.sql`. All timestamps are UTC with millisecond precision and come from the owning service.

## Transaction schema (`cs6510_transaction`)

### `accepted_scan` (changed)

| Change | Rule |
|---|---|
| `delivered_at` renamed `published_at` | Set only after the broker confirms the `ScanAccepted` record (D10). It no longer means Analytics received it. |
| Index `(delivered_at, scan_id)` renamed `(published_at, scan_id)` | Relay selection |

The event identity `(transaction_id, scan_sequence)` and the `event_id` derived from it, `scan:<transaction_id>:<scan_sequence>`, are unchanged, so a re-sent row is always the same event.

### `outbox_event` (new; also in Inventory with the same shape)

| Column | Type | Rules |
|---|---|---|
| `outbox_seq` | BIGINT auto-increment, PK | Publication order within the service |
| `event_id` | CHAR(36), UNIQUE | Envelope `event_id`; reused on every re-send |
| `topic` | VARCHAR(64) | One of the topics in the [event catalog](contracts/event-catalog.md) |
| `routing_key` | VARCHAR(64) | Kafka record key |
| `event_type` | VARCHAR(48) | Owned by this service (FR-009) |
| `aggregate_key` | VARCHAR(64) | `<transaction_id>:<attempt_number>` for checkout events, used by recovery and duplicate handling to find the recorded event; null for catalog events |
| `envelope` | MEDIUMBLOB | The complete serialized `EventEnvelope`, so a re-send is byte-identical |
| `created_at` | TIMESTAMP(3) | Same local transaction as the causing state change |
| `published_at` | TIMESTAMP(3), null | Set by the batched marker after broker confirmation |
| `send_count` | INT, default 0 | Incremented by the sweep and by re-sends, for diagnostics |

Indexes: `(published_at, outbox_seq)` for the sweep, and `(aggregate_key)`. Rows are written only inside the transaction whose state change they announce, and they are never updated except `published_at` and `send_count`.

### `checkout_attempt` (changed)

| Change | Rule |
|---|---|
| New column `request_event_id` CHAR(36), not null | The `CheckoutRequested` outbox row written by T1. Recovery re-sends this row (D7). |
| `next_retry_at` | T1 sets it to `created_at + CHECKOUT_REPUBLISH_AFTER_MS`. It is now the republish schedule, not an RPC retry schedule. |
| `last_error` | Records `REPUBLISHED`, `PUBLISH_FAILED`, or `STUCK` (age ≥ `CHECKOUT_STUCK_ALARM_SECONDS`). Integrity faults reported by the outcome handler record `INTEGRITY:<reason>`. |
| `rejection_reason` | Unchanged values: `INSUFFICIENT_STOCK`, `INVALID_REQUEST`, `STOCK_RESTORED` |
| `client_outcome` | Same values. Written by the outcome handler inside T3 or reopen when it claimed the responder (D7), or by the request with `PENDING_ERROR` when its budget ends first. |

### `product_replica` and `product_replica_meta` (unchanged shape, new source)

They are filled only by `CatalogConsumer` from `CatalogItemPublished` and `CatalogSnapshotPublished` events (D9). The whole replica is replaced in one local transaction when a complete snapshot of a new version has been read. The source is no longer a gRPC call.

### `dead_letter_event` (new; also in Inventory and Analytics with the same shape)

| Column | Type | Rules |
|---|---|---|
| `dead_letter_id` | BIGINT auto-increment, PK | |
| `event_id` | VARCHAR(80), null, UNIQUE when not null | Null only when the envelope could not be decoded |
| `topic`, `partition_no`, `record_offset` | VARCHAR(64), INT, BIGINT | Where the record was read |
| `event_type`, `schema_version` | VARCHAR(48) null, INT null | As decoded, if possible |
| `reason` | VARCHAR(64) | `UNDECODABLE`, `UNKNOWN_TYPE`, `UNSUPPORTED_VERSION`, `INVALID_PAYLOAD`, `PAYLOAD_MISMATCH`, `UNKNOWN_ATTEMPT`, `INTEGRITY`, `HANDLER_ERROR` |
| `detail` | VARCHAR(500) | |
| `record_bytes` | MEDIUMBLOB | The raw record value, for operators |
| `first_parked_at`, `last_parked_at`, `park_count` | TIMESTAMP(3), TIMESTAMP(3), INT | A parked `event_id` that arrives again increments `park_count` |

A parked row and the offset commit that follows it are the consumer's acknowledgement of a poison record (D6). Parked rows are never deleted automatically.

## Inventory schema (`cs6510_inventory`)

### `checkout_operation` (changed)

| Change | Rule |
|---|---|
| New column `outcome_event_id` CHAR(36), not null | The outcome event (`StockDeducted`, `StockRejected`, or `StockDeductionRefused`) written in the same transaction as the operation row (D8) |
| New column `restoration_event_id` CHAR(36), null | The `StockRestored` or `RestorationNotNeeded` event written by the reserved restoration |
| A BLOCKED tombstone | Gets an `outcome_event_id` too, for `RestorationNotNeeded` |

The apply transaction now contains the locking SELECT, the INSERT of `checkout_operation`, the CASE UPDATE when applied, and the INSERT of `outbox_event`, then the commit (D8).

### `catalog_meta` (changed)

| Change | Rule |
|---|---|
| New column `published_at` TIMESTAMP(3), null | Set in the startup publication transaction that writes the 2,001 catalog outbox events (D9) |

### `outbox_event` and `dead_letter_event` (new)

Same shape as in Transaction. Inventory's outbox holds catalog and outcome events. Its dead-letter table holds parked `CheckoutRequested` and `StockRestorationRequested` records.

## Analytics schema (`cs6510_analytics`)

### `consumer_offset` (new)

| Column | Type | Rules |
|---|---|---|
| `topic`, `partition_no` | VARCHAR(64), INT; PK together | |
| `next_offset` | BIGINT ≥ 0 | The next offset to read. It is written in the same ingestion transaction as the stored events and positions (D10), so the database, not the broker, is the record of consumption progress (FR-049). |
| `updated_at` | TIMESTAMP(3) | |

### `accepted_scan`, `product_name`, `scan_sequence`, windows, checkpoint (unchanged)

They are fed by `ScanAcceptedConsumer` instead of the gRPC ingest service. Deduplication by `(source_transaction_id, source_scan_sequence)` and position assignment are unchanged.

### `dead_letter_event` (new)

Same shape as in Transaction. It holds parked `ScanAccepted` records.

## Events

Event types, owners, keys, and payload fields are specified in [contracts/event-catalog.md](contracts/event-catalog.md) and [contracts/proto/](contracts/proto/). Each event is an immutable fact. It is durable in its producer's outbox before it reaches the broker, and its effect in a consumer is idempotent under the key listed below.

| Event | Producer's durable record | Consumer's idempotency key |
|---|---|---|
| `CatalogItemPublished`, `CatalogSnapshotPublished` | Inventory `outbox_event` | `(sku, catalog_version)`; the replica version |
| `CheckoutRequested` | Transaction `outbox_event`, linked from `checkout_attempt.request_event_id` | Inventory `checkout_operation (transaction_id, attempt_number)` |
| `StockDeducted`, `StockRejected`, `StockDeductionRefused` | Inventory `outbox_event`, linked from `checkout_operation.outcome_event_id` | Transaction attempt state and guarded T3 or reopen |
| `StockRestorationRequested` (reserved) | Transaction's format; produced only by direct tests (D8) | Inventory `checkout_operation` state |
| `StockRestored`, `RestorationNotNeeded` (reserved) | Inventory `outbox_event`, linked from `checkout_operation.restoration_event_id` | Transaction attempt state |
| `ScanAccepted` | Transaction `accepted_scan` | Analytics `(source_transaction_id, source_scan_sequence)` |

## In-memory projections (changes)

| Projection | Change |
|---|---|
| Product catalog snapshot (Transaction) | Swapped after `CatalogConsumer` commits a replica replacement, instead of after a gRPC refresh |
| Catalog version buffer (Transaction, new) | The items of a catalog version being read, held until its snapshot marker. It is never authoritative and is discarded at restart, when the topic is replayed from offset 0. |
| Settlement registry (Transaction) | Each entry gains a responder state, WAITING, ANSWERING, or TIMED_OUT, changed by compare and set (D7). Futures are completed by the outcome handler. |
| Recovery schedule (Transaction) | Schedules republishes of recorded `CheckoutRequested` events instead of `ApplyCheckout` retries |
| Dispatch queue and marking batch (every producing service, new) | Event IDs committed but not yet sent or marked. Lost at a crash, which is safe because the sweep re-sends unpublished rows (D5). |
| Catalog cache (Inventory) | Loaded at startup; no gRPC version check |

All other 005 projections are unchanged. Every projection is still process-local and never the sole record of acknowledged state (Principle II).

## State machines (changes)

### Checkout attempt (Transaction)

The 005 state machine is unchanged. Only its triggers change:

| Transition | Trigger now |
|---|---|
| T1 begin | Complete request. The commit also records `CheckoutRequested` in `outbox_event`. |
| T3 record completion | `CheckoutOutcomeHandler` consumes `StockDeducted` for the active attempt |
| Reopen | The handler consumes `StockRejected`, `StockDeductionRefused`, or `StockRestored` for the active attempt |
| Stay pending | No outcome yet, a lost or parked event, or the broker down. Recovery re-sends the recorded request on its schedule, and an age of 60 s or more raises the stuck alarm. |

### Inventory checkout operation

Unchanged from 005. Each transition also writes its outcome event in the same transaction, and a repeated request re-sends the recorded event instead of returning an RPC reply.

### Outbox entry (every producer)

```text
(written with its state change) ──broker confirms──▶ published (published_at set)
          └── send fails / process crashes ──▶ remains unpublished ──sweep re-sends──▶ published
published ──recovery or duplicate request re-sends the same event_id──▶ published (send_count + 1)
```

### Dead-letter entry

```text
(absent) ──permanent failure after 5 attempts──▶ parked (park_count = 1) ──same event_id parked again──▶ park_count + 1
```

## Validation rules (additions)

| Rule | Enforced by | Requirement |
|---|---|---|
| Every published event has an outbox row (or `accepted_scan` row) committed with its cause | Outbox INSERTs inside T1, apply, restore, catalog publication, and Scan; `OutboxOnlyPublishTest` | FR-010 |
| An event is acknowledged only after its consumer's local commit | `ConsumerLoop` offset commits after handler commit; tests with a kill between the two | FR-011 |
| A late or older catalog record never replaces a newer replica | One partition, version marker with count, atomic replacement | FR-012, FR-017 |
| Analytics progress survives the loss of broker offsets | `consumer_offset` written in the ingestion transaction; seek on assignment | FR-049 |
| Poison records are parked, visible, and never block a partition for long | `dead_letter_event`, five attempts, status line | FR-013 |
| No unpublished outbox row, no parked event, and no consumer lag at the end of a benchmark | `db/validate-invariants.ps1`, harness lag sample | FR-059, FR-061, SC-013 |
