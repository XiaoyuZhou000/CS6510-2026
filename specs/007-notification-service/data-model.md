# Data Model: Sale Notification Service

Phase 1 design for [plan.md](plan.md). Decisions are cited as N*n* from [research.md](research.md). Feature 006's data model (`specs/006-event-based-architecture/data-model.md`) is unchanged except where this document says otherwise.

## 1. Transaction Service (`cs6510_transaction`)

No table or column is added. `outbox_event` gains one event type:

| Column | `SaleCompleted` row |
|---|---|
| `event_id` | Name-based UUID (v3) of `sale:<transaction_id>` (N2). The existing `uq_outbox_event_id` allows one per transaction. |
| `topic` | `<prefix>selfcheckout.sale-completed.v1` |
| `routing_key` | Transaction ID |
| `event_type` | `SaleCompleted` |
| `aggregate_key` | `sale:<transaction_id>` |
| `envelope` | Serialized `EventEnvelope` wrapping `SaleCompleted` ([contracts/sale-completed-event.md](contracts/sale-completed-event.md)) |

**T3 (changed)**: `UPDATE transaction` (guarded) → `UPDATE checkout_attempt` → `INSERT outbox_event (SaleCompleted)` → commit. Three statements and one commit; the budget changes from 2 to 3 (N15).

**Invariant** (SC-001): `COUNT(transaction WHERE status = 'COMPLETED') = COUNT(outbox_event WHERE event_type = 'SaleCompleted')`, and each row's lines equal the transaction's per-SKU `accepted_scan` counts.

## 2. Notification Service (`cs6510_notification`)

### `subscription_clock`

| Column | Type | Rule |
|---|---|---|
| `id` | TINYINT PK | Always 1 (CHECK) |
| `version` | BIGINT NOT NULL | Current history version; 0 at baseline. Changes hold it `FOR UPDATE`; selections hold it `FOR SHARE` (N4, N5). |

### `subscriber`

| Column | Type | Rule |
|---|---|---|
| `subscriber_id` | VARCHAR(40) PK | `^[A-Za-z0-9_-]{1,40}$`; also part of the channel topic name |
| `status` | ENUM('ACTIVE','CANCELLED') | |
| `channel_topic` | VARCHAR(200) NOT NULL | `<prefix>selfcheckout.notify.<subscriber_id>.v1` |
| `channel_ready` | BOOLEAN NOT NULL DEFAULT FALSE | Set after the topic has been created (N7) |
| `created_version` | BIGINT NOT NULL | History version of the first registration |
| `updated_version` | BIGINT NOT NULL | History version of the latest effective change; returned for no-op repeats |
| `created_at`, `updated_at` | TIMESTAMP(3) | Informational |

### `subscription`

Current SKUs of active subscribers only.

| Column | Type | Rule |
|---|---|---|
| `subscriber_id` | VARCHAR(40) | FK → `subscriber` |
| `sku` | VARCHAR(20) | A SKU present in the catalog when added (N6) |
| `added_version` | BIGINT NOT NULL | |
| PK | (`subscriber_id`, `sku`) | |
| Index | (`sku`, `subscriber_id`) | Index build and catch-up |

### `subscription_change`

The ordered history (FR-007). Rows are immutable.

| Column | Type | Rule |
|---|---|---|
| `version` | BIGINT PK | Contiguous from 1; equals the clock after the change commits |
| `subscriber_id` | VARCHAR(40) NOT NULL | |
| `change_type` | ENUM('REGISTER','UPDATE','CANCEL') | `REGISTER` also covers re-registration after cancellation |
| `added_skus`, `removed_skus` | TEXT NOT NULL | Canonical JSON arrays, sorted; empty arrays allowed |
| `resulting_skus` | TEXT NOT NULL | The subscriber's SKU set after the change (empty after `CANCEL`) |
| `committed_at` | TIMESTAMP(3) NOT NULL | Informational |

**Replay rule**: the state at version V is obtained by applying rows 1…V in order. Each row's `resulting_skus` is the complete state for its subscriber, so replay needs only the latest row per subscriber with `version ≤ V`.

### `sale_selection`

One row per selected sale, including sales with no recipients (FR-011).

| Column | Type | Rule |
|---|---|---|
| `sale_id` | VARCHAR(40) PK | The transaction ID; the duplicate key makes redelivered events no-ops (FR-012) |
| `sale_event_id` | CHAR(36) NOT NULL | The `SaleCompleted` envelope `event_id` |
| `completed_at` | TIMESTAMP(3) NOT NULL | From the event |
| `lines` | TEXT NOT NULL | Canonical `[{"sku","quantity"}]`, sorted |
| `subscription_version` | BIGINT NOT NULL | The history point used (rule 4) |
| `recipient_count` | INT NOT NULL | Distinct selected subscribers |
| `notification_count` | INT NOT NULL | Rows inserted into `notification` |
| `selected_at` | TIMESTAMP(3) NOT NULL | |

### `notification`

The selection result per SKU, the delivery record, and the outbox for the subscriber's channel (N8).

| Column | Type | Rule |
|---|---|---|
| `notification_id` | CHAR(36) PK | Name-based UUID (v3) of `notification:<sale_id>:<sku>:<subscriber_id>` (FR-014) |
| `sale_id` | VARCHAR(40) NOT NULL | FK → `sale_selection` |
| `sku` | VARCHAR(20) NOT NULL | |
| `subscriber_id` | VARCHAR(40) NOT NULL | |
| `quantity` | INT NOT NULL | Units of `sku` in the sale; CHECK > 0 |
| `completed_at` | TIMESTAMP(3) NOT NULL | Copied for the message and latency |
| `state` | ENUM('PENDING','DELIVERED','PARKED') | See the state machine below |
| `attempt_count` | INT NOT NULL DEFAULT 0 | Every send attempt |
| `charged_failures` | INT NOT NULL DEFAULT 0 | Failures attributed to the channel (N8) |
| `uncharged_failures` | INT NOT NULL DEFAULT 0 | Broker-wide failures |
| `next_attempt_at` | TIMESTAMP(3) NOT NULL | Due time for the sweep |
| `last_error` | VARCHAR(255) NULL | |
| `created_at` | TIMESTAMP(3) NOT NULL | Selection commit time |
| `delivered_at` | TIMESTAMP(3) NULL | Set only by a confirmed publish |
| `parked_at` | TIMESTAMP(3) NULL | |
| `operator_retries` | INT NOT NULL DEFAULT 0 | |
| Unique | (`sale_id`, `sku`, `subscriber_id`) | At most one effective notification per key |
| Index | (`state`, `next_attempt_at`) | The sweep |
| Index | (`subscriber_id`, `state`) | Per-subscriber status and retry |

CHECKs: `(state = 'DELIVERED') = (delivered_at IS NOT NULL)`; `(state = 'PARKED') = (parked_at IS NOT NULL)`.

### `dead_letter_event`

The same shape as in 006 (shared `JdbcDeadLetterStore`). It parks undecodable or invalid `SaleCompleted` and catalog records (FR-018).

## 3. State machines

**Subscriber**

```text
(none) --REGISTER--> ACTIVE --UPDATE (result non-empty)--> ACTIVE
ACTIVE --CANCEL--> CANCELLED --REGISTER--> ACTIVE
```

A change that would leave an active subscriber with no SKU is rejected (`409 LAST_SKU`). An identical repeat is a no-op without a history row.

**Notification**

```text
selection commit --> PENDING
PENDING --confirmed publish--> DELIVERED                (terminal)
PENDING --failed send--> PENDING (attempt_count+1; charged or uncharged; next_attempt_at = backoff)
PENDING --8th charged failure--> PARKED (alarm)
PARKED  --operator retry--> PENDING (charged_failures = 0, operator_retries+1, next_attempt_at = now)
```

`DELIVERED` never changes back. A crash after a confirmed publish but before the mark leaves the row `PENDING`, and the re-send is a redelivery with the same identity.

**Sale event at Notification**

```text
consumed --selection commit--> selected (offset committed)
consumed --duplicate sale_id--> no-op (offset committed)
consumed --permanent failure ×5--> parked in dead_letter_event (alarm)
consumed --transient failure--> retried indefinitely (006 D6)
```

## 4. In-memory structures (never authoritative)

| Structure | Content | Source and rebuild | Consistency rule |
|---|---|---|---|
| `SubscriptionIndex` | Immutable map SKU → sorted subscriber IDs, plus its `version` | At startup from `subscription` and `subscription_clock` in one consistent read; then committed changes in version order | Every selection compares `version` with the locked clock and catches up from `subscription_change` first (N5) |
| `KnownSkus` | The catalog's SKU set | Replay of the compacted catalog topic (N6) | Used only to validate requests; empty until a complete snapshot arrives |
| Dispatcher lanes | Per-subscriber queues of due notification IDs, at most 64 in flight each | Selection commits and the sweep | The `notification` table decides; lanes are refilled from it after a restart |
| Charging tracker | Time of the last confirmed send per channel | Send callbacks | Decides only charged vs. uncharged; never the state |

## 5. Subscriber test application files (`<run>/subscribers/`)

| File | Content |
|---|---|
| `snapshot.json` | `seed`, settings (`subscribers`, `skusPerSubscriber`, `skuDistribution`, `idPrefix`), catalog SHA-256, and `subscribers: [{id, skus[]}]` sorted by ID |
| `ready.json` | `readyAt`, `subscriptionVersion` (the highest confirmed version), and the per-subscriber confirmed versions |
| `receipts/<id>.jsonl` | One JSON object per received record: `subscriberId`, `notificationId`, `saleId`, `sku`, `quantity`, `completedAtMs`, `receivedAtMs`, `channelOffset`, `redelivery` (true when the notification ID was already recorded) |
| `status.json` | Rewritten every second: per-subscriber `received`, `redeliveries`, `lag`, plus totals |
| `changes.jsonl` | Scenario E only: each attempted change with request time, outcome, confirmed version, and any sale completed after it |

## 6. Validation rules (from the spec)

| Rule | Where enforced |
|---|---|
| Subscriber ID format, non-empty SKU set, known SKUs (FR-006) | `SubscriptionService` before the change transaction |
| Conflicting re-registration of an active subscriber → `409 SUBSCRIBER_EXISTS` (FR-006) | Change transaction, under the clock lock |
| At least one SKU while active (FR-006) | Change transaction |
| Effective only at commit; failed change has no effect (FR-007) | Clock lock + index updated only after commit |
| One selection per sale (FR-012) | `sale_selection` PK |
| One effective notification per (sale, SKU, subscriber) (FR-013, FR-015) | `notification` unique key + deterministic ID |
| `DELIVERED` only after a confirmed publish (FR-016) | Marking batch from send callbacks |
| `SaleCompleted` has no recipient data (FR-004) | Proto schema + `TransactionHasNoRecipientLogicTest` |
