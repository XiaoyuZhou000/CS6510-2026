# Phase 1 Data Model: Self-Checkout Monolithic Server

This feature does not modify `db/init.sql`; the tables below already exist there and are
reused as-is. Each entity from `spec.md`'s Key Entities section is mapped to its persisted
and/or in-memory representation.

## Persisted entities (MySQL, `db/init.sql`, unchanged)

### CatalogItem → `catalog_item`

| Field | Type | Notes |
|---|---|---|
| sku | VARCHAR(20) PK | e.g. `SKU-000123` |
| name | VARCHAR(100) | |
| price | DECIMAL(10,2) | captured into a basket line at scan time; immutable during a run |

Source of truth for `GET /items` and for resolving `name`/`unitPrice` on scan.

### StockLevel → `inventory`

| Field | Type | Notes |
|---|---|---|
| sku | VARCHAR(20) PK, FK → catalog_item | |
| stock_quantity | INT, `CHECK >= 0` | decremented only at completion (§ research.md #1) |
| low_stock_threshold | INT | per-SKU default threshold; overridable per-request (FR-015) |

**Validation rules**: `stock_quantity` MUST NOT go negative (DB-level `CHECK` constraint is a
backstop; the application-level conditional `UPDATE` in research.md #1 is the primary guard so
that a violation is rejected cleanly rather than throwing a constraint-violation exception).

### Transaction → `transaction`

| Field | Type | Notes |
|---|---|---|
| transaction_id | VARCHAR(40) PK | generated at start (e.g. UUID) |
| station_id | VARCHAR(40) | from `StartTransactionRequest.stationId` |
| status | ENUM(OPEN, COMPLETED, CANCELLED) | inserted as OPEN at start; transitions to COMPLETED only via the guarded UPDATE in research.md #1 |
| total_amount | DECIMAL(12,2) | written at completion from the in-memory basket's running total |
| started_at | TIMESTAMP(3) | set on creation |
| completed_at | TIMESTAMP(3), NULL | set only on successful completion |

**State transitions**: `OPEN → COMPLETED` (via completion, exactly once) or `OPEN → CANCELLED`
(not exercised by any endpoint in this spec's scope, but the column exists in the schema; no
functional requirement in `spec.md` currently drives a transition into `CANCELLED`, so no code path
sets it — it exists for schema completeness and future weeks per the constitution's
forward-compatibility note). No transition leaves `COMPLETED`.

**Write timing**: The `transaction` row is inserted once, **at start** (`status='OPEN'`,
`total_amount=0`) — not first-written at completion. This one INSERT per transaction (not per
scan) is what gives the completion-time guard UPDATE in research.md §1
(`UPDATE transaction SET status='COMPLETED' ... WHERE status='OPEN'`) a row to target; without it,
that guard would have nothing to match and idempotent completion would have no row-based mechanism.
It also directly satisfies FR-003 ("starting a transaction returns a transaction identifier and
its initial open, zero-item state"). Completion then runs that guarded UPDATE against the
already-existing row and inserts `transaction_line` rows at that point; *scanning* itself never
touches the database (see Basket, below) — only start (one INSERT) and completion (the guarded
UPDATE plus line inserts) do. `GET /transactions/{id}` (debugging only, low volume per FR-012)
reads the DB row for status/timestamps but reports `itemCount`/`runningTotal` from the in-memory
basket while OPEN, falling back to the persisted `total_amount` and a `COUNT`/`SUM` over
`transaction_line` once COMPLETED, since the in-memory basket may have been evicted by then.

### ScannedLine/Basket (at completion) → `transaction_line`

| Field | Type | Notes |
|---|---|---|
| transaction_id | VARCHAR(40) PK, FK | |
| sku | VARCHAR(20) PK, FK | one row per distinct SKU per transaction |
| quantity | INT, `CHECK > 0` | total units of that SKU scanned |
| unit_price | DECIMAL(10,2) | price captured at scan time |

Written only at completion, from the in-memory basket (research.md #3). Never written for a
transaction that is not completed.

### Receipt

Not a separate table — a `Receipt` (per the OpenAPI `Receipt` schema) is assembled at completion
time directly from: the `transaction` row (`transaction_id`, `station_id`, `total_amount`,
`started_at`, `completed_at`) plus the just-inserted `transaction_line` rows (joined to
`catalog_item` for `name`), returned in the same response — no additional read is required since
the server already holds all of this data at the end of the completion transaction.

### PopularItemsWindow → `popular_window` + `popular_item`

| `popular_window` field | Type | Notes |
|---|---|---|
| window_id | BIGINT PK, auto-increment | |
| window_start | BIGINT | global scan-counter position, inclusive |
| window_end | BIGINT | global scan-counter position, inclusive |
| computed_at | TIMESTAMP(3) | |

| `popular_item` field | Type | Notes |
|---|---|---|
| window_id | BIGINT PK, FK | |
| rank_pos | INT PK | 1..10 |
| sku | VARCHAR(20), FK | |
| scan_count | BIGINT | |

Written once per 500-scan boundary (research.md #4). `GET /analytics/popular-items` reads the
row with the highest `window_id` (equivalently, latest `computed_at`) and its `popular_item`
rows, joined to `catalog_item` for `name`, limited to the requested `limit` (default 10, per
FR-014).

## In-memory entities (server process, not persisted until completion)

### Basket (open transaction working state)

```
Basket {
  transactionId: String
  stationId: String
  startedAt: Instant
  lines: Map<sku, { quantity: int, unitPrice: BigDecimal, name: String }>   // mutated under a per-basket lock
}
```

Held in `ConcurrentHashMap<transactionId, Basket>` (research.md #3). `itemCount` = sum of
`quantity` across `lines`; `runningTotal` = sum of `quantity * unitPrice`. Removed from the map
once its transaction completes (its durable state now lives entirely in `transaction` +
`transaction_line`); a `GET /transactions/{id}` for a completed transaction after eviction falls
back to the DB read described above.

### Analytics window state

```
AnalyticsState {
  globalScanCounter: AtomicLong        // = MAX(window_end) read at startup; 0 means no saved checkpoint (fresh DB or crash before scan 500) — research.md #9
  ringBuffer: String[1000]             // SKU per scan position, index = (counter - 1) % 1000
  lock: Object                         // guards counter increment + ring buffer write + boundary snapshot + task submission
  checkpointExecutor: ExecutorService  // single-threaded — guarantees checkpoints commit in boundary order, retries in place (bounded retries per checkpoint; overall report freshness is NOT bounded — research.md §4)
  skipNextCheckpoint: boolean          // = (MAX(window_end) > 0) at startup — true only when a saved checkpoint exists (a true restart); false when none exists, since a missing checkpoint alone can't distinguish "fresh" from "crashed before scan 500" — research.md #9
}
```

Not persisted directly. Every 500th scan copies a snapshot (still under `lock`) — unless
`skipNextCheckpoint` applies to this boundary, in which case the snapshot is discarded and the flag
is cleared (research.md §9) — and submits a persistence task to `checkpointExecutor`, which
tallies, ranks, and writes one `popular_window` + up to 10 `popular_item` rows, retrying in place
on failure before moving to the next queued checkpoint (research.md §4). No scan response ever
waits on this: analytics persistence is fully best-effort and decoupled from the checkout path.
`GET /analytics/popular-items` reads whatever `popular_window` row currently has the highest
`window_id` — the latest checkpoint that actually succeeded.

## Relationships

```
catalog_item 1───1 inventory
catalog_item 1───* transaction_line *───1 transaction
transaction  1───1 Basket (in-memory, while OPEN; absent once COMPLETED)
popular_window 1───* popular_item *───1 catalog_item
```

## Validation rules summary (cross-referenced to functional requirements)

- `inventory.stock_quantity >= 0` always — FR-008, enforced by the conditional UPDATE + DB CHECK.
- `transaction.status` transitions `OPEN → COMPLETED` at most once per row — FR-009, enforced by
  the guarded UPDATE's affected-row check.
- A scan MUST target an existing SKU and an OPEN transaction — FR-004, checked against
  `catalog_item` and the in-memory `Basket`'s presence/status before mutating `lines`.
- Completion MUST reject an empty basket — FR-007, checked against `lines.isEmpty()` before
  starting the DB transaction (no DB round trip needed to reject this case).
