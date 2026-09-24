# Phase 1 Data Model: Layered Self-Checkout Server

The migration changes ownership and interfaces, not the persistent schema. `db/init.sql` remains
the source of truth. Boundary records are immutable Java records owned by the contract that emits
them; they are not additional database entities.

## Persisted entities

### Catalog Item — `catalog_item`

| Field | Type | Rules |
|---|---|---|
| `sku` | `VARCHAR(20)` primary key | Required and unique |
| `name` | `VARCHAR(100)` | Required |
| `price` | `DECIMAL(10,2)` | Required; captured into basket line at scan time |

One catalog item has exactly one inventory record and may appear in many completed transaction
lines and popular-item ranks.

### Inventory Record — `inventory`

| Field | Type | Rules |
|---|---|---|
| `sku` | `VARCHAR(20)` primary/foreign key | References `catalog_item` |
| `stock_quantity` | `INT` | Must remain non-negative; changes only during committed completion |
| `low_stock_threshold` | `INT` | Per-SKU default; request override is not persisted |

Invariant: for each SKU, baseline stock minus final stock equals the quantity in completed
transaction lines. Conditional updates reject insufficient stock before commit.

### Transaction — `transaction`

| Field | Type | Rules |
|---|---|---|
| `transaction_id` | `VARCHAR(40)` primary key | Generated once at start |
| `station_id` | `VARCHAR(40)` | Non-blank public input |
| `status` | `ENUM(OPEN, COMPLETED, CANCELLED)` | Current feature creates `OPEN` and permits only `OPEN → COMPLETED` |
| `total_amount` | `DECIMAL(12,2)` | Zero at start; committed basket total on completion |
| `started_at` | `TIMESTAMP(3)` | Set at start |
| `completed_at` | `TIMESTAMP(3)` nullable | Set only by successful completion |

State transitions:

```text
          successful atomic completion
OPEN  ─────────────────────────────────> COMPLETED
  │                                          │
  └─ failed/insufficient attempt ────────────┘ no state change

COMPLETED + repeated completion -> COMPLETED, no writes, domain NOT_OPEN result
```

`CANCELLED` is reserved by the schema; this feature adds no cancellation endpoint or transition.

### Basket Line / Completed Sale Line — `transaction_line`

| Field | Type | Rules |
|---|---|---|
| `transaction_id` | `VARCHAR(40)` composite primary/foreign key | References `transaction` |
| `sku` | `VARCHAR(20)` composite primary/foreign key | One row per distinct SKU |
| `quantity` | `INT` | Positive; aggregated accepted scans for this SKU |
| `unit_price` | `DECIMAL(10,2)` | Price captured at scan time |

Lines are inserted only within the same database transaction that completes the transaction and
decrements inventory. A failed completion leaves no lines.

### Popular-Item Window — `popular_window`

| Field | Type | Rules |
|---|---|---|
| `window_id` | `BIGINT` auto-increment primary key | Persistence identity |
| `window_start` | `BIGINT` | First claimed scan position |
| `window_end` | `BIGINT` | Last claimed scan position |
| `computed_at` | `TIMESTAMP(3)` | Persistence time |

The first uninterrupted checkpoint is at scan 500 and describes the available portion of the
1,000-slot buffer; after warm-up, checkpoints describe the most recent 1,000 accepted scans and
advance by 500 positions.

### Popular-Item Rank — `popular_item`

| Field | Type | Rules |
|---|---|---|
| `window_id` | `BIGINT` composite primary/foreign key | References `popular_window` |
| `rank_pos` | `INT` composite primary key | 1–10 |
| `sku` | `VARCHAR(20)` foreign key | References `catalog_item` |
| `scan_count` | `BIGINT` | Positive count within the claimed window |

One window has zero to ten ranks, ordered by descending count with deterministic SKU tie-breaking
in the analytics layer.

## In-memory entities

### Open Basket

```text
OpenBasket
├── transactionId: String
├── stationId: String
├── startedAt: Instant
├── state: OPEN | COMPLETING | COMPLETED
└── lines: Map<sku, BasketLine>
    ├── sku: String
    ├── name: String
    ├── unitPrice: BigDecimal
    └── quantity: positive int
```

The transaction layer owns baskets in a concurrent map and serializes mutation per basket.
`itemCount` and `runningTotal` are derived. `COMPLETING` temporarily closes scan admission; failure
returns to `OPEN`, while commit transitions to `COMPLETED` and evicts the basket.

### Analytics State

```text
AnalyticsState
├── globalScanCounter: long
├── ringBuffer: sku[1000]
├── skipNextCheckpoint: boolean
└── orderedCheckpointExecutor: single worker
```

The analytics layer owns this state. It records exactly one event for each successfully admitted
scan, snapshots every 500 events, and exposes only persisted window results.

## Layer-boundary records

| Contract | Main records | Validation/failure outcomes |
|---|---|---|
| API → transaction | `StartCommand`, `ScanCommand`, `TransactionView`, `ScanView`, `ReceiptView`, `CatalogItemView`, `LowStockView` | Missing/invalid input is API validation; domain failures are `NOT_FOUND`, `NOT_OPEN`, `EMPTY_BASKET`, `UNKNOWN_SKU`, `INSUFFICIENT_STOCK` |
| API → analytics | `PopularItemsView`, `RankedItemView` | `limit` is a non-negative integer; persistence/unavailability maps once at API boundary |
| transaction → database | open/read transaction records, catalog/inventory rows, `CompletionCommand`, `CompletionResult` | Database contracts expose typed absence/conflict outcomes; no `SQLException` or `Connection` crosses upward |
| analytics → database | `PopularWindowSnapshot`, `PopularWindowView` | Window bounds are coherent; ranks are capped at ten and ordered |
| transaction scan callback | accepted SKU | Emitted once and only after basket admission succeeds |

## Relationships

```text
catalog_item 1 ─── 1 inventory
catalog_item 1 ─── * transaction_line * ─── 1 transaction
transaction  1 ─── 0..1 OpenBasket (only while active in this process)
popular_window 1 ─── 0..10 popular_item * ─── 1 catalog_item
```

## Reset baseline

Every validation run recreates exactly 2,000 catalog rows and 2,000 inventory rows with 10,000
units per SKU and clears transactions, completed lines, and popular windows. A fresh server process
must follow reset so no prior basket, catalog cache, counter, or ring-buffer state survives.
