# Phase 1 Data Model: Pipeline Analytics

This feature changes in-memory analytics ownership, messages, and lifecycle. It does not change the
persistent schema; `db/init.sql` remains authoritative. Queue messages are immutable Java records,
not database entities.

## In-memory entities

### Accepted Scan

| Field | Type | Rules |
|---|---|---|
| `sku` | `String` | Required, non-blank, at most 20 characters; identifies one admitted physical-unit scan |

Exactly one message is offered after a valid scan has entered an open basket. Rejected scans never
enter analytics. The ingress message has no producer-assigned position: the Window Filter assigns
the next position when it consumes the FIFO message, preventing producer concurrency from making
counter order disagree with queue order.

### Window Filter State

| Field | Type | Rules |
|---|---|---|
| `scanPosition` | `long` | Seeded from maximum persisted window end; incremented once per consumed scan |
| `acceptedSinceStart` | `long` | Counts fresh events in this process; a snapshot requires at least 1,000 |
| `ringBuffer` | 1,000 SKU slots | Owned and mutated only by the Window worker |
| `lifecycle` | worker state | `NEW`, `RUNNING`, `DRAINING`, `TERMINATED`, or `FAILED` |

Fresh start emits positions 1–1,000, then 501–1,500. Restart from persisted end `N` with no ring
recovery emits nothing until 1,000 new scans exist, then emits `N+1`–`N+1000`.

### Window Snapshot

| Field | Type | Rules |
|---|---|---|
| `windowStart` | `long` | Positive inclusive first position |
| `windowEnd` | `long` | Positive inclusive last position; `windowEnd - windowStart + 1 = 1000` |
| `skus` | immutable `List<String>` | Exactly 1,000 non-blank SKUs in analytics order |

Successive uninterrupted snapshots advance 500 positions and overlap by 500. The list is a deep
immutable copy in logical position order; it never exposes the live ring. Partial snapshots are not
emitted.

### Ranked Item

| Field | Type | Rules |
|---|---|---|
| `rank` | `int` | 1–10; contiguous in its ranked window |
| `sku` | `String` | Required, non-blank, at most 20 characters, unique within a ranked window |
| `scanCount` | `long` | Positive count in the complete 1,000-event snapshot |

Items are ordered by `scanCount` descending, then `sku` ascending. Catalog name is deliberately
absent; the current database read joins `catalog_item` to add it to the API view.

### Ranked Window

| Field | Type | Rules |
|---|---|---|
| `windowStart` | `long` | Copied unchanged from the snapshot |
| `windowEnd` | `long` | Copied unchanged from the snapshot |
| `ranks` | immutable `List<RankedItem>` | Zero to ten unique SKUs in deterministic contiguous rank order |

Exactly one ranked window is derived from each snapshot. The Ranking Filter has no persistence
collaborator.

### Pipeline Operational Record

This entity is a structured JSON log record only; it is never persisted or exposed through HTTP.

| Field | Type | Rules |
|---|---|---|
| `timestamp` | ISO-8601 instant | Required |
| `eventType` | enum string | Required stable event name |
| `stage` | enum string | `INGRESS`, `WINDOW`, `RANKING`, `PERSISTENCE`, or `PIPELINE` |
| `queueDepth` | non-negative integer | Required for backlog events |
| `retryCount` | positive integer | Required for persistence retry |
| `windowStart`, `windowEnd` | positive long | Required when a retry/window is identified |
| `shutdownOutcome` | enum string | Required for shutdown completion/forced events |
| `errorType`, `reason` | sanitized strings | Required for terminal failure/rejection as applicable |

Stable event types are `BACKLOG`, `PERSISTENCE_RETRY`, `INGESTION_REJECTED`, `DRAIN_STARTED`,
`SHUTDOWN_COMPLETED`, `SHUTDOWN_FORCED`, and `WORKER_FAILED`.

## Persisted entities (unchanged)

### Popular Window — `popular_window`

| Field | SQL type | Rules |
|---|---|---|
| `window_id` | `BIGINT` auto-increment | Primary persistence identity |
| `window_start` | `BIGINT` | Positive inclusive first position |
| `window_end` | `BIGINT` | Positive inclusive last position |
| `computed_at` | `TIMESTAMP(3)` | Set during atomic persistence |

One header and all its ranks commit in one JDBC transaction. Query visibility begins only after
commit. The single Persistence worker preserves ascending commit order; recovery seeds the next
in-memory position from `MAX(window_end)`.

### Popular Item — `popular_item`

| Field | SQL type | Rules |
|---|---|---|
| `window_id` | `BIGINT` | Foreign key to `popular_window`; part of composite primary key |
| `rank_pos` | `INT` | 1–10; part of composite primary key |
| `sku` | `VARCHAR(20)` | Foreign key to `catalog_item` |
| `scan_count` | `BIGINT` | Positive count within the complete window |

A popular window owns zero to ten ranks. Failure inserting any rank rolls back the header and all
ranks, so no partial window becomes visible.

## Existing API view (unchanged)

`AnalyticsOperations.PopularItemsView` carries `windowSize=1000`, `slideInterval=500`, bounds,
`computedAt`, and up to ten items. A ranked API item adds the catalog name read by the database
adapter. Before the first committed result, the view has bounds `0/0`, `Instant.EPOCH`, and an empty
item list. The query reads only persisted state.

## Relationships

```text
catalog_item 1 ─── * Accepted Scan
ordered Accepted Scans ─── Window Snapshot (size 1000, hop 500)
Window Snapshot 1 ─── 1 Ranked Window
Ranked Window   1 ─── 1 popular_window
popular_window 1 ─── 0..10 popular_item * ─── 1 catalog_item
```

An accepted scan can appear in one or two complete hopping windows. Transaction code connects to
the facade only through `AcceptedScanSink`; filters do not know transactions or HTTP types.

## State transitions

### Pipeline lifecycle

```text
NEW ──start──> RUNNING ──shutdown──> DRAINING ──all stages drained──> TERMINATED
                   │                    │
                   └─worker failure─────┴──────────────────────────> FAILED
                                        └─deadline exceeded───────> FORCED
```

Only `RUNNING` accepts ingestion. Shutdown is idempotent. `DRAINING`, `TERMINATED`, `FAILED`, and
`FORCED` reject new events and log the rejection.

### Work progression

```text
Accepted Scan: QUEUED -> POSITIONED -> retained for future membership -> RETIRED
Window:        QUEUED_FOR_RANKING -> RANKED -> QUEUED_FOR_PERSISTENCE
Ranked Window: PERSISTING -> COMMITTED
                         └-> RETRY_WAIT -> PERSISTING
```

The persistence worker retains the oldest failed ranked window locally and does not dequeue a
newer result until the older one commits. A scan is retired from the ring only after it can no
longer belong to a future complete window; there is no ordinary drop state.
