# Scan Event Delivery and Analytics Ordering Contract

**Requirements**: FR-019, FR-040–FR-049 | **Decisions**: R10–R12, R18, R20, R22 | **Data**: [data-model.md](../data-model.md) | **RPC**: [grpc-internal-contracts.md](grpc-internal-contracts.md#analyticsingestservice)

## End-to-end flow

```text
Scan (Transaction local tx: guarded UPDATE transaction + INSERT accepted_scan, delivered_at NULL)
          │ (asynchronous; Scan never waits)
Outbox relay (1 thread) ──DeliverScanEvents──▶ Analytics ingestion (local tx: sequence lock, dedup,
          ◀──── OK (stored, duplicates) ────────┘   multi-row INSERT with positions, sequence UPDATE)
relay sets delivered_at (one batched UPDATE)
                                               Poller ─▶ Window ─▶ Ranking ─▶ Persistence (unchanged stages)
                                               Window + ranks + checkpoint commit atomically
```

## 1. Accepted-scan write (Transaction Service)

- **Atomicity**: each successful Scan runs one local transaction (R10, R19) and commits it before HTTP 200. A rejected Scan writes nothing (FR-019, FR-040). By design, the transaction contains 2 statements and a commit, which is the R22 budget:
  1. the guarded UPDATE of `transaction`, which sets `item_count` and `total_amount`;
  2. one INSERT into `accepted_scan`.
- **Event identity**: `(transaction_id, scan_sequence)`, where `scan_sequence` is the new `item_count` and is protected by the guard.
- **Captured values**: the row carries the SKU and the line's captured name and price, from the cache.
- **No separate basket write**: these rows are the basket record.
- **No remote call and no read** on the warm path.

## 2. Relay (Transaction Service)

| Rule | Value |
|---|---|
| Selection | Up to 500 rows (`OUTBOX_BATCH_SIZE`) with `delivered_at IS NULL`, ordered by `scan_id` (index `(delivered_at, scan_id)`) |
| Call | `DeliverScanEvents` with a 5 s deadline |
| On `OK` | One `UPDATE accepted_scan SET delivered_at=? WHERE scan_id IN (…)` over exactly the rows sent, then commit (1 SELECT + 1 UPDATE + 1 commit per batch, R22). Then select the next batch immediately. |
| On any other status | Back off from 100 ms to 5 s; the same rows are reselected and re-sent |
| Idle | Poll every 50 ms |
| Concurrency | One relay thread per Transaction process |
| Restart | Resumes from rows still undelivered; no cursor is kept, so late-committed rows are never skipped |

If Transaction crashes after Analytics acknowledges but before `delivered_at` commits, those events are redelivered and Analytics ignores them as duplicates. Rows are retained: they are the basket and audit record.

## 3. Ingestion with position assignment (Analytics Service; R11)

One local transaction per batch, at READ COMMITTED. Its design budget (R22) is at most 5 statements and 1 commit, regardless of batch size:
1. `SELECT last_position FROM scan_sequence WHERE sequence_key=1 FOR UPDATE`. This serializes ingestion batches.
2. One tuple-IN SELECT on the unique key `(source_transaction_id, source_scan_sequence)` finds which identities of the batch are already stored.
3. Duplicates are dropped, including repeats inside the batch. The new events are assigned `last_position + 1 …` in batch order and stored with one multi-row INSERT into `accepted_scan`.
4. `product_name` is upserted only for SKUs whose name is absent from, or differs in, the in-memory name map. After warm-up, no name writes occur (R12).
5. `UPDATE scan_sequence SET last_position=?`, then commit. The response returns `stored` and `duplicates`.

**Acknowledgement after commit**: the gRPC `OK` is sent only after the step 5 commit has returned successfully. By then, the positions and the deduplication keys of every event in the batch are durable. Nothing in the response path runs before that commit.

**Failure paths**:
- A failure before commit leaves no rows and no position change, and returns `UNAVAILABLE`; the relay retries.
- A commit whose outcome is unknown also returns `UNAVAILABLE`. If that commit did succeed, the retry finds every event already stored and reports the batch as duplicates, so positions are never reassigned.
- A crash anywhere between receiving the batch and sending `OK` leads to the same redelivery.

An end-to-end crash test covers each hook in §8 (FR-041–FR-043).

## 4. Ordering and positions (FR-043)

| Property | Guarantee |
|---|---|
| Unique | `position` is the primary key |
| Gapless | Positions are consecutive within each batch under the singleton lock, and `scan_sequence.last_position` advances in the same commit |
| Immutable | Rows are only inserted, never updated |
| Deterministic | Once committed, every replay and restart derives identical windows from the stored positions |
| Ordering rule | Analytics' durable first-acceptance order: batch order, with batches serialized by the sequence lock. It need not match Transaction's commit order, acceptance time, or delivery order. |
| Duplicates | A redelivered event is filtered before position assignment, so it never receives a second position |
| Out-of-order or late delivery | A late event receives the next position when it arrives, so no gap appears |

The former separate SQL Materializer step, and its NULL-position pending state, is removed. Its algorithm (lock the singleton, assign consecutive positions, advance the checkpoint in the same commit) is now part of ingestion.

## 5. Windows, ranking, and publication (FR-044–FR-047)

Unchanged from feature 004:
- `AcceptedScanPoller` reads contiguous positions after the window checkpoint, in batches of 256.
- `WindowFilter` keeps the 1,000-position ring in memory. Frequencies are derived from the ring at each boundary, which needs no database read.
- `RankingFilter` and `PersistenceFilter` exchange immutable messages through bounded FIFO queues.
- Window boundaries are 1,000, then every 500 positions. Ranks are ordered by count descending, then SKU ascending, with at most ten.
- The window header, its ranks, and the `analytics_checkpoint` advance commit together before any read can see them.
- `GET /analytics/popular-items` returns the latest published window, or the established empty response, with names from `product_name`.

## 6. Visibility delay (FR-049, SC-007)

| Measure | Target |
|---|---|
| Healthy default workload: time from the commit of the scan that completes a boundary to that window's publication | Under 5 s at p99 (relay idle poll 50 ms; poller idle poll 100 ms) |
| After load stops, either benchmark workload | Published window reaches the highest eligible boundary within 60 s |
| During an Analytics outage | Lag appears only as an older published window; Scan and Complete are unaffected |

Measurement: the Analytics metrics record publication time against the boundary event's `accepted_at`, which the event carries. The harness samples backlog every 2 s (R21). The relay and the Analytics poller count their empty polls (`transaction.relay.idlePolls`, `analytics.poller.idlePolls`), so checkpoint CP2 can show whether idle reads matter. Signal-driven wake-up remains a deferred option (R23).

## 7. Failure behavior

| Condition | Behavior |
|---|---|
| Analytics down | Relay backs off. Undelivered rows accumulate durably. Scan and Complete continue. After restart, the backlog is ingested in batches and windows are published in order (FR-041). |
| Transaction down | No new events. Analytics keeps processing what it stored and serving its latest window. |
| Analytics crash after ingestion commit, before reply | Relay retries; duplicates are filtered; events are counted once (FR-042) |
| Analytics crash during computation or persistence | Recovers from `analytics_checkpoint` and the 1,000-position overlap prewarm, as in feature 004 (FR-048). Positions are already durable. |
| Duplicate batch, or a batch overlapping an earlier one | `stored + duplicates = batch size`; counts and positions are unchanged |
| Malformed event (`INVALID_ARGUMENT`) | Should not occur because Scan validates first. The relay alarms and retries; it never skips, so nothing is lost. |

## 8. Test-only fault hooks (R15)

| Hook | Location |
|---|---|
| `relay.afterAck` | After `OK` is received, before `delivered_at` commits |
| `analytics.ingest.received` | After a batch arrives and passes validation, before the ingestion transaction begins |
| `analytics.ingest.beforeCommit` and `analytics.ingest.afterCommit` | Around the ingestion commit. `afterCommit` runs after the commit and before the reply. |
| `analytics.persist.afterCommit` | After a window, ranks, and checkpoint commit |
