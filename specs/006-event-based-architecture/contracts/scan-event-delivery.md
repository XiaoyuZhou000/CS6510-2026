# Scan Event Delivery and Analytics Ordering Contract

**Requirements**: FR-026, FR-047–FR-055 | **Decisions**: D5, D6, D10 | **Events**: [event-catalog.md](event-catalog.md) | **Schema**: [scan_events.proto](proto/scan_events.proto)

## End-to-end flow

```text
Scan (Transaction local tx: guarded UPDATE transaction + INSERT accepted_scan, published_at NULL)
          │ (asynchronous; Scan never waits for the relay or the broker)
ScanEventRelay ──batch of ≤ 500 ScanAccepted──▶ Kafka selfcheckout.scan-accepted.v1 (1 partition)
          ◀── acks=all confirmations ──┘
relay sets published_at (one batched UPDATE + commit)
                                   ScanAcceptedConsumer (Analytics, group analytics-scan-ingest, 1 thread)
                                   ingestion tx: sequence lock, dedup, multi-row INSERT with positions,
                                                 name upserts, scan_sequence UPDATE, consumer_offset UPSERT, commit
                                   then commitAsync(offset)
                                   Poller ─▶ Window ─▶ Ranking ─▶ Persistence (unchanged stages)
```

## 1. Accepted-scan write (Transaction)

Unchanged from 005: one guarded UPDATE and one INSERT, then the commit, before HTTP 200 (FR-026). There is no read, no remote call, and no broker involvement on the warm path (FR-015). The event identity is `(transaction_id, scan_sequence)`, and the envelope's `event_id` is `scan:<transaction_id>:<scan_sequence>`.

## 2. Relay (Transaction)

| Rule | Value |
|---|---|
| Selection | Up to 500 rows (`OUTBOX_BATCH_SIZE`) with `published_at IS NULL`, ordered by `scan_id` |
| Send | `KafkaScanEventSink` sends every row as one record (key `transaction_id`) with `linger.ms=5`, then waits for all confirmations |
| All confirmed | One `UPDATE accepted_scan SET published_at=? WHERE scan_id IN (…)`, then commit; select the next batch at once |
| Any send fails | `Uncertain`: back off from 100 ms to 5 s; the same rows are reselected and re-sent |
| Idle | Poll every 50 ms |
| Concurrency | One relay thread per Transaction process; per-batch sends preserve `scan_id` order on the single partition |
| Restart | Resumes from unpublished rows. A crash between confirmation and marking re-sends records that Analytics deduplicates. |

## 3. Ingestion with position assignment (Analytics)

`ScanAcceptedConsumer` polls up to 500 records, validates each envelope (D6), and passes the decoded events to `ScanIngestionService`. The ingestion transaction is 005's, plus one statement:
1. Lock the `scan_sequence` row.
2. Run the duplicate SELECT on `(source_transaction_id, source_scan_sequence)`.
3. Run the multi-row INSERT with consecutive positions.
4. Upsert `product_name` only where the name map differs.
5. UPDATE `scan_sequence`.
6. **UPSERT `consumer_offset(topic, partition, next_offset = last record offset + 1)`**.
7. Commit.

Only then does the consumer call `commitAsync`. That is acknowledgement after commit (FR-011).

| Situation | Behavior |
|---|---|
| Partition assigned (startup, rebalance) | Seek to `consumer_offset.next_offset`, or to offset 0 if no row exists |
| Stored offset beyond the log end (the topic was recreated without an Analytics reset) | Log an alarm and seek to the earliest offset; deduplication absorbs any overlap |
| Crash after the commit, before `commitAsync` | Redelivered from the broker's committed offset, or from the database's on reassignment; duplicates are filtered |
| Database unavailable | Transient: pause, retry with backoff, commit nothing (D6) |
| Undecodable or invalid record | Parked in Analytics' `dead_letter_event` after five attempts. Its offset is included in the next stored `next_offset`, so it never blocks the partition. |

## 4. Ordering and positions (FR-050)

| Property | Guarantee |
|---|---|
| Ordering rule | Analytics' durable first-acceptance order is the offset order of the single partition, which is Transaction's publication order. It need not match commit order, acceptance time, or any order across Transaction processes. |
| Unique, gapless, immutable | Unchanged from 005 R11: assigned under the singleton lock in the ingestion transaction, and rows are never updated |
| Duplicates | Filtered before position assignment, so a duplicate never holds a second position |
| Late events | Receive the next position when they arrive |

## 5. Windows, ranking, publication (FR-051–FR-054)

Unchanged from 005, which itself is unchanged from feature 004.

## 6. Visibility delay (FR-055, SC-008)

| Measure | Target |
|---|---|
| Healthy default workload: from the commit of a boundary scan to its window's publication | Under 5 s at p99 (relay poll 50 ms, broker hop, consumer fetch at most 50 ms, poller 100 ms) |
| After load stops, either benchmark workload | The highest eligible boundary is published within 60 s |
| During an Analytics or broker outage | Lag appears only as an older published window; Scan and Complete are unaffected |

Measurement: `analytics.window.publishLag` (005), plus `analytics.consumer.lag` (record timestamp to ingestion commit) and the harness's sampled consumer-group lag.

## 7. Failure behavior

| Condition | Behavior |
|---|---|
| Broker down | Relay sends fail; unpublished rows accumulate durably; Scan and Complete pre-checks continue; Analytics keeps serving its latest window. On return, the backlog is published and ingested in order. |
| Broker restarts | Confirmed records survive in the log. Unconfirmed ones are re-sent from `accepted_scan`. |
| Analytics down | Records wait in the topic (7-day retention). After restart, Analytics resumes from `consumer_offset`. |
| Transaction down | No new records; Analytics drains what is in the topic |
| Broker's committed offsets lost or reset | Analytics resumes from `consumer_offset` (FR-049) |

## 8. Test-only fault hooks

| Hook | Location |
|---|---|
| `relay.afterConfirm` | After all confirmations, before `published_at` commits |
| `analytics.ingest.received` | After a poll returns, before the ingestion transaction begins |
| `analytics.ingest.beforeCommit`, `analytics.ingest.afterCommit` | Around the ingestion commit; `afterCommit` runs before `commitAsync` |
