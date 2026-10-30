# Quickstart Validation: Event-Based Architecture

Outcomes of the scenarios in `specs/006-event-based-architecture/quickstart.md` §5, recorded as each user story's tasks run them. Scenarios not listed yet belong to later stories.

## Scenario A: Saga success and insufficient stock (T078, US1)

**Run**: 2026-10-29, on the benchmark machine. The broker ran natively (Kafka 4.1.0, flush at the broker default). `db/reset-baseline.ps1` reset the schemas and topics. Then Inventory, Transaction, and Analytics started from the built jars, and Nginx from `deploy/nginx.conf`. Every request below went through the gateway on port 8080.

| Step | Expected | Observed |
|---|---|---|
| Complete a basket of two `SKU-000001` and one `SKU-000002` | 200 with the receipt; stock reduced exactly | 200, `itemCount` 3, `totalAmount` 2.90; stock 10000 → 9998 and 10000 → 9999 |
| Complete two units of `SKU-000003` with one unit in stock | 409 `INSUFFICIENT_STOCK`, no stock change, basket OPEN | 409; stock stayed 1; lookup `OPEN` |
| Outbox rows afterwards | Every row has `published_at` | Transaction 2 rows, Inventory 2 rows, 0 unpublished in either; 0 parked |
| Transaction's log | No checkout RPC | No `applyCheckout` |

## Scenario B: broker outage (T139, US2)

**Run**: 2026-10-30, 15:13 local time, on the benchmark machine, with the native broker. `db/reset-baseline.ps1` reset the schemas and topics. Inventory, Transaction, and Analytics then started from the built jars, and Nginx from `deploy/nginx.conf`. Every request below went through the gateway on port 8080. A warm-up Complete with the broker up returned 200.

| Step | Expected | Observed |
|---|---|---|
| `deploy/kafka/stop-broker.ps1` | Port 19092 closed | Closed |
| Start, then Scan `SKU-000003`, `SKU-000004`, `SKU-000003` | 201 and 200s with the right names and prices, without waiting on the broker | 201 in 24 ms; 200s in 18–19 ms: `Item 3` 1.55, `Item 4` 1.90, total 5.00 |
| Lookup | OPEN with the same count and total | 200 in 14 ms: OPEN, 3 items, 5.00 |
| `/items`, `/inventory/low-stock`, `/analytics/popular-items` | 200 | 200 in 69, 17, and 15 ms |
| Complete the basket | 500 within 10 s, attempt PENDING | 500 in 8.0 s; attempt PENDING with `client_outcome = PENDING_ERROR`; stock unchanged |
| Kill Transaction and restart it, broker still down | It starts and serves; the basket survives | Started; Lookup 200 in 12 ms, same status, count, total, and start time; Scan, Start, and Scan 200/201 in 17–21 ms |
| Before the broker returns | Accepted scans wait unpublished | 8 accepted, 6 unpublished (the 2 warm-up scans were published before the stop) |
| `start-broker.ps1 -NoTopics` | The attempt settles with no manual step; every scan counted once | Attempt COMPLETED (`PENDING_ERROR`, a background completion) and the transaction COMPLETED; `SKU-000003` 10000 → 9998, `SKU-000004` 10000 → 9999. A repeated Complete returned 409. Every scan was published, and Analytics stored all 8 accepted scans as 8 distinct identities. 0 parked events |

The settlement time was not measured. The scenario script hung on the broker's inherited output handle after `start-broker.ps1` had started the broker. The checks above were taken when the run resumed at 15:59, against the same deployment.

## Scenario D: Inventory outage (T139, US2)

**Run**: the deployment of scenario B, right after it, with the broker up.

| Step | Expected | Observed |
|---|---|---|
| Stop Inventory; Start and Scan `SKU-000008`, `SKU-000009`, `SKU-000008` | Scans succeed | 200s in 10–12 ms |
| Complete | 500 within 10 s; no stock change | 500 in 8.0 s; attempt PENDING with `PENDING_ERROR`; stock still 10000 and 10000 |
| `/items` and `/inventory/low-stock` while Inventory is down | Only Inventory's routes fail | 502 from the gateway |
| Restart Inventory | The attempt settles (SC-006) | COMPLETED 3 s after the start; `SKU-000008` 10000 → 9998, `SKU-000009` 10000 → 9999 |
| End of both scenarios | Every scan counted once; stock conserved | 11 accepted scans, 0 unpublished, 11 stored by Analytics as 11 distinct identities; 0 parked; 0 pending attempts; total units 19,999,992 = 20,000,000 − 8 completed units |

## Scenario E: Analytics outage (T129, US5)

**Run**: 2026-10-30, 03:13–03:15 local time, on the benchmark machine, with the native broker. `db/reset-baseline.ps1` reset the schemas and topics. Inventory, Transaction, and Analytics then started from the built jars, and Nginx from `deploy/nginx.conf`. The unchanged Load Client ran its default workload (`java -cp out Main`, 10 stations, 60 s) through the gateway on port 8080. Scans now reach Analytics only as `ScanAccepted` records on `selfcheckout.scan-accepted.v1`.

| Step | Expected | Observed |
|---|---|---|
| Analytics killed 20.1 s into the run (a forced stop) | Scan and Complete keep working | Analytics had stored 15,032 positions, with window 15,000 published |
| 15 s with Analytics down | Scans keep being published; Analytics' windows stay at the last one | The topic grew to 28,238 records; Analytics stayed at window 15,000 |
| Analytics restarted 35.9 s into the run | It resumes from `consumer_offset` and catches up | It resumed from offset 15,065 |
| End of the run | No Scan or Complete errors | 4,501 Starts, 47,146 Scans, and 4,501 Completes, all with zero errors; p99 30.1, 40.8, and 113.0 ms |
| After the run | Windows catch up to the highest boundary (SC-007) | Within 0.1 s of the client's end: every scan published, 47,146 positions for 47,146 distinct identities, checkpoint at 47,000 (the highest eligible boundary), zero parked events, stored next offset 47,146 |

## Scenario F: fault hooks (T099, US3)

`integration/CheckoutEventFaultTest` and `messaging/ConsumerCrashAfterCommitTest` passed against the live broker with service processes (2026-10-29).

- **`CheckoutEventFaultTest`**: halted a service at ten Saga hooks and restarted it.
  - Transaction: `checkout.afterBegin`, `outbox.beforeSend`, `outbox.afterConfirm`, `consumer.afterHandlerCommit`, `checkout.outcome.beforeCommit`, `checkout.afterRecordCompletion`.
  - Inventory: `outbox.beforeSend`, `inventory.apply.afterCommit`, `outbox.afterConfirm`, `consumer.afterHandlerCommit`.

  Every attempt settled COMPLETED once, with one deduction and one APPLIED operation. No stock was restored or blocked, no attempt was left pending, and no event was parked.
- **`ConsumerCrashAfterCommitTest`**: Inventory's and then Transaction's consumer halted between the handler's commit and the offset commit. After the restart, the redelivered request deducted nothing, Inventory re-sent its recorded outcome under the same `event_id`, and the redelivered `StockDeducted` was a no-op.

## Scenario G: poison event (T099, US3)

**Run**: the deployment of scenario A. `support.PublishRawRecord selfcheckout.checkout.v1 poison-key not-an-event-envelope` was published directly to the broker.

| Expected | Observed |
|---|---|
| Parked in Inventory's `dead_letter_event` within about 1 s | Parked after 861 ms: reason `UNDECODABLE`, null `event_id`, topic `selfcheckout.checkout.v1`, `park_count` 1 |
| Other partitions keep flowing | A checkout made right afterwards completed with 200 |
| Visible in the status line | `status service=inventory applied=2 rejected=1 restored=0 blocked=0 unpublished_outbox=0 parked_events=1 checkout_consumer_lag=0` |

## Scenario H: catalog replay (T114, US4)

**Run**: 2026-10-30, on the benchmark machine, with the native broker. `db/reset-baseline.ps1` reset the schemas and topics. Inventory and Transaction then started from the built jars. Both were stopped, `db/reset-service.ps1 -Service transaction` reset Transaction only, and Transaction was restarted with Inventory still stopped.

| Step | Expected | Observed |
|---|---|---|
| Inventory startup | The whole catalog is recorded in its outbox and published | 2,001 outbox rows (2,000 `CatalogItemPublished` and the marker), all published; `catalog_meta.published_at` set |
| First Transaction start | The replica reaches Inventory's `catalog_version` | Version `23425b7e-…`, 2,000 products |
| Transaction reset, Inventory stopped | The replica is empty | `catalog_version` NULL, 0 products |
| Transaction restart | The replica is rebuilt from the compacted topic with no call to Inventory | Same version and 2,000 products within 1.6 s of the process start, with Inventory down; log line `Product replica now at catalog version 23425b7e-… (2000 products)` and no gRPC or Inventory address |
| Scan afterwards | Priced from the rebuilt replica | `SKU-000007`: `Item 7`, 2.95 |

## Direct restoration (SC-016)

`messaging.RestorationEventDirectTest` runs alone against Inventory Service over the live broker. Its result is reported apart from the end-to-end Saga results, because no production path produces `StockRestorationRequested` (FR-039, D8).

- **Result**: 5 tests, 0 failed, 0 skipped. This was the same in the direct-restoration section of the full `run-tests.sh` runs on 2026-10-29.
- **What it covers**:
  - An applied attempt is restored once; a redelivered request re-sends its recorded `StockRestored`, and a later request is `ALREADY_RESTORED`.
  - Ten concurrent requests restore once.
  - A rejected attempt has nothing to restore.
  - An absent attempt is blocked, and its delayed `CheckoutRequested` gets `StockDeductionRefused` with no deduction.
  - An Inventory halt at `inventory.restore.afterCommit` is redelivered after the restart and restores nothing twice.
