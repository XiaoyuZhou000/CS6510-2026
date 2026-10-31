# Quickstart Validation: Event-Based Architecture

Outcomes of `specs/006-event-based-architecture/quickstart.md`. The first section is the final validation (T172): every quickstart section was run against the phase 11 build, each loaded scenario on its own fresh baseline. The sections after it are the earlier per-story runs, kept as recorded.

## Final validation (T172, 2026-10-31)

**Build**: commit `0eaf47e` plus the uncommitted phase 11 documentation, built with `server/build-tests.sh` (which runs `server/build.sh`). The machine and tools match the CP4 runs: Windows 10, JDK 21.0.2, MySQL 8.4.4, Nginx 1.31.6, and native Kafka 4.1.0. The scenarios ran from 05:42 to 05:48 local time, from [drivers/t172-driver.ps1](drivers/t172-driver.ps1). The driver reuses the T139 helpers: it starts each service from the built jars and Nginx from `deploy/nginx.conf`, and sends every request through the gateway on port 8080.

### §1 Build and broker

| Expected | Observed |
|---|---|
| `out/` holds `contracts.jar`, `common.jar`, and the three service jars | `analytics.jar`, `common.jar`, `contracts.jar`, `inventory.jar`, `transaction.jar` |
| `lib/grpc/` no longer exists; `lib/messaging/` holds `kafka-clients-4.1.0.jar` | `lib/grpc` absent; `kafka-clients-4.1.0.jar` present |
| `deploy/kafka/status.ps1` lists four topics with 1, 16, 16, and 1 partitions | `selfcheckout.catalog.v1` 1, `selfcheckout.checkout.v1` 16, `selfcheckout.checkout-outcome.v1` 16, `selfcheckout.scan-accepted.v1` 1 |

### §2 Reset and start

| Expected | Observed |
|---|---|
| The reset prints 2,000 products, 20,000,000 units, and zero transactions, outbox rows, parked events, and topic offsets | `db/reset-baseline.ps1`: 2,000 products, 2,000 stock rows, 20,000,000 units; 0 checkout operations, outbox rows, parked events, transactions, attempts, accepted scans, and stored consumer offsets; Analytics `last_position` 0; every topic present with its partitions and an end offset of 0. It printed "The broker and all three service schemas are at the baseline." |
| Within 5 s, `replica_version` equals Inventory's `catalog_version`, and all three groups are assigned (SC-009) | The replica matched the catalog 2 s after the first service start. First status lines: Transaction `replica_version=a0d4777b-…` with `replica_products=2000`, Inventory `catalog_version=a0d4777b-…`. `status.ps1`: `inventory-checkout`, `transaction-checkout-outcome`, and `analytics-scan-ingest` all `stable` with lag 0. |

### §3 Public contract and §4 automated suites

- **§3**: `SERVER_BASE_URL=http://localhost:8080 ./run-tests.sh contract` against a freshly reset deployment: 25 tests, 0 failed, 0 skipped.
- **§4**: the release suite (T171) passed. 504 tests: 501 passed, 0 failed, and 3 skipped by design. Direct restoration passed 5/5 and is reported separately. The command, the skipped methods, and every passing class are in [final-gate-tests.txt](final-gate-tests.txt). The suite includes the architecture diagram consistency test and the broker-restart, fault, and isolation suites.

### §5 Scenarios

| # | Expected | Observed (T172) |
|---|---|---|
| A | 200 with exact stock reduction; then 409 `INSUFFICIENT_STOCK`, no stock change, basket OPEN | Two `SKU-000001` and one `SKU-000002`: 200 in 68 ms, `itemCount` 3, `totalAmount` 2.90, captured prices 0.85 and 1.20; stock 10000 → 9998 and 10000 → 9999. Two `SKU-000003` with 1 unit in stock (set by the admin account): 409 in 27 ms; stock stayed 1; lookup OPEN with 2 items. Transaction's 2 and Inventory's 2,003 outbox rows (the catalog included) were all published; 0 parked. |
| B | All succeed except Complete, which returns 500 within 10 s; after the broker returns, the attempt settles and every scan is counted once | With the broker stopped: Start 201 in 29 ms; three Scans 200 in 24–31 ms with names and prices; Lookup 200 OPEN 7.10; `/items`, low stock, and popular items 200 in 20–52 ms. Complete 500 in 8.0 s, attempt PENDING with `PENDING_ERROR`, stock unchanged. Transaction killed and restarted with the broker still down: the basket survived with the same start time, and Start and Scan answered. Before the broker returned, 4 of 11 accepted scans were unpublished. After `start-broker.ps1 -NoTopics`: the attempt settled COMPLETED (a background completion) 22.3 s after the start began, stock 10000 → 9998 and 10000 → 9999, and a repeated Complete returned 409. All 11 scans were published and stored once (11 distinct identities); 0 parked. |
| C | Invariants pass after settling; zero lost scans and zero double deductions (SC-005) | Default client for 60 s. The broker was stopped at 20.7 s and back at 33.6 s, then stopped at 40.7 s, kept down for 10 s, and back at 64.7 s (start-up took about 14 s). Client: 1,693 `totalTransactions`; Start and Scan had zero errors (1,727 and 18,125); Complete 1,693 successes and 34 errors (500 during the outages). Within 1.1 s of the broker's return, all work had drained. `validate-invariants.ps1` passed for all three services: 1,727 COMPLETED, 34 background completions (exactly the 34 failed Completes: remaining difference 34, every one accounted for), 0 completions without a recorded response, 0 parked, final lag 0. 18,125 scans accepted, published, and stored as 18,125 distinct identities; 0 transactions with more than one applied deduction; units 20,000,000 → 19,981,875, which is 18,125 completed units. |
| D | Scans succeed; Completes return 500 and settle after restart (SC-006) | With Inventory stopped: Scans 200 in 24 ms; Complete 500 in 8.0 s with the attempt PENDING; `/items` and low stock 502 from the gateway; popular items 200. Inventory restarted: the attempt settled COMPLETED 0.9 s after its start, stock 10000 → 9998 and 10000 → 9999. At the end of A, G, B, and D: 14 scans accepted, published, and stored once; 0 pending, 0 parked; 5 completed transactions with 11 units and 5 applied operations. Units 20,000,000 → 19,989,990 = 20,000,000 − 11 sold − 9,999 removed by hand in A. |
| E | No Scan or Complete errors; windows catch up to the highest boundary (SC-007) | Default client for 60 s. Analytics was killed at 20.2 s (`last_position` 15,460) and restarted at 39 s. The scan topic had reached 30,022 records while Analytics' window stayed at 15,500. Client: 4,625 `totalTransactions`, zero errors on every operation, p99 Start 30.1, Scan 41.2, Complete 124.5 ms. Drained 0.1 s after the client ended. The validator passed with 0 background completions. Analytics stored all 48,059 scans as 48,059 distinct identities, its checkpoint reached 48,000 (the highest eligible boundary), and its stored next offset was 48,059. |
| F | Every attempt settles once; no restoration is triggered (SC-003) | `integration.CheckoutEventFaultTest` (1/1) and `messaging.ConsumerCrashAfterCommitTest` (1/1) passed in the T171 release suite against the live broker. |
| G | Parked in Inventory's `dead_letter_event` within about 1 s, visible in the status line; other partitions keep flowing | `support.PublishRawRecord selfcheckout.checkout.v1 poison-key not-an-event-envelope`: parked 313 ms after the publisher exited, reason `UNDECODABLE`, null `event_id`, `park_count` 1. A checkout right afterwards: 200. Status line: `status service=inventory applied=2 rejected=1 … parked_events=1 … checkout_consumer_lag=0`. The parked row was then removed with the admin account, so the later scenarios started with zero parked events. |
| H | The replica is rebuilt from the compacted topic with no call to Inventory (US4) | After `db/reset-service.ps1 -Service transaction` with Inventory stopped, the replica was empty (version NULL, 0 products). Transaction restarted with Inventory still down and rebuilt the replica within 1.1 s of its process start: version `a0d4777b-…`, Inventory's, with 2,000 products. Log: `Product replica now at catalog version a0d4777b-… (2000 products)`; 0 log lines name gRPC or an Inventory address. Scan `SKU-000007` afterwards: `Item 7`, 2.95. |

**Notes**: the driver's capture of error bodies returned empty text, so the 409 and 500 bodies are not quoted here. Their status codes and the durable state are what the table records. The first two attempts of the driver stopped on a bug in the driver itself, before any scenario outcome: its query helper indexed a single-column result as a string. Those attempts are not used.

### §6 Official evidence and §7 architecture diagram

- **§6**: `cp4-default-1` and `cp4-stress-1` are `comparable=true` with passing validators, zero parked events, and zero final lag. The default run has zero errors and zero background completions (SC-013). See [README.md](README.md).
- **§7**: [docs/architecture/event-based-architecture.md](../../../docs/architecture/event-based-architecture.md) holds the Mermaid flowchart with the gateway, the three services and their ports, the three schemas with their outbox and `dead_letter_event` tables, the broker, and the four topics with producer and consumer-group edges. `architecture.ArchitectureDiagramConsistencyTest` passed 4/4 in the US8 gate and in T171, and the exported SVG is current. The page's rendering on GitHub was not checked, because the branch is not pushed. GitHub renders `mermaid` code blocks natively, and the same source rendered without errors in Mermaid CLI 11.17.0.

## Public contract smoke (T144, US6)

**Run**: 2026-10-30, 16:18–16:25 local time, on the benchmark machine, with the native broker. This is quickstart section 3.

| Check | Expected | Observed |
|---|---|---|
| `git diff 8891c57 -- spec/self-checkout-openapi.yaml load-client/src deploy/nginx.conf` | Empty | Empty |
| The unchanged Load Client for 20 s through the gateway: `benchmark-event-based.ps1 -Workload smoke -OutputLabel us6-smoke-1`, which runs `java -cp out Main --duration=20` after `db/reset-baseline.ps1` | Zero errors (SC-001) | 1,406 Starts, 14,870 Scans, and 1,406 Completes, all with zero errors; validator passed, drained, 0 parked events, 0 final lag (`us6-smoke-1/`; a smoke run is never `comparable`) |
| `SERVER_BASE_URL=http://localhost:8080 ./run-tests.sh contract`, with Inventory, Transaction, and Analytics started from the built jars and Nginx from `deploy/nginx.conf` | Every contract test passes | 25 tests, 0 failed, 0 skipped |

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
