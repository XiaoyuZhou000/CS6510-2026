# US3 smoke: subscriber test application and reconciliation (T080)

Feature 007, phase 5 (User Story 3), on branch `feature/notification-service`. Scenario C ran on 2026-11-11; scenarios B and D ran
on 2026-11-11/12 (machine local time, UTC-8), after `NOTIFICATION_DB_PASSWORD` was set and `db/bootstrap.ps1` had run.

| Step | Outcome |
|---|---|
| `subscriber-client/run-tests.sh` | **PASS**: 49 tests, 0 failed (T064–T067, plus `SubscriberThreadTest` and `DecodeSaleEventsTest`) |
| Quickstart scenario C (SC-003) | **PASS**: every injected mismatch fails with exactly its category and key (below) |
| Quickstart scenario D (SC-002, SC-007) | **PASS**, twice: `d-default-1` and `d-default-2` |
| Snapshot comparison (SC-013) | **PASS**: both `subscribers/snapshot.json` files are byte-identical (SHA-256 `cc452a44…c555`) |
| Quickstart scenario B (US2) | **PASS**: `b/` |

## Scenario D: main load test with fixed subscriptions

```powershell
server/benchmark-notification.ps1 -Subscribers 100 -Seed 20261111 -Workload default -RunName us3-smoke/d-default-1 `
    -NginxBin C:\Users\18422\cs6510-tools\nginx-1.31.6\nginx.exe -IdleCpuCores 2.0 `
    -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights
```

`d-default-2` ran with the same arguments and `-RunName us3-smoke/d-default-2`. Both runs used the deployment at `423193b`.

| | d-default-1 | d-default-2 |
|---|---|---|
| Completed sales | 3,911 | 3,917 |
| Sales/s (active window) | 65.1 | 65.2 |
| Owed = delivered = received | 37,446 | 37,414 |
| Redeliveries, charged/uncharged failures, parked | 0 | 0 |
| End-to-end latency p50 / p95 / p99 / max (ms) | 27 / 101 / 159 / 302 | 26 / 92 / 157 / 274 |
| Completion to `DELIVERED` p50 / p95 / p99 (ms) | 49 / 132 / 196 | 48 / 123 / 183 |
| Notifications received/s | 622.5 | 624.1 |
| Max backlog: unpublished sale events / pending notifications / channel lag | 4 / 77 / 92 | 6 / 60 / 94 |
| Drain: final completion to the last first receipt | 0.035 s | 0.023 s |
| Harness drain (two all-zero backlog samples) | 2.1 s | 2.1 s |
| Validator / reconciliation | PASS / PASS, every count 0 | PASS / PASS, every count 0 |

In both runs, `ready.json` was written at subscription version 100 before the Load Client started. The highest history version is
100, and the Load Client's report was moved byte-identical to `load-client-report.json`.

Completion to `DELIVERED` is later than end-to-end delivery. That is expected: `delivered_at` is written once the send is
acknowledged, and those writes are batched, so the subscriber often has the record first.

The raw receipts (`subscribers/receipts/*.jsonl`) and `exports/*.tsv` of the two D runs are about 22 MB per run. They are kept on
the benchmark machine and left out of git (`.gitignore` here); every summary computed from them is committed.

### Two harness bugs found by the first live run

1. **`-DrainSeconds` was overwritten.** PowerShell variable names ignore case, so the harness's local `$drainSeconds = $null`
   assigned the validated `-DrainSeconds` parameter, and the run stopped at once with "0 is not a valid value". The local
   variable is now `$drainElapsedSeconds`.
2. **Every exported time was 8 hours late.** The time columns are `TIMESTAMP(3)`. The services connect with
   `serverTimezone=UTC`, so Connector/J writes each instant's UTC wall clock into a session in the server's own zone (SYSTEM, UTC-8
   here), and reads it back the same way. The services are therefore consistent with themselves. The export, however, used
   `UNIX_TIMESTAMP()`, which converts the stored instant correctly and so came out 8 hours after the instant the service meant.
   Latencies were unaffected, because both ends of each one came from the same source. The drain and the received rate were
   wrong, because they compare a database time with a subscriber's receive time: the first report gave `drainSeconds` 0
   (clamped) and `receivedPerSecond` 0. The export SQL now converts with
   `TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', col) DIV 1000`, as the driver does. The reconciler now also fails
   `saleEvents` when a sale's exported completion time differs from its `SaleCompleted` event's
   (`ReconcileFaultInjectionTest.aSaleEventCompletionTimeThatDiffersFromTheSaleFailsTheSaleEventCheck`). With the old export,
   that check failed all 3,911 sales of `d-default-1`, each by exactly 28,800,000 ms.

   `d-default-1` was re-exported and reconciled with the fixed export. The database still held that run's data, because nothing
   had run since. The first report is kept as `*.pre-tz-fix.*`. `d-default-2` ran with the fix.

The services' own clock handling was not changed: no service SQL compares a JDBC-written time with `NOW()`. The 006 exports that
use `UNIX_TIMESTAMP` were not reviewed here.

## Scenario B: notify once per sold SKU

```powershell
load-client/reports/notification-service/us3-smoke/b/scenario-b.ps1 -NginxBin C:\Users\18422\cs6510-tools\nginx-1.31.6\nginx.exe
```

[`b/scenario-b.ps1`](b/scenario-b.ps1) does the following:
1. Resets the baseline and starts the four services and the gateway.
2. Runs `SubscriberMain --assignments=b.json`, which assigns S1 = {A, B} and S2 = {B}, where A = `SKU-000001`, B = `SKU-000002`,
   and C = `SKU-000003`.
3. Completes 3×A + 1×B + 2×C through the gateway, and then a sale of C alone.
4. Exports the evidence and reconciles in fixed mode (`b/reconciliation.json`: PASS).

| Expected | Observed |
|---|---|
| S1 receives A×3 and B×1 | `S1.jsonl`: `SKU-000001` quantity 3 and `SKU-000002` quantity 1, one notification each |
| S2 receives B×1 | `S2.jsonl`: `SKU-000002` quantity 1 |
| Nobody is notified for C | No receipt or notification row for `SKU-000003` |
| The selection records version 2 | Both `sale_selection` rows have `subscription_version` 2; history has REGISTER S1 (v1) and S2 (v2) |
| A sale of C only selects nobody | That sale's selection has `recipient_count = 0` and `notification_count = 0` |

## Scenario C: reconciliation detects every mismatch

`ReconcileFaultInjectionTest` builds a passing run directory (`SyntheticRun`: 3 subscribers, 3 sales, 4 owed notifications, and
1 redelivery). It checks that the clean copy passes, then applies each injection to a fresh copy. Each injection must make
exactly one category non-zero, and that category's samples must name the affected key.

| Injection | Category | Key |
|---|---|---|
| removed `DELIVERED` row | `missingNotDelivered` | `txn-2/SKU-000003/sub-0003` |
| row left `PENDING` | `missingNotDelivered` | `txn-1/SKU-000002/sub-0002` |
| removed receipt | `missingDeliveredNotReceived` | `txn-1/SKU-000002/sub-0002` |
| extra receipt | `unexpected` | `txn-3/SKU-000004/sub-0002` |
| extra row | `unexpected` | `txn-3/SKU-000004/sub-0003` |
| undecodable receipt | `unexpected` | `receipt:sub-0003@1` |
| wrong row quantity | `quantityMismatch` | `txn-1/SKU-000001/sub-0001` |
| wrong received quantity | `quantityMismatch` | `txn-2/SKU-000003/sub-0003` |
| second row identity for one key | `duplicate` | `txn-1/SKU-000002/sub-0001` |
| second received identity for one key | `duplicate` | `txn-1/SKU-000002/sub-0002` |
| selection at another version | `selectionMismatch` | `txn-2` |
| missing selection of a sale with no recipient | `selectionMismatch` | `txn-3` |
| history row above the ready version | `fixedSubscriptions` | `version:4` |
| history disagreeing with the snapshot | `fixedSubscriptions` | `sub-0003` |
| `PARKED` row | `parked` | `txn-1/SKU-000002/sub-0001` |
| parked event | `parked` | `event:e-bad` |
| missing `SaleCompleted` row | `saleEvents` | `txn-2` |
| second `SaleCompleted` row | `saleEvents` | `txn-1` |
| `SaleCompleted` lines that differ from the sale | `saleEvents` | `txn-3` |
| `SaleCompleted` completion time 8 hours early | `saleEvents` | `txn-2` |

Redeliveries are counted (`totals.redeliveries`) but never fail a run (`ReconcilerTest`).
