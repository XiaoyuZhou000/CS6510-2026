# Scenario F: duplicates, failures, and restarts (T103, US5, SC-005)

Feature 007, phase 7 (User Story 5), run on 2026-11-12 on branch `feature/notification-service`. The changes under test are
uncommitted on top of `bc85a45` (`deploymentCommit` and `gitDirty` in each `run.json`); they were committed right after
these runs.

| Step | Outcome |
|---|---|
| `server/run-tests.sh integration messaging database unit` (with `KAFKA_TEST_BROKER_CONTROL=1`, `KAFKA_HOME`, `NGINX_BIN`) | **PASS** apart from one known flaky test: 503 tests, 502 successful, 1 failed. The failure is `KafkaTransportLiveTest.sendsCompleteAfterAcksAllAndTheIdempotentProducerKeepsPerKeyOrder` (a `TimeoutException` on a freshly created topic), which also failed this way in 006 and is unrelated to US5 |
| US5 tests in that run | **PASS**: `ChargingRuleTest` (T095), `ParkingAndOperatorRetryTest` (T096), `NotificationBrokerRestartTest` (T097: 50 sales, 39 uncharged failures, 0 charged, 0 parked), `SaleEventParkingLiveTest` (T098), `TransactionRestartSaleEventsTest` (T099), `NotificationCrashMatrixTest` (T094/T102, 9 cases) |
| `subscriber-client/run-tests.sh` | **PASS**: 59 tests, 0 failed (including `SubscriberResumeLiveTest`, T100) |
| Scenario F items 1–5 | **PASS**: reconciliation `--mode=fixed` passes in each |
| Scenario F item 6 | **FAIL by design**: the one malformed record is parked; nothing owed is missing |

## Command

```powershell
load-client/reports/notification-service/fault-matrix/run-fault-matrix.ps1 -NginxBin C:\Users\18422\cs6510-tools\nginx-1.31.6\nginx.exe
load-client/reports/notification-service/fault-matrix/run-fault-matrix.ps1 -NginxBin C:\Users\18422\cs6510-tools\nginx-1.31.6\nginx.exe -Items 1,2,3,4,5
```

[`run-fault-matrix.ps1`](run-fault-matrix.ps1) runs `server/benchmark-notification.ps1 -Workload default -Subscribers 100
-Seed 20261111 -DisruptAfterSeconds 20 -OutageSeconds 30 -DrainSeconds 300` once per item, with the idle gate
(`-IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights`), into `fault-matrix/<item>/`. The
harness's `-Disruption` injects the fault 20 s into the 60 s load and recovers from it before the load loop continues:

| Item | `-Disruption` | What the harness does |
|---|---|---|
| 1 | `notification-selection-crash` | `POST /notifications/ops/faults` `notification.selection.afterCommit:halt` (Notification runs with `NOTIFICATION_TEST_FAULTS=1`); waits for the exit (code 137, `Runtime.halt`); restarts Notification without the fault |
| 2 | `notification-send-crash` | the same with `notification.delivery.afterSend:halt` |
| 3 | `transaction-restart` | stops Transaction (a forced kill, as every stop is on Windows) and starts it again (006 procedure) |
| 4 | `broker-outage` | `deploy/kafka/stop-broker.ps1`, 30 s, `start-broker.ps1 -NoTopics` |
| 5 | `subscriber-outage` | stops `SubscriberMain` through its stop file, 30 s, restarts it with `--resume` |
| 6 | `malformed-sale` | `support.PublishRawRecord` sends one malformed record with key `scenario-f-malformed` to `selfcheckout.sale-completed.v1` |

The quickstart's `:crash` is the `halt` action of `FaultHooks`. The fault steps, with their UTC times, are in each
`run.json` → `disruption.events` and in `<item>.harness.txt`; the logs of a stopped process are kept as
`<service>-std*.crash1.log` / `restart1.log` / `subscriber-std*.resume1.log`.

## Results

| | 1 selection crash | 2 send crash | 3 Transaction restart | 4 broker outage | 5 subscriber outage | 6 malformed sale |
|---|---|---|---|---|---|---|
| Reconciliation | **PASS** | **PASS** | **PASS** | **PASS** | **PASS** | **FAIL** (parked = 1) |
| Completed sales | 3,170 | 3,753 | 2,985 | 1,280 | 3,842 | 3,755 |
| Owed = delivered = received | 30,770 | 35,184 | 28,199 | 12,512 | 36,339 | 35,439 |
| Redeliveries | **15** | **12** | 0 | 0 | 0 | 0 |
| Charged / uncharged failures | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Parked notifications / events | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / **1** |
| Load Client transactions (errors) | 3,170 (0) | 3,753 (0) | 2,975 (20: 1 start, 9 scan, 10 complete) | 1,230 (50 complete) | 3,842 (0) | 3,755 (0) |
| Background completions | 0 | 0 | 10 | 50 | 0 | 0 |
| End-to-end p50 / p95 / max (ms) | 30 / 11,349 / 13,967 | 31 / 8,479 / 11,567 | 26 / 89 / 2,879 | 29 / 147 / 3,667 | 3,006 / 29,922 / 32,944 | 27 / 113 / 791 |
| Drain after the load | 2.1 s | 2.1 s | 2.1 s | 4.1 s | 2.1 s | 2.1 s |
| Validator (stock invariant) | pass | pass | pass | pass | pass | pass |
| Idle gate (background cores) | pass (0.52) | pass (0.38) | pass (0.21) | pass (0.50) | pass (0.36) | pass (0.43) |

"Redeliveries" are extra copies of a notification ID on a channel; the subscriber application deduplicates them, so
each owed notification is received once in effect, and the reconciliation's `duplicate` category stays 0 (`reconciliation.json` → `totals.receipts` counts the copies).

### Against the quickstart's expectations

- **Every run passes except item 6: met.** Item 6's reconciliation fails only on `parked=1` (`event:dead-letter-1`, reason
  malformed). The quickstart expected item 6 to also report "that sale as missing", but the malformed record was
  published by `PublishRawRecord` and is not a sale Transaction completed, so no owed notification is missing:
  owed = delivered = received = 35,439. Notification parked it at the first poll (`consumer.notification-sale-completed.parked=1`)
  and raised the `PARKED_EVENTS` alarm in its status line; the sales after it in the partition were not held up.
- **Redeliveries only in items 2 and 5: not quite.** Item 2 has 12, as expected: sends confirmed by the broker
  whose `DELIVERED` mark the halt lost, resent after the restart. Item 1 also has 15, because `Runtime.halt` stops the
  whole JVM, not only the selection lane: the delivery lanes' in-flight sends die the same way as in item 2.
  The rows the halted selection committed were all delivered after the restart (0 missing). Item 5 has 0, because
  the stop file makes `SubscriberMain` shut down cleanly and commit its position, and `--resume` continues from it;
  the 30 s outage shows instead in item 5's end-to-end latency (p95 29.9 s). A crash of the subscriber, which would
  redeliver, is covered by `SubscriberResumeLiveTest` (T100: at most 10 redeliveries with `max.poll.records=10`).
- **Broker downtime causes uncharged failures and no parked notifications: no parked rows met; no uncharged failures
  in this run.** During the outage no sale could complete: the 50 `COMPLETE_TRANSACTION` calls made then failed at the Load Client
  and those sales completed in the background after the restart (`backgroundCompletions` = 50), so Notification had nothing to send while the
  broker was down. The uncharged path itself is shown by `NotificationBrokerRestartTest` (T097), which delays
  selections into the outage: 39 uncharged failures, 0 charged, 0 parked, every notification on its channel once.

### Notes

- Items 1 and 2: the restarted Notification was ready about 1.1 s after the halt (`disruption.events`), but the
  end-to-end p95 is 8–11 s. This fits the halted member's partitions waiting for its consumer-group session to
  expire before they are reassigned; it was not measured separately. The pending notifications drained, and the
  run drained 2.1 s after the load.
- Item 3: the 20 Load Client errors are the requests in flight at the Transaction kill. The 10 sales whose
  `COMPLETE_TRANSACTION` failed completed in the background (006 behavior), and their notifications were owed and delivered.
- The backlog columns of `backlog.tsv` are not sampled while a disruption step runs (the step blocks the sampling loop
  for up to the 30 s outage), so their maxima do not cover the outage itself.

## Harness fixes found by these runs

The first attempt (`runner.txt`, attempt 1) ran all six items; items 1–5 ran their load, fault, recovery, drain,
validator, and export, then failed writing `run.json`: Windows PowerShell 5.1 throws "Argument types do not match" on
`@(<generic List>)` inside a hashtable literal. The harness now uses `.ToArray()`. Item 6 started after the fix and
is kept. In attempt 2 item 1 failed at the fault request: under load the halt fired before Notification answered
`POST ops/faults`, so the harness now tolerates a lost reply and relies on the halt wait that follows. Attempt 3
re-ran item 1. The failed attempts' folders were discarded; every folder here is from a complete run.

## Files

Per item: `run.json`, `reconcile.txt`, `reconciliation.json`, `notification-report.{json,md}`, `backlog.tsv`,
`load-client-report.json`, `validator.txt`, `kafka-status-final.json`, the service logs, and the subscriber status
files. The receipts (`subscribers/receipts/`) and exported TSVs (`exports/*.tsv`), about 22 MB per run, are kept on
the benchmark machine only ([`.gitignore`](.gitignore)).
