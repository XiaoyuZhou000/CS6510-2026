# Scenario E: subscription changes in their own scenario (T093, US4, SC-004)

Feature 007, phase 6 (User Story 4), run on 2026-11-12 on branch `feature/notification-service`. The changes under test are
uncommitted on top of `cac4cfb` (`deploymentCommit` in each `scenario-e.json`); they were committed right after these runs.

| Step | Outcome |
|---|---|
| `server/run-tests.sh architecture unit database contract integration` | **PASS** after one fix: 540 tests, 502 successful, 1 failed, 12 aborted. The failure was `LayerDependencyTest`'s false positive on `"DELETE"` (below); after the fix `run-tests.sh architecture` passes, 40 successful and 1 aborted. The aborted ones are the live-server and opt-in suites (`SERVER_BASE_URL`, `PARITY_GATE`, `RUN_RESET_BASELINE_TEST` unset) |
| `subscriber-client/run-tests.sh` | **PASS**: 58 tests, 0 failed (including `HistoryReconcileTest`, T086) |
| Scenario E alone (`e-alone`) | **PASS**: `ReconcileMain --mode=history`, every category 0 |
| Scenario E with a parallel default Load Client (`e-with-load`) | **PASS**: `ReconcileMain --mode=history`, every category 0 |

## Command

```powershell
load-client/reports/notification-service/change-scenario/scenario-e.ps1 -RunName e-alone -NginxBin C:\Users\18422\cs6510-tools\nginx-1.31.6\nginx.exe
load-client/reports/notification-service/change-scenario/scenario-e.ps1 -RunName e-with-load -WithLoad -NginxBin C:\Users\18422\cs6510-tools\nginx-1.31.6\nginx.exe
```

The four `*_DB_PASSWORD` variables and `KAFKA_HOME` must be in the shell. [`scenario-e.ps1`](scenario-e.ps1) runs these steps:
1. Resets the baseline, then starts the four services (Notification with `NOTIFICATION_TEST_FAULTS=1`) and the gateway.
2. Runs `ChangeScenarioMain --seed=20261112 --subscribers=10 --faults-endpoint=http://127.0.0.1:8084/notifications/ops/faults`
   (plus `--with-load`). The driver's output goes to `<run>/subscribers/`.
3. With `-WithLoad`: starts the unchanged default Load Client (`java -cp out Main`) once the driver waits for
   `load.started`, and writes `load.done` when the client exits. The client's report is moved, unchanged, to
   `load-client-report.json`.
4. Restarts Notification and compares `indexVersion` with `subscriptionVersion` (`scenario-e.json` → `afterRestart`).
5. Exports the evidence and runs `ReconcileMain --mode=history --report-out=notification-report.json`.

## What the driver does (`ChangeScenarioMain`, research N12)

The same seed gives the same sequence of changes in both runs: the same 75 change records and 39 history versions.

| Step | Requests | Answers |
|---|---|---|
| Setup | register `chg-0001`..`chg-0010`, three SKUs each from a seeded pool of 12 | 10 × 201 |
| Confirmed-before cases | register `chg-new`, add, remove, cancel `chg-0002`, re-register `chg-0002`, each followed by a sale of the affected SKU | 201, 200, 200, 200, 201 |
| Selected before the cancel | stall `chg-0002`'s channel, sell an SKU it holds, wait for the selection, cancel, clear the stall | see below |
| Burst | 50 seeded random registers, updates, and cancels, with the driver's own sales in parallel (and the Load Client in `e-with-load`) | 31 × 200/201, 19 × 409 (exists, cancelled, or last SKU) |
| Failed changes | with `notification.subscription.beforeCommit:throw` set through `POST /notifications/ops/faults`: register `chg-fail-1`, update and cancel `chg-guard` | 3 × 500 |
| Rejected requests (FR-006) | no SKU, unknown SKU, an active subscriber with another set, an unknown subscriber, the last SKU, an SKU in both lists | 400, 400, 409, 404, 409, 400 |

## Results

| | e-alone | e-with-load |
|---|---|---|
| Completed sales | 63 (all by the driver) | 3,953 (3,902 by the Load Client, 51 by the driver) |
| Distinct selection versions | 27 | 27 |
| Selections at a version below 38 | 58 | 146 |
| History versions | 1..39, no gap | 1..39, no gap |
| Owed = delivered = received | 212 | 334 |
| Redeliveries, parked | 0, 0 | 0, 0 |
| Index version = clock after the restart | 39 = 39 | 39 = 39 |
| Load Client exit code | – | 0 |

Each check of SC-004 holds in both runs (`reconciliation.json`, every category 0):
- **Every selection equals the history replayed to its version.** No `selectionMismatch`: each sale's notification rows are
  exactly the subscribers holding each sold SKU at that version, and `recipient_count` and `notification_count` match.
- **Every sale's version is at least the version confirmed before its Complete.** No `history` finding of the form `causality:`,
  for the 63 and 51 sales the driver recorded with the highest version confirmed before it sent Complete.
- **Every confirmed change is in the history at its version**, and the versions are contiguous.
- **Failed changes appear nowhere.** The guarded subscribers `chg-fail-1`, `chg-rej-1`, and `chg-rej-2` have no history row, and
  `chg-guard` has only its registration.
- **Rejected requests return their documented codes**: no unexpected answer, so the driver exits 0.
- **After a Notification restart, the index version equals the clock** (39).
- **A notification selected before a cancellation is still received.** In both runs the sale was selected at v13 with
  `chg-0002` as a recipient, the cancel committed at v14, and the stalled send was delivered after the cancel, with one charged
  failure, and received.

## Found while running

- **The first version of the "selected before the cancel" step did not test that case.** Complete returns before Notification
  selects the sale, so the cancel committed first, and the selection correctly left `chg-0002` out. Reconciliation passed, but
  the case was not exercised. The driver now waits for `salesSelected` to advance and stalls the channel through the faults
  endpoint. Both runs were repeated with that driver; these folders hold the repeats.
- **`LayerDependencyTest`'s SQL detector flagged the HTTP method literal `"DELETE"`** in `NotificationHttpHandler`. The pattern
  now ignores a literal that holds only the keyword; every SQL statement still matches.

The raw Transaction exports of `e-with-load` (`sales.tsv` and `sale-events.tsv`, about 4 MB) stay on the benchmark machine
(`.gitignore` here); `selections.tsv`, `notifications.tsv`, and `history.tsv` are committed.
