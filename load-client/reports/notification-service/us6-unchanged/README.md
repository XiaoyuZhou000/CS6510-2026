# Scenario G: checkout, Analytics, and the Load Client unchanged by Notification (T104–T109, US6, SC-008, SC-009)

Feature 007, phase 8 (User Story 6), run on 2026-11-12 and 2026-11-13 on branch `feature/notification-service`. The
changes under test are uncommitted on top of `4ee0f4f` (`deploymentCommit` and `gitDirty` in `run.json`); they were
committed right after these runs. Phase 8 changes no production code: only tests, test support, the harness, and the
coverage ledger.

| Step | Outcome |
|---|---|
| T104 `NotificationOutageCheckoutTest` | **PASS**. A fixed script of starts, scans, and completions gives the same normalized responses, commits, and rows (`transaction`, `accepted_scan`, `checkout_attempt`, `outbox_event`, `SaleCompleted`, stock) without Notification and with Notification crashed for the whole script; after the restart each sale is selected once. With `notification.selection.beforeCommit:delay:2000`, 10 sales complete with a median of 20 ms (healthy: 25 ms), all under 2,000 ms, nothing left unpublished, and the 23 notifications drain |
| T105 `AnalyticsUnaffectedByNotificationsTest` | **PASS**. 320 seeded sales (1,600 scans) through the real Transaction, scan relay, and Analytics publish the windows ending at 1,000 and 1,500 with identical ranks with and without Notification at 100 subscribers, and both match an independent recomputation |
| T106 `ClientAndContractUnchangedTest` | **PASS**. Every file under `load-client/src/` has the same git blob hash as at `4a91686`, the file set is the same, `spec/self-checkout-openapi.yaml` is identical, and `deploy/nginx.conf` equals the `4a91686` file once the added `notification` upstream and `/notifications/` location are removed (each occurs once) |
| T107 `GatewayServiceOutageTest` (extended) | **PASS**. With Notification serving, `/notifications/ops/status` answers 200 through the gateway; once it is closed, four `/notifications/**` requests get 502 with the same body as a stopped Analytics, and the existing routes still answer from their owners |
| T108 `server/tests/COVERAGE-LEDGER.md` | Section "Feature 007 (Notification service)" added. T104–T107 revealed no regression; no production code changed |
| T109 full strict gate | **PASS** apart from one known flaky test (below) |
| T109 scenario G | **PASS**: 0 Load Client errors, validator PASS, reconciliation `--mode=fixed` PASS |

## Full strict gate (T109)

`server/run-tests.sh` with no selector, `STRICT_LIVE_GATES=1`, `KAFKA_TEST_BOOTSTRAP=127.0.0.1:19092`,
`KAFKA_TEST_BROKER_CONTROL=1`, `NGINX_BIN`, `KAFKA_HOME`, and `SERVER_BASE_URL=http://localhost:8080`. As in 006 (T171),
the live suites need a running deployment: the baseline was reset, the four services were started from their jars, and
Nginx from `deploy/nginx.conf`, and all of it was stopped after the run.

```text
[       643 tests found           ]
[         3 tests aborted         ]
[       639 tests successful      ]
[         1 tests failed          ]
Summary (excluding direct restoration): 638 tests, 1 failed, 3 skipped
Direct restoration (SC-016) [messaging.RestorationEventDirectTest]: 5 tests, 0 failed, 0 skipped
```

- No live suite was skipped, so the strict gate itself held. The three aborted tests are skipped by design: two
  destructive `ResetBaselineTest` cases (need `-DRUN_RESET_BASELINE_TEST=true`) and `parityGateHasNoPendingOrUnverifiedRows`
  (needs `PARITY_GATE`).
- The one failure is `messaging.KafkaTransportLiveTest.sendsCompleteAfterAcksAllAndTheIdempotentProducerKeepsPerKeyOrder`
  (`TimeoutException` on a freshly created topic). It failed the same way in 006 and in phase 7, is unrelated to US6, and
  passed 5 of 5 times when re-run on its own. The run's exit code was 1 because of it.

## Scenario G

```powershell
load-client/reports/notification-service/us6-unchanged/run-scenario-g.ps1 -NginxBin C:\Users\18422\cs6510-tools\nginx-1.31.6\nginx.exe -IdleCpuCores 64
```

[`run-scenario-g.ps1`](run-scenario-g.ps1) runs `server/benchmark-notification.ps1 -Workload default -Subscribers 100
-Seed 20261111 -Disruption notification-stopped -DrainSeconds 300` into `g-notification-stopped/`. The new disruption
kind stops Notification once the 100 subscribers have registered through it (they need it to register), runs the whole
Load Client with Notification down, starts Notification again when the Load Client exits, and then drains, exports the
evidence, and reconciles.

**Idle gate, this run only.** The usual gate is `-IdleCpuCores 2.0`. The first launch waited from 23:20 to 02:09
because a game kept background CPU above 2 cores. Scenario G checks correctness, not speed, so it was run once with the
CPU limit lifted (`-IdleCpuCores 64`); the blocking-process list and the port check still applied. `run.json` records
3.03 background cores during the run (`BioShockInfinite=2.64`). The latencies below are therefore not comparable with
other runs, and none of the pass criteria depend on them.

### Timeline (`run.json` disruption events, `backlog.tsv`)

| Time (UTC) | Event |
|---|---|
| 10:11:02.8 | backlog `start` sample |
| 10:11:03.2 | Notification stopped (pid 44512); its logs up to here are `notification-*.before-load.log` |
| 10:11:08.3–10:12:13.4 | Load Client running (backlog `load` samples) |
| 10:12:16.5 | Notification started (pid 39216), 73.3 s after it stopped |
| 10:12:04.5 → 10:12:25.2 | final completion → last notification received (drain 20.7 s, deadline 60 s) |

### Results (`g-notification-stopped/`)

| Check | Value |
|---|---|
| Load Client (`client-stdout.txt`) | 3,872 transactions, 40,543 scans; **0 errors** in START_TRANSACTION, SCAN_ITEM, COMPLETE_TRANSACTION |
| Validator (`validator.txt`) | **PASS** (transaction, inventory, analytics): 3,872 completed, 0 background completions, 0 completions without response, 0 parked events, final consumer lag 0, remaining difference 0 |
| Sale events while Notification was down (`backlog.tsv`) | at most 5 unpublished at any sample, 0 at the end of the load: Transaction kept publishing to the broker |
| Backlog after the restart | sale consumer lag up to 868, pending notifications up to 701, channel lag up to 146; all 0 by 10:12:26.8 |
| Reconciliation (`reconcile.txt`) | **PASS** (fixed): 3,872 sales, 36,737 owed = delivered = received, 0 redeliveries, every mismatch category 0 |
| Delivery failures | 0 charged, 0 uncharged, 0 parked |
| End-to-end latency | p50 45.5 s, p99 75.6 s, max 78.2 s: the time sales waited for Notification to come back, as expected |
| Subscribers | exit code 0; `subscriber-stderr.log` holds 30 Kafka client warnings that a subscriber consumer could not reach `127.0.0.1:19092`; every notification was still received |

`run.json`'s `disruption.afterSeconds` (20) and `outageSeconds` (30) are the harness's defaults and are not used by
`notification-stopped`; `faults` is `none` because no fault was injected into Notification.

The raw receipts (`subscribers/receipts/`) and exported TSVs (`exports/*.tsv`), about 20 MB, are in `.gitignore` and
stay on the benchmark machine.

## Harness changes (`server/benchmark-notification.ps1`)

- `-Disruption notification-stopped` (above). Like `none`, it injects nothing during the load; it acts before the
  Load Client starts and after it exits.
