# Notification Service evidence (feature 007)

The performance and correctness evidence of `specs/007-notification-service`. It comes from the four-service deployment (Transaction, Inventory, Analytics, and Notification, plus Nginx and one native Kafka 4.1.0 broker), driven by the unchanged Load Client and the subscriber test application ([subscriber-client/](../../../subscriber-client/README.md)). Every run follows a fresh baseline (`db/reset-baseline.ps1`) and ends with `db/validate-invariants.ps1` and a reconciliation of every sale against every notification owed, delivered, and received. The architecture is drawn in [docs/architecture/notification-service.md](../../../docs/architecture/notification-service.md).

All runs are on one Windows machine: JDK 21, MySQL 8.4 with `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1`, Nginx 1.31.6, and Kafka 4.1.0 with page-cache flushing. The phase 10 runs are of 2026-11-13. The checkpoints and their investigation are in [checkpoints.md](checkpoints.md).

## Index

| Evidence | What it shows | Task |
|---|---|---|
| [Official default report](default-report.json) ([notification report](default-notification-report.md), [reconciliation](default-reconciliation.json)), run folder [cp4/default](cp4/default/) | The standard default run with 100 subscribers (SC-011) | T126 |
| [Official stress report](stress-report.json) ([notification report](stress-notification-report.md), [reconciliation](stress-reconciliation.json)), run folder [cp4/stress](cp4/stress/) | The standard 100-station, 120 s run with 100 subscribers (SC-011) | T126 |
| [scaling/10](scaling/10/), [25](scaling/25/), [50](scaling/50/), [100](scaling/100/) | Four subscriber counts at default (SC-010) | T124 |
| [isolation/](isolation/README.md) | 3 healthy, 3 reject, and 3 stall runs, and the SC-006 verdict | T125 |
| [cp2/](cp2/) and [checkpoints.md](checkpoints.md) | CP2 (default ×2, stress), two same-day controls without Notification, and the investigation of the drop against CP1 | T122 |
| [cp0/](cp0/), [cp1/](cp1/) | CP0 (feature 006 unmodified) and CP1 (`SaleCompleted` in T3, Notification not running) | T002, T035 |
| [us1-scenario-a.md](us1-scenario-a.md) | Sale events without Notification (SC-001) | T034 |
| [us3-smoke/](us3-smoke/README.md) | Scenarios B, C, and D, and the repeated snapshot (SC-002, SC-003, SC-013) | T080 |
| [change-scenario/](change-scenario/README.md) | Subscription changes, alone and under load (SC-004) | T093 |
| [fault-matrix/](fault-matrix/README.md) | Duplicates, failures, and restarts (SC-005) | T103 |
| [us6-unchanged/](us6-unchanged/README.md) | Checkout, Analytics, and the client unchanged, with Notification stopped (SC-008, SC-009) | T109 |
| [us7-operations.md](us7-operations.md) | Independent operation, isolation of credentials, and the baseline (SC-012) | T117 |

The raw receipts and exported TSV files of each 100-subscriber run (about 20 MB per run) stay on the benchmark machine (`.gitignore` in each folder). Every committed run folder keeps `run.json`, the reports, `reconciliation.json`, `backlog.tsv`, the validator output, and the service logs.

**How the phase 10 runs were made**: [`run-evidence.ps1`](run-evidence.ps1) runs `server/benchmark-notification.ps1 -Seed 20261111 -SkusPerSubscriber 20 -DrainSeconds 300` for each entry (`campaign-*.txt` are its logs). The isolation runs add `-FaultSubscriber sub-0001 -Fault none|reject|stall`.

The idle gate:
- CP2, scaling, and CP4 ran with the CPU limit lifted (`-IdleCpuCores 64`), on the user's instruction to run at once. The game-process list and the port check still applied. The game that had held the gate had closed, and `run.json` records 0.16–0.60 background cores, inside the usual 2-core gate.
- The isolation runs and the CP2 controls ran with the normal gate (`-IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights`).

`run.json` reports `gitDirty: true` for the phase 10 runs, because this phase's sources were edited while they ran. The service jars were built from `b518303` and did not change; the harness changed only for the isolation runs (`-Fault`).

## Results per subscriber count (SC-010)

The default workload (10 stations, 60 s) with `-Seed 20261111` and 20 SKUs per subscriber. The subscriber sets are nested, so the 10 subscribers are the first 10 of the 100. Latency is end-to-end, from each sale's completion to its first receipt, by nearest rank. Every run reconciled: zero missing, unexpected, duplicate, quantity-mismatched, or parked.

| Subscribers | Sales/s | Owed notifications | Delivered/s | Received/s | p50 / p95 / p99 (ms) | Max (ms) | Failures (charged / uncharged / parked) | Max backlog: unpublished sales / pending / channel lag | Drain after the last sale |
|---:|---:|---:|---:|---:|---|---:|---|---|---:|
| [10](scaling/10/notification-report.md) | 63.3 | 3,152 | 52.5 | 52.5 | 28 / 64 / 137 | 449 | 0 / 0 / 0 | 5 / 9 / 13 | 0 s |
| [25](scaling/25/notification-report.md) | 53.7 | 5,802 | 96.5 | 96.6 | 27 / 59 / 127 | 3,928 | 0 / 0 / 0 | 5 / 13 / 26 | 0.04 s |
| [50](scaling/50/notification-report.md) | 63.2 | 14,449 | 240.7 | 240.7 | 26 / 69 / 143 | 1,965 | 0 / 0 / 0 | 5 / 34 / 47 | 0.03 s |
| [100](scaling/100/notification-report.md) | 64.0 | 36,370 | 605.6 | 605.6 | 28 / 91 / 183 | 318 | 0 / 0 / 0 | 5 / 69 / 90 | 0.02 s |

The median stays at 26–28 ms from 10 to 100 subscribers, while the notification rate grows twelvefold. The tail grows with it: p95 from about 60 ms to 91 ms, and p99 from about 130 ms to 183 ms. The sale consumer's lag was 0 at every 2 s sample. The multi-second maxima at 25 and 50 subscribers lie far above those runs' p99 and well inside the 60 s drain deadline.

## The standard reports compared with feature 006 (SC-011)

Both runs follow a fresh baseline and run 100 subscribers. The Load Client reports are byte-identical copies of what the client wrote. `db/validate-invariants.ps1` passed after each ([validator.txt](cp4/default/validator.txt), [stress](cp4/stress/validator.txt)), and so did each reconciliation.

| Run | `totalTransactions` | Transactions/s | Start p95 / p99 (ms) | Scan p95 / p99 (ms) | Complete p95 / p99 (ms) | Complete errors | Notifications owed = received | Drain |
|---|---:|---:|---|---|---|---:|---:|---:|
| 006 [default](../event-based-architecture/default-report.json) | 4,371 | 72.7 | 12.1 / 27.6 | 13.6 / 41.8 | 71.0 / 140.0 | 0 | — | — |
| 007 [default](default-report.json) | 3,523 (−19.4%) | 58.3 | 16.1 / 51.6 | 17.3 / 59.4 | 110.9 / 187.3 | 0 | 33,520 | 0.02 s |
| 006 [stress](../event-based-architecture/stress-report.json) | 10,553 | 87.6 | 23.4 / 60.7 | 26.0 / 66.9 | 1,427.1 / 2,671.6 | 5,954 (36.1%) | — | — |
| 007 [stress](stress-report.json) | 9,210 (−12.7%) | 76.2 | 44.0 / 85.5 | 47.4 / 100.9 | 2,064.0 / 2,784.8 | 2,775 (23.2%) | 83,393 | 0.04 s |

- **Errors**: the default run has zero errors for every operation. Every stress error is a 409 `INSUFFICIENT_STOCK` once the hot SKU sells out, as in feature 006; Start and Scan have none.
- **Notification latency** (end-to-end p50 / p95 / p99): default 27 / 94 / 182 ms; stress 29 / 154 / 250 ms.
- **Throughput is lower than feature 006's**, and the cause was measured (CP2 in [checkpoints.md](checkpoints.md)). Two same-day control runs with Notification not running gave a median of 4,039, so the machine itself was about 7% slower that day than on feature 006's and CP1's days. Against those controls, the runs with Notification are about 7% lower, and Complete p95 is about 30 ms higher. The cost does not grow with the subscriber count: 10 subscribers cost as much as 100. It comes from a fourth service sharing the CPU and the single MySQL server, whose durable commits Complete waits on. Correctness is unaffected, and checkout never waits for Notification (US6). The optional tuning checkpoint CP3 was not run.

## Isolation of one faulted subscriber (SC-006)

Nine default runs with 100 subscribers and seed 20261111, interleaved healthy, reject, stall. In the faulted runs, every send to `sub-0001`'s channel fails at once (`reject`) or after 10 s (`stall`) for the whole load. The verdict comes from `IsolationSummary` ([isolation/README.md](isolation/README.md)). The latency compared is the p95 end-to-end latency of the other 99 subscribers (`latencyMs.endToEndExcluding.p95`), which the healthy runs also report for the same 99.

| Mode | p95 of the other 99 per run (ms) | Median (ms) | Ratio to healthy |
|---|---|---:|---:|
| healthy | 97, 94, 98 | 97 | — |
| reject | 93, 98, 100 | 98 | 1.01 |
| stall | 100, 94, 85 | 94 | 0.97 |

**SC-006: PASS**. Both faulted medians are within 20% of the healthy median; in fact they are within noise of it. All nine runs reconciled with zero missing and zero duplicate notifications.

How the faulted runs behave:
- **During the load**, the faulted channel's failures were charged to it: 116–193 per reject run and 356–369 per stall run. A stalled send holds its slot for 10 s, but only in that subscriber's own lane.
- **After the load**, no other channel was sending any more, so the remaining failures counted as broker-wide and uncharged: 1,867–2,359 per reject run and 44–65 per stall run, since a stalled attempt takes 10 s. No notification reached the 8 charged failures that park it.
- **The first drain** waited only for the other 99 subscribers. It took about 2 s, the same as a healthy run, with about 200–250 of `sub-0001`'s notifications still pending.
- **Then** the harness cleared the fault and called `ops/parked/retry`. It retried 0 notifications, because none was parked. The pending notifications were delivered on their next attempt, and the second drain took 8–10 s after reject and 14 s after stall.
- **All runs** received every owed notification once, with 0 redeliveries.

`stall-3` ran last, the same evening, after the machine had been restarted. A first attempt waited at the idle gate while a game ran, was stopped, and was rerun from scratch under the normal gate. It had the highest throughput of the nine (4,045 transactions) and the lowest p95 (85 ms). Without it, the stall median would be 97 ms (the mean of 100 and 94), a ratio of 1.00, so the verdict does not depend on it.

## Drain deadline (SC-007)

In the official default run, with every component healthy and 100 subscribers, the last of 33,520 owed notifications was received 0.02 s after the final completed sale, against a 60 s deadline: `drain.deadlineMet: true` in [default-notification-report.json](default-notification-report.json). The stress run reports its drain time as 0.04 s. The scaling and CP2 runs also met it, with drains of 0.04 s or less.

## Success criteria

| Criterion | Status | Evidence |
|---|---|---|
| SC-001: one `SaleCompleted` per COMPLETED transaction | **Met** | Every run's validator and reconciliation (`saleEvents` = 0); scenario A ([us1-scenario-a.md](us1-scenario-a.md)); the fault matrix ([fault-matrix/](fault-matrix/README.md)) |
| SC-002: every main run reconciles | **Met** | Scaling, CP2, CP4, and isolation runs in this folder; scenario D ([us3-smoke/](us3-smoke/README.md)) |
| SC-003: every injected mismatch is named | **Met** | Scenario C ([us3-smoke/](us3-smoke/README.md)) |
| SC-004: selections follow the subscription history | **Met** | Scenario E ([change-scenario/](change-scenario/README.md)) |
| SC-005: exactly once through failures and restarts | **Met** | Scenario F ([fault-matrix/](fault-matrix/README.md)); item 6 parks the malformed record by design |
| SC-006: one faulted channel does not slow the others | **Met** (ratios 1.01 and 0.97) | [isolation/README.md](isolation/README.md) |
| SC-007: drain within 60 s | **Met** | Above; [default-notification-report.md](default-notification-report.md) |
| SC-008: checkout unaffected by a stopped Notification | **Met** | Scenario G ([us6-unchanged/](us6-unchanged/README.md)) |
| SC-009: 006 suite passes, client and contract unchanged | **Met** | Final gate: 646 tests, 0 failed ([final-gate-tests.txt](final-gate-tests.txt)); hash checks in [us6-unchanged/](us6-unchanged/README.md) |
| SC-010: reports at three or more subscriber counts | **Met** | Four counts, above |
| SC-011: two standard reports with notification reports | **Met** | Above; the throughput cost is recorded, not hidden |
| SC-012: independent operation and credential isolation | **Met** | [us7-operations.md](us7-operations.md) |
| SC-013: the same seed gives the same subscribers | **Met** | Byte-identical snapshots ([us3-smoke/](us3-smoke/README.md)); all phase 10 runs share seed 20261111 |
| SC-014: the diagram matches the catalog | **Met** | [notification-service.md](../../../docs/architecture/notification-service.md) and its SVG, checked by `architecture.ArchitectureDiagramConsistencyTest` |

## Appendix: requirement coverage (T131)

Every functional requirement of [spec.md](../../../specs/007-notification-service/spec.md) is covered by at least one passing test or by committed evidence. Server tests are under `server/tests/<package>/`, and subscriber-client tests under `subscriber-client/test/`. All of them pass in the final gate ([final-gate-tests.txt](final-gate-tests.txt)). The spec has not changed since commit `5986398`, so its quality checklist ([checklists/requirements.md](../../../specs/007-notification-service/checklists/requirements.md), 16 of 16 items) still holds as written.

| Requirement | Tests | Evidence |
|---|---|---|
| FR-001 Sale exists only after durable COMPLETED | `integration.SaleCompletedEventTest`, `database.JdbcCheckoutAttemptStoreTest` | Scenario A |
| FR-002 Event recorded in T3's own commit | `architecture.SaleEventOnlyFromT3Test`, `messaging.SaleEventPublishLiveTest`, `performance.OutcomeHandlerStatementBudgetTest`, `unit.transaction.SaleEventsTest` | CP1 |
| FR-003 Exactly one event per sale | `integration.SaleCompletedEventTest`, `integration.TransactionRestartSaleEventsTest`, `integration.CheckoutMessagesOnlyTest` | Every run's `saleEvents` = 0 |
| FR-004 No recipient data in Transaction | `architecture.TransactionHasNoRecipientLogicTest`, `messaging.SaleEventContractTest` | — |
| FR-005 Complete never waits for notification | `integration.NotificationOutageCheckoutTest` | Scenario G ([us6-unchanged/](us6-unchanged/README.md)) |
| FR-006 Register, add, remove, cancel | `contract.NotificationSubscribersEndpointTest`, `unit.notification.SubscriptionChangeRulesTest`, `database.JdbcSubscriptionStoreTest` | Scenario E |
| FR-007 Effective at commit | `integration.SubscriptionChangeConsistencyTest`, `database.SelectionLockingTest` | Scenario E |
| FR-008 Index from committed data only | `unit.notification.SubscriptionIndexTest`, `integration.SubscriptionChangeConsistencyTest` | Scenario E restart check |
| FR-009 ≥ 100 subscribers, independent channels | `messaging.ChannelIsolationLiveTest`, `unit.notification.NotificationDispatcherTest` | Scaling to 100; isolation (SC-006) |
| FR-010 Selection rule | `unit.notification.SelectionPlanTest`, `database.SelectionLockingTest` | Scenarios B and E |
| FR-011 Selection recorded durably | `database.JdbcSelectionStoreTest`, `performance.SelectionStatementBudgetTest` | `selections.tsv` of every run |
| FR-012 Selections are final | `integration.SubscriptionChangeConsistencyTest`, `HistoryReconcileTest` | Scenario E (cancelled subscriber still receives) |
| FR-013 One effective notification per (sale, SKU, subscriber) | `integration.NotificationFlowTest`, `ReconcilerTest` | SC-002 runs |
| FR-014 Stable notification identity | `database.JdbcNotificationStoreTest`, `ReceiptWriterTest` | Receipts' `redelivery` flags (scenario F) |
| FR-015 At-least-once transport, once in effect | `integration.NotificationCrashMatrixTest`, `messaging.NotificationBrokerRestartTest`, `integration.RestartDuringWorkloadTest` | Scenario F ([fault-matrix/](fault-matrix/README.md)) |
| FR-016 Durable delivery record | `database.JdbcNotificationStoreTest` | `notifications.tsv` of every run |
| FR-017 Bounded retry, parking, operator retry | `unit.notification.ChargingRuleTest`, `database.ParkingAndOperatorRetryTest` | Isolation runs (fault, then retry) |
| FR-018 Unprocessable sale events parked | `messaging.SaleEventParkingLiveTest` | Scenario F item 6 |
| FR-019 Subscriber test application | `SubscriberThreadTest`, `SubscriberResumeLiveTest` | Every phase 10 run |
| FR-020 Reproducible assignment | `AssignmentDeterminismTest` | Byte-identical snapshots ([us3-smoke/](us3-smoke/README.md)) |
| FR-021 Structured receipts | `ReceiptWriterTest`, `SubscriberResumeLiveTest` | `subscribers/receipts/` (on the benchmark machine) |
| FR-022 Change-scenario driver | `HistoryReconcileTest` | Scenario E ([change-scenario/](change-scenario/README.md)) |
| FR-023 Registered before the load | `ReconcilerTest` (`fixedSubscriptions`) | `ready.json` before the load in every run |
| FR-024 Four independent sources | `DecodeSaleEventsTest`, `ReconcilerTest` | `exports/` of every run |
| FR-025 Drain condition | `NotificationReportTest` | `backlog.tsv` and `drain` of every run |
| FR-026 Reconciliation verdict | `ReconcilerTest`, `ReconcileFaultInjectionTest` | Scenario C; every `reconciliation.json` |
| FR-027 History-mode reconciliation | `HistoryReconcileTest` | Scenario E |
| FR-028 Three or more subscriber counts | — | [scaling/](scaling/) (SC-010) |
| FR-029 Two genuine reports | — | [default-report.json](default-report.json), [stress-report.json](stress-report.json) (SC-011) |
| FR-030 Operational status | `contract.OpsFaultsEndpointTest`, `database.ServiceStatusQueriesTest` | `ops/status` sampled in every `backlog.tsv` |
| FR-031 Existing contract unchanged | `architecture.ClientAndContractUnchangedTest`, the 006 contract suites | Final gate (SC-009) |
| FR-032 Additive endpoints | `contract.NotificationSubscribersEndpointTest`, `integration.GatewayServiceOutageTest` | `spec/notification-openapi.yaml` |
| FR-033 No effect on checkout or Analytics | `integration.NotificationOutageCheckoutTest`, `integration.AnalyticsUnaffectedByNotificationsTest` | Scenario G |
| FR-034 Independent build and run | `integration.IndependentServiceDeploymentTest` | [us7-operations.md](us7-operations.md) |
| FR-035 Documented operation | `integration.DatabaseIsolationTest` (credentials) | `server/README.md`, `deploy/kafka/README.md`, `subscriber-client/README.md` |
| FR-036 One baseline for everything | `integration.ResetBaselineTest` | Quickstart §1 ([us7-operations.md](us7-operations.md)) |
| FR-037 Updated diagram | `architecture.ArchitectureDiagramConsistencyTest` | [notification-service.md](../../../docs/architecture/notification-service.md) |
| FR-038 Required automated tests | All of the above; `IsolationSummaryTest` for the isolation verdict | — |
