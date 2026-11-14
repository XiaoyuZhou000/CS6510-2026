# Quickstart: Validate the Sale Notification Service

These are runnable validation scenarios for [spec.md](spec.md). Commands, ports, and files are defined in the [contracts](contracts/). The scenarios run on the Windows benchmark machine with Git Bash and PowerShell 5.1, as in feature 006. Every command below is the one the implementation provides (T128). Each scenario also names the committed script that ran it and its evidence. [load-client/reports/notification-service/quickstart-validation.md](../../load-client/reports/notification-service/quickstart-validation.md) records the full pass.

## 0. Prerequisites

- Feature 006 prerequisites: JDK 21, Maven, MySQL 8.4 on 3307, Nginx, and the native Kafka 4.1.0 (`KAFKA_HOME`); see `specs/006-event-based-architecture/quickstart.md` §0.
- User-level environment variables `TRANSACTION_DB_PASSWORD`, `INVENTORY_DB_PASSWORD`, `ANALYTICS_DB_PASSWORD`, and the new **`NOTIFICATION_DB_PASSWORD`**, loaded into the shell and never printed.
- The MySQL admin credentials in `DB_USER` and `DB_PASSWORD`: Notification's user is DML-only, so its schema is applied as admin by `db/reset-service.ps1`, `db/reset-all.ps1`, and `db/reset-baseline.ps1`.
- `db/bootstrap.ps1` run once more to create `cs6510_notification` and its user.
- Each harness below takes `-NginxBin <path to nginx.exe>`. On the shared machine, add the idle gate `-IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights`.

## 1. Build and start

```bash
server/build.sh all
```

```bash
subscriber-client/build.sh
```

```powershell
deploy/kafka/start-broker.ps1; db/reset-baseline.ps1 -RunDir runs/smoke
```

Start the four services (`server/run-inventory.sh`, `run-transaction.sh`, `run-analytics.sh`, `run-notification.sh`) and Nginx (`server/README.md`). **Expected**:
- `GET :8080/notifications/ops/status` shows `catalogLoaded: true` and `subscriptionVersion: 0`;
- `deploy/kafka/status.ps1` lists five topics, and the four consumer groups once the services run;
- every 006 route answers as before.

`load-client/reports/notification-service/us7-operations/quickstart-1.ps1 -Repo <repository> -NginxBin <nginx.exe> -KafkaHome <KAFKA_HOME> -Out <dir>` does all of this and checks every expected result.

## Scenario A — Sale events without Notification Service (US1, SC-001)

Stop Notification. Complete known baskets: 3×A + 1×B; a basket rejected for insufficient stock and then completed; a repeated Complete; and a checkout that completes in the background after the fault `checkout.afterRecordCompletion` (006). Run `db/export-notification-evidence.ps1 -RunDir runs/a` and `ReconcileMain --mode=fixed` with an empty snapshot. `load-client/reports/notification-service/us1-scenario-a/scenario-a.ps1 -NginxBin <nginx.exe>` does all of this.

**Expected**:
- one `SaleCompleted` per COMPLETED transaction, with matching lines;
- none for scans, for pending or rejected attempts, or for repeats;
- the reconciler's `saleEvents` category is zero.

The reconciler's overall verdict is FAIL with `selectionMismatch` equal to the number of sales, because without Notification no sale has a selection.

## Scenario B — Notify once per sold SKU (US2)

Run `subscriber-client/run.sh SubscriberMain --assignments=b.json --out=runs/b`, where `b.json` assigns S1 = {A, B} and S2 = {B}, and wait for `ready.json`. Complete 3×A + 1×B + 2×C. `load-client/reports/notification-service/us3-smoke/b/scenario-b.ps1 -NginxBin <nginx.exe>` does all of this. **Expected**:
- S1 receives A×3 and B×1; S2 receives B×1; nobody is notified for C;
- the `sale_selection` row records version 2;
- a sale of C only creates a selection with `recipient_count = 0`.

## Scenario C — Reconciliation detects every mismatch (SC-003)

```bash
subscriber-client/run-tests.sh
```

Among them, `ReconcileFaultInjectionTest` copies a passing run's exports and receipts and injects, in turn:
- a removed delivery row;
- a removed receipt;
- an extra receipt;
- a wrong quantity;
- a second identity for one key;
- a selection-version change;
- a history row during the run.

**Expected**: each case fails with exactly its category and key. With `KAFKA_TEST_BOOTSTRAP=127.0.0.1:19092`, the same command also runs the live resume test.

## Scenario D — Main load test with fixed subscriptions (US3, SC-002)

```powershell
server/benchmark-notification.ps1 -Subscribers 100 -Seed 20261111 -Workload default -RunName d-default -NginxBin <nginx.exe>
```

**Expected**:
- `ready.json` is written before the Load Client starts;
- no history version is above it;
- the drain completes;
- `reconciliation.json` shows `verdict: PASS` with zero missing, unexpected, duplicate, quantity, and parked counts;
- `notification-report.json` gives every metric with `drain.deadlineMet: true` (SC-007);
- the Load Client report is unmodified.

Run it twice with the same seed: the two `subscribers/snapshot.json` files are byte-identical (SC-013).

## Scenario E — Subscription changes in their own scenario (US4, SC-004)

```bash
subscriber-client/run.sh ChangeScenarioMain --subscribers=10 --seed=7 --out=runs/e --faults-endpoint=http://localhost:8084/notifications/ops/faults
```

Run it with Notification started with `NOTIFICATION_TEST_FAULTS=1`. Optionally add `--with-load` with a parallel default Load Client run (the driver waits for `load.started` and `load.done` in its `--out` folder). Then export and run `ReconcileMain --mode=history`. `load-client/reports/notification-service/change-scenario/scenario-e.ps1 -RunName <name> [-WithLoad]` does all of this, including the restart check. **Expected**:
- every selection equals the history replayed to its version;
- every sale's version is at least the version confirmed before its Complete;
- failed changes appear nowhere;
- rejected requests return their documented codes;
- after a Notification restart, the index version equals the clock;
- already-selected notifications to a later-cancelled subscriber are still received.

## Scenario F — Duplicates, failures, and restarts (US5, SC-005)

Run the scenario D harness at default, once per item, with `-Disruption <kind>`. The harness injects each fault 20 s into the load (`-DisruptAfterSeconds`), recovers from it, drains, and reconciles. `load-client/reports/notification-service/fault-matrix/run-fault-matrix.ps1 -NginxBin <nginx.exe> [-Items 1,2,...]` runs the items in order:

| Item | `-Disruption` | What happens |
|---|---|---|
| 1 | `notification-selection-crash` | `notification.selection.afterCommit:halt`, set through `ops/faults` (the run sets `NOTIFICATION_TEST_FAULTS=1`), kills Notification once; the harness restarts it without the fault |
| 2 | `notification-send-crash` | The same with `notification.delivery.afterSend:halt` |
| 3 | `transaction-restart` | Transaction is stopped and started during the load (006 procedure) |
| 4 | `broker-outage` | `deploy/kafka/stop-broker.ps1` for 30 s (`-OutageSeconds`), then `start-broker.ps1 -NoTopics` |
| 5 | `subscriber-outage` | `SubscriberMain` stops (stop file) for 30 s and restarts with `--resume` |
| 6 | `malformed-sale` | `support.PublishRawRecord` publishes a malformed `SaleCompleted`; needs `server/build-tests.sh` |

**Expected**:
- every run passes reconciliation, except item 6, which reports that sale as missing and a parked event, by design;
- redeliveries appear only where a delivery was cut short: items 1 and 2, because a halt kills the whole Notification JVM between a confirmed send and its mark. Item 5 shows none when the subscriber stops cleanly. Every redelivery is kept once by its ID;
- broker downtime causes uncharged failures and no parked notifications.

## Scenario G — Existing behavior unchanged (US6, SC-008, SC-009)

```bash
STRICT_LIVE_GATES=1 SERVER_BASE_URL=http://localhost:8080 KAFKA_TEST_BOOTSTRAP=127.0.0.1:19092 KAFKA_TEST_BROKER_CONTROL=1 NGINX_BIN=<nginx.exe> KAFKA_HOME=<KAFKA_HOME> server/run-tests.sh
```

With no package selector, this runs feature 006's suite plus the new suites, against a running deployment (§1). `architecture.ClientAndContractUnchangedTest` hash-checks `load-client/src/**` and `spec/self-checkout-openapi.yaml` against `4a91686`. Then run the default Load Client with Notification stopped:

```powershell
server/benchmark-notification.ps1 -Subscribers 100 -Seed 20261111 -Workload default -RunName g -Disruption notification-stopped -DrainSeconds 300 -NginxBin <nginx.exe>
```

The harness stops Notification once the subscribers are registered, runs the whole load without it, and starts it when the Load Client exits (`us6-unchanged/run-scenario-g.ps1`). **Expected**:
- zero Load Client errors;
- the stock invariant holds;
- popular-items windows match the scan-only calculation;
- after Notification starts, the drain completes and the reconciliation passes.

## Scenario H — Evidence (US8, SC-006, SC-010, SC-011, SC-014)

Each run is `server/benchmark-notification.ps1 -Seed 20261111 -SkusPerSubscriber 20 -DrainSeconds 300 -NginxBin <nginx.exe>` after its own baseline reset. `load-client/reports/notification-service/run-evidence.ps1 -NginxBin <nginx.exe> -Runs <RunName>=<Workload>=<Subscribers>[=<Fault>],...` runs a list of them.

1. **Scaling**: `-Workload default` with `-Subscribers` set to 10, 25, 50, and 100, into `scaling/<n>`.
2. **Standard reports**: `-Workload default` and `-Workload stress` with 100 subscribers, into `cp4/default` and `cp4/stress`. Copy each Load Client report to `default-report.json` and `stress-report.json`, with its notification report and reconciliation.
3. **Isolation**: 3 × `-Fault none`, 3 × `-Fault reject`, and 3 × `-Fault stall`, interleaved, with `-Subscribers 100 -FaultSubscriber sub-0001`, into `isolation/{healthy,reject,stall}-<n>`. With a fault, the harness drains twice: first the other subscribers, then everyone, after it clears the fault and calls `POST /notifications/ops/parked/retry`. Then run `subscriber-client/run.sh IsolationSummary --dir=load-client/reports/notification-service/isolation`.
4. **Diagram**: export `docs/architecture/notification-service.md` with the pinned Mermaid CLI (the command is on that page) and run `server/run-tests.sh architecture`, which includes `ArchitectureDiagramConsistencyTest`.

**Expected**:
- every run has a passing reconciliation;
- in the isolation runs, the median of `endToEndExcluding.p95` for the faulted runs is within 20% of the healthy median, and the other 99 subscribers have no missing or duplicate notifications: `IsolationSummary` prints `SC-006 PASS`;
- `load-client/reports/notification-service/README.md` summarizes the results and compares them with `load-client/reports/event-based-architecture/`.
