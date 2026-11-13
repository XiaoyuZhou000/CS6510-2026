# Quickstart: Validate the Sale Notification Service

These are runnable validation scenarios for [spec.md](spec.md). Commands, ports, and files are defined in the [contracts](contracts/). The scenarios run on the Windows benchmark machine with Git Bash and PowerShell 5.1, as in feature 006.

## 0. Prerequisites

- Feature 006 prerequisites: JDK 21, Maven, MySQL 8.4 on 3307, Nginx, and the native Kafka 4.1.0 (`KAFKA_HOME`); see `specs/006-event-based-architecture/quickstart.md` §0.
- User-level environment variables `TRANSACTION_DB_PASSWORD`, `INVENTORY_DB_PASSWORD`, `ANALYTICS_DB_PASSWORD`, and the new **`NOTIFICATION_DB_PASSWORD`**, loaded into the shell and never printed.
- `db/bootstrap.ps1` run once more to create `cs6510_notification` and its user.

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

Start the four services (`server/run-*.sh`) and Nginx. **Expected**:
- `GET :8080/notifications/ops/status` shows `catalogLoaded: true` and `subscriptionVersion: 0`;
- `deploy/kafka/status.ps1` lists five topics;
- every 006 route answers as before.

## Scenario A — Sale events without Notification Service (US1, SC-001)

Stop Notification. Complete known baskets: 3×A + 1×B; a basket rejected for insufficient stock and then completed; a repeated Complete; and a checkout that completes in the background after the fault `checkout.afterRecordCompletion` (006). Run `db/export-notification-evidence.ps1 -RunDir runs/a` and `ReconcileMain --mode=fixed` with an empty snapshot. **Expected**: one `SaleCompleted` per COMPLETED transaction with matching lines, none for scans, pending or rejected attempts, or repeats; the reconciler's `saleEvents` category is zero. Its overall verdict is FAIL with `selectionMismatch` equal to the number of sales, because without Notification no sale has a selection.

## Scenario B — Notify once per sold SKU (US2)

Run `SubscriberMain --assignments=b.json --out=runs/b`, where `b.json` assigns S1 = {A, B} and S2 = {B}, and wait for `ready.json`. Complete 3×A + 1×B + 2×C. **Expected**:
- S1 receives A×3 and B×1; S2 receives B×1; nobody is notified for C;
- the `sale_selection` row records version 2;
- a sale of C only creates a selection with `recipient_count = 0`.

## Scenario C — Reconciliation detects every mismatch (SC-003)

Run the `ReconcileFaultInjectionTest` suite with `subscriber-client/run-tests.sh`. It copies a passing run's exports and receipts and injects, in turn: a removed delivery row, a removed receipt, an extra receipt, a wrong quantity, a second identity for one key, a selection-version change, and a history row during the run. **Expected**: each case fails with exactly its category and key.

## Scenario D — Main load test with fixed subscriptions (US3, SC-002)

```powershell
server/benchmark-notification.ps1 -Subscribers 100 -Seed 20261111 -Workload default -RunName d-default
```

**Expected**:
- `ready.json` is written before the Load Client starts;
- no history version is above it;
- the drain completes;
- `reconciliation.json` shows `verdict: PASS` with zero missing, unexpected, duplicate, quantity, and parked counts;
- `notification-report.json` gives every metric with `drain.deadlineMet: true` (SC-007);
- the Load Client report is unmodified.

Run it twice with the same seed: the two `snapshot.json` files are byte-identical (SC-013).

## Scenario E — Subscription changes in their own scenario (US4, SC-004)

```bash
subscriber-client/run.sh ChangeScenarioMain --subscribers=10 --seed=7 --out=runs/e --faults-endpoint=http://localhost:8084/notifications/ops/faults
```

Run it with Notification started with `NOTIFICATION_TEST_FAULTS=1`. Optionally add `--with-load` with a parallel default Load Client run. Then export and run `ReconcileMain --mode=history`. **Expected**:
- every selection equals the history replayed to its version;
- every sale's version is at least the version confirmed before its Complete;
- failed changes appear nowhere;
- rejected requests return their documented codes;
- after a Notification restart, the index version equals the clock;
- already-selected notifications to a later-cancelled subscriber are still received.

## Scenario F — Duplicates, failures, and restarts (US5, SC-005)

Run the scenario D harness at default, once for each item below. Drain and reconcile after each run, then clear the fault.
1. Set fault `notification.selection.afterCommit:crash`, which kills Notification once; restart it.
2. Set `notification.delivery.afterSend:crash`, then restart.
3. Restart Transaction during load (006 procedure).
4. Run `deploy/kafka/stop-broker.ps1` for 30 s mid-run, then start it.
5. Stop `SubscriberMain` for 30 s, then restart it with `--resume`.
6. Publish a malformed `SaleCompleted` with `PublishRawRecord`.

**Expected**:
- every run passes reconciliation, except item 6, which reports that sale as missing and a parked event, by design;
- redeliveries appear only in items 2 and 5;
- broker downtime causes uncharged failures and no parked notifications.

## Scenario G — Existing behavior unchanged (US6, SC-008, SC-009)

```bash
STRICT_LIVE_GATES=1 server/run-tests.sh
```

With no package selector, this runs feature 006's suite plus the new suites. Hash-check `load-client/src/**` and `spec/self-checkout-openapi.yaml` against `4a91686`. Then run the default Load Client with Notification stopped. **Expected**:
- zero Load Client errors;
- the stock invariant holds;
- popular-items windows match the scan-only calculation;
- after Notification starts, the drain completes and the reconciliation passes.

## Scenario H — Evidence (US8, SC-006, SC-010, SC-011, SC-014)

1. **Scaling**: run the harness with `-Subscribers` set to 10, 25, 50, and 100 at default, seed `20261111`.
2. **Standard reports**: run `-Workload default` and `-Workload stress` with 100 subscribers, each after a fresh baseline.
3. **Isolation**: run 3 × `-Fault none`, 3 × `-Fault reject`, and 3 × `-Fault stall`, interleaved, with `-FaultSubscriber sub-0001`.
4. **Diagram**: render the diagram and run `ArchitectureDiagramConsistencyTest`.

**Expected**:
- every run has a passing reconciliation;
- in the isolation runs, the median of `endToEndExcluding.p95` for the faulted runs is within 20% of the healthy median, and the other 99 subscribers have no missing or duplicate notifications;
- `load-client/reports/notification-service/README.md` summarizes the results and compares them with `load-client/reports/event-based-architecture/`.
