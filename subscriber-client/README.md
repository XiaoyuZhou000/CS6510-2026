# Subscriber Test Application

The test tool of feature 007 ([contract](../specs/007-notification-service/contracts/subscriber-client-and-reconciliation.md)). It plays the subscribers of the Notification Service, records every notification they receive, and reconciles a run: every completed sale against the notifications owed, delivered, and received. It is a test harness, not a service. It compiles only against the generated event classes and the Kafka client, never against `server/` code.

**It does not replace or change the Load Client.** The Load Client (`load-client/`) stays byte-identical to commit `4a91686` (`architecture.ClientAndContractUnchangedTest`) and keeps generating the checkout workload and its own report. This application runs beside it: it registers the subscribers before the Load Client starts and reads their channels while it runs. Its notification report is a separate file, and the Load Client's report is never modified.

## Build and test

```bash
server/build.sh contracts        # out/contracts.jar and server/lib/messaging/, which this build needs
subscriber-client/build.sh       # compiles src/ into subscriber-client/out/
subscriber-client/run-tests.sh   # every test in test/, with JUnit 5
```

`run.sh <MainClass> [--key=value ...]` runs one main class and builds first if needed. Relative paths in the arguments are resolved against the caller's directory. `SubscriberResumeLiveTest` needs the broker at `127.0.0.1:19092`; the other tests need nothing running.

## SubscriberMain

```bash
subscriber-client/run.sh SubscriberMain --out=runs/d --subscribers=100 --seed=20261111
```

1. Reads the catalog through `GET /items` and writes `snapshot.json`.
2. Registers every subscriber through `POST /notifications/subscribers`, 8 at a time, with idempotent retries.
3. Waits until every channel is ready.
4. Starts one consumer thread per subscriber on its channel topic at offset 0. The threads use manual assignment and no consumer group.
5. Writes `ready.json` with the highest confirmed subscription version. The harness starts the Load Client only after this file exists.
6. Runs until the stop file appears, rewriting `status.json` every second, then stops cleanly.

| Option | Default | Meaning |
|---|---|---|
| `--out=DIR` | required | Run directory |
| `--subscribers=N` | 100 | Subscribers and consumer threads, 1–9,999 |
| `--seed=S` | generated and recorded | Assignment seed |
| `--skus-per-subscriber=K` | 20 | Distinct SKUs per subscriber, 1–2,000 |
| `--sku-distribution=uniform\|popularity` | `uniform` | `popularity` weights the draw like the Load Client (Zipf, 1/rank) |
| `--id-prefix=P` | `sub` | Identities `P-0001` … `P-N` |
| `--assignments=FILE` | none | An explicit `{"subscriberId": [skus]}` map instead of the seeded draw |
| `--base-url=URL` | `http://localhost:8080` | The gateway |
| `--bootstrap=HOST:PORT` | `127.0.0.1:19092` | The broker |
| `--topic-prefix=X` | empty | The services' `KAFKA_TOPIC_PREFIX` |
| `--resume` | off | Reuse `DIR`: no registration, and each thread resumes after its highest recorded offset |
| `--stop-file=PATH` | `DIR/stop` | Stop cleanly when this file appears |

The seeded assignment draws each subscriber's SKUs from its own random stream, so a subscriber's set never depends on the subscriber count, and two runs with the same seed write byte-identical `snapshot.json` files.

Exit codes: 0 clean stop; 2 invalid options or run directory; 3 registration or channel failure; 4 an unrecoverable consumer or receipt error.

## ChangeScenarioMain

```bash
subscriber-client/run.sh ChangeScenarioMain --out=runs/e --subscribers=10 --seed=7 --faults-endpoint=http://localhost:8084/notifications/ops/faults
```

The driver of quickstart scenario E. It registers `chg-0001` … `chg-N` and consumes their channels like `SubscriberMain`. Then it makes subscription changes at controlled points between sales and records each one in `changes.jsonl` for `ReconcileMain --mode=history`. The changes are confirmed-before cases, a burst of 50 seeded changes during sales, injected commit failures, and one request of each rejected kind. Finally it drains.

| Option | Default | Meaning |
|---|---|---|
| `--out=DIR` | required | Run directory |
| `--subscribers=N` | 10 | 3–999 |
| `--seed=N` | generated | Seed of the SKU pool and of the change burst |
| `--base-url=URL`, `--bootstrap=HOST:PORT` | as for `SubscriberMain` | |
| `--with-load` | off | Waits for `DIR/load.started` before the burst and for `DIR/load.done` after it, around an external Load Client run |
| `--faults-endpoint=URL` | none | Notification's `POST /notifications/ops/faults`, which needs `NOTIFICATION_TEST_FAULTS=1`. It enables the failed-change cases and the stalled channel before a cancellation. |

Exit codes: 0 every request answered as expected and the run drained; 1 an unexpected answer or no drain; 2 usage; 3 setup failure. `load-client/reports/notification-service/change-scenario/scenario-e.ps1` runs the whole scenario, including the export and the reconciliation.

## Run directory

`server/benchmark-notification.ps1` lays a run out as below. `RunData` also accepts the subscriber files directly in `DIR`.

| Path | Written by | Contents |
|---|---|---|
| `subscribers/snapshot.json` | `SubscriberMain` | The seed, the settings, the catalog's size and SHA-256, and every subscriber's SKUs |
| `subscribers/ready.json` | `SubscriberMain` | `readyAt`, the highest confirmed `subscriptionVersion`, each subscriber's confirmed version, and the channel topics (read by `--resume`) |
| `subscribers/receipts/<subscriberId>.jsonl` | each subscriber thread | One line per channel record received: `subscriberId`, `notificationId`, `saleId`, `sku`, `quantity`, `completedAtMs`, `receivedAtMs`, `channelOffset`, and `redelivery` (true for a notification ID already recorded). An undecodable record is kept as an `undecodable` line. |
| `subscribers/status.json` | `SubscriberMain`, every second | Per subscriber: received, redeliveries, and lag (channel end minus position, -1 while unknown), plus totals. The harness waits for `totals.lag` 0. |
| `subscribers/changes.jsonl` | `ChangeScenarioMain` | Every change and its answer, and the versions at which each guarded subscriber may change |
| `exports/*.tsv` | `db/export-notification-evidence.ps1` | `sales`, `sale-events`, `selections`, `notifications`, `history`, and `parked-events`: tab-separated with a header row, `NULL` for SQL NULL, times in epoch milliseconds |
| `run.json`, `backlog.tsv` | the harness | The run's identity, and the backlog samples taken every 2 s |
| `reconciliation.json` | `ReconcileMain` | The verdict |

A receipt file is flushed after every poll and read back on `--resume`, so a restart loses nothing; at most the tail received after the last flush arrives again and is recorded as redeliveries.

## ReconcileMain

```bash
subscriber-client/run.sh ReconcileMain --run-dir=runs/d --mode=fixed --report-out=runs/d/notification-report.json
```

It writes `DIR/reconciliation.json`: `verdict`, `mode`, `counts` per category, `totals` (`completedSales`, `owed`, `delivered`, `received`, `redeliveries`, and more), and `samples`, the first 20 failing keys of each category.

| Mode | Use | Owed notifications |
|---|---|---|
| `fixed` | Subscriptions do not change during the run (scenarios D, F, G, H) | From `snapshot.json`; no history version may be above `ready.json`'s |
| `history` | Scenario E | Each sale's selection is replayed against the subscription history up to the version it recorded, using `changes.jsonl` |

The categories, each of which must be zero for a PASS: `saleEvents` (each COMPLETED transaction has exactly one matching `SaleCompleted`), `fixedSubscriptions` or `history`, `selectionMismatch`, `missingNotDelivered`, `missingDeliveredNotReceived`, `unexpected`, `duplicate` (one key with two identities), `quantityMismatch`, and `parked`. Redeliveries of the same notification ID are counted, not failed. A subscriber keeps the first copy.

Exit codes: 0 PASS; 1 FAIL; 2 invalid arguments, an unknown mode, or an unreadable run directory, including a fixed-mode run without `snapshot.json` and `ready.json`.

## The notification report

With `--report-out=FILE`, `ReconcileMain` also writes the notification report as `FILE` (JSON) and its Markdown rendering beside it:

| Section | Fields |
|---|---|
| Run | `id`, `startedAt`, `deploymentCommit`, `subscribers`, `seed`, `skusPerSubscriber`, `skuDistribution`, `loadClient`, `faults` |
| Sales and notifications | completed sales, the active window and sales per second, background completions; notifications owed, delivered, received, and redeliveries with their rates; charged and uncharged failures, parked, and operator retries |
| Latency | End-to-end (first receipt minus the sale's completion) and completion to delivered: p50, p95, p99, max, and count, by nearest rank. A notification never received is excluded and counted as missing. |
| Backlog | Maximum and final of unpublished sale events, sale consumer lag, pending notifications, and channel lag, from `backlog.tsv` |
| Drain | From the final completion to the last first receipt; the deadline (60 s) is met when nothing is missing and the drain took at most 60 s |
| Reconciliation counts | Every category |

Rates use the active window, from the first to the last completed sale.

## DecodeSaleEvents

```bash
subscriber-client/run.sh DecodeSaleEvents --in=raw-sale-events.tsv --out=exports/sale-events.tsv
```

`db/export-notification-evidence.ps1` calls it. It decodes Transaction's exported `SaleCompleted` outbox rows (Protobuf envelopes in hex) into `sale-events.tsv`, with the sale ID, completion time, and lines as canonical JSON. A row that cannot be decoded keeps its identity and fails the reconciler's `saleEvents` check.
