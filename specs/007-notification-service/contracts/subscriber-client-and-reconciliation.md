# Contract: Subscriber Test Application, Reconciliation, and Notification Report

The subscriber test application is a separate application in `subscriber-client/`, outside every service (FR-019). See research N9–N12.

## Build and run

```text
subscriber-client/build.sh        # javac --release 21 against server/out/contracts.jar and server/lib/messaging/*
subscriber-client/run.sh <Main> [--key=value ...]
```

It requires `server/build.sh contracts` to have run. It uses only public HTTP routes (`GET /items`, `/notifications/subscribers`, and, for scenario E, the transaction routes) and the subscribers' channel topics.

## `SubscriberMain`

| Option | Default | Meaning |
|---|---|---|
| `--subscribers=N` | 100 | Number of subscribers, and of subscriber threads (one each) |
| `--seed=S` | generated and recorded | Random seed for SKU assignment |
| `--skus-per-subscriber=K` | 20 | Distinct SKUs per subscriber (1–2,000) |
| `--sku-distribution=uniform\|popularity` | `uniform` | `popularity` uses the Load Client's Zipf weights |
| `--id-prefix=P` | `sub` | Identities `P-0001` … `P-N` |
| `--assignments=FILE` | none | Explicit `{subscriberId: [skus]}` for small functional scenarios; replaces seeded assignment and is copied into `snapshot.json` |
| `--base-url=URL` | `http://localhost:8080` | Gateway |
| `--bootstrap=HOST:PORT` | `127.0.0.1:19092` | Kafka |
| `--topic-prefix=X` | empty | Must equal the services' `KAFKA_TOPIC_PREFIX` |
| `--out=DIR` | required | Run directory (`snapshot.json`, `ready.json`, `receipts/`, `status.json`) |
| `--resume` | off | Reuse `DIR`: no registration; resume each channel after its highest recorded offset |
| `--stop-file=PATH` | `DIR/stop` | Exit cleanly when this file appears |

**Assignment**: the catalog SKUs are sorted ascending. Subscriber *i* draws K distinct SKUs with `SplittableRandom(mix64(seed + i))`, using a partial Fisher–Yates shuffle for `uniform`, or weighted sampling without replacement for `popularity`. The same seed and settings always give byte-identical `snapshot.json` (SC-013), and subscriber *i*'s set does not depend on N.

**Lifecycle**:
1. Write `snapshot.json`.
2. Register with 8 parallel requests; retry 500 responses up to 10 times, which is safe because registration is idempotent; fail the run on any 4xx.
3. Wait until every `channelReady` is true.
4. Start the threads at offset 0.
5. Write `ready.json`, with the maximum confirmed `subscriptionVersion`.
6. Run until the stop file appears, flushing receipts after every poll and rewriting `status.json` every second.

Exit code 0 means a clean stop; non-zero means a registration failure or an unrecoverable consumer error, which is also logged.

## `ChangeScenarioMain` (scenario E)

`--subscribers=N` (default 10), `--seed`, `--base-url`, `--bootstrap`, `--out`, `--with-load` (wait for an external Load Client run), and `--faults-endpoint` (to inject `notification.subscription.beforeCommit`). It performs the steps of research N12 and writes `changes.jsonl`. Each completed sale it makes is recorded with the version confirmed just before its Complete request was sent.

With `--with-load` it waits for `OUT/load.started` before its burst of changes and for `OUT/load.done` after it; the harness creates both. It consumes each registered subscriber's channel into `OUT/receipts/`, as `SubscriberMain` does, and exits 0 only when every request got its expected answer and the run drained.

`changes.jsonl` holds one JSON object per line, distinguished by `kind`:

| `kind` | Fields | Checked by `--mode=history` |
|---|---|---|
| `change` | `step`, `op` (`REGISTER`/`UPDATE`/`CANCEL`), `subscriberId`, `add`, `remove`, `status`, `error`, `version` (2xx only), `confirmedAtMs` | a 2xx `version` must be a history row of that subscriber |
| `sale` | `step`, `transactionId`, `skus`, `confirmedVersion` (the highest version confirmed before Complete was sent), `completeStatus` | the sale must be completed, and its selection's version at least `confirmedVersion` |
| `guard` | `subscriberId`, `allowedVersions`, `reason`: the target of failed or rejected requests | no history row of the subscriber outside `allowedVersions` |
| `run`, `pool`, `faults`, `end` | the run's settings, the SKU pool, each fault change, and the drain result | not checked |

Failed and rejected requests target guarded subscribers only, so "failed changes appear nowhere" is an exact check.

## Evidence export (`db/export-notification-evidence.ps1 -RunDir DIR`)

Each source is exported by its owner's credentials, to tab-separated files under `DIR/exports/` (FR-024):

| File | Owner | Content |
|---|---|---|
| `sales.tsv` | Transaction | `transaction_id`, `completed_at`, `sku`, `quantity`: COMPLETED transactions × `COUNT(accepted_scan)` per SKU |
| `sale-events.tsv` | Transaction | `SaleCompleted` outbox rows: `event_id`, `aggregate_key`, `published_at`, decoded lines |
| `selections.tsv` | Notification | `sale_selection` |
| `notifications.tsv` | Notification | `notification` |
| `history.tsv` | Notification | `subscription_change` |
| `parked-events.tsv` | Notification | `dead_letter_event` |

## `ReconcileMain`

`--run-dir=DIR --mode=fixed|history [--report-out=FILE]`

| Check | Fails when |
|---|---|
| Sale events (SC-001) | A COMPLETED transaction has zero or more than one `SaleCompleted`, or its lines differ from `sales.tsv` |
| Fixed subscriptions (FR-023, `fixed` mode) | Any history version exceeds `ready.json`'s version, or a selection's version differs from it, or the subscription state at that version differs from `snapshot.json` |
| History (FR-027, `history` mode) | A selection differs from the history replayed to its version, or its version is below the version confirmed before its Complete, or a failed change appears in the history |
| Missing, not delivered | An owed key has no `DELIVERED` row |
| Missing, delivered but not received | An owed key is `DELIVERED` but has no receipt |
| Unexpected | A row or receipt has no owed key, or a receipt could not be decoded |
| Duplicate effective | More than one notification identity or row for one key, or one identity with two keys |
| Quantity mismatch | A row's or receipt's quantity differs from the owed quantity |
| Parked | Any parked notification or parked event |

Redeliveries are counted and reported, but they never fail the check. The output, `reconciliation.json`, holds `{verdict: "PASS"|"FAIL", mode, counts: {...}, samples: {category: [first 20 keys]}}`, and the exit code is non-zero on FAIL.

## Notification report (`notification-report.json`, rendered as `notification-report.md`)

It is written by the harness for each run, next to the unmodified Load Client JSON (clarification: separate reports).

```text
run:          id, startedAt, deployment commit, subscribers, seed, skusPerSubscriber, skuDistribution,
              loadClient {stations, durationSeconds}, faults (none | reject:<id> | stall:<id>)
sales:        completed, activeWindowSeconds, perSecond, backgroundCompletions
notifications: owed, delivered, received, redeliveries, deliveredPerSecond, receivedPerSecond
latencyMs:    endToEnd {p50, p95, p99, max, n}            # completion → first receipt (N9)
              toDelivered {p50, p95, p99}                 # completion → delivered_at
              endToEndExcluding {subscriberId, p50, p95, p99}   # isolation runs only (SC-006)
deliveryFailures: chargedFailures, unchargedFailures, parked, operatorRetried
backlog:      max {saleEventsUnpublished, saleConsumerLag, notificationsPending, channelLag},
              final {same fields}
drain:        finalCompletionAt, allReceivedAt, drainSeconds, deadlineSeconds: 60, deadlineMet  # SC-007
reconciliation: verdict, counts by category (from reconciliation.json)
```

Percentiles use the nearest-rank method over every owed notification's first receipt. A notification that was never received is excluded from the percentiles and counted as missing.
