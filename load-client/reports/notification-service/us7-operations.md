# Operating and resetting Notification independently (T110–T117, US7, SC-012)

Feature 007, phase 9 (User Story 7), run on 2026-11-13 on branch `feature/notification-service`, on top of `1a6721a`. The changes under test were uncommitted and were committed right after these runs. Evidence files are in [us7-operations/](us7-operations/).

| Step | Outcome |
|---|---|
| T110 `integration.DatabaseIsolationTest` | **PASS** (5 of 5). New: `theNotificationUserAndTheOtherThreeAreDeniedEachOthersSchemas` over disposable schemas, and the same against the deployed users |
| T111 `integration.IndependentServiceDeploymentTest` | **PASS** (3 of 3). New: `notificationBuildsAloneAndStartsWithOnlyTheBrokerAndMySql` |
| T112 `integration.ResetBaselineTest` with `-DRUN_RESET_BASELINE_TEST=true` and `KAFKA_TEST_BROKER_CONTROL=1` | **PASS** (3 of 3), including both destructive methods |
| T113 `integration.RestartDuringWorkloadTest` | **PASS**: the new Notification restart, and the 006 restart of each service. The 006 broker-restart method was skipped in that run, because it needs `KAFKA_TEST_BROKER_CONTROL=1`; it ran in the gate below |
| T114 scripts | `deploy/kafka/status.ps1` reports `notification-sale-completed`, and the 006 harnesses refuse to start while port 8084 is in use |
| T115, T116 documentation | Notification section in [server/README.md](../../../server/README.md#notification-service-feature-007), [deploy/kafka/README.md](../../../deploy/kafka/README.md#topics) updated, and [subscriber-client/README.md](../../../subscriber-client/README.md) written |
| Quickstart §1 (T117) | **PASS**: every expected result ([transcript](us7-operations/quickstart-1.txt)) |
| `server/run-tests.sh architecture` | **PASS**: 44 tests, 43 successful, and 1 skipped by design (`PARITY_GATE` unset) |
| `server/run-tests.sh unit database integration messaging` (with `KAFKA_TEST_BROKER_CONTROL=1`, `KAFKA_HOME`, `NGINX_BIN`, no `SERVER_BASE_URL`) | 512 tests, 2 failed, 6 skipped. The skipped tests are the four live-server suites and the two destructive reset methods; the destructive methods passed in their own run above. The failures are below. |

**The two gate failures.**
- `messaging.KafkaTransportLiveTest.sendsCompleteAfterAcksAllAndTheIdempotentProducerKeepsPerKeyOrder` is the known flaky test (a `TimeoutException` on a freshly created topic). It also failed in 006 and in phases 7 and 8, and it is unrelated to US7.
- `integration.NotificationOutageCheckoutTest` (phase 8's T104) counted 14 Transaction commits where the comparison run had 13. This was a race in the test, not in the services. Transaction's outbox marker gathers confirmations for up to 20 ms before marking them published, so a sale's `CheckoutRequested` and `SaleCompleted` marks share one commit or take two depending on timing alone. The test now counts commits on every thread except `transaction-outbox-marker`. It still compares every row and requires each one published before the next request. It passed 5 of 5 times afterwards.

## US7 acceptance scenarios

**1. Independent build and start (T111, FR-034).**
- `build.sh`'s `notification)` branch calls `build_service` for `notification` only.
- `out/notification.jar` has `Main-Class: notification.NotificationMain` and no class of `transaction/`, `inventory/`, or `analytics/`.
- `run-notification.sh` runs that jar with no peer artifact.
- Notification was started as its own JVM with nothing but MySQL and the broker. It used a disposable schema whose own user holds only `SELECT, INSERT, UPDATE, DELETE`, as `db/bootstrap.sql` grants. It answered `GET /notifications/ops/status` with 200 at `subscriptionVersion` 0, and answered `GET /notifications/subscribers/nobody` with 404.

**2. Credential isolation in both directions (T110, FR-035).**
- Over four disposable schemas with bootstrap-equivalent users, Notification's user was denied, with MySQL error 1142, every statement on the other three schemas.
- Each of the other three users was denied every statement on Notification's schema.
- The statements tried were a read, an insert, an update, a delete, and a read and a no-op delete of every table, plus `CREATE TABLE`, `DROP TABLE IF EXISTS`, and `ALTER TABLE`.
- Notification's user can read and write its own rows but cannot change its own schema (the same three DDL statements are denied).
- Row counts, table counts, and table comments in all four schemas were unchanged afterwards.
- The deployed users (`cs6510_*_svc`, from the user-level `*_DB_PASSWORD` variables) were denied the same DML in both directions.

**3. The baseline (T112, FR-036).** Before running `db/reset-baseline.ps1 -RunDir <tmp>`, the test created a leftover channel topic `selfcheckout.notify.reset-probe-<id>.v1` and a run directory with a receipt file. Afterwards:
- the broker held exactly the five fixed topics, each with end offsets of 0;
- the leftover channel and the run directory were gone;
- Notification's `subscription_clock.version` was 0, and all six of its other tables were empty;
- every 006 baseline assertion still held.

`resetProducesExactBaselineForFreshServer` (006) also passed.

**4. A restart during a workload (T113, SC-012).**
- 10 subscribers were registered through Notification's HTTP route.
- Six stations ran Start, three to six Scans, and Complete for 30 s, through Transaction, Inventory, and Analytics running as JVMs.
- Notification was killed 10 s in and started again 5 s later.
- While it was down, the other three services answered their probe routes and the workload kept being served. All 13,313 requests of the workload succeeded.
- After the drain, the run had 2,044 completed sales and 28,952 owed notifications (one per sale, sold SKU, and subscriber of that SKU):
  - each sale was selected once;
  - every owed notification was `DELIVERED`, with one row each;
  - nothing was parked;
  - each subscriber's channel held exactly its owed notification IDs.
- 23 copies were redelivered, and the subscriber keeps the first by ID. Those are notifications that reached the broker before the kill but were not yet marked delivered, so they were sent again after the restart.

## Quickstart §1

```bash
server/build.sh all              # exit 0 (before this phase, "all" was rejected with a usage error; now accepted)
subscriber-client/build.sh       # exit 0
```

```powershell
us7-operations/quickstart-1.ps1 -Repo <repository> -NginxBin <nginx.exe> -KafkaHome <KAFKA_HOME> -Out <dir>
```

[`quickstart-1.ps1`](us7-operations/quickstart-1.ps1):
1. Runs `db/reset-baseline.ps1 -RunDir <dir>/runs/smoke` against a run directory holding a receipt ([output](us7-operations/reset-baseline.txt)).
2. Starts Inventory, Transaction, Analytics, and Notification from their jars, then Nginx from `deploy/nginx.conf`, with `server/benchmark-common.ps1`.
3. Checks the expected results, then stops everything. The broker was already running.

| Expected | Observed |
|---|---|
| The reset passes and removes the run directory | Exit 0. The baseline table shows Notification's version 0, every Notification count 0, 5 fixed topics, and 0 channel topics. The directory is gone. |
| `GET :8080/notifications/ops/status`: `catalogLoaded: true`, `subscriptionVersion: 0` | 200, `catalogLoaded=True`, `subscriptionVersion=0`, `activeSubscribers=0` |
| `deploy/kafka/status.ps1` lists five topics | The catalog (1 partition), checkout (16), checkout-outcome (16), scan-accepted (1), and sale-completed (16) topics. All four groups are stable with lag 0. |
| Every 006 route answers as before | `GET /items` 200 (2,000 items), `GET /inventory/low-stock` 200 (no alerts), `GET /analytics/popular-items` 200, `GET /transactions/{unknown}` 404, `POST /transactions` 201, `POST …/items` 200, `GET /transactions/{id}` 200, `POST …/complete` 200 |
| (extra) Notification sees the smoke sale | `salesSelected=1`, nothing pending |

## Changes in this phase

- **Production scripts**: `deploy/kafka/kafka-tools.ps1` lists `notification-sale-completed` as the fourth group. `status.ps1` therefore reports its lag, and `reset-topics.ps1` checks that it is gone after a reset. `server/benchmark-event-based.ps1` and `server/benchmark-compare.ps1` include port 8084 in their port checks; `benchmark-notification.ps1` gets the same check through `benchmark-compare.ps1`'s idle gate whenever that gate is on. `server/build.sh` accepts `all`, as quickstart §1 writes it. No Java production code changed.
- **Test support**:
  - `TestSchema.ownPrivileges()` gives a disposable Notification user bootstrap's DML-only grant. The 006 schemas keep `ALL PRIVILEGES`.
  - `ServiceProcesses` starts `notification` (with `NOTIFICATION_HTTP_PORT`) and scrubs `NOTIFICATION_DB_PASSWORD` from every child.
  - `LiveSchemas.notification()` is new.
  - `ResetBaselineTest` reads `reset-baseline.ps1`'s output from a file instead of a pipe. The broker that the reset restarts inherits the script's output handles, so a pipe would not close while the broker runs.
- **Ledger**: `server/tests/COVERAGE-LEDGER.md`, Feature 007, "Phase 9", records the changed 006 test (`ResetBaselineTest`) and the added ones.
