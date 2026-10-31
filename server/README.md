# Self-Checkout Microservices

Three independently built and deployed services, each with its own MySQL schema and its own database user. They never call each other: every interaction is a versioned domain event carried by one Apache Kafka broker, written to the producer's transactional outbox first and consumed idempotently. Nginx keeps the unchanged public base URL, `http://localhost:8080`, which the Load Client and the contract tests use. The authoritative contracts are [broker-and-deployment.md](../specs/006-event-based-architecture/contracts/broker-and-deployment.md) and the [event catalog](../specs/006-event-based-architecture/contracts/event-catalog.md); feature 005's [service-deployment.md](../specs/005-microservices-architecture/contracts/service-deployment.md) stays in force except where they change it.

| Service | Artifact / entry point | HTTP port | Public routes | Database (user) |
|---|---|---:|---|---|
| Transaction | `out/transaction.jar`, `transaction.TransactionMain` | 8081 (`TRANSACTION_PORT`) | `POST /transactions`, `POST /transactions/{id}/items`, `POST /transactions/{id}/complete`, `GET /transactions/{id}` | `cs6510_transaction` (`cs6510_transaction_svc`) |
| Inventory | `out/inventory.jar`, `inventory.InventoryMain` | 8082 (`INVENTORY_PORT`) | `GET /items`, `GET /inventory/low-stock` | `cs6510_inventory` (`cs6510_inventory_svc`) |
| Analytics | `out/analytics.jar`, `analytics.AnalyticsMain` | 8083 (`ANALYTICS_PORT`) | `GET /analytics/popular-items` | `cs6510_analytics` (`cs6510_analytics_svc`) |
| Broker | Kafka 4.1.0 at `KAFKA_HOME`, `deploy/kafka/` | 19092 (clients), 19093 (controller) | none | `KAFKA_DATA_DIR` |
| Gateway | `deploy/nginx.conf` | 8080 | the seven routes above | none |

No service has a gRPC port or reads a peer's host or port; the former gRPC ports 9092 and 9093 are gone.

- **Transaction** keeps baskets in its own schema and a durable product replica of Inventory's catalog, so Scan makes no remote call. Complete runs the checkout Saga over events: it records `CheckoutRequested` in its outbox and settles the attempt when Inventory's outcome event arrives, with durable recovery of interrupted attempts. A relay publishes accepted scans as `ScanAccepted` events.
- **Inventory** owns the catalog and stock, serves the catalog and low-stock routes, publishes its whole catalog at every startup, and applies or restores checkouts idempotently from the checkout topic.
- **Analytics** consumes `ScanAccepted`, assigns positions in offset order, and publishes popular-item windows.
- `common.jar` holds only shared HTTP/JSON, JDBC, metrics, the Kafka-free event framework (`events`), and the Kafka adapters (`messaging`). `contracts.jar` holds only the generated event classes (`selfcheckout.events.v1`). Neither contains business logic.

### Topics

| Topic | Partitions | Producer | Consumer (group) | Event types |
|---|---:|---|---|---|
| `selfcheckout.catalog.v1` | 1 | Inventory | Transaction (manual assignment, replayed from offset 0 at every start) | `CatalogItemPublished`, `CatalogSnapshotPublished` |
| `selfcheckout.checkout.v1` | 16 | Transaction | Inventory (`inventory-checkout`) | `CheckoutRequested`, `StockRestorationRequested` |
| `selfcheckout.checkout-outcome.v1` | 16 | Inventory | Transaction (`transaction-checkout-outcome`) | `StockDeducted`, `StockRejected`, `StockDeductionRefused`, `StockRestored`, `RestorationNotNeeded` |
| `selfcheckout.scan-accepted.v1` | 1 | Transaction | Analytics (`analytics-scan-ingest`) | `ScanAccepted` |

A record that cannot be processed after its retries is parked in the consuming service's own `dead_letter_event` table and its offset is committed. The event catalog gives every field, key, and idempotency rule.

## Prerequisites

- JDK 21 (`javac --release 21`). On Windows, the `java` on `PATH` may be Oracle's `javapath` launcher shim; a process manager should use the real `C:\Program Files\Java\jdk-21\bin\java.exe`.
- Maven 3.9 or later, for the contracts module only. The first build needs Maven Central. Afterwards `MAVEN_OFFLINE=1` runs Maven with `-o`.
- MySQL 8.4 listening on `127.0.0.1:3307`.
- The Kafka 4.1.0 binary release unpacked at `KAFKA_HOME`; see [deploy/kafka/README.md](../deploy/kafka/README.md) for installation, operation, and the Windows constraints.
- Nginx, for the public gateway.
- Bash (Git Bash on Windows) for the build, run, and test scripts, and Windows PowerShell for the broker, database, and benchmark scripts.

## Build

From `server/`:

```bash
./build.sh                 # contracts (Maven: event classes + Kafka client) → common → transaction, inventory, analytics
./build.sh transaction     # contracts → common → transaction only
./build.sh inventory       # contracts → common → inventory only
./build.sh analytics       # contracts → common → analytics only
```

1. Maven builds `contracts/pom.xml`. It generates the event classes from `contracts/src/main/proto/selfcheckout/events/v1/**`, the authoritative schemas, writes `out/contracts.jar`, and copies the messaging runtime (`kafka-clients` and its compression libraries, `protobuf-java`, `slf4j-simple`) to `lib/messaging/`, which is git-ignored. It deletes any `lib/grpc/` left by an older build and fails if a gRPC jar reaches the classpath.
2. `javac --release 21` and `jar` build `out/common.jar`, then the requested service jars.

**Independent builds (SC-012)** were verified on 2026-10-30 (T159) with JDK 21.0.2, Maven 3.9.9, and `MAVEN_OFFLINE=1`. Each command ran in a fresh copy of `server/` with no `out/`, `lib/messaging/`, or `contracts/target/`:

| Command | Exit | Jars in `out/` | `lib/messaging/` | `lib/grpc/` |
|---|---:|---|---:|---|
| `./build.sh transaction` | 0 | `common.jar`, `contracts.jar`, `transaction.jar` | 7 jars | absent |
| `./build.sh inventory` | 0 | `common.jar`, `contracts.jar`, `inventory.jar` | 7 jars | absent |
| `./build.sh analytics` | 0 | `common.jar`, `contracts.jar`, `analytics.jar` | 7 jars | absent |

Each service's runtime classpath is its own jar, `out/common.jar`, `out/contracts.jar`, `lib/messaging/*`, and `lib/mysql-connector-j-9.0.0.jar`. No gRPC jar remains.

## Configuration

Every setting comes from the environment. Each service reads only its own `<SERVICE>_DB_*` variables. The former shared `DB_NAME`, `DB_USER`, and `DB_PASSWORD` are never read, so one exported variable cannot give two services the same account.

| Variable | Applies to | Default |
|---|---|---|
| `DB_HOST`, `DB_PORT` | all | `127.0.0.1`, `3307` |
| `TRANSACTION_DB_NAME`, `TRANSACTION_DB_USER`, `TRANSACTION_DB_PASSWORD` | Transaction | `cs6510_transaction`, `cs6510_transaction_svc`, none |
| `TRANSACTION_DB_POOL_SIZE`, `TRANSACTION_DB_POOL_MAX_SIZE` | Transaction | 32, 64 |
| `INVENTORY_DB_NAME`, `INVENTORY_DB_USER`, `INVENTORY_DB_PASSWORD` | Inventory | `cs6510_inventory`, `cs6510_inventory_svc`, none |
| `INVENTORY_DB_POOL_SIZE`, `INVENTORY_DB_POOL_MAX_SIZE` | Inventory | 16, 48 |
| `ANALYTICS_DB_NAME`, `ANALYTICS_DB_USER`, `ANALYTICS_DB_PASSWORD` | Analytics | `cs6510_analytics`, `cs6510_analytics_svc`, none |
| `ANALYTICS_DB_POOL_SIZE`, `ANALYTICS_DB_POOL_MAX_SIZE` | Analytics | 10, 10 |
| `TRANSACTION_PORT`, `INVENTORY_PORT`, `ANALYTICS_PORT` | own service | 8081, 8082, 8083. An optional numeric argument to a run script overrides it. |
| `KAFKA_BOOTSTRAP` | all | `127.0.0.1:19092` |
| `KAFKA_TOPIC_PREFIX` | all | empty. Tests set a per-run prefix so their topics and groups never meet the deployment's. |
| `CHECKOUT_CONSUMER_THREADS` | Transaction (outcome group), Inventory (checkout group) | 8 |
| `CHECKOUT_RESPONSE_BUDGET_MS` | Transaction | 8000 |
| `CHECKOUT_REPUBLISH_AFTER_MS`, `CHECKOUT_REPUBLISH_MAX_MS` | Transaction | 2000, 5000. The recovery worker re-sends a still-pending `CheckoutRequested` after this backoff. |
| `CHECKOUT_STUCK_ALARM_SECONDS` | Transaction | 60. A pending attempt older than this is counted as `stuck_attempts`. |
| `CHECKOUT_RECOVERY_SCAN_SECONDS`, `CHECKOUT_WAIT_BACKSTOP_MS` | Transaction | 30, 1000. These are the recovery safety-scan interval and the waiter's database backstop. Waiting and recovery are otherwise event-driven. |
| `OUTBOX_SWEEP_MS`, `OUTBOX_MARK_BATCH_MS` | Transaction, Inventory | 1000, 20. The dispatcher's sweep for rows a signal missed, and how long it gathers confirmations before marking them published. |
| `OUTBOX_BATCH_SIZE`, `OUTBOX_COALESCE_MS` | Transaction (scan relay) | 500, 0 (off). A CP3 A/B switch: after a partial batch, the relay waits up to `OUTBOX_COALESCE_MS` (at most 1000) before its next round. |
| `SCAN_PRODUCER_LINGER_MS` | Transaction (scan producer) | 5 |
| `CONSUMER_MAX_ATTEMPTS` | all consumers | 5. A record that fails permanently this many times is parked. |
| `TRANSACTION_CACHE_MAX_ENTRIES` | Transaction | 100000. 0 disables the transaction state cache, for A/B runs. |
| `DB_SERVER_PREPARED_STATEMENTS` | all | `false`. A CP3 A/B switch that adds `useServerPrepStmts=true`. |
| `PERF_METRICS`, `PERF_METRICS_INTERVAL_SECONDS` | all | `false`, 10. When enabled, each service prints one JSON metrics line per interval and at shutdown. |
| `STATUS_INTERVAL_SECONDS` | all | 30. The interval of the operational status line (see below). |
| `KAFKA_FLUSH_EVERY_MESSAGE` | broker (`deploy/kafka/start-broker.ps1`) | `false` |
| `KAFKA_HOME`, `KAFKA_DATA_DIR` | broker scripts, benchmark harness, live tests | none (required), `<KAFKA_HOME>\cs6510-data` |

**Removed by feature 006**: `INVENTORY_GRPC_PORT`, `ANALYTICS_GRPC_PORT`, `INVENTORY_GRPC_TARGET`, `ANALYTICS_GRPC_TARGET`, `INVENTORY_RPC_DEADLINE_MS`, `ANALYTICS_RPC_DEADLINE_MS`, and `CATALOG_REFRESH_SECONDS`. `architecture.MicroserviceBoundaryTest` fails if a service source or run script reads any of them or a peer's address.

- **Connection budget**: the maximum pools total 64 + 48 + 10 = 122 connections, below MySQL's default `max_connections` of 151. Check this budget before raising any pool.
- **Port overrides**: the gateway map in `deploy/nginx.conf` assumes 8081, 8082, and 8083. Update it when you move a service's HTTP port. Services find each other only through the broker, so no other setting changes.

### Credential handling

- **Passwords**: come only from `<SERVICE>_DB_PASSWORD` or from the scripts' secure prompts. They reach `mysql` through stdin and `MYSQL_PWD`, never as a command argument. They are never committed or logged, and `DatabaseConfig.toString()` omits them.
- **Admin password**: only `db/bootstrap.ps1` uses MySQL admin credentials, from `MYSQL_ADMIN_PASSWORD` or a prompt. No service and no reset uses them.
- **Grants**: each service user holds `ALL PRIVILEGES` on its own schema only, for both `localhost` and `127.0.0.1`. Any statement on another service's tables, including its `outbox_event`, `dead_letter_event`, and `consumer_offset`, fails with MySQL error 1142 (`integration.DatabaseIsolationTest`). The broker has no database access.

## Database bootstrap, reset, seed, and validation

Run these from the repository root in Windows PowerShell. Each script finds `mysql.exe` on `PATH` or in the standard MySQL install folders, or takes `-MySqlExe`.

| Script | Credentials | Effect |
|---|---|---|
| `db/bootstrap.ps1` | MySQL admin | One-time and idempotent. Creates the three schemas and users, granting each user its own schema only. A rerun keeps the data, resets the passwords, and narrows the grants again. |
| `db/reset-service.ps1 -Service transaction` | `cs6510_transaction_svc` | Drops and recreates Transaction's tables, empty, including `outbox_event` and `dead_letter_event` |
| `db/reset-service.ps1 -Service inventory` | `cs6510_inventory_svc` | Drops and recreates Inventory's tables, then seeds 2,000 products at 10,000 units with threshold 50 and a new `catalog_version` |
| `db/reset-service.ps1 -Service analytics` | `cs6510_analytics_svc` | Drops and recreates Analytics' tables, with both singletons at 0 and no stored consumer offset |
| `db/reset-all.ps1` | each service's own | Runs the three resets in the order Inventory, Transaction, Analytics, then checks the database baseline |
| `db/reset-baseline.ps1` | each service's own | The benchmark baseline (FR-059): refuses while a port 8081–8083 answers, resets the broker with `deploy/kafka/reset-topics.ps1`, runs `db/reset-all.ps1`, and prints and checks the whole baseline (below). Needs `KAFKA_HOME`. |
| `db/validate-invariants.ps1` | each service's own | Runs `db/<service>/validate.sql` per service, reconciles stock, attempts, and events across services, and prints PASS with the FR-061 completion counts. Exits non-zero on any violation. |

The schema, seed, and validation files live in `db/transaction/`, `db/inventory/`, and `db/analytics/`. None of them names another service's schema. Resetting a single service is allowed for tests, but invariants are then judged from that reset point.

**The baseline** that `db/reset-baseline.ps1` checks: 2,000 products, 2,000 stock rows, and 20,000,000 units; zero transactions, checkout attempts, checkout operations, and accepted scans; zero outbox rows, parked events, and stored consumer offsets; Analytics `last_position` 0; all four topics present with their partitions and a total end offset of 0, and no consumer group. After the services start, the only new rows are Inventory's published catalog in its outbox and Transaction's product replica.

## Start, stop, and restart

Start the broker once per boot, then each service in its own Bash terminal from `server/`, in this order:

```bash
powershell -File ../deploy/kafka/start-broker.ps1   # needs KAFKA_HOME; creates the four topics idempotently
./run-inventory.sh      # publishes its catalog at startup
./run-transaction.sh    # loads its durable replica, then replays the catalog topic
./run-analytics.sh
```

- **Startup independence**: any service starts while its peers or the broker are down. The HTTP server starts first; producers are created on their first send and consumers open their streams on their own threads, so they connect and reconnect in the background, and no request waits on the broker for more than `max.block.ms` (1 s). Start, Scan, and Lookup never touch the broker at all.
- **Transaction startup**: Transaction serves its last durable product replica, replaces it once the catalog topic's latest complete version is read, resumes pending checkout attempts, and resumes its outbox dispatcher and scan relay.
- **Analytics startup**: Analytics prewarms its window pipeline, then starts its consumer at the offset stored with its last ingestion commit.
- **Run scripts**: each script checks for its own jar, `common.jar`, `contracts.jar`, `lib/messaging/`, and Connector/J, then replaces the shell with the JVM. It uses the Windows `;` classpath separator under Git Bash.
- **Stop and restart**: stop a service with Ctrl+C in its terminal, and rerun only that script to restart it. Its peers, the broker, and the gateway keep running. A stopped owner yields 502 on its own routes only; while Inventory or the broker is down, Complete answers 500 within 10 s and every other route is unaffected. Events wait in the producer's outbox or on the topic until the consumer returns.
- **Shutdown order**: each shutdown hook stops HTTP, then the consumers (with a final `commitSync` of processed offsets), then the outbox dispatcher (with a final mark), then the background workers (recovery worker, scan relay, status and metrics reporters, the Analytics pipeline), then the producers, then the pool (`architecture.CompositionRootTest`).
- **Broker restart**: `deploy/kafka/stop-broker.ps1`, then `start-broker.ps1`. Data, topics, and committed offsets persist, and the services reconnect on their own.
- **Direct launch**: for a service manager on Windows, run from `server/`:

  ```
  java -cp "out/transaction.jar;out/common.jar;out/contracts.jar;lib/messaging/*;lib/mysql-connector-j-9.0.0.jar" transaction.TransactionMain
  ```

  Substitute the jar and main class for the other two services.

Start Nginx from the repository root on Unix:

```bash
mkdir -p logs temp
nginx -p "$(pwd)/" -c deploy/nginx.conf
# later: nginx -p "$(pwd)/" -c deploy/nginx.conf -s stop
```

On Windows, use an extracted official Nginx for Windows. Create an ASCII-only runtime directory with `conf/`, `logs/`, and `temp/`, and copy `deploy/nginx.conf` to `conf/nginx.conf`. Then run, with your own paths:

```powershell
$nginx = 'C:\path\to\nginx.exe'
$prefix = 'C:\path\to\nginx-runtime'
& $nginx -p "$prefix/" -c conf/nginx.conf -t
Start-Process -FilePath $nginx -WorkingDirectory $prefix -ArgumentList @('-p',"$prefix/",'-c','conf/nginx.conf') -WindowStyle Hidden
# later: & $nginx -p "$prefix/" -c conf/nginx.conf -s stop
```

The gateway sends only the seven routes to their owners, with a 1 s connect timeout, a 30 s read timeout, and `error_page 504 =502`. A handler whose internal work fails answers 500 `{"error":"INTERNAL_ERROR","message":"Internal server error"}`, with no broker, topic, offset, or SQL detail.

## Operational status (FR-046)

Every `STATUS_INTERVAL_SECONDS` (30) each service prints one status line from read-only queries on its own pool, plus its consumer lag:

```
status service=transaction pending_work=no pending_attempts=0 alarmed_attempts=0 unpublished_scans=0 background_completions=0 completions_without_response=0 replica_version=<uuid> replica_products=2000 replica_age_seconds=12 unpublished_outbox=0 parked_events=0 stuck_attempts=0 outcome_consumer_lag=0
status service=inventory applied=4321 rejected=0 restored=0 blocked=0 unpublished_outbox=0 parked_events=0 catalog_version=<uuid> catalog_published_at=<time> checkout_consumer_lag=0
status service=analytics pending_work=no last_position=21604 last_window_end=21500 highest_eligible_boundary=21500 unpublished_windows=0 parked_events=0 consumer_next_offset=21604 scan_consumer_lag=0
```

If a query fails, the line reads `status service=<name> unavailable error=<exception type>`, and the next interval tries again. `deploy/kafka/status.ps1` (or `-Json`) reports the broker's reachability, every topic's end offsets, and each group's lag. The same questions can be asked directly, each with the owning service's credentials:

| Service | Question | Query (summary) |
|---|---|---|
| Transaction | Pending checkout attempts | `SELECT COUNT(*) FROM checkout_attempt WHERE state='PENDING'` |
| Transaction | Alarmed attempts | Pending attempts whose `last_error` starts with `FAILED_PRECONDITION:`, `INVALID_ARGUMENT:`, or `INTEGRITY:`. These are also logged as `[checkout-alarm]`. |
| Transaction | Stuck attempts | Pending attempts older than `CHECKOUT_STUCK_ALARM_SECONDS` |
| Transaction | Unpublished scans | `SELECT COUNT(*) FROM accepted_scan WHERE published_at IS NULL` |
| Transaction, Inventory | Unpublished outbox rows | `SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL` |
| all | Parked events | `SELECT COUNT(*) FROM dead_letter_event` |
| Transaction | Background completions (FR-061) | `SELECT COUNT(*) FROM checkout_attempt WHERE state='COMPLETED' AND client_outcome='PENDING_ERROR'` |
| Transaction | Completions without a recorded response | `… state='COMPLETED' AND client_outcome='NONE'` |
| Transaction | Product replica | Version, count, and `refreshed_at` from `product_replica_meta` |
| Inventory | Operations by state; catalog | `SELECT state, COUNT(*) FROM checkout_operation GROUP BY state`; `catalog_version` and `published_at` from `catalog_meta` |
| Analytics | Stored events; consumer position | `SELECT last_position FROM scan_sequence WHERE sequence_key=1`; `SELECT MAX(next_offset) FROM consumer_offset` |
| Analytics | Eligible windows not yet published | The highest boundary (1,000, 1,500, …) at or below `last_position`, compared with `analytics_checkpoint.last_window_end` |

**Pending work is zero** when there are no pending attempts, no unpublished outbox rows or scans in any service, consumer lag is 0 for all three groups, and Analytics' checkpoint equals its highest eligible boundary. Parked events are reported separately; a benchmark run with any parked event is not comparable.

## Benchmarks

Benchmark baseline procedure:

1. Stop all three services; leave the broker running.
2. Run `db/reset-baseline.ps1`.
3. Start Inventory, then Transaction, then Analytics, then the gateway. Ready means Transaction's `replica_version` equals Inventory's `catalog_version` and every consumer group holds all its partitions.
4. Run the Load Client.
5. Wait until no work is pending.
6. Run `db/validate-invariants.ps1`.

`server/benchmark-event-based.ps1` automates these steps. It needs the three `<SERVICE>_DB_PASSWORD` variables and `KAFKA_HOME`, and it requires ports 8080–8083 to be free and the broker to answer with its four topics:

```powershell
./benchmark-event-based.ps1 -Workload default -OutputLabel cp4-default-1 -NginxBin C:\path\to\nginx.exe -JavaExe 'C:\Program Files\Java\jdk-21\bin\java.exe'
```

- **Workloads**: `default` runs exactly `java -cp out Main`, `stress` runs `java -cp out Main --stations=100 --duration=120`, and `smoke` is a 20 s default run.
- **Run**: it starts the services with `PERF_METRICS=true` and the unchanged `deploy/nginx.conf`. While the client runs, it samples the pending work, unpublished outbox rows, parked events, and `SHOW GLOBAL STATUS` every 2 s, and consumer lag every 10 s. Afterwards it waits for zero pending work, validates, and records the MySQL durability settings, the broker version, the flush setting, and the broker's JDK.
- **Output**: it writes `load-client/reports/event-based-architecture/<label>/`: the byte-identical client report, `samples.csv`, the logs, the validator output, and `evidence.json`.
- **Comparability**: `comparable=true` requires all three services, the exact Load Client command, a drained run with a passing validator, zero parked events, zero final consumer lag, a broker that stayed up, and, when the idle gate is on, an idle machine.
- **Variants**: `-ExtraEnv @{ NAME = 'value' }` passes a variant's settings to every service and to the broker restarted by the reset.

`server/benchmark-microservices.ps1` is feature 005's harness. It stays for the CP0 baseline, which `benchmark-compare.ps1` runs from a worktree of commit `8891c57` with that worktree's own copy.

`server/benchmark-compare.ps1` runs two variants as interleaved pairs (A B, B A, A B, …), each run after its own reset, and writes `compare/<label>/compare.json` and `compare.md`. `-BaselineCommit 8891c57` (or `-BaselineWorktree`) runs variant A as the feature 005 baseline. It runs exactly `-Pairs` pairs (default 1). Δ is variant B's `totalTransactions` median against variant A's, and differences within ±3% are practically equivalent.

1. **Screening** (one pair): promising at Δ ≥ +5%, regression at Δ ≤ −3%, otherwise no demonstrated improvement.
2. **Confirmation** (three fresh pairs, only for a promising variant): improvement at Δ ≥ +3% with B ahead in at least two pairs; otherwise no demonstrated improvement or regression. Close results are not extended.
3. **Latency veto**: a Scan or Complete p99 median more than 10% above A's is flagged as a latency regression. Resource use is reported only.

Before each run an idle-machine gate waits until no `-BlockingProcess` runs, no other program holds a benchmark port, and background CPU stays at or below `-IdleCpuCores` (1.0). On the shared benchmark machine use `-IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR`. A loaded or failed-to-start run is moved to `compare/<label>/invalidated/` and repeated. Statistical significance is never claimed: one pair screens and three pairs confirm a clear effect.

Checkpoints CP0–CP4 of feature 006 are recorded in [checkpoints.md](../load-client/reports/event-based-architecture/checkpoints.md). Every checkpoint reports the client-observed `totalTransactions` against the CP0 baseline. Fewer statements or lower latency never count as an improvement on their own.

**MySQL durability settings are retained unchanged.** No service or script changes a MySQL server variable: `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, and binary logging stay as configured, and each benchmark records them.

## Tests

```bash
./build-tests.sh
./run-tests.sh                                          # unit, architecture, database, messaging, and process-level tests
SERVER_BASE_URL=http://localhost:8080 ./run-tests.sh    # plus the live contract and gateway tests
```

- **Admin credentials**: disposable test schemas and users are created with the MySQL admin credentials from `DB_USER` and `DB_PASSWORD`, which `run-tests.sh` passes as `-DDB_USER` and `-DDB_PASS`. They are dropped after each test. Services never read these variables.
- **Broker**: live tests use the broker at `KAFKA_TEST_BOOTSTRAP` (default `127.0.0.1:19092`) with per-run topic prefixes, so they never touch the deployment's topics. `KAFKA_TEST_BROKER_CONTROL=1` (with `KAFKA_HOME`) lets the broker-restart tests stop and start the native broker.
- **Live runs**: with `SERVER_BASE_URL` set, the three `<SERVICE>_DB_PASSWORD` variables must be set, and `NGINX_BIN` must point to `nginx.exe` for the gateway isolation tests. A skipped live test in `contract.`, `integration.`, or `performance.` fails the run; `STRICT_LIVE_GATES=1` also fails a skipped `messaging.` test.
- **Reports**: JUnit XML goes to `out/test-results/`. The direct restoration tests (`messaging.RestorationEventDirectTest`) are counted apart from the end-to-end results (SC-016).
- **Reset baseline tests**: `integration.ResetBaselineTest` checks without side effects that `db/reset-baseline.ps1` refuses while a service port answers. Its other two methods are destructive and disabled by default. With the deployment stopped, `-DRUN_RESET_BASELINE_TEST=true` runs the three per-service resets with only their own credentials, starts fresh services, and verifies the baseline they serve; with `KAFKA_TEST_BROKER_CONTROL=1` as well, it also runs `db/reset-baseline.ps1`, which resets the broker, and checks that every topic is empty.

**Coverage ledger and parity gates (FR-065)**: [tests/COVERAGE-LEDGER.md](tests/COVERAGE-LEDGER.md) records every test method from before each redesign, where its assertions live now, and the gate that last verified it. Feature 005's section ends at commit `26c89af`; feature 006's section starts from commit `8891c57`. A story gate runs the whole suite with the ledger check for that story:

```bash
PARITY_GATE=006:US7 STRICT_LIVE_GATES=1 ./run-tests.sh
```

`architecture.CoverageParityLedgerTest` fails while a row owned by that gate or an earlier one is `PENDING`, or while an earlier gate's row lacks `Verified at`. `PARITY_GATE=006:final` requires every feature 006 row to be verified; gates without the `006:` prefix check the feature 005 section.
