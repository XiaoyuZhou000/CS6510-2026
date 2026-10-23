# Self-Checkout Microservices

Three independently built and deployed services, each with its own MySQL schema and its own database user. They talk to each other only through versioned gRPC contracts. Nginx keeps the unchanged public base URL, `http://localhost:8080`, which the Load Client and the contract tests use. The authoritative deployment contract is [service-deployment.md](../specs/005-microservices-architecture/contracts/service-deployment.md).

| Service | Artifact / entry point | HTTP port | gRPC port | Public routes | Database (user) |
|---|---|---:|---:|---|---|
| Transaction | `out/transaction.jar`, `transaction.TransactionMain` | 8081 (`TRANSACTION_PORT`) | none (client only) | `POST /transactions`, `POST /transactions/{id}/items`, `POST /transactions/{id}/complete`, `GET /transactions/{id}` | `cs6510_transaction` (`cs6510_transaction_svc`) |
| Inventory | `out/inventory.jar`, `inventory.InventoryMain` | 8082 (`INVENTORY_PORT`) | 9092 (`INVENTORY_GRPC_PORT`) | `GET /items`, `GET /inventory/low-stock` | `cs6510_inventory` (`cs6510_inventory_svc`) |
| Analytics | `out/analytics.jar`, `analytics.AnalyticsMain` | 8083 (`ANALYTICS_PORT`) | 9093 (`ANALYTICS_GRPC_PORT`) | `GET /analytics/popular-items` | `cs6510_analytics` (`cs6510_analytics_svc`) |
| Gateway | `deploy/nginx.conf` | 8080 | none | the seven routes above | none |

- **Transaction** keeps baskets in its own schema and a durable product replica of Inventory's catalog, so Scan makes no remote call. Complete runs the checkout Saga: exactly one `ApplyCheckout` call to Inventory per attempt, with durable recovery of interrupted attempts. An outbox relay delivers accepted scans to Analytics.
- **Inventory** owns the catalog and stock, serves the catalog and low-stock routes, and applies or restores checkouts idempotently over gRPC.
- **Analytics** ingests delivered scans over gRPC, assigns their positions, and publishes popular-item windows.
- `common.jar` holds only shared HTTP/JSON, JDBC, gRPC, and metrics utilities, and `contracts.jar` holds only the generated gRPC stubs. Neither contains business logic.

## Prerequisites

- JDK 21 (`javac --release 21`). On Windows, the `java` on `PATH` may be Oracle's `javapath` launcher shim; a process manager should use the real `C:\Program Files\Java\jdk-21\bin\java.exe`.
- Maven 3.9 or later, for the contracts module only. The first build needs Maven Central. Afterwards `MAVEN_OFFLINE=1` runs Maven with `-o`.
- MySQL 8.4 listening on `127.0.0.1:3307`.
- Nginx, for the public gateway.
- Bash (Git Bash on Windows) for the build, run, and test scripts, and Windows PowerShell for the database and benchmark scripts.

## Build

From `server/`:

```bash
./build.sh                 # contracts (Maven) → common → transaction, inventory, analytics
./build.sh transaction     # contracts → common → transaction only
./build.sh inventory       # contracts → common → inventory only
./build.sh analytics       # contracts → common → analytics only
```

1. Maven builds `contracts/pom.xml`. It generates the stubs from `contracts/src/main/proto/**`, which are the authoritative proto files. It writes `out/contracts.jar` and copies the pinned gRPC runtime to `lib/grpc/`, which is git-ignored.
2. `javac --release 21` and `jar` build `out/common.jar`, then the requested service jars.

**Independent builds (FR-050)** were verified on 2026-10-21 (T121) with JDK 21.0.2, Maven 3.9.9, and `MAVEN_OFFLINE=1`. Each command ran in a fresh copy of `server/` with no `out/`, `lib/grpc/`, or `contracts/target/`:

| Command | Exit | Jars in `out/` | `lib/grpc/` |
|---|---:|---|---:|
| `./build.sh transaction` | 0 | `common.jar`, `contracts.jar`, `transaction.jar` | 22 jars |
| `./build.sh inventory` | 0 | `common.jar`, `contracts.jar`, `inventory.jar` | 22 jars |
| `./build.sh analytics` | 0 | `common.jar`, `contracts.jar`, `analytics.jar` | 22 jars |

Each service's runtime classpath is its own jar, `out/common.jar`, `out/contracts.jar`, `lib/grpc/*`, and `lib/mysql-connector-j-9.0.0.jar`.

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
| `ANALYTICS_DB_POOL_SIZE` | Analytics | 10 |
| `TRANSACTION_PORT`, `INVENTORY_PORT`, `ANALYTICS_PORT` | own service | 8081, 8082, 8083. An optional numeric argument to a run script overrides it. |
| `INVENTORY_GRPC_PORT`, `ANALYTICS_GRPC_PORT` | own service | 9092, 9093 |
| `INVENTORY_GRPC_TARGET`, `ANALYTICS_GRPC_TARGET` | Transaction | `127.0.0.1:9092`, `127.0.0.1:9093` |
| `CHECKOUT_RESPONSE_BUDGET_MS`, `INVENTORY_RPC_DEADLINE_MS` | Transaction | 8000, 2000 |
| `CHECKOUT_RECOVERY_SCAN_SECONDS`, `CHECKOUT_WAIT_BACKSTOP_MS` | Transaction | 30, 1000. These are the recovery safety-scan interval and the waiter's database backstop. Waiting and recovery are otherwise event-driven. |
| `CATALOG_REFRESH_SECONDS` | Transaction | 30 |
| `OUTBOX_BATCH_SIZE`, `ANALYTICS_RPC_DEADLINE_MS` | Transaction | 500, 5000 |
| `OUTBOX_COALESCE_MS` | Transaction | 0 (off). A CP3 A/B switch: after a partial batch, the relay waits this long before its next round, so rounds carry more events. At most 1000. |
| `TRANSACTION_CACHE_MAX_ENTRIES` | Transaction | 100000. 0 disables the transaction state cache, for A/B runs. |
| `DB_SERVER_PREPARED_STATEMENTS` | all | `false`. A CP3 A/B switch that adds `useServerPrepStmts=true`. |
| `PERF_METRICS`, `PERF_METRICS_INTERVAL_SECONDS` | all | `false`, 10. When enabled, each service prints one JSON metrics line per interval and at shutdown. This replaces the former `SCAN_PROFILE`. |
| `STATUS_INTERVAL_SECONDS` | all | 30. The interval of the operational status line (see below). |

- **Connection budget**: the maximum pools total 64 + 48 + 10 = 122 connections, below MySQL's default `max_connections` of 151. Check this budget before raising any pool.
- **Port overrides**: the gateway map in `deploy/nginx.conf` assumes 8081, 8082, and 8083. Update it when you move a service's HTTP port, and set `INVENTORY_GRPC_TARGET` or `ANALYTICS_GRPC_TARGET` when you move a gRPC port.

### Credential handling

- **Passwords**: come only from `<SERVICE>_DB_PASSWORD` or from the scripts' secure prompts. They reach `mysql` through stdin and `MYSQL_PWD`, never as a command argument. They are never committed or logged, and `DatabaseConfig.toString()` omits them.
- **Admin password**: only `db/bootstrap.ps1` uses MySQL admin credentials, from `MYSQL_ADMIN_PASSWORD` or a prompt. No service and no reset uses them.
- **Grants**: each service user holds `ALL PRIVILEGES` on its own schema only, for both `localhost` and `127.0.0.1`. Any statement on another service's tables fails with MySQL error 1142 (`integration.DatabaseIsolationTest`).

## Database bootstrap, reset, seed, and validation

Run these from the repository root in Windows PowerShell. Each script finds `mysql.exe` on `PATH` or in the standard MySQL install folders, or takes `-MySqlExe`.

| Script | Credentials | Effect |
|---|---|---|
| `db/bootstrap.ps1` | MySQL admin | One-time and idempotent. Creates the three schemas and users, granting each user its own schema only. A rerun keeps the data, resets the passwords, and narrows the grants again. |
| `db/reset-service.ps1 -Service transaction` | `cs6510_transaction_svc` | Drops and recreates Transaction's tables, empty |
| `db/reset-service.ps1 -Service inventory` | `cs6510_inventory_svc` | Drops and recreates Inventory's tables, then seeds 2,000 products at 10,000 units with threshold 50 and a new `catalog_version` |
| `db/reset-service.ps1 -Service analytics` | `cs6510_analytics_svc` | Drops and recreates Analytics' tables, with both singletons at 0 |
| `db/reset-all.ps1` | each service's own | Runs the three resets in the order Inventory, Transaction, Analytics, then checks the baseline: 2,000 / 2,000 / 20,000,000, zero transactions, and zero events |
| `db/validate-invariants.ps1` | each service's own | Runs `db/<service>/validate.sql` per service, reconciles stock, attempts, and events across services, and prints PASS with the FR-054 completion counts. Exits non-zero on any violation. |

The schema, seed, and validation files live in `db/transaction/`, `db/inventory/`, and `db/analytics/`. None of them names another service's schema. Resetting a single service is allowed for tests, but invariants are then judged from that reset point.

## Start, stop, and restart

Start each service in its own Bash terminal from `server/`, in this order:

```bash
./run-inventory.sh
./run-transaction.sh
./run-analytics.sh
```

- **Startup independence**: any service starts while its peers are down, because gRPC channels connect lazily.
- **Transaction startup**: Transaction serves its last durable product replica, refreshes it once Inventory is reachable, resumes pending checkout attempts, and resumes the outbox relay.
- **Analytics startup**: Analytics prewarms its window pipeline before its gRPC server accepts deliveries.
- **Run scripts**: each script checks for its own jar, `common.jar`, `contracts.jar`, `lib/grpc/`, and Connector/J, then replaces the shell with the JVM. It uses the Windows `;` classpath separator under Git Bash.
- **Stop and restart**: stop a service with Ctrl+C in its terminal, and rerun only that script to restart it. Its peers and the gateway keep running. A stopped owner yields 502 on its own routes only. Each shutdown hook stops HTTP, then gRPC, then the background workers, channels, and reporters, then the pool.
- **Direct launch**: for a service manager on Windows, run from `server/`:

  ```
  java -cp "out/transaction.jar;out/common.jar;out/contracts.jar;lib/grpc/*;lib/mysql-connector-j-9.0.0.jar" transaction.TransactionMain
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

The gateway sends only the seven routes to their owners, with a 1 s connect timeout, a 30 s read timeout, and `error_page 504 =502`. A handler whose internal work fails answers 500 `{"error":"INTERNAL_ERROR","message":"Internal server error"}`, with no gRPC or SQL detail.

## Operational status (FR-039)

Every `STATUS_INTERVAL_SECONDS` (30) each service prints one status line from read-only queries on its own pool:

```
status service=transaction pending_work=no pending_attempts=0 alarmed_attempts=0 undelivered_scans=0 background_completions=0 completions_without_response=0 replica_version=<uuid> replica_products=2000 replica_age_seconds=12
status service=inventory applied=4321 rejected=0 restored=0 blocked=0
status service=analytics pending_work=no last_position=21604 last_window_end=21500 highest_eligible_boundary=21500 unpublished_windows=0
```

If a query fails, the line reads `status service=<name> unavailable error=<exception type>`, and the next interval tries again. The same questions can be asked directly, each with the owning service's credentials:

| Service | Question | Query (summary) |
|---|---|---|
| Transaction | Pending checkout attempts | `SELECT COUNT(*) FROM checkout_attempt WHERE state='PENDING'` |
| Transaction | Alarmed attempts | Pending attempts whose `last_error` starts with `FAILED_PRECONDITION:`, `INVALID_ARGUMENT:`, or `INTEGRITY:`. These are also logged as `[checkout-alarm]`. |
| Transaction | Undelivered events | `SELECT COUNT(*) FROM accepted_scan WHERE delivered_at IS NULL` |
| Transaction | Background completions (FR-054) | `SELECT COUNT(*) FROM checkout_attempt WHERE state='COMPLETED' AND client_outcome='PENDING_ERROR'` |
| Transaction | Completions without a recorded response | `… state='COMPLETED' AND client_outcome='NONE'` |
| Transaction | Product replica | Version, count, and `refreshed_at` from `product_replica_meta`. The age counts from the last replacement, so it grows while Inventory's catalog version is unchanged. |
| Inventory | Operations by state | `SELECT state, COUNT(*) FROM checkout_operation GROUP BY state` |
| Analytics | Stored events | `SELECT last_position FROM scan_sequence WHERE sequence_key=1` |
| Analytics | Eligible windows not yet published | The highest boundary (1,000, 1,500, …) at or below `last_position`, compared with `analytics_checkpoint.last_window_end` |

**Pending work is zero** when Transaction has no pending attempts and no undelivered events, and Analytics' checkpoint equals the highest eligible boundary. Inventory holds no pending work, because every operation state is final.

## Benchmarks

Benchmark baseline procedure:

1. Stop all three services.
2. Run `db/reset-all.ps1`.
3. Start Inventory, then Transaction (it loads the new replica at startup), then Analytics, then confirm the gateway.
4. Run the Load Client.
5. Wait until the status lines show no pending work.
6. Run `db/validate-invariants.ps1`.

`server/benchmark-microservices.ps1` automates these steps. It needs the three `<SERVICE>_DB_PASSWORD` variables, and it requires ports 8080–8083, 9092, and 9093 to be free:

```powershell
./benchmark-microservices.ps1 -Workload default -OutputLabel cp4-default-1 -NginxBin C:\path\to\nginx.exe -JavaExe 'C:\Program Files\Java\jdk-21\bin\java.exe'
```

- **Workloads**: `default` runs exactly `java -cp out Main`, `stress` runs `java -cp out Main --stations=100 --duration=120`, and `smoke` is a 20 s default run.
- **Run**: it starts the services with `PERF_METRICS=true` and the unchanged `deploy/nginx.conf`. While the client runs, it samples the pending work and `SHOW GLOBAL STATUS` every 2 s. Afterwards it waits for zero pending work, validates, and records `innodb_flush_log_at_trx_commit`, `sync_binlog`, and `log_bin`.
- **Output**: it writes `load-client/reports/microservices-architecture/<label>/`: the byte-identical client report, `samples.csv`, the logs, the validator output, and `evidence.json`.
- **Comparability**: `comparable=true` requires all three services, the exact Load Client command, and a passing validator.
- **Variants**: `-ExtraEnv @{ NAME = 'value' }` passes a variant's settings to every service.

`server/benchmark-compare.ps1` runs two variants as interleaved pairs (A B, B A, A B, …), each run after its own reset, and writes `compare/<label>/compare.json` and `compare.md`. `-BaselineWorktree` runs variant A as the CP0 baseline from a worktree of commit `26c89af`. It runs exactly `-Pairs` pairs (default 1) and classifies the result by the R22 rule. Δ is variant B's `totalTransactions` median against variant A's, and differences within ±3% are practically equivalent.

1. **Screening** (one pair): promising at Δ ≥ +5%, regression at Δ ≤ −3%, otherwise no demonstrated improvement.
2. **Confirmation** (three fresh pairs, only for a promising variant): improvement at Δ ≥ +3% with B ahead in at least two pairs; otherwise no demonstrated improvement or regression. Close results are not extended.
3. **Latency veto**: a Scan or Complete p99 median more than 10% above A's is flagged as a latency regression. Resource use is reported only.

Before each run an idle-machine gate waits until no `-BlockingProcess` runs, no other program holds a benchmark port, and background CPU stays at or below `-IdleCpuCores` (1.0). A loaded or failed-to-start run is moved to `compare/<label>/invalidated/` and repeated. Rerunning an interrupted comparison with the same label keeps its complete pairs. Statistical significance is never claimed: one pair screens and three pairs confirm a clear effect.

Checkpoints CP0–CP4 are recorded in [checkpoints.md](../load-client/reports/microservices-architecture/checkpoints.md). CP1–CP3 attribute costs, and only the interleaved CP0-versus-CP4 runs form the final comparison. Every checkpoint reports the client-observed `totalTransactions` against the CP0 baseline. Fewer statements or lower latency never count as an improvement on their own.

**MySQL durability settings are retained unchanged.** No service or script changes a MySQL server variable: `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, and binary logging stay as configured, and each benchmark records them.

## Tests

```bash
./build-tests.sh
./run-tests.sh                                          # unit, architecture, database, and process-level tests
SERVER_BASE_URL=http://localhost:8080 ./run-tests.sh    # plus the live contract and gateway tests
```

- **Admin credentials**: disposable test schemas and users are created with the MySQL admin credentials from `DB_USER` and `DB_PASSWORD`, which `run-tests.sh` passes as `-DDB_USER` and `-DDB_PASS`. They are dropped after each test. Services never read these variables.
- **Live runs**: with `SERVER_BASE_URL` set, the three `<SERVICE>_DB_PASSWORD` variables must be set, and `NGINX_BIN` must point to `nginx.exe` for the gateway isolation tests. A skipped live test in `contract.`, `integration.`, `rpc.`, or `performance.` fails the run.
- **Reports**: JUnit XML goes to `out/test-results/`.
- **Reset baseline test**: `integration.ResetBaselineTest` is destructive and disabled by default. With the deployment stopped, pass `-DRUN_RESET_BASELINE_TEST=true` to `run-tests.sh` to run the three per-service resets with only their own credentials, start fresh services on unused ports, and verify the baseline they serve.

**Coverage ledger and parity gates (FR-057)**: [tests/COVERAGE-LEDGER.md](tests/COVERAGE-LEDGER.md) records every test method from before the redesign, where its assertions live now, and the gate that last verified it. Parked legacy tests live in `tests-legacy/`, which is never compiled. A story gate runs the whole suite with the ledger check for that story:

```bash
PARITY_GATE=US6 SERVER_BASE_URL=http://localhost:8080 ./run-tests.sh
```

`architecture.CoverageParityLedgerTest` fails while a row owned by that gate or an earlier one is `PENDING`, or while an earlier gate's row lacks `Verified at`. `PARITY_GATE=final` requires every row to be verified.
