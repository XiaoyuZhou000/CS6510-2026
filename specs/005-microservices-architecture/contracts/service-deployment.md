# Service Deployment Contract

**Requirements**: FR-001, FR-004, FR-008, FR-015, FR-039, FR-050–FR-055 | **Decisions**: R1, R3, R4, R13, R16

## Units

| Unit | Build output | Entry point | HTTP port | gRPC port | Database (user) |
|---|---|---|---:|---:|---|
| Transaction | `server/out/transaction.jar` | `transaction.TransactionMain` | 8081 (`TRANSACTION_PORT`) | none (client only) | `cs6510_transaction` (`cs6510_transaction_svc`) |
| Inventory | `server/out/inventory.jar` | `inventory.InventoryMain` | 8082 (`INVENTORY_PORT`) | 9092 (`INVENTORY_GRPC_PORT`) | `cs6510_inventory` (`cs6510_inventory_svc`) |
| Analytics | `server/out/analytics.jar` | `analytics.AnalyticsMain` | 8083 (`ANALYTICS_PORT`) | 9093 (`ANALYTICS_GRPC_PORT`) | `cs6510_analytics` (`cs6510_analytics_svc`) |
| Gateway | `deploy/nginx.conf` | Nginx | 8080 | none | none |

- **Runtime classpath** for each service: its own jar, `out/common.jar`, `out/contracts.jar`, `lib/grpc/*`, and `lib/mysql-connector-j-9.0.0.jar`. `common.jar` and `contracts.jar` contain no business logic (FR-003; checked by architecture tests).
- **Startup independence**: any service starts while its peers are down. Channels connect lazily.
- **Transaction startup**: Transaction serves its last durable product replica, refreshes it when Inventory becomes reachable, resumes PENDING attempts, and resumes the outbox relay.

## Build (R1)

```bash
cd server
./build.sh                 # contracts (Maven) → common → transaction, inventory, analytics
./build.sh transaction     # contracts → common → transaction only
./build.sh inventory
./build.sh analytics
```

- **Step 1**: `build.sh` runs Maven on `server/contracts/pom.xml`. That module generates stubs from `src/main/proto/**`, writes `out/contracts.jar`, and copies the pinned runtime dependencies to `lib/grpc/`, which is git-ignored.
- **Step 2**: the existing `javac --release 21` and `jar` steps follow.
- **Prerequisites**: JDK 21 and Maven 3.9 or later. The first build needs access to Maven Central; afterwards `MAVEN_OFFLINE=1` runs Maven with `-o`.

## Configuration

| Variable | Applies to | Default |
|---|---|---|
| `DB_HOST`, `DB_PORT` | all | `127.0.0.1`, `3307` |
| `TRANSACTION_DB_NAME`, `TRANSACTION_DB_USER`, `TRANSACTION_DB_PASSWORD` | Transaction | `cs6510_transaction`, `cs6510_transaction_svc`, none |
| `TRANSACTION_DB_POOL_SIZE`, `TRANSACTION_DB_POOL_MAX_SIZE` | Transaction | 32, 64 |
| `INVENTORY_DB_NAME`, `INVENTORY_DB_USER`, `INVENTORY_DB_PASSWORD` | Inventory | `cs6510_inventory`, `cs6510_inventory_svc`, none |
| `INVENTORY_DB_POOL_SIZE`, `INVENTORY_DB_POOL_MAX_SIZE` | Inventory | 16, 48 |
| `ANALYTICS_DB_NAME`, `ANALYTICS_DB_USER`, `ANALYTICS_DB_PASSWORD` | Analytics | `cs6510_analytics`, `cs6510_analytics_svc`, none |
| `ANALYTICS_DB_POOL_SIZE` | Analytics | 10 |
| `INVENTORY_GRPC_TARGET`, `ANALYTICS_GRPC_TARGET` | Transaction | `127.0.0.1:9092`, `127.0.0.1:9093` |
| `CHECKOUT_RESPONSE_BUDGET_MS`, `INVENTORY_RPC_DEADLINE_MS` | Transaction | 8000, 2000 |
| `CHECKOUT_RECOVERY_SCAN_SECONDS`, `CHECKOUT_WAIT_BACKSTOP_MS` | Transaction | 30, 1000. The recovery safety-scan interval, and the waiter's database backstop interval. Waiting and recovery are otherwise event-driven (R7). |
| `CATALOG_REFRESH_SECONDS` | Transaction | 30 |
| `OUTBOX_BATCH_SIZE`, `ANALYTICS_RPC_DEADLINE_MS` | Transaction | 500, 5000 |
| `TRANSACTION_CACHE_MAX_ENTRIES` | Transaction | 100000 (0 disables the transaction state cache, for A/B runs and as a fallback; R19) |
| `DB_SERVER_PREPARED_STATEMENTS` | all | `false`. When `true`, adds `useServerPrepStmts=true` to the JDBC URL. This is a checkpoint CP3 A/B switch (R20), and the default changes only with measured evidence. |
| `PERF_METRICS`, `PERF_METRICS_INTERVAL_SECONDS` | all | `false`, 10. When enabled, each service logs a one-line JSON metrics summary every interval and at shutdown (R21). This replaces `SCAN_PROFILE`. |

- **JDBC sessions** (R20): each service builds its JDBC URL with `useLocalSessionState=true`, `useLocalTransactionState=true`, `cachePrepStmts=true`, `prepStmtCacheSize=256`, `prepStmtCacheSqlLimit=2048`, and `rewriteBatchedStatements=true`. `useServerPrepStmts` is kept only if checkpoint CP3 shows a gain.
- **Pooled connections**: opened once in manual-commit mode at READ COMMITTED. No code changes autocommit or isolation per transaction.
- **MySQL server**: no MySQL server variable is changed by any service or script. `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, and binary logging stay as configured, and the benchmark harness records them.

- **Passwords**: come only from the environment or the reset scripts' secure prompts. They are never written as command arguments or committed.
- **Old variables**: services do not read the former shared `DB_NAME`, `DB_USER`, and `DB_PASSWORD`, so one exported variable cannot give two services the same account.
- **Connection budget**: maximum pools total 64 + 48 + 10 = 122 connections, below MySQL's default `max_connections` of 151 (R16). Raising any pool requires checking this budget.

## Start, stop, restart

```bash
cd server
./run-inventory.sh
./run-transaction.sh
./run-analytics.sh
```

Each script runs in its own terminal. Use Ctrl+C in a service's terminal to stop it, and rerun only that script to restart it. Peers and the gateway keep running (FR-050, FR-051). Gateway start and stop are unchanged from feature 004 (`server/README.md`). Each shutdown hook stops HTTP, then gRPC, then background workers, then channels, then the pool.

## Database bootstrap, reset, and seed (R13)

| Script | Credentials | Effect |
|---|---|---|
| `db/bootstrap.ps1` | MySQL admin, prompted | One-time and idempotent: creates the three schemas and three users, granting each user privileges on its own schema only |
| `db/reset-service.ps1 -Service transaction` | `cs6510_transaction_svc` | Drops and recreates Transaction's tables (empty) |
| `db/reset-service.ps1 -Service inventory` | `cs6510_inventory_svc` | Drops and recreates Inventory's tables, seeds 2,000 products at 10,000 units and threshold 50, and writes a new `catalog_version` |
| `db/reset-service.ps1 -Service analytics` | `cs6510_analytics_svc` | Drops and recreates Analytics' tables with the singletons at 0 |
| `db/reset-all.ps1` | each service's own | Runs the three resets in the order Inventory, Transaction, Analytics, and prints the baseline counts: 2,000 / 2,000 / 20,000,000; zero transactions; zero events |
| `db/validate-invariants.ps1` | each service's own | Runs `db/<service>/validate.sql` per service, then reconciles the aggregates across services in the script. Prints PASS plus the FR-054 completion counts. Non-zero exit on any violation. |

**Benchmark baseline procedure**:
1. Stop all three services.
2. Run `db/reset-all.ps1`.
3. Start Inventory, then Transaction (it loads the new replica at startup), then Analytics, then confirm the gateway.
4. Run the Load Client.
5. Wait until the operational status shows no pending work.
6. Run `db/validate-invariants.ps1`.

Resetting a single service is allowed for tests, but invariants are judged from that reset point.

## Operational status (FR-039)

Each query runs with the owning service's credentials. Each service also logs a one-line status summary every 30 s.

| Service | Question | Query (summary) |
|---|---|---|
| Transaction | Pending checkout attempts | `SELECT COUNT(*) FROM checkout_attempt WHERE state='PENDING'` |
| Transaction | Alarmed attempts | Pending attempts whose `last_error` is `FAILED_PRECONDITION` or `INVALID_ARGUMENT` |
| Transaction | Undelivered events | `SELECT COUNT(*) FROM accepted_scan WHERE delivered_at IS NULL` |
| Transaction | Background completions | `SELECT COUNT(*) FROM checkout_attempt WHERE state='COMPLETED' AND client_outcome='PENDING_ERROR'` |
| Transaction | Completions without a recorded response | `… state='COMPLETED' AND client_outcome='NONE'` |
| Transaction | Product replica | Version, count, and age from `product_replica_meta` |
| Inventory | Operations by state | `SELECT state, COUNT(*) FROM checkout_operation GROUP BY state` |
| Analytics | Stored events (all positioned) | `SELECT last_position FROM scan_sequence WHERE sequence_key=1` |
| Analytics | Eligible windows not yet published | Highest eligible boundary from `scan_sequence.last_position`, compared with `analytics_checkpoint.last_window_end` |

**Pending work is zero when**: Transaction has no pending attempts and no undelivered events, and Analytics' checkpoint equals the highest eligible boundary of its `last_position`.

## Benchmark harness and checkpoints (R21, R22)

`server/benchmark-microservices.ps1 -Workload default|stress|smoke -OutputLabel <label>` performs these steps:
1. Checks that ports 8080–8083, 9092, and 9093 are free.
2. Runs `db/reset-all.ps1`.
3. Starts Inventory, then Transaction, then Analytics with `PERF_METRICS=true`, plus the gateway.
4. Runs the unchanged Load Client from `load-client/`: exactly `java -cp out Main` for `default`, exactly `java -cp out Main --stations=100 --duration=120` for `stress`, and a 20 s default variant for `smoke`.
5. Samples every 2 s: undelivered events, pending attempts, unpublished boundaries, and `SHOW GLOBAL STATUS` (`Innodb_row_lock_waits`, `Innodb_row_lock_time`, `Com_commit`, `Innodb_log_waits`).
6. Waits for zero pending work.
7. Runs `db/validate-invariants.ps1`.
8. Records `innodb_flush_log_at_trx_commit`, `sync_binlog`, and `log_bin`.
9. Writes `load-client/reports/microservices-architecture/<label>/`, containing a byte-identical copy of the client report, `evidence.json`, the service metric summaries, the samples, and the validator output.

**Evidence file**: `evidence.json` also records:
- pool borrow-wait and hold-time p99 per service;
- `TransactionLocks` wait and hold time p99, contention, and stripe collisions;
- the recovery query, wait-check, and idle-poll counters;
- `comparable=true` when all three services ran, the exact Load Client command was used, and the validator passed.

**A/B runs**: `server/benchmark-compare.ps1` (R22) runs two variants as interleaved pairs, each after its own reset. Variant A may instead be the baseline implementation, run from a worktree with its own `benchmark-scan.ps1`. The script applies the R22 decision rule: at least 3 pairs, up to 5 when the result is close, deciding on `totalTransactions`, then p99 latency, then resource use. It writes `compare/<label>/compare.json`.

**Reporting rule**: every checkpoint reports the actual client-observed `totalTransactions` and its change against the CP0 baseline. Statement counts and latencies explain results but are never presented as an improvement on their own.

**Official reports** (FR-055): the first CP4 run of each workload in the final interleaved CP0-versus-CP4 comparison, designated before the runs start. Checkpoint results CP0–CP4 are summarized in `load-client/reports/microservices-architecture/checkpoints.md`. Only the interleaved CP0 and CP4 runs form the final comparison; the other checkpoints attribute costs.
