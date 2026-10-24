---

description: "Task list for the memory-first self-checkout microservices redesign"
---

# Tasks: Self-Checkout Microservices with Database per Service, gRPC Contracts, and Recoverable Saga Checkout

**Input**: Design documents from `/specs/005-microservices-architecture/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md) (R1–R23), [data-model.md](data-model.md), [contracts/](contracts/), [quickstart.md](quickstart.md)

**Tests**: Requested by spec FR-056, FR-057, and plan R22.
- **Test first**: within each story, write the test tasks first and confirm that they fail before starting the implementation tasks.
- **Parity gate**: each story ends with a parity gate (FR-057). A story is complete only when every legacy assertion it owns has a passing equivalent, as recorded in `server/tests/COVERAGE-LEDGER.md`.

**Organization**: Tasks are grouped by user story.
- **Story order**: User Story 2 (Scan) comes before User Story 1 (Checkout), because completing a checkout needs scanned baskets. Both are P1.
- **Checkpoints**:
  - **Final comparison**: CP0 (baseline) against CP4 (official evidence), measured interleaved on the same day (T152).
  - **Attribution only**: CP1 (core checkout path), CP2 (full system), and CP3 (tuning A/B).
  - See plan.md's *Incremental Evolution Path* and research R22.
- **Task IDs**: IDs are stable.
  - The 2026-10-20 review added T134–T152, each placed in the phase where it executes.
  - It moved T041, T105, T106, T109, T112, T114, T115, and T116 to the earliest story in which the behavior they test exists. They keep their IDs.
  - Execute tasks in document order within each phase, subject to the dependencies below.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel: different files, and no dependency on an incomplete task.
- **[Story]**: The user story the task serves (US1–US7).
- Every task names its exact file paths.

## Path Conventions

- **Production code**: `server/<service>/src/<package>/…`. Packages are `api`, the business package (`transaction`, `inventory`, `analytics`), `database`, and `rpc`. Shared code goes in `server/common/src/{api,database,rpc,metrics}/`. Generated stubs go in `server/contracts/`.
- **Tests**: `server/tests/{unit,architecture,database,contract,integration,rpc,performance,support}/…`, compiled by `server/build-tests.sh` and run by `server/run-tests.sh`.
- **Legacy tests** (FR-057):
  - **Compatible tests** stay in `server/tests/` unchanged.
  - **Parking**: a legacy test moves to `server/tests-legacy/` (not compiled) only when it breaks. The implementation task that breaks it moves it, keeping its relative path. A replacement written at the same path moves the original first, and only within the story that breaks the original.
  - **Deletion**: legacy tests are deleted only after verified parity (T130).
  - **Tracking**: `server/tests/COVERAGE-LEDGER.md` (T021) tracks every test method, and `CoverageParityLedgerTest` (T134) enforces it.
- **Database tooling**: `db/…`. **Benchmark evidence**: `load-client/reports/microservices-architecture/…`.
- **Never modify**: `load-client/src/**`, `spec/self-checkout-openapi.yaml`, or any MySQL server variable.

## Implementation Rules (approved 2026-10-20)

These rules bind every task below.

1. **Order and testing**:
   - Execute tasks in dependency order, test first.
   - At every story gate, run the full applicable regression suite, not only the story's new tests.
2. **Striped locks** (R19):
   - `TransactionLocks` uses 4,096 fixed stripes. A collision between two transactions on one stripe may only make one of them wait; it never changes an outcome.
   - Lock wait time, hold time, contention, and collisions are recorded for benchmark analysis (T049, T020).
3. **Process-local components** (R7, R19):
   - The state cache, the locks, `SettlementRegistry`, and `RecoverySchedule` are process-local optimizations, not distributed coordination.
   - Every correctness guarantee must hold when several Transaction Service instances concurrently scan, complete, or recover the same transaction.
   - Database guards and Inventory idempotency stay authoritative (T140, T144, T147, T078).
4. **In-memory state ownership**:
   - Business services own no mutable maps.
   - Only these designated components may hold in-memory state: `TransactionStateCache`, `TransactionLocks`, `ProductCatalogReplica`, `SettlementRegistry`, `RecoverySchedule`, `CatalogCache`, and `ProductNameMap`.
   - The database stays authoritative (T027, T037).
5. **Constitution**: v5.1.0 was approved and applied on 2026-10-20. Any further amendment needs explicit approval of its exact text before it is applied. Durable forward recovery and every existing safety invariant are preserved (T133).
6. **Benchmark claims**:
   - Every benchmark checkpoint reports the actual client-observed `totalTransactions` and compares it with the CP0 baseline for the same workload.
   - No performance improvement is claimed from fewer SQL statements or lower latency alone (T075, T104, T123, T152, T128).

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Record the baseline before any change, then add the gRPC contract build.

- [X] T001 Run the CP0 attribution baseline (R22) on the current, unmodified implementation, commit 26c89af, before any other task, and record it in `load-client/reports/microservices-architecture/checkpoints.md`:
  - Build with `server/build.sh` and `load-client/build.sh`, and set `DB_USER` and `DB_PASSWORD`.
  - From `server/`, run three default runs with labels `cp0-default-1`, `cp0-default-2`, and `cp0-default-3`:
    `./benchmark-scan.ps1 -MySqlExe <mysql.exe> -NginxBin <nginx.exe> -JavaExe <JDK21-java.exe> -Variant retained -Workload default -TransactionPoolSize 32 -TransactionPoolMaxSize 64 -OutputLabel cp0-default-1`
  - Run the same command twice with `-Workload stress`, labels `cp0-stress-1` and `cp0-stress-2`.
  - Create `load-client/reports/microservices-architecture/checkpoints.md` with a results table and record the five runs:
    - **Columns**: checkpoint, label, workload, `totalTransactions`, change against the CP0 median of the same workload (%), Start/Scan/Complete p95/p99 (client JSON), Complete error count, `Innodb_row_lock_waits` delta, `innodb_flush_log_at_trx_commit`, `sync_binlog`, and a link to the harness output.
    - **Summary**: one row per workload with the median and the range (minimum to maximum) of `totalTransactions`.
  - These runs are the reference for attributing costs at CP1–CP3. The final CP0-versus-CP4 comparison uses fresh, interleaved runs (T152).
- [X] T002 Create `server/contracts/pom.xml` (R1):
  - **Module**: `jar` packaging, groupId `cs6510.selfcheckout`, artifactId `contracts`, Java release 21.
  - **Dependencies**: import `io.grpc:grpc-bom` at one pinned current stable release. Add `grpc-protobuf`, `grpc-stub`, `grpc-netty-shaded`, `grpc-inprocess`, and the matching `protobuf-java` at compile scope, plus the annotations dependency that the pinned grpc-java README requires for generated code on Java 9+, at provided scope.
  - **Code generation**: `kr.motd.maven:os-maven-plugin` as a build extension. The protobuf Maven plugin uses `com.google.protobuf:protoc:<pinned>:exe:${os.detected.classifier}` and `io.grpc:protoc-gen-grpc-java:<grpc version>:exe:${os.detected.classifier}`, with goals `compile` and `compile-custom`.
  - **Outputs**: maven-jar-plugin with `finalName` `contracts` and `outputDirectory` `${project.basedir}/../out`. maven-dependency-plugin `copy-dependencies` (includeScope runtime) to `${project.basedir}/../lib/grpc` at `package`.
- [X] T003 [P] Copy the proto files into `server/contracts/src/main/proto/` and make the server copies authoritative (Constitution III):
  - Copy `specs/005-microservices-architecture/contracts/proto/inventory_service.proto` verbatim to `server/contracts/src/main/proto/selfcheckout/inventory/v1/inventory_service.proto`.
  - Copy `contracts/proto/analytics_ingest.proto` verbatim to `server/contracts/src/main/proto/selfcheckout/analytics/v1/analytics_ingest.proto`.
  - Replace the body of each spec copy with a one-line comment pointing to its new server path.
- [X] T004 Update `server/build.sh`:
  - Add `build_contracts`. It exits with a clear message if `mvn` is missing, runs `mvn -q -f contracts/pom.xml -DskipTests package` (adding `-o` when `MAVEN_OFFLINE=1`), and verifies that `out/contracts.jar` and `lib/grpc/` exist.
  - Call it before `build_common` for `all` and for every single-service target.
  - Compile `common` with `out/contracts.jar${SEP}lib/grpc/*${SEP}${MYSQL_CONNECTOR}`, and each service with `out/common.jar${SEP}out/contracts.jar${SEP}lib/grpc/*${SEP}${MYSQL_CONNECTOR}`. Quote the wildcard.
  - Run `./build.sh` and `./build.sh inventory`, and confirm that the jars are produced.
- [X] T005 [P] Update `server/run-transaction.sh`, `server/run-inventory.sh`, and `server/run-analytics.sh`:
  - Fail clearly if `out/contracts.jar` or `lib/grpc/` is missing.
  - Launch with `out/<service>.jar${SEP}out/common.jar${SEP}out/contracts.jar${SEP}lib/grpc/*${SEP}lib/mysql-connector-j-9.0.0.jar`.
  - Keep the Windows `;` separator and the optional port argument.
- [X] T006 [P] Update `server/build-tests.sh` and `server/run-tests.sh`:
  - Add `out/contracts.jar` and `lib/grpc/*` to the javac and java classpaths, and keep recursive discovery under `server/tests/`. Never compile `server/tests-legacy/`.
  - When `SERVER_BASE_URL` is set, skipped tests in the `rpc.` and `performance.` packages also fail the run, alongside the existing `contract.` and `integration.` checks.
  - When `PARITY_GATE` is set, for example `PARITY_GATE=US2`, pass `-Dparity.gate=$PARITY_GATE` to the test JVM for `CoverageParityLedgerTest` (T134).
- [X] T007 [P] Add `server/lib/grpc/` and `server/contracts/target/` to `.gitignore`.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared runtime utilities, per-service schemas and tooling, test infrastructure, the coverage ledger, the statement-budget guard, the A/B harness, and architecture gates. Every user story depends on this phase.

**⚠️ CRITICAL**: No user-story task starts until this phase is complete.

- [X] T008 [P] Create the dependency-free metrics package in `server/common/src/metrics/` (R21):
  - `PerfMetrics`: a static registry of named counters, gauges, and timers.
  - `LatencyHistogram`: `LongAdder` buckets on a log scale from 50 µs to 30 s. `record(nanos)` records a value; a snapshot returns count, p50, p95, p99, and max.
  - `MetricsReporter`: `start(serviceName)` reads `PERF_METRICS` (default `false`) and `PERF_METRICS_INTERVAL_SECONDS` (default 10). When enabled, it prints one JSON line `{"service":…,"ts":…,"timers":{…},"counters":{…},"gauges":{…}}` each interval and once on `stop()`.
  - Recording is always on and lock-free, so tests can read snapshots.
- [X] T009 [P] Create `server/common/src/rpc/` (R2, R6, R15):
  - `GrpcChannels`: one plaintext Netty channel per target, created lazily once and reused; `shutdownAll()`.
  - `GrpcServers`: starts a Netty gRPC server on a port with a virtual-thread-per-task executor; ordered `stop(timeout)`.
  - `RpcStatusClassifier`: OK is DEFINITIVE. INVALID_ARGUMENT and FAILED_PRECONDITION map to themselves. UNAVAILABLE, DEADLINE_EXCEEDED, ABORTED, RESOURCE_EXHAUSTED, CANCELLED, UNKNOWN, and INTERNAL are UNCERTAIN, per `contracts/grpc-internal-contracts.md`.
  - `ResponseBudget`: start instant and total; `remaining()`, `expired()`, and `nextDeadline(perCall)`, which returns the smaller of `perCall` and `remaining()`.
  - `FaultHooks`: `hit(name)` parses the JVM property `selfcheckout.faults` as comma-separated `name:action` entries. Actions are `halt` (`Runtime.halt(137)`), `delay:<ms>`, and `throw`. It re-parses only when the property value changes and is inert when the property is absent.
- [X] T010 [P] Add the commit-ambiguity flag to `server/common/src/database/StoreFailure.java` and create `server/common/src/database/DatabaseConfig.java` (R3, R20):
  - Add a `commitOutcomeUnknown` flag (constructor overload and accessor) to `server/common/src/database/StoreFailure.java`.
  - Create `server/common/src/database/DatabaseConfig.java` with `fromEnvironment(prefix, defaultSchema, defaultUser, defaultInitialPool, defaultMaxPool)`.
  - It reads `DB_HOST` (default 127.0.0.1), `DB_PORT` (3307), `<PREFIX>_DB_NAME`, `<PREFIX>_DB_USER`, `<PREFIX>_DB_PASSWORD`, `<PREFIX>_DB_POOL_SIZE`, `<PREFIX>_DB_POOL_MAX_SIZE`, and `DB_SERVER_PREPARED_STATEMENTS` (default `false`). It never reads the former shared `DB_NAME`, `DB_USER`, or `DB_PASSWORD`.
  - `jdbcUrl()` returns `jdbc:mysql://host:port/name?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&useLocalSessionState=true&useLocalTransactionState=true&cachePrepStmts=true&prepStmtCacheSize=256&prepStmtCacheSqlLimit=2048&rewriteBatchedStatements=true`, plus `&useServerPrepStmts=true` only when the switch is true.
- [X] T011 Extend `server/common/src/database/ConnectionPool.java` (R16, R20, R21):
  - Add a constructor taking `DatabaseConfig`, a metrics name, and an optional `UnaryOperator<Connection>` decorator. The decorator is a test-only hook; production passes identity.
  - Pools built by this constructor open every physical connection once with `setAutoCommit(false)` and `setTransactionIsolation(TRANSACTION_READ_COMMITTED)`. In `release()`, they roll back any open transaction and restore manual-commit mode before returning the connection.
  - The existing constructors keep their current behavior, including pool growth, until their last caller is migrated. Retained tests and not-yet-migrated services are therefore unaffected.
  - Metrics:
    - time `borrow()` into `<name>.pool.borrowWait`;
    - time borrow-to-release into `<name>.pool.holdTime`;
    - expose occupancy and its high-water mark as gauges.
- [X] T012 Create `server/common/src/database/JdbcUnitOfWork.java` (R20):
  - `inTransaction(pool, metricName, work)` borrows a connection, runs the work, and commits; it rolls back on any exception before commit.
  - It retries the whole unit only on deadlock (MySQL error 1213 or SQLState 40001) when no commit was attempted: at most 3 attempts with 10 ms × attempt backoff.
  - It throws `StoreFailure` with `commitOutcomeUnknown=true` when `commit()` itself fails, times the unit into `metricName`, and always releases the connection.
  - `read(pool, metricName, work)` ends the unit with rollback.
- [X] T013 [P] Create `db/bootstrap.sql` (statement template) and `db/bootstrap.ps1` (R3):
  - Prompt securely for the MySQL admin password. Read `TRANSACTION_DB_PASSWORD`, `INVENTORY_DB_PASSWORD`, and `ANALYTICS_DB_PASSWORD` from the environment, or prompt securely.
  - Idempotently create schemas `cs6510_transaction`, `cs6510_inventory`, and `cs6510_analytics` (utf8mb4, utf8mb4_0900_ai_ci).
  - Create users `cs6510_transaction_svc`, `cs6510_inventory_svc`, and `cs6510_analytics_svc` for both `localhost` and `127.0.0.1`, or reset their passwords if they exist. Grant each user ALL PRIVILEGES on its own schema only.
  - Pass every password through stdin or `MYSQL_PWD`, never on a command line. Reuse the `mysql.exe` discovery logic of `db/init-db.ps1`, and print the schemas and users.
- [X] T014 [P] Create `db/transaction/schema.sql` per data-model.md. It drops and recreates only these tables and references no other schema.
  - **`transaction`**:
    - "`transaction_id` | VARCHAR(40), PK", "`station_id` | VARCHAR(40), not null", "`status` | ENUM(`OPEN`,`COMPLETED`,`CANCELLED`), default `OPEN`";
    - "`item_count` | INT, not null, default 0, ≥ 0", "`total_amount` | DECIMAL(12,2), not null, default 0", "`attempt_count` | INT, not null, default 0";
    - "`active_attempt_number` | INT, null", with an index for the recovery worker;
    - "`started_at` | TIMESTAMP(3)", "`completed_at` | TIMESTAMP(3), null";
    - checks "`status = COMPLETED` if and only if `completed_at IS NOT NULL`, and `status = COMPLETED` implies `active_attempt_number IS NULL`".
  - **`accepted_scan`**:
    - "`scan_id` | BIGINT, auto-increment, PK", "`transaction_id` | VARCHAR(40), FK → `transaction`", "`scan_sequence` | INT, ≥ 1";
    - "`sku` | VARCHAR(20)", "`product_name` | VARCHAR(100)", "`unit_price` | DECIMAL(10,2), ≥ 0";
    - "`accepted_at` | TIMESTAMP(3)", "`delivered_at` | TIMESTAMP(3), null";
    - "Unique key: `(transaction_id, scan_sequence)`" and "Index: `(delivered_at, scan_id)`".
  - **`product_replica`**: "`sku` (PK), `name`, `price` DECIMAL(10,2), `catalog_version` CHAR(36)".
  - **`product_replica_meta`**: "`meta_key` (PK = 1), `catalog_version` (null until the first load), `product_count`, `refreshed_at`". Insert the singleton row `(1, NULL, 0, NULL)`.
  - **`checkout_attempt`**:
    - "`transaction_id` | VARCHAR(40), FK → `transaction`", "`attempt_number` | INT, ≥ 1", "`state` | ENUM(`PENDING`,`COMPLETED`,`REJECTED`)";
    - "`rejection_reason` | ENUM(`INSUFFICIENT_STOCK`,`INVALID_REQUEST`,`STOCK_RESTORED`), null", non-null exactly when `state = REJECTED`;
    - "`rejected_sku` | VARCHAR(20), null", "`lines` | TEXT, not null", `item_count` INT, `total_amount` DECIMAL(12,2);
    - "`client_outcome` | ENUM(`NONE`,`SUCCESS`,`REJECTED`,`PENDING_ERROR`), default `NONE`";
    - "`created_at`, `settled_at` | TIMESTAMP(3); `settled_at` null while PENDING";
    - `retry_count` INT default 0, `next_retry_at` TIMESTAMP(3) null, `last_error` VARCHAR(255) null;
    - "Primary key `(transaction_id, attempt_number)`", "Index `(state, next_retry_at)`";
    - unique stored generated columns "`pending_transaction_id = IF(state='PENDING', transaction_id, NULL)` and `completed_transaction_id = IF(state='COMPLETED', transaction_id, NULL)`".
- [X] T015 [P] Create `db/inventory/schema.sql` and `db/inventory/seed.sql` per data-model.md.
  - **`catalog_item`**: "`sku` (PK), `name`, `price` DECIMAL(10,2) ≥ 0", with `sku` VARCHAR(20) and `name` VARCHAR(100).
  - **`inventory`**: "`sku` (PK, FK → `catalog_item`), `stock_quantity` INT CHECK ≥ 0, `low_stock_threshold` INT ≥ 0".
  - **`catalog_meta`**: "`meta_key` (PK = 1), `catalog_version` CHAR(36), `product_count`, `seeded_at`".
  - **`checkout_operation`**:
    - "`transaction_id`, `attempt_number` | VARCHAR(40), INT; PK together", "`state` | ENUM(`APPLIED`,`REJECTED`,`RESTORED`,`BLOCKED`)";
    - "`rejection_reason` | ENUM(`INSUFFICIENT_STOCK`,`INVALID_REQUEST`), null", "`rejected_sku` | VARCHAR(20), null";
    - "`lines` | TEXT, null", null only for a BLOCKED tombstone;
    - "`applied_transaction_id` | VARCHAR(40), stored generated column `IF(state='APPLIED', transaction_id, NULL)`, UNIQUE";
    - "`processed_at`, `restored_at` | TIMESTAMP(3); `restored_at` null unless RESTORED".
  - **`seed.sql`**:
    - Use a recursive CTE for n = 1..2000: `sku = CONCAT('SKU-', LPAD(n, 6, '0'))`, `name = CONCAT('Item ', n)`, `price = ROUND(0.5 + MOD(n, 47) * 0.35, 2)`.
    - Stock is 10,000 per product with threshold 50. `catalog_meta` is `(1, UUID(), 2000, NOW(3))`.
    - End with a sanity SELECT of counts and total units.
- [X] T016 [P] Create `db/analytics/schema.sql` per data-model.md:
  - **`accepted_scan`**: "`position` BIGINT PK, > 0; `source_transaction_id` VARCHAR(40); `source_scan_sequence` INT ≥ 1; `sku`; `received_at`", with "Unique key `(source_transaction_id, source_scan_sequence)`". `sku` is VARCHAR(20) with no foreign key.
  - **`product_name`**: "`sku` (PK), `name`, `updated_at`".
  - **`scan_sequence`**: "`sequence_key` (PK = 1), `last_position` ≥ 0", seeded `(1, 0)`.
  - **`popular_window`**: "`window_id` PK, `window_start`, `window_end` UNIQUE, `computed_at`", with checks "CHECK `window_end ≥ 1000`, `(window_end − 1000) mod 500 = 0`, and `window_start = window_end − 999`".
  - **`popular_item`**: "`window_id` FK, `rank_pos` 1–10, `sku` FK → `product_name`, `scan_count` > 0", with "Primary key `(window_id, rank_pos)`" and ON DELETE CASCADE from `popular_window`.
  - **`analytics_checkpoint`**: "`checkpoint_key` (PK = 1), `last_window_end`", which is "0 or a valid boundary", seeded `(1, 0)`.
- [X] T017 Create `db/reset-service.ps1` and `db/reset-all.ps1` (R13):
  - `reset-service.ps1 -Service transaction|inventory|analytics [-MySqlHost 127.0.0.1] [-Port 3307] [-MySqlExe]` runs `db/<service>/schema.sql`, plus `seed.sql` for Inventory, against `cs6510_<service>`.
  - It connects only as `<SERVICE>_DB_USER` (default `cs6510_<service>_svc`), with the password from `<SERVICE>_DB_PASSWORD` or a secure prompt passed through `MYSQL_PWD`, and prints the resulting counts.
  - `reset-all.ps1` resets Inventory, then Transaction, then Analytics, and stops at the first failure.
  - It prints the baseline: 2,000 catalog rows, 2,000 inventory rows, 20,000,000 units, zero transactions, zero `accepted_scan` rows, and `last_position` 0.
- [X] T018 [P] Create `db/transaction/validate.sql`, `db/inventory/validate.sql`, and `db/analytics/validate.sql`. Each uses an `assert_zero` procedure that raises SQLSTATE 45000 and drops it at the end.
  - **Transaction**:
    - totals and `item_count` match `accepted_scan` sums and counts;
    - `scan_sequence` is contiguous from 1 per transaction;
    - one `unit_price` per `(transaction_id, sku)`;
    - COMPLETED transactions have `completed_at` and at least one scan; OPEN transactions have no `completed_at`;
    - no PENDING attempts, and every `active_attempt_number` is NULL;
    - each COMPLETED transaction has exactly one COMPLETED attempt whose `lines` equal its grouped scans;
    - a PASS row prints counts and the FR-054 counts of COMPLETED attempts by `client_outcome`.
  - **Inventory**:
    - 2,000 catalog rows and 2,000 inventory rows; stock ≥ 0;
    - APPLIED and RESTORED operations have `lines`, and REJECTED operations have a reason;
    - per SKU, 10,000 minus stock equals the quantities in the APPLIED `lines`, using `JSON_TABLE`;
    - a PASS row.
  - **Analytics**:
    - `scan_sequence` singleton exists; positions are contiguous from 1 to `last_position`;
    - the checkpoint equals the highest eligible boundary;
    - windows are valid and published exactly once;
    - ranks are recomputed from `accepted_scan`, reusing the window-rank SQL of the retired `db/validate-invariants.sql`;
    - every `popular_item` SKU has a name;
    - a PASS row.
- [X] T019 Create `db/validate-invariants.ps1` (R13):
  - Runs the three validate scripts, each with its own service credentials. `-SkipAnalytics` omits the Analytics checks and the delivery checks.
  - Exports aggregates with `mysql --batch` and reconciles them in PowerShell:
    - per SKU, Inventory sold units equal Transaction quantities from the `accepted_scan` rows of COMPLETED transactions;
    - Inventory APPLIED keys equal Transaction COMPLETED attempt keys;
    - Analytics' row count and per-source-transaction counts equal Transaction's delivered rows, and undelivered rows are 0.
  - Prints PASS and the FR-054 counts: COMPLETED transactions, background completions (`PENDING_ERROR`), and completions without a recorded response (`NONE`). With `-TotalTransactions <n>`, it also prints the remaining difference.
  - Exits non-zero on any violation.
- [X] T020 Create `server/benchmark-microservices.ps1` (R21; contracts/service-deployment.md *Benchmark harness and checkpoints*).
  - **Parameters**: `-Workload default|stress|smoke`, `-OutputLabel`, `-MySqlExe`, `-NginxBin`, `-JavaExe`, `-SkipAnalytics`, and `-ExtraEnv` (a hashtable for CP3 variants).
  - **Steps**:
    1. Check that ports 8080–8083, 9092, and 9093 are free. Build `load-client/` if needed.
    2. Run `db/reset-all.ps1`.
    3. Start Inventory, then Transaction, then Analytics (unless `-SkipAnalytics`) with `PERF_METRICS=true`, plus Nginx.
    4. From `load-client/`, run exactly `java -cp out Main` for default, exactly `java -cp out Main --stations=100 --duration=120` for stress, or `java -cp out Main --duration=20` for smoke.
    5. Every 2 s, sample undelivered rows, pending attempts, unpublished boundaries, and `SHOW GLOBAL STATUS` (`Innodb_row_lock_waits`, `Innodb_row_lock_time`, `Com_commit`, `Innodb_log_waits`).
    6. Wait for zero pending work.
    7. Run `db/validate-invariants.ps1`.
    8. Record `innodb_flush_log_at_trx_commit`, `sync_binlog`, and `log_bin`.
    9. Stop all processes.
  - **Output**: `load-client/reports/microservices-architecture/<label>/` with a byte-identical client report, the raw samples, the service logs, and `evidence.json`. The evidence file holds:
    - label, workload, command, git commit, JAR SHA-256s, and the `-ExtraEnv` values;
    - key client fields, durability variables, sample peaks, final service metric lines, validator result, and FR-054 counts.
  - **Recovery and resource evidence** (R21): `evidence.json` also holds these final values:
    - per service, `<service>.pool.borrowWait` p99, `<service>.pool.holdTime` p99, and peak occupancy;
    - `transaction.recovery.scanQueries`, `transaction.recovery.scanRows`, and `transaction.wait.dbChecks`;
    - `transaction.lock.wait` p99, `transaction.lock.hold` p99, `transaction.lock.contended`, and `transaction.lock.collisions`;
    - `transaction.relay.idlePolls` and `analytics.poller.idlePolls`.
  - **Comparability flag**: `evidence.json` sets `comparable=true` only when all of these hold:
    - all three services ran, so no `-SkipAnalytics`;
    - the exact Load Client command for the workload was used;
    - the validator passed.
    
    Only comparable runs can decide a CP3 variant or enter the final comparison.
- [X] T021 [P] Create the coverage-parity ledger `server/tests/COVERAGE-LEDGER.md` (FR-057). This task moves no files.
  - **Rows**: one row per `@Test` method of every test file under `server/tests/` at commit 26c89af (56 files), plus one file-level row for each support class. Columns:
    - the legacy file and method;
    - the status;
    - the owner task;
    - the replacement `File#method` list;
    - the governing requirement or equivalence note;
    - `Verified at`: the gate task whose run last passed the test or its replacement.
  - **Statuses**:
    - `RETAINED`: the file stays unchanged and keeps passing.
    - `ADAPTED`: the file stays at its path and is edited in place, keeping every method and assertion.
    - `PENDING`: replacement not yet written.
    - `MIGRATED`: equivalent assertions exist in the replacement methods.
    - `SUPERSEDED`: the assertion encodes behavior that the spec replaces. The replacement asserts the specified behavior, and the row cites the governing requirement (FR-057).
  - **Initial classification**:
    - `RETAINED`, compatible with the new design:
      - `unit/JsonRoundTripTest`, `unit/AnalyticsRecorderRecoveryTest`, `unit/RingBufferWindowMathTest`, and `unit/LowStockThresholdResolutionTest`;
      - the 11 `unit/analytics/*` tests;
      - `unit/api/AnalyticsHttpHandlerTest` and `unit/api/StoreQueryHttpHandlerTest`;
      - `contract/ItemsEndpointTest`, `contract/TransactionLifecycleTest`, and `contract/TransactionLookupTest`;
      - `integration/PublicRoutingIsolationTest` and `integration/PublicRouterTestSupport`;
      - `support/PipelineTestSupport` and `architecture/PipelineArchitectureTest`.
    - `ADAPTED`: `database/ConnectionPoolGrowthTest` (T025), `architecture/LayerDependencyTest` (T026), `unit/transaction/StoreQueryServiceTest` (T035), `unit/api/TransactionHttpHandlerTest` (T107), and `architecture/CompositionRootTest` (T118).
    - `PENDING`: the other 27 files, with the owner tasks named in the tasks below.
    - `SUPERSEDED`: in `integration/InventoryServiceIsolationTest#inventoryRestartReadsCommittedSaleWhileTransactionRunsDuringOutage`, the assertion that Complete returns 200 while Inventory is down. FR-030, FR-037, and the spec's *Availability and Failure Behavior* table replace it: Complete returns 500 `INTERNAL_ERROR`, the attempt stays pending, and it settles after Inventory returns (T114). The method's other assertions migrate normally.
  - **Updates**: every task that writes a replacement updates its rows. Every task that breaks a legacy file moves it to `server/tests-legacy/`.
- [X] T022 [P] Create the per-schema test support in `server/tests/support/`. The new names avoid clashing with the legacy `AnalyticsDatabase` and `CheckoutDatabase`, which stay untouched until their users are migrated:
  - `SqlScripts`: runs a `db/<service>/schema.sql` file statement by statement.
  - `TransactionTestSchema`, `InventoryTestSchema`, and `AnalyticsTestSchema`:
    - Each creates a disposable schema named `txn_test_<uuid>`, `inv_test_<uuid>`, or `ana_test_<uuid>` from its service's schema file, using the admin credentials from `-DDB_USER` and `-DDB_PASS`.
    - Each can optionally create a disposable user granted only that schema, and exposes a `ConnectionPool` built through `DatabaseConfig`-equivalent parameters with an optional decorator.
    - Helpers: Inventory provides `seed(products, stock)`, `stock(sku)`, and `setStock(sku, n)`. Transaction provides scan, attempt, and outbox queries and `setTableUnavailable(table, boolean)`. Analytics provides position and window queries.
    - Each drops its schema and user on `close()`.
- [X] T023 [P] Create more test support in `server/tests/support/`:
  - `CountingConnections`: a `java.lang.reflect.Proxy` decorator over `Connection`, `Statement`, and `PreparedStatement`. It counts executions by SQL verb (SELECT, INSERT, UPDATE, DELETE, other) and counts `commit`, `rollback`, `setAutoCommit`, and `setTransactionIsolation` calls, with per-thread scopes.
  - `FaultingConnections`: a decorator that scripts commit faults for one thread or one connection. It can throw before delegating `commit()`, so the commit does not happen. It can delegate and then throw, so the commit happens but the caller sees a failure. It can also delay `commit()` by a given time.
  - `ServiceProcesses`: starts a service JVM from `server/out` jars, `lib/grpc/*`, and Connector/J, with per-service environment variables (ports, schema, user, password, gRPC targets, `PERF_METRICS`) and an optional `-Dselfcheckout.faults=…`. It provides readiness probes, stop, and wait-for-exit (including halt).
  - `GrpcTestSupport`: an in-process server and channel for a `BindableService`, plus free ports.
  - `FaultInjection`: builders for fault specifications.
- [X] T024 [P] Write unit tests in `server/tests/unit/common/`:
  - `PerfMetricsTest`: bucket boundaries, percentile estimates, and the JSON line format.
  - `RpcStatusClassifierTest`: every status in the contract table.
  - `ResponseBudgetTest`: `remaining`, `expired`, and `nextDeadline`.
  - `FaultHooksTest`: inert without the property; `halt`, `delay`, and `throw` parsing; re-parse when the property changes.
- [X] T025 Write `server/tests/database/ConnectionPoolSessionTest.java`. Then adapt `server/tests/database/ConnectionPoolGrowthTest.java` in place to use a `TransactionTestSchema` instead of `cs6510_selfcheckout`, keeping its method and assertions (ledger: `ADAPTED`). Assert that:
  - pooled connections from the `DatabaseConfig` constructor are in manual-commit mode at READ COMMITTED, and the existing constructors behave as before;
  - a `JdbcUnitOfWork` issues no `setAutoCommit` or `setTransactionIsolation` per unit, using `CountingConnections`;
  - a commit failure surfaces `commitOutcomeUnknown`, for both `FaultingConnections` commit faults;
  - deadlocks retry only before commit;
  - borrow wait, hold time, and peak occupancy are recorded.
- [X] T026 [P] Update `server/tests/architecture/LayerDependencyTest.java`:
  - `rpc` packages may import `io.grpc` and `contracts.*`, and business packages (`transaction`, `inventory`, `analytics`) must not.
  - The `metrics` and `rpc` packages in `common` contain no business package or import.
  - SQL stays in database packages, and HTTP stays in `api` packages and `*Main` classes.
  - `out/contracts.jar` contains no `transaction/`, `inventory/`, or `analytics/` classes.
- [X] T027 [P] Create `server/tests/architecture/MicroserviceBoundaryTest.java`. It asserts that:
  - no jar in `server/lib/**` matches `redis|jedis|lettuce|kafka|rabbit|activemq`;
  - no production source contains `SET GLOBAL`, `innodb_flush_log_at_trx_commit`, `sync_binlog`, or `skip-log-bin`;
  - `DatabaseConfig.java` contains every R20 URL flag;
  - no service source, and no `db/<service>/` file, names another service's schema (`cs6510_<other>`);
  - `FaultHooks.hit` does nothing when `selfcheckout.faults` is unset;
  - **in-memory state ownership** (Implementation Rule 4):
    - checked by reflection over the classes in `out/transaction.jar`, `out/inventory.jar`, and `out/analytics.jar`;
    - in the business packages `transaction`, `inventory`, and `analytics`, no class declares a static or instance field assignable to `java.util.Map`, except the designated components and their nested classes: `TransactionStateCache`, `TransactionLocks`, `ProductCatalogReplica`, `SettlementRegistry`, `RecoverySchedule`, `CatalogCache`, and `ProductNameMap`;
    - the list grants permission only, so a component that does not exist yet does not fail the test, and every class added later is checked automatically.
- [X] T134 [P] Write `server/tests/architecture/CoverageParityLedgerTest.java` (FR-057). It parses `server/tests/COVERAGE-LEDGER.md` and fails when:
  - a `@Test` method in `server/tests-legacy/**` has no ledger row, or a ledger row names a method that its legacy file no longer contains;
  - a `RETAINED` or `ADAPTED` row names a method missing from its file at the original path;
  - a `MIGRATED` or `SUPERSEDED` row names a replacement `File#method` that does not exist under `server/tests/`, or a `SUPERSEDED` row cites no requirement;
  - a legacy file named in the ledger exists in neither `server/tests/` nor `server/tests-legacy/`, unless every one of its rows is `MIGRATED` or `SUPERSEDED` with a `Verified at` value;
  - with `-Dparity.gate=<phase>`:
    - a row owned by that phase, or by an earlier phase, is still `PENDING`;
    - a row owned by an earlier phase lacks `Verified at`.
    
    The phase order is Foundational, US2, US1, US3, US5, US4, US6, US7. The value `final` requires every row to be non-`PENDING` with a `Verified at` value.
- [X] T135 [P] Create the statement-budget guard (R22) in `server/tests/performance/statement-budgets.properties`, `server/tests/support/StatementBudgets.java`, and `server/tests/performance/StatementBudgetFileTest.java`:
  - **File**: one ceiling per operation and counter, such as `scan.warm.statements=2`, plus its design value, such as `scan.warm.statements.design=2`. Initial values are the design budgets in research R22 and plan.md *Key flows and statement budgets*.
  - **Loader**: `StatementBudgets.assertWithin(key, actual)` fails when `actual` exceeds the ceiling, and prints the actual value and the ceiling of every check. The file is read relative to the working directory of `run-tests.sh`.
  - **Revision rule**: a ceiling may exceed its design value only with `<key>.evidence=<label>`. The label names an A/B record in `load-client/reports/microservices-architecture/checkpoints.md`, produced by `server/benchmark-compare.ps1` (T136). That record must show higher `totalTransactions` beyond noise, with no measurable regression in p99 latency or lock contention (R22).
  - **Self-check**: `StatementBudgetFileTest` fails in either of these cases:
    - a key lacks a design value;
    - a ceiling above its design value lacks evidence, or names a label that is not in `checkpoints.md`.
    
    Lowering a ceiling needs no evidence.
- [X] T136 [P] Create `server/benchmark-compare.ps1` for interleaved A/B measurement (R22):
  - **Parameters**:
    - `-Workload default|stress` and `-Label`;
    - `-VariantA` and `-VariantB`, each an `-ExtraEnv` hashtable for `benchmark-microservices.ps1`;
    - `-BaselineWorktree <path>`, which instead runs variant A through that worktree's `server/benchmark-scan.ps1` with the T001 arguments;
    - `-Pairs` (default 3) and `-MaxPairs` (default 5).
  - **Runs**:
    - Pairs alternate their order (A B, then B A, then A B, …) to cancel linear drift. Each run starts from its own full reset, on an otherwise idle machine.
    - Labels are `<label>-a-<n>` and `<label>-b-<n>`.
    - A run that is not `comparable` (T020) aborts the comparison. A baseline-worktree run must pass the T001 harness's own success checks.
  - **Statistics**, per variant:
    - the median and range of `totalTransactions`;
    - the median and range of Scan and Complete p99 from the client JSON;
    - the median and range of resource use: pool borrow-wait p99, `inventory.apply.lockWait` p99, the `Innodb_row_lock_waits` delta, and `Com_commit` per completed transaction;
    - a one-sided Mann–Whitney U test on `totalTransactions`.
  - **Decision rule**:
    1. **Clear**: after `-Pairs`, the medians differ by at least 3% and the ranges do not overlap. The faster variant wins.
    2. **Close**: otherwise, run up to `-MaxPairs`. Decide on `totalTransactions` only if U is at or below the one-sided 5% critical value for that number of pairs (U ≤ 4 for 5 against 5).
    3. **Tie**: otherwise, compare Scan and Complete p99, then resource use, and decide on a metric only when its ranges do not overlap. If none decides, keep the simpler or incumbent variant.
  - **Output**: `load-client/reports/microservices-architecture/compare/<label>/compare.json`, plus a Markdown row to paste into `checkpoints.md`: variants, pairs, medians, ranges, U, decision, and the deciding metric.
  - **Revised 2026-10-22 during T123** (streamlined policy, R22):
    - The script runs exactly `-Pairs` pairs with no extension; `-MaxPairs` is removed.
    - It classifies the result by Δ instead of deciding with the Mann–Whitney U test:
      - with one pair: promising (Δ ≥ +5%), regression (Δ ≤ −3%), or no demonstrated improvement;
      - with more pairs: improvement (Δ ≥ +3% and ahead in at least two pairs), regression, or no demonstrated improvement.
    - A Scan or Complete p99 median more than 10% above the incumbent's is flagged as a latency regression.
    - It records the per-pair differences, and the U value only as description.
    - The idle-machine gate, retries for startup failures, and resume of interrupted comparisons were added during T123, so runs stay valid on a shared machine.
    - The gate limit is 2.0 of the 24 logical cores, as passed by the CP3 and CP4 drivers. At 1.0, ordinary desktop load of about 1.1–1.5 cores invalidated every attempt; the games that contaminated earlier runs are blocked by name. Each run records its measured background load.
- [X] T137 Verify the foundation and run its parity gate with `server/run-tests.sh`:
  - Run `server/build.sh` and `server/build-tests.sh`, then `PARITY_GATE=Foundational server/run-tests.sh`. Set `SERVER_BASE_URL` against the pre-migration deployment, so the live retained tests run too.
  - The whole suite must pass: every `RETAINED` and `ADAPTED` test, every legacy test that still compiles, and T024–T027, T134, and T135.
  - Record `T137` in `Verified at` for every row whose test passed.

**Checkpoint**: Foundation ready. The schemas, tooling, harnesses, test support, coverage ledger, budget guard, and architecture gates exist, and every retained test still passes.

---

## Phase 3: User Story 2 - Scan Durably Without Per-Scan Remote Dependencies (Priority: P1)

**Goal**: Transaction Service validates and prices each Scan from its in-memory catalog snapshot and transaction state cache. It commits one guarded transaction-row UPDATE plus one `accepted_scan` row per Scan, with no read and no remote call. Scanning keeps working while Inventory or Analytics is down, including across a Transaction restart.

**Independent Test**: Load the replica once, then stop Inventory and Analytics. Start, scan, and look up transactions on Transaction's port. Restart Transaction and keep scanning. Verify that stock is unchanged and that `accepted_scan` rows are durable and undelivered. Delivery is verified in US5.

### Tests for User Story 2 ⚠️ write first; they must fail before implementation

- [X] T028 [P] [US2] Write `server/tests/rpc/InventoryCatalogContractTest.java`. Run an in-process `InventoryGrpcService` over an `InventoryTestSchema` seeded with the deterministic catalog, and assert that:
  - `GetCatalogVersion` returns the seeded version and `product_count` 2000;
  - `GetCatalog` returns 2,000 products in ascending SKU order with exact `price_cents` (SKU-000001 → 85);
  - after `catalog_meta.catalog_version` and one name change in the database, the next `GetCatalogVersion` returns the new version and `GetCatalog` shows the new name (cache reload, R8);
  - `StoreQueryService.listItems` serves the same cached catalog.
- [X] T029 [P] [US2] Write `server/tests/database/JdbcProductReplicaStoreTest.java`. Assert that:
  - `replace` writes every product row plus the meta row in one commit;
  - `load` returns that snapshot, and a null meta version means unusable;
  - a failed `replace` leaves the previous replica intact.
- [X] T030 [P] [US2] Write `server/tests/unit/transaction/ProductCatalogReplicaTest.java` with fake `InventoryGateway` and `ProductReplicaStore`. Assert that:
  - startup serves the durable replica while Inventory is down;
  - a refresh fetches only when the version differs;
  - the in-memory snapshot swaps only after the store commit;
  - a never-populated replica is unusable and retries every 1 s;
  - failures back off from 1 s to 30 s;
  - `CATALOG_REFRESH_SECONDS` is honored.
- [X] T031 [P] [US2] Write `server/tests/unit/transaction/TransactionStateCacheTest.java`. Assert that:
  - `getOrLoad` calls the loader once, and entries change only through post-commit update methods;
  - `invalidate` removes the entry, so the next access reloads; this is the required step after a `commitOutcomeUnknown` failure;
  - every read or update of an entry requires the caller to hold that transaction's lock from `TransactionLocks`, and fails otherwise;
  - COMPLETED entries are evicted, and the LRU cap is enforced;
  - `TRANSACTION_CACHE_MAX_ENTRIES=0` loads fresh on every access.
- [X] T138 [P] [US2] Write `server/tests/unit/transaction/TransactionLocksTest.java` (R19). Assert that:
  - `lockFor(id)` returns the same lock object for an ID before and after its cache entry is evicted, invalidated, or reloaded, and before and after evictions of other IDs;
  - 64 threads working on 8 transaction IDs, with a cache cap of 1 forcing evictions on almost every access, never put two threads inside the same transaction's critical section at once, measured with a per-ID counter;
  - locks for different IDs are independent apart from stripe collisions, and the production stripe count is the fixed power of two 4,096;
  - **metrics** (Implementation Rule 2):
    - every acquisition records `transaction.lock.wait` and every release records `transaction.lock.hold`;
    - an acquisition that cannot take the lock immediately increments `transaction.lock.contended`;
    - contention is attributed to the same transaction or, when the holder is a different transaction on the same stripe, to `transaction.lock.collisions`. The test uses the test-only stripe-count constructor to force both cases deterministically.
- [X] T032 [P] [US2] Write `server/tests/database/JdbcScanStoreTest.java`, migrating `server/tests/database/JdbcAcceptedScanStoreTest.java`, and update its ledger rows. Assert that:
  - `appendScan` runs the guarded UPDATE and the `accepted_scan` INSERT in one commit. This covers `eachSuccessfulScanCommitsLineTotalAndUnpositionedEventTogether`: the totals and the undelivered event commit together.
  - an SQL failure in either statement leaves neither. This covers the SQL half of `invalidSkuAndSqlFailureLeaveNoPartialScan`; T036 covers the invalid-SKU half.
  - a wrong expected `item_count`, a non-OPEN status, or a non-null `active_attempt_number` gives `GUARD_MISS` and writes nothing;
  - concurrent `appendScan` calls with the same expectation give exactly one OK, and every committed row has a distinct `scan_sequence`. This covers the store half of `concurrentScansCommitDistinctUnpositionedEvents`; T036 covers the service half.
  - `scan_sequence` equals the new `item_count`, and the row stores the supplied captured price and name. T033 and T036 cover `repeatedSkuKeepsFirstCapturedPriceAfterCatalogChange`.
  - a deadlock retries only before commit, and a failing commit raises `commitOutcomeUnknown` without a retry, for both `FaultingConnections` commit faults.
- [X] T033 [P] [US2] Write `server/tests/database/JdbcTransactionStoreTest.java`, migrating the transaction parts of `server/tests/database/JdbcStoreAdaptersTest.java`. Assert that:
  - `insertOpen` stores the service-supplied `started_at`;
  - `findById` returns status, `item_count`, total, and `started_at`;
  - `loadState` returns the row plus lines grouped from `accepted_scan`: quantity equals count, with the first captured price and name even after a later catalog change. This is the cold half of `JdbcAcceptedScanStoreTest#repeatedSkuKeepsFirstCapturedPriceAfterCatalogChange`.
  - an unknown ID returns empty;
  - SQL failures surface as `StoreFailure`, with no JDBC type in the store interface. This is the transaction half of `sqlFailuresAreWrappedAsTransportNeutralStoreFailures`.
- [X] T034 [P] [US2] Write `server/tests/database/InventoryReadAdaptersTest.java`, migrating the inventory parts of `server/tests/database/JdbcStoreAdaptersTest.java` with `JdbcCatalogStore` and `JdbcInventoryStore` over `InventoryTestSchema`. Assert:
  - the catalog in SKU order;
  - low stock with an inclusive threshold, in SKU order, with default threshold 50 and override behavior;
  - SQL failures surface as `StoreFailure`, the inventory half of `sqlFailuresAreWrappedAsTransportNeutralStoreFailures`.

  Then, together with T033, set both `JdbcStoreAdaptersTest` rows to `MIGRATED`.
- [X] T035 [P] [US2] Adapt `server/tests/unit/transaction/StoreQueryServiceTest.java` in place, keeping its three methods (ledger: `ADAPTED`). `listItems` is served from `CatalogCache` through fake catalog and meta stores, while `listLowStock` still reads `InventoryStore` on every call.
- [X] T036 [P] [US2] Rewrite `server/tests/unit/transaction/TransactionServiceTest.java` for the new collaborators, using fakes. First copy the original to `server/tests-legacy/unit/transaction/TransactionServiceTest.java` as the parity reference. Assert that:
  - Start returns OPEN with zero items and a millisecond-precision `startedAt`;
  - Start, Scan, and Lookup survive service reconstruction. This is the non-Complete half of `startScanCompleteAndDurableLookupAcrossServiceReconstruction`; T058 covers the Complete half.
  - Scan precedence is 400, then 404 `TRANSACTION_NOT_FOUND`, then 409 `TRANSACTION_NOT_OPEN`, then 404 `SKU_NOT_FOUND` (FR-020). Each 404 or 409 is confirmed against the store before responding, and a rejected Scan writes nothing. This covers the missing and unknown cases of `reportsMissingUnknownEmptyInsufficientAndDuplicateCases`, and the invalid-SKU half of `JdbcAcceptedScanStoreTest#invalidSkuAndSqlFailureLeaveNoPartialScan`.
  - a repeated SKU keeps its first captured price after a replica swap to a new price. This is the warm half of `JdbcAcceptedScanStoreTest#repeatedSkuKeepsFirstCapturedPriceAfterCatalogChange`.
  - a failed store write acknowledges nothing and corrupts no other basket (`failedStoreWriteCannotAcknowledgeScanOrCorruptAnotherBasket`);
  - concurrent Scans on one transaction all succeed with contiguous sequences and distinct rows. This is the service half of `JdbcAcceptedScanStoreTest#concurrentScansCommitDistinctUnpositionedEvents`.
  - `GUARD_MISS` reloads and re-evaluates;
  - an unusable replica raises `ProductCatalogUnavailableFailure`, not `SKU_NOT_FOUND` (FR-023);
  - Lookup reads the store, not the cache.
- [X] T037 [P] [US2] Rewrite `server/tests/unit/transaction/TransactionBoundaryIsolationTest.java` for the new collaborators. First copy the original to `server/tests-legacy/unit/transaction/TransactionBoundaryIsolationTest.java`. Keep both methods:
  - `publicBusinessContractExposesNoHttpOrJdbcTypes`, unchanged.
  - `serviceUsesOnlyDurableStoreBoundariesWithoutProcessBasketAuthority`:
    - there is still exactly one constructor, and the test asserts its exact parameter-type array, which T058 updates when Complete is delegated;
    - every field is final;
    - `TransactionService` has no `Map` or `ConcurrentHashMap` field.
  - The ledger records the equivalence note: the cache is a rebuildable projection behind guarded writes, and T039 and T139–T141 prove that it holds no basket authority (FR-057). T027 additionally keeps mutable maps out of every business service, so only the designated cache components hold in-memory state.
- [X] T038 [P] [US2] Write `server/tests/performance/ScanStatementBudgetTest.java` (R22). Use `CountingConnections` and `FaultingConnections` through the pool decorator, an in-process `TransactionService` over `TransactionTestSchema`, and a fake `InventoryGateway` that fails if called.
  - **Strict, exact, gating**:
    - a Scan's response is produced only after its commit returns: with the commit delayed 300 ms, no result is available before the commit completes;
    - a commit failure of either kind yields a failure and no acknowledgement;
    - the guarded UPDATE and the `accepted_scan` INSERT share one commit;
    - Start and Scan make zero gateway calls.
  - **Budget guard**, through `StatementBudgets` (T135) with keys `scan.warm.*`, `start.*`, and `scan.cold.*`:
    - a warm Scan's statements (design 2: 1 UPDATE, 1 INSERT), SELECTs (design 0), commits (design 1), and `setAutoCommit` or `setTransactionIsolation` calls (design 0);
    - Start's statements (design 1) and SELECTs (design 0);
    - a cold Scan's extra reads (design 2), once per load.
- [X] T039 [P] [US2] Write `server/tests/performance/TransactionCacheCorrectnessTest.java` (R19). The database's guarded writes must decide every outcome.
  - Change the row behind a warm cache: set status COMPLETED, set `active_attempt_number`, or change `item_count`. The next Scan must give the same outcome as with `TRANSACTION_CACHE_MAX_ENTRIES=0`, the guarded-write miss counter must increase, and the entry must reload.
  - A stale-OPEN cache plus an unknown SKU must still answer 409 when the database says COMPLETED (error confirmation, R19).
  - A scripted sequence must produce identical responses and identical rows with the cache on and off.
  - With the cache on, no write the database rejected is acknowledged: every 200 has its `accepted_scan` row.
- [X] T139 [P] [US2] Write `server/tests/performance/CacheEvictionRaceTest.java` (R19). Run an in-process `TransactionService` over `TransactionTestSchema` with `TRANSACTION_CACHE_MAX_ENTRIES=2`. 32 threads issue Scans and Lookups across 8 OPEN transactions in random order for 10 s, so entries are evicted and reloaded while other requests for the same transaction are in flight. Assert that:
  - per transaction, `item_count` equals the number of `accepted_scan` rows, sequences are contiguous from 1, and `total_amount` equals the sum of captured prices;
  - every 200 response has exactly one row, and every row has exactly one 200 response;
  - `TransactionLocks` never admits two holders for one transaction, measured with an instrumented counter;
  - the final cached state equals a fresh reload from the database;
  - **collisions affect performance only**: the same workload runs a second time with the test-only stripe count of 1, so every pair of transactions collides. It must finish within its time limit with no deadlock, satisfy every invariant above, and give the same per-transaction outcomes as the 4,096-stripe run. Only `transaction.lock.collisions` and lock wait time may differ.
- [X] T140 [P] [US2] Write `server/tests/performance/MultiInstanceTransactionTest.java` (R19, rule 7). Run two independent in-process `TransactionService` instances, each with its own cache and locks, over one `TransactionTestSchema`, and send concurrent Scans for the same transactions through both. Assert that:
  - every response is one the spec allows, and every acknowledged Scan has its row;
  - the per-transaction invariants of T139 hold;
  - guarded-write misses are counted, and the instance that misses reloads and re-evaluates;
  - every 404 or 409 from either instance is confirmed against the database.

  This shows that the database, not either cache, decides outcomes. The deployment runs one instance, but no guarantee may depend on that (Implementation Rule 3).
- [X] T141 [P] [US2] Write `server/tests/performance/AmbiguousCommitReconcileTest.java` (R19). Use `FaultingConnections` to make one Scan commit fail in each of two ways: before the commit, and after a commit that succeeded. In both cases, assert that:
  - the Scan answers 500 `INTERNAL_ERROR` and is not acknowledged;
  - the cache entry is invalidated before the transaction's lock is released;
  - the next request for that transaction reloads it from the database before any mutation, observed as one cold load;
  - the next Scan gets `scan_sequence` equal to the durable `item_count` plus 1, so there is no duplicate sequence, no gap, and no lost row, whichever way the commit went;
  - other transactions' entries are unaffected.
- [X] T040 [P] [US2] Write `server/tests/performance/CacheRebuildAfterRestartTest.java`. With Inventory stopped, restart the Transaction process through `ServiceProcesses`. Assert that:
  - the product snapshot loads from `product_replica`, and scans succeed with the correct names and prices;
  - the lazily rebuilt transaction cache gives the same `itemCount`, `runningTotal`, and lines as before the restart.
- [X] T042 [P] [US2] Write `server/tests/integration/TransactionRestartDurabilityTest.java` for the per-schema topology. First move the original to `server/tests-legacy/integration/TransactionRestartDurabilityTest.java`. Migrate `failedDatabaseWriteReturnsInternalErrorWithoutAcknowledgingScan` with every assertion. T142 migrates the two methods that need Complete.
- [X] T043 [P] [US2] Write `server/tests/integration/ScanWithoutPeersTest.java` covering US2 scenarios 1, 2, 5, and 6 with service processes:
  - Stop Inventory and Analytics after the replica loads. Start, Scan, and Lookup succeed, including after a Transaction restart.
  - Scanning leaves stock unchanged (`InventoryTestSchema.stock`).
  - Rejected scans (unknown SKU, completed transaction) write no `accepted_scan` row.
  - Accepted rows keep `delivered_at` NULL.
- [X] T105 [P] [US2] Rewrite `server/tests/contract/LowStockEndpointTest.java` for Inventory's own schema. First move the original to `server/tests-legacy/contract/LowStockEndpointTest.java`. Keep both methods with every assertion:
  - `thresholdsAreInclusivePerSkuAndOverridesAreRequestLocal`: default threshold 50, the override, the inclusive boundary, and SKU order. Stock fixtures change in `cs6510_inventory` with Inventory's own credentials (`INVENTORY_DB_USER`, `INVENTORY_DB_PASSWORD`), and every fixture change is restored so the invariants hold.
  - `rejectsInvalidThresholdsAndUnmatchedRoutes`: `INVALID_THRESHOLD`, wrong methods, and unmatched paths.

### Implementation for User Story 2

- [X] T044 [P] [US2] Create the Inventory catalog cache in `server/inventory/src/inventory/CatalogCache.java` and `server/inventory/src/database/JdbcCatalogMetaStore.java` (R8):
  - `server/inventory/src/database/JdbcCatalogMetaStore.java` reads the `catalog_meta` singleton.
  - `server/inventory/src/inventory/CatalogCache.java` holds an immutable snapshot (version plus products in SKU order) loaded at construction. `currentVersion()` re-reads `catalog_meta` and reloads the snapshot on a change. `snapshot()` returns the cached snapshot. Stock is never cached.
- [X] T045 [US2] Wire the catalog cache into Inventory through `server/inventory/src/inventory/StoreQueryService.java`, `server/inventory/src/rpc/InventoryGrpcService.java`, and `server/inventory/src/inventory/InventoryMain.java`:
  - `server/inventory/src/inventory/StoreQueryService.java`: serve `listItems` from `CatalogCache`; low stock still reads `JdbcInventoryStore`.
  - Create `server/inventory/src/rpc/InventoryGrpcService.java` with `GetCatalogVersion` and `GetCatalog`. `price_cents` is price × 100, exactly, in SKU order. The checkout RPCs return UNIMPLEMENTED until T069.
  - Rewire `server/inventory/src/inventory/InventoryMain.java`:
    - `DatabaseConfig.fromEnvironment("INVENTORY", "cs6510_inventory", "cs6510_inventory_svc", 16, 48)` with `new ConnectionPool(...)`;
    - HTTP on `INVENTORY_PORT` (8082) with the existing handlers, and gRPC on `INVENTORY_GRPC_PORT` (9092) through `GrpcServers`;
    - `MetricsReporter.start("inventory")`;
    - a shutdown hook that stops HTTP, then gRPC, then the reporter, then the pool;
    - keep `HttpServer.create`, `createContext(`, `newVirtualThreadPerTaskExecutor`, `new ConnectionPool(`, and `addShutdownHook` for `CompositionRootTest`.
- [X] T046 [P] [US2] Create `server/transaction/src/database/ProductReplicaStore.java` and `JdbcProductReplicaStore.java` (R4):
  - `load()` returns empty when the meta version is NULL.
  - `replace(version, products)` runs in one `JdbcUnitOfWork`: DELETE `product_replica`, a batched multi-row INSERT, then UPDATE `product_replica_meta` with version, count, and `refreshed_at`.
- [X] T047 [P] [US2] Create the Inventory gateway port `server/transaction/src/transaction/InventoryGateway.java` and its gRPC adapter `server/transaction/src/rpc/GrpcInventoryGateway.java`:
  - Port `server/transaction/src/transaction/InventoryGateway.java` with `catalogVersion()` and `catalog()`.
  - `server/transaction/src/rpc/GrpcInventoryGateway.java` implements it over a reused channel, with deadlines of 2 s (version) and 5 s (catalog). `RpcStatusClassifier` maps uncertain statuses to a new `server/transaction/src/transaction/DependencyUnavailableFailure.java`.
- [X] T048 [US2] Create the in-memory catalog snapshot `server/transaction/src/transaction/ProductCatalogReplica.java` and its refresher `server/transaction/src/transaction/CatalogReplicaRefresher.java` (R4):
  - `server/transaction/src/transaction/ProductCatalogReplica.java`: a volatile immutable snapshot with `lookup(sku)` and `usable()`.
  - `server/transaction/src/transaction/CatalogReplicaRefresher.java`, a daemon:
    - loads the durable replica at startup;
    - every `CATALOG_REFRESH_SECONDS` (30), compares Inventory's version with the stored one, and on a change or an empty replica, fetches, calls `replace`, then swaps;
    - retries every 1 s while never populated, and backs off from 1 s to 30 s on failure;
    - records the `transaction.replica.refresh` metric.
- [X] T049 [P] [US2] Create `server/transaction/src/transaction/TransactionStateCache.java` and `server/transaction/src/transaction/TransactionLocks.java` (R19):
  - **`TransactionLocks`**:
    - a fixed array of 4,096 `ReentrantLock` stripes, indexed by the spread hash of the transaction ID, plus a test-only constructor that takes a smaller power-of-two stripe count;
    - stripes are never created, evicted, or replaced, so evicting or invalidating a cache entry can never produce a second lock for one transaction;
    - code holds at most one stripe at a time, and never holds one across a remote call or a wait. A collision can therefore only delay a request by one short local transaction, and can never deadlock or change an outcome.
    - `acquire(txId)` returns a handle whose `close()` releases the stripe. It tries the lock without waiting first, and records:
      - `transaction.lock.wait` and `transaction.lock.hold` histograms;
      - the counters `transaction.lock.acquisitions` and `transaction.lock.contended`;
      - `transaction.lock.collisions`, when the current holder, kept in a per-stripe field, is a different transaction.
  - **`TransactionStateCache` entries** follow data-model.md's "Transaction state cache": status, `item_count`, total, `attempt_count`, `active_attempt_number`, and lines (SKU to quantity, price, and name). Entries hold no lock.
  - **Methods**: `getOrLoad(txId, loader)`, post-commit updates, `invalidate`, and `evict`. Each checks that the caller holds `TransactionLocks.lockFor(txId)`, and callers read an entry only after acquiring that lock.
  - The LRU cap comes from `TRANSACTION_CACHE_MAX_ENTRIES` (default 100000; 0 disables caching).
  - Counters for hits, misses, guard misses, and invalidations.
- [X] T050 [P] [US2] Replace the scan store with `server/transaction/src/database/JdbcScanStore.java` and extend `server/transaction/src/database/JdbcTransactionStore.java` (R10, R19; contracts/scan-event-delivery.md §1):
  - Replace `server/transaction/src/database/AcceptedScanStore.java` and `JdbcAcceptedScanStore.java` with `ScanStore.java` and `JdbcScanStore.java`.
  - `appendScan(txId, expectedItemCount, newTotal, row)` runs, in one `JdbcUnitOfWork`:
    1. `UPDATE transaction SET item_count=?, total_amount=? WHERE transaction_id=? AND item_count=? AND status='OPEN' AND active_attempt_number IS NULL`;
    2. `INSERT INTO accepted_scan (transaction_id, scan_sequence, sku, product_name, unit_price, accepted_at)`.

    It returns OK, or `GUARD_MISS` with a rollback when zero rows are updated. A failing commit raises `StoreFailure` with `commitOutcomeUnknown` and is never retried.
  - Update `server/transaction/src/database/TransactionStore.java` and `JdbcTransactionStore.java`:
    - `insertOpen(txId, stationId, startedAt)`, with no read-back;
    - `findById`;
    - `loadState`: the row plus `SELECT sku, COUNT(*), MIN(unit_price), MIN(product_name) FROM accepted_scan WHERE transaction_id=? GROUP BY sku`.
  - **Legacy tests broken by this task**: deleting `JdbcAcceptedScanStore` and changing `TransactionStore` breaks the compilation of `server/tests/support/CheckoutDatabase.java` and its users. Move to `server/tests-legacy/` each of these still in `server/tests/`. Their ledger rows keep their status and owners:
    - `support/CheckoutDatabase`;
    - `database/JdbcCheckoutCompletionStoreTest` (T055, T056);
    - `integration/DurableBasketTest` (T041);
    - `integration/DatabaseOutageContractTest` (T109);
    - `integration/ScanCompletionConvergenceTest` (T059, T079);
    - `integration/InventoryServiceIsolationTest` (T114);
    - `integration/AnalyticsBacklogIsolationTest` (T116);
    - `integration/AnalyticsCheckoutIsolationTest` (T115);
    - `integration/IndependentServiceDeploymentTest` (T112);
    - `database/JdbcAcceptedScanStoreTest` (T032, T033, T036);
    - `database/JdbcStoreAdaptersTest` (T033, T034).
- [X] T051 [US2] Rewrite Start, Scan, and Lookup in `server/transaction/src/transaction/TransactionService.java` (R19, FR-019–FR-023):
  - Start uses a millisecond-truncated service timestamp and caches the entry.
  - Scan acquires `TransactionLocks.lockFor(txId)` first, and only then reads or loads the cache entry:
    - it enforces validation precedence, confirming 404 and 409 against the database first;
    - it prices from `ProductCatalogReplica`, reusing the first captured price and name for repeated SKUs;
    - it throws a new `ProductCatalogUnavailableFailure` (unmapped, so 500) when the replica is unusable;
    - on `GUARD_MISS`, it invalidates, reloads, and re-evaluates, at most 3 times;
    - on a `StoreFailure` with `commitOutcomeUnknown`, it invalidates the entry before releasing the lock and answers 500. The next request then reconciles from the database before any mutation.
    - a pending attempt raises a new `CheckoutPendingFailure` (unmapped, so 500) until T085.
  - Lookup reads the store.
  - Delete `server/transaction/src/transaction/database/JdbcCatalogStore.java` and `server/transaction/src/database/ScanProfiler.java`.
  - Replace the profiler calls in `server/transaction/src/api/TransactionHttpHandler.java` with `PerfMetrics` timers `transaction.http.start|scan|complete|get`, leaving responses unchanged.
- [X] T052 [US2] Rewire `server/transaction/src/transaction/TransactionMain.java`:
  - `DatabaseConfig.fromEnvironment("TRANSACTION", "cs6510_transaction", "cs6510_transaction_svc", 32, 64)` with `new ConnectionPool(...)`.
  - `JdbcProductReplicaStore`, `ProductCatalogReplica`, and `CatalogReplicaRefresher`, using `GrpcInventoryGateway` on `INVENTORY_GRPC_TARGET` (127.0.0.1:9092).
  - `TransactionLocks`, `TransactionStateCache`, and `TransactionService`. Keep the existing completion store temporarily until T074.
  - HTTP on `TRANSACTION_PORT` (8081) and `MetricsReporter.start("transaction")`.
  - A shutdown hook that stops HTTP, then the refresher, then calls `GrpcChannels.shutdownAll()`, then stops the reporter, then the pool.
  - **Legacy tests broken by this task**: Transaction no longer uses `cs6510_selfcheckout`, which these live tests still read or reset. Move them to `server/tests-legacy/`; their ledger rows keep their status and owners:
    - `integration/ConcurrentCompletionTest` (T060);
    - `integration/DuplicateCompletionTest` (T061);
    - `integration/StockConservationTest` (T062);
    - `integration/CompletionDurabilityTest` (T063);
    - `integration/ResetBaselineTest` (T113).
- [X] T053 [US2] Verify US2 and run its parity gate with `server/run-tests.sh`:
  - Run `server/build.sh`, `server/build-tests.sh`, and `PARITY_GATE=US2 server/run-tests.sh` against MySQL with the new Inventory and Transaction services.
  - The whole suite must pass, including T028–T040, T042, T043, T105, and T138–T141, apart from the exception below.
  - **Exception**: the live tests that call Complete cannot pass between T052 and T074, because Complete moves to the Saga in US1. These are `contract/TransactionLifecycleTest`, `contract/TransactionLookupTest#getCompletedTransaction_fallsBackToPersistedTotals`, and `contract/LowStockEndpointTest#thresholdsAreInclusivePerSkuAndOverridesAreRequestLocal`, whose Complete step checks that a sale lowers stock. The US1 gate (T146) verifies them.
  - Then perform the US2 Independent Test through `http://localhost:8081`, and confirm that the `accepted_scan` rows are undelivered.
  - Record `T053` in `Verified at` for every row whose test passed.

**Checkpoint**: Scan is memory-first, durable, and independent of Inventory and Analytics. Every US2-owned legacy assertion has a passing equivalent.

---

## Phase 4: User Story 1 - Complete Checkout Across Independently Owned Data (Priority: P1) 🎯 MVP (with US2)

**Goal**: Complete runs the Transaction-orchestrated Saga with forward recovery:
- T1 is a guarded begin-attempt.
- Inventory's single batched `ApplyCheckout` deducts stock atomically and idempotently.
- T3 records completion.

The receipt and every error match the established contract, with captured prices and no overselling.

**Independent Test**: With Transaction and Inventory on separate schemas, seed controlled stock, then scan and complete known baskets, including concurrently and with insufficient stock. Compare receipts, transaction status, and Inventory stock with expected values.

### Tests for User Story 1 ⚠️ write first; they must fail before implementation

- [X] T054 [P] [US1] Write `server/tests/rpc/InventoryCheckoutContractTest.java` (contracts/grpc-internal-contracts.md §ApplyCheckout). Run an in-process `InventoryGrpcService` over `InventoryTestSchema`, and assert:
  - **Outcomes**:
    - APPLIED deducts every line once;
    - INSUFFICIENT_STOCK names the first short SKU in SKU order and deducts nothing;
    - INVALID_REQUEST names the first unknown SKU and deducts nothing.
  - **Replays**:
    - repeating an attempt returns the recorded outcome with `replayed=true` and no stock change;
    - the same key with different lines returns FAILED_PRECONDITION.
  - **Concurrency**:
    - 20 concurrent duplicates of one attempt deduct once;
    - two attempts racing for the last unit give one APPLIED, one INSUFFICIENT_STOCK, and final stock 0;
    - contention never yields INSUFFICIENT_STOCK when stock suffices.
  - **Validation**: empty lines, a duplicate SKU, quantity 0, a `transaction_id` longer than 40 characters, or `attempt_number` 0 return INVALID_ARGUMENT and record nothing.
  - **Integrity**: a second APPLIED attempt for the same transaction returns FAILED_PRECONDITION.
  - **Outcome query**: `GetCheckoutOutcome` returns NOT_PROCESSED for unknown keys.
  - **Multiple instances** (Constitution II): run two `InventoryGrpcService` instances, each with its own catalog cache and pool, over one schema. The duplicate and last-unit races give the same results when split across instances: one deduction, no overselling, final stock 0.
- [X] T055 [P] [US1] Write `server/tests/database/JdbcCheckoutOperationStoreTest.java`, migrating the inventory side of `server/tests-legacy/database/JdbcCheckoutCompletionStoreTest.java`. Assert:
  - APPLIED deducts every line exactly once and stores the canonical `lines` text, and a replay deducts nothing. This is the inventory half of `commitsPersistedDistinctLinesOnceAndRejectsDuplicate`.
  - INSUFFICIENT_STOCK leaves every stock row unchanged. This is the inventory half of `insufficientStockRollsBackStatusAndEveryDecrement`.
  - two attempts racing for the last unit give one APPLIED, one INSUFFICIENT_STOCK, and stock 0 (`contendingCompletionsCannotBothSellLastUnit`);
  - the CHECK constraint blocks negative stock, and deadlocks retry before commit;
  - restore follows the state table: APPLIED→RESTORED; RESTORED→already restored; REJECTED or BLOCKED→nothing to restore; absent→BLOCKED;
  - apply after RESTORED or BLOCKED returns STOCK_RESTORED.
- [X] T056 [P] [US1] Write `server/tests/database/JdbcCheckoutAttemptStoreTest.java`, migrating the transaction side of `server/tests-legacy/database/JdbcCheckoutCompletionStoreTest.java`. Together with T055, set its three ledger rows to `MIGRATED`. Assert that:
  - `begin` is guarded (OPEN, no active attempt, expected `item_count`, expected `attempt_count`). In one commit, it inserts the PENDING attempt with `lines`, totals, and `next_retry_at = created_at + CHECKOUT_RESPONSE_BUDGET_MS`. A stale expected `attempt_count` gives `GUARD_MISS` and writes nothing, never a duplicate-key failure.
  - `recordClientOutcome(key, outcome)` updates only an attempt whose `client_outcome` is `NONE`, and changes nothing otherwise;
  - `complete` is guarded by `active_attempt_number`, and settles the attempt and the transaction together with the supplied `completed_at`.
  - the unique generated columns reject a second PENDING attempt, and a second COMPLETED attempt. The latter is the transaction half of `commitsPersistedDistinctLinesOnceAndRejectsDuplicate`.
  - `reopen` clears the active attempt, records the reason and `rejected_sku`, and leaves the transaction OPEN with its basket. This is the transaction half of `insufficientStockRollsBackStatusAndEveryDecrement`.
  - `markPendingError` changes only `NONE` to `PENDING_ERROR` on PENDING attempts, and sets `next_retry_at` to now in the same UPDATE;
  - `findPending(dueBefore, afterKey, limit)` returns at most `limit` rows in `(next_retry_at, transaction_id, attempt_number)` order with keyset pagination. Its `EXPLAIN` shows a range scan on the `(state, next_retry_at)` index, not a full scan.
- [X] T057 [P] [US1] Write `server/tests/unit/transaction/CheckoutOrchestratorTest.java` with a fake `InventoryGateway`, fake stores, and a fake `RecoverySchedule` (contracts/checkout-saga.md). Assert that:
  - APPLIED runs T3 and returns a receipt built from the cached snapshot and the supplied `completedAt`;
  - INSUFFICIENT_STOCK reopens the basket and answers 409 `INSUFFICIENT_STOCK` with "Insufficient stock for SKU: <sku>";
  - INVALID_REQUEST or STOCK_RESTORED reopens and raises an unmapped failure (500);
  - uncertain statuses are retried with the same `(transaction_id, attempt_number)` after 50, 100, 200, and 400 ms within the 8 s budget;
  - the transaction's stripe lock is not held while `ApplyCheckout` runs;
  - on budget expiry, the orchestrator:
    1. runs `markPendingError`;
    2. enqueues the attempt in `RecoverySchedule`, even when that write fails;
    3. raises `CheckoutPendingFailure`, with the attempt still PENDING.
  - a T1 commit with an unknown outcome invalidates the cache entry, enqueues a reconcile entry for `(transaction_id, n)`, and raises an unmapped failure (500) without calling Inventory;
  - a T3 commit with an unknown outcome is retried within the budget. If the retry's guard misses because the first commit succeeded, the orchestrator reads the settled attempt and returns 200 with the durable `completed_at`.
  - **answering from another actor's settlement**: when a T3 or reopen guard misses because another actor settled the request's own attempt, the request records `SUCCESS` or `REJECTED` with `recordClientOutcome` before it answers 200 or 409. The other actor may be the recovery worker of this or another instance. If that write fails, the request answers 500 instead.
  - FAILED_PRECONDITION never reopens the basket;
  - pre-checks run in the order 404, then 409 `TRANSACTION_NOT_OPEN`, then 409 `EMPTY_BASKET`, each confirmed against the store.
- [X] T058 [P] [US1] Extend `server/tests/unit/transaction/TransactionServiceTest.java` with the Complete halves of the legacy methods. Then update the constructor assertion in `server/tests/unit/transaction/TransactionBoundaryIsolationTest.java` for the delegated orchestrator.
  - Complete after service reconstruction returns the receipt with captured prices. This is the Complete half of `startScanCompleteAndDurableLookupAcrossServiceReconstruction`.
  - An empty basket gives 409 `EMPTY_BASKET`, insufficient stock gives 409 `INSUFFICIENT_STOCK`, and a duplicate Complete gives 409 `TRANSACTION_NOT_OPEN`. These are the Complete cases of `reportsMissingUnknownEmptyInsufficientAndDuplicateCases`.
  - A repeated Complete after COMPLETED makes no gateway call (FR-036).
  - Set the `TransactionServiceTest` and `TransactionBoundaryIsolationTest` ledger rows to `MIGRATED`.
- [X] T059 [P] [US1] Write `server/tests/integration/CheckoutSagaTest.java` covering US1 scenarios 1, 2, 5, and 6, with Transaction and Inventory processes over disposable schemas:
  - Two units of A and one of B give a receipt with captured prices, stock decreases of 2 and 1, and lookup COMPLETED.
  - Insufficient stock gives 409, no stock change, and the transaction stays OPEN. Raising stock, scanning again, and retrying Complete then succeeds. This migrates `ScanCompletionConvergenceTest#failedCompletionLeavesOpenBasketForScanningAndRetry` with every assertion.
  - An unknown transaction gives 404. An empty basket gives 409 `EMPTY_BASKET`, and Inventory reports NOT_PROCESSED.
  - A catalog price change in Inventory after scanning leaves the receipt at the captured price.
  - **Reopen after INVALID_REQUEST, end to end** (Constitution IX):
    - Remove a scanned SKU from Inventory's disposable catalog before Complete, so Inventory answers INVALID_REQUEST.
    - Complete returns 500 `INTERNAL_ERROR`, the transaction reopens with its basket, the attempt is REJECTED with that reason, and no stock changes.
    - With the insufficient-stock case above, this covers every compensation that a normal Saga path can trigger.
- [X] T060 [P] [US1] Write `server/tests/integration/ConcurrentCompletionTest.java` (US1 scenario 3), migrating `concurrentCompletion_exactlyOneSucceeds_stockNeverNegative` with every assertion. Read and set stock in `cs6510_inventory` with Inventory's own credentials.
- [X] T061 [P] [US1] Write `server/tests/integration/DuplicateCompletionTest.java` (US1 scenario 4), migrating `duplicateCompletion_stockDecrementedExactlyOnce` with every assertion. Read stock in `cs6510_inventory` with Inventory's own credentials instead of `cs6510_selfcheckout`.
- [X] T062 [P] [US1] Write `server/tests/integration/StockConservationTest.java`, migrating `concurrentCompletions_stockConservation` with every assertion. Account for any fixture stock restore, so that the invariants still hold.
- [X] T063 [P] [US1] Write `server/tests/integration/CompletionDurabilityTest.java`, migrating `completedTransaction_committedBeforeResponseReturned` with every assertion. Check the transaction row in `cs6510_transaction` with Transaction's credentials, and stock in `cs6510_inventory` with Inventory's.
- [X] T064 [P] [US1] Write `server/tests/rpc/ProtoCompatibilityTest.java` and its baseline `server/tests/rpc/baseline/descriptor-summary.txt` (FR-013):
  - Generate the baseline once from the initial `contracts.jar` descriptors.
  - Assert that every baseline message field keeps its number, name, and type; every enum value keeps its number; and every RPC keeps its request and response types. Additions are allowed.
- [X] T065 [P] [US1] Write `server/tests/performance/CompleteStatementBudgetTest.java` (R22).
  - **Strict, exact, gating**:
    - T1 commits before `ApplyCheckout` is sent: with the T1 commit delayed, the fake gateway sees no call until the commit returns;
    - no 200 is produced before the T3 commit returns;
    - a warm Complete makes exactly one `ApplyCheckout` call;
    - no read follows T3 on the warm path, because the receipt comes from memory.
  - **Budget guard**, with `StatementBudgets` keys `complete.t1.*`, `complete.t3.*`, and `complete.reopen.*`:
    - T1: statements (design 2: 1 UPDATE, 1 INSERT) and commits (design 1);
    - T3: statements (design 2 UPDATEs) and commits (design 1);
    - reopen: statements (design 2 UPDATEs) and commits (design 1).
- [X] T066 [P] [US1] Write `server/tests/performance/InventoryApplyStatementBudgetTest.java` (R22).
  - **Strict**: the outcome row and the stock UPDATE share one commit, and a replay or a rejection issues no stock UPDATE.
  - **Budget guard**, with keys `inventory.apply.applied.*` and `inventory.apply.rejected.*`, for baskets of 1, 5, and 20 lines:
    - APPLIED: statements (design 3: 1 locking SELECT, 1 INSERT, 1 UPDATE) and commits (design 1);
    - rejected: statements (design 2: 1 SELECT, 1 INSERT) and commits (design 1);
    - the counts must not grow with basket size.
- [X] T067 [P] [US1] Write `server/tests/performance/HeldConnectionDuringRpcTest.java`. While a fake `InventoryGateway` blocks inside `ApplyCheckout`, assert that:
  - the Transaction pool's active count is back at its baseline;
  - another connection can run `SELECT … FROM transaction WHERE transaction_id=? FOR UPDATE NOWAIT` immediately, so no row lock is held (FR-033).
- [X] T041 [P] [US1] Write `server/tests/integration/DurableBasketTest.java` for the per-schema topology, with Transaction and Inventory over disposable schemas. Migrate all three legacy methods with every assertion: `completedBasketKeepsCapturedPriceAndRejectsFurtherScansOrCompletion`, `failedCompletionKeepsTheSameOpenBasketAndAllowsAnotherScan`, and `concurrentScansCommitPositiveQuantitiesAndDistinctEvents`. Each one completes a checkout, so the file belongs to US1.
- [X] T109 [P] [US1] Write `server/tests/integration/DatabaseOutageContractTest.java`, migrating `failedScanAndCompletionReturnInternalErrorThenCommittedStateRecovers` with every assertion:
  - **During the outage**: Transaction's `transaction` table is unavailable, renamed through `TransactionTestSchema`. Scan and Complete give 500 `INTERNAL_ERROR`, no `accepted_scan` row is written, and stock is unchanged.
  - **After recovery**: Scan gives 200; a Complete during a renewed outage gives 500; the next Complete gives 200, stock drops by one, and lookup shows COMPLETED.
  - **Isolation**: Inventory's routes are unaffected by Transaction's outage.
- [X] T142 [P] [US1] Extend `server/tests/integration/TransactionRestartDurabilityTest.java` with the two legacy methods that need Complete, keeping every assertion:
  - `acknowledgedOpenBasketSurvivesProcessRestartAndCompletesExactlyOnce`: the basket survives a Transaction process restart and completes with the captured receipt, and a repeated Complete gives 409.
  - `publicTransactionErrorsKeepTheirStatusesAndShapes`: the 400, 404, and 409 codes with the `{error, message}` shape, including `EMPTY_BASKET` and `TRANSACTION_NOT_OPEN`.
- [X] T143 [P] [US1] Extend `server/tests/performance/TransactionCacheCorrectnessTest.java` and `server/tests/performance/AmbiguousCommitReconcileTest.java` for Complete (R19):
  - **Stale cache**: change the row behind a warm cache before each guarded write (T1, T3, reopen), including a stale `attempt_count` before T1. Each guard misses, the entry reloads, and the outcome equals the cache-off outcome, with no duplicate-key failure.
  - **Ambiguous T1 commit**, for both `FaultingConnections` faults:
    - the Complete answers 500, the entry is invalidated, a reconcile entry is queued, and the next Complete reloads first;
    - if the first commit happened, the next Complete finds the pending attempt; otherwise, it begins attempt n again;
    - no second PENDING attempt ever exists.
  - **Ambiguous T3 commit**: the retry's guard miss leads to a 200 with the durable `completed_at`, one COMPLETED attempt, and one deduction.
- [X] T144 [P] [US1] Extend `server/tests/performance/MultiInstanceTransactionTest.java` for Complete:
  - Concurrent Completes of one transaction through both instances create exactly one attempt and one Inventory deduction, and give exactly one 200.
  - The other requests give 409 `TRANSACTION_NOT_OPEN`, or 500 while the attempt is pending; T085 later replaces the 500 with waiting.
  - A Scan through one instance during the other instance's pending attempt never joins the frozen basket.
  - After one instance's attempt is rejected for insufficient stock and the basket reopens, a Complete through the other instance, whose cache still holds the old `attempt_count`, misses its guard. It reloads and begins attempt n+1, with no spurious 500.

### Implementation for User Story 1

- [X] T068 [P] [US1] Create `server/inventory/src/database/CheckoutOperationStore.java` and `JdbcCheckoutOperationStore.java`:
  - Implement statements 1–5 of contracts/grpc-internal-contracts.md §ApplyCheckout, the §RestoreCheckout table, and `GetCheckoutOutcome`.
  - Deadlocks and lock-wait timeouts retry through `JdbcUnitOfWork`. Exhaustion raises a contention failure (ABORTED).
  - A duplicate primary key reads the recorded row: different lines are a mismatch, RESTORED or BLOCKED gives STOCK_RESTORED, and otherwise the outcome is replayed.
  - A unique `applied_transaction_id` violation raises an integrity failure (FAILED_PRECONDITION).
  - Metrics: `inventory.apply.lockWait` (locking-SELECT duration), `inventory.apply.commit`, and `inventory.apply.retries`.
- [X] T069 [US1] Create `server/inventory/src/inventory/CheckoutStockService.java`:
  - Contract validation.
  - Canonical lines `[{"sku":"…","quantity":n}]`, sorted by SKU with no whitespace.
  - Outcome mapping.

  Then extend `server/inventory/src/rpc/InventoryGrpcService.java` with `ApplyCheckout`, `GetCheckoutOutcome`, and `RestoreCheckout`:
  - validation errors give INVALID_ARGUMENT;
  - payload mismatch or an integrity violation gives FAILED_PRECONDITION;
  - `StoreFailure` gives UNAVAILABLE, contention gives ABORTED, and anything unexpected gives INTERNAL;
  - record the `inventory.grpc.apply` metric.
- [X] T070 [P] [US1] Create `server/transaction/src/database/CheckoutAttemptStore.java` and `JdbcCheckoutAttemptStore.java` per the contracts/checkout-saga.md Steps table:
  - `begin`: guarded UPDATE, whose `WHERE` clause includes the expected `attempt_count`, plus INSERT of the attempt with `next_retry_at = created_at + CHECKOUT_RESPONSE_BUDGET_MS`. Recovery therefore leaves a live request's attempt alone until its budget ends.
  - `complete`: guarded UPDATE plus an attempt UPDATE that sets `client_outcome='SUCCESS'` when the request is still being answered.
  - `reopen`, and `markPendingError`, which also sets `next_retry_at` to now.
  - `recordClientOutcome(key, outcome)`: `UPDATE checkout_attempt SET client_outcome=? WHERE transaction_id=? AND attempt_number=? AND client_outcome='NONE'`, then commit.
  - Recovery reads for T084:
    - `findPending(dueBefore, afterKey, limit)`, keyset-paginated over the `(state, next_retry_at)` index with `LIMIT` (default 100);
    - `findAttempt(key)`;
    - `scheduleRetry(key, nextRetryAt, lastError)`.
- [X] T071 [P] [US1] Add `applyCheckout(key, lines, deadline)` and `checkoutOutcome(key)` to `server/transaction/src/transaction/InventoryGateway.java`, and implement them in `server/transaction/src/rpc/GrpcInventoryGateway.java`:
  - The per-call deadline comes from `ResponseBudget`.
  - Results map to Applied, Rejected(reason, sku), StockRestored, Uncertain, PayloadMismatch, or InvalidRequest.
  - Record `transaction.rpc.applyCheckout` latency and status counters.
- [X] T145 [P] [US1] Create `server/transaction/src/transaction/RecoverySchedule.java` (R7):
  - It is an in-memory delay queue of attempt keys with due times, de-duplicated by key.
  - `enqueue(key, dueAt)` and `reconcile(key)` (due now) add entries; `take(timeout)` returns the next due key; `size()` feeds a gauge.
  - US1 only fills it; the recovery worker (T084) consumes it.
  - T084's startup scan rebuilds it from the database, so a crash loses nothing.
- [X] T072 [US1] Create `server/transaction/src/transaction/SettlementRegistry.java` and `server/transaction/src/transaction/CheckoutOrchestrator.java`. Implement the contracts/checkout-saga.md steps and response mapping within `CHECKOUT_RESPONSE_BUDGET_MS` (8000) and `INVENTORY_RPC_DEADLINE_MS` (2000):
  - `SettlementRegistry` holds one settlement future per pending attempt, plus live-request ownership. A settler completes a future only while holding the transaction's stripe lock, after the settlement commit.
  - T1 runs under `TransactionLocks.lockFor(txId)`, which is released before the RPC. Its guard repeats the cached `attempt_count`, so a stale entry leads to a reload instead of a duplicate attempt number.
  - In-request retries happen after 50, 100, 200, and 400 ms.
  - T3 and reopen run under the lock, followed by a cache update or eviction and registry completion.
  - On a T1 commit with an unknown outcome, it invalidates the entry, calls `RecoverySchedule.reconcile`, and raises an unmapped failure.
  - On a T3 commit with an unknown outcome, it retries within the budget. If the guard then misses, it reads the settled attempt and responds from it.
  - When another actor settled the request's own attempt, it records the request's `SUCCESS` or `REJECTED` outcome with `recordClientOutcome` before answering. If that write fails, it answers 500. FR-054's counts therefore stay exact when several instances or the recovery worker race.
  - On budget expiry, it writes `PENDING_ERROR` (with `next_retry_at` now), enqueues the attempt in `RecoverySchedule` in a `finally` block, then raises `CheckoutPendingFailure`.
  - The receipt comes from the cached snapshot.
  - Metrics: `transaction.complete.t1|t3|budgetExpired|ambiguousCommit`.
- [X] T073 [US1] Finish the Complete wiring in `server/transaction/src/transaction/TransactionService.java`:
  - Delegate Complete to `CheckoutOrchestrator`.
  - Delete `server/transaction/src/database/CheckoutCompletionStore.java` and `JdbcCheckoutCompletionStore.java`.
  - Confirm that `server/transaction/src/api/ApiErrorMapper.java` still maps only `TransactionFailure` codes. These failures therefore become 500 `INTERNAL_ERROR`: `CheckoutPendingFailure`, `ProductCatalogUnavailableFailure`, `DependencyUnavailableFailure`, and the INVALID_REQUEST and STOCK_RESTORED failure.
- [X] T074 [US1] Wire `JdbcCheckoutAttemptStore`, `RecoverySchedule`, `SettlementRegistry`, and `CheckoutOrchestrator` into `server/transaction/src/transaction/TransactionMain.java`. Remove the temporary completion store, and read the budget and deadline environment variables.
- [X] T146 [US1] Verify US1 and run its parity gate with `server/run-tests.sh`:
  - With Transaction and Inventory on their own schemas, run `PARITY_GATE=US1 server/run-tests.sh` with `SERVER_BASE_URL` set.
  - The whole suite must pass, including T041, T054–T067, T109, T142–T144, and the live Complete tests excused at T053.
  - Record `T146` in `Verified at` for every row whose test passed.
- [X] T075 [US1] CP1 attribution checkpoint (R22), recorded in `load-client/reports/microservices-architecture/checkpoints.md`:
  - Run `server/benchmark-microservices.ps1 -Workload default -SkipAnalytics -OutputLabel cp1-default-1`, then repeat with `cp1-default-2`.
  - Record in `checkpoints.md`:
    - the actual client-observed `totalTransactions` of each run, and its change against the CP0 default median (Implementation Rule 6);
    - server-side Scan and Complete p95/p99;
    - Transaction and Inventory pool borrow-wait and hold-time p99;
    - `transaction.lock.wait` p99, `transaction.lock.contended`, and `transaction.lock.collisions`;
    - `inventory.apply.lockWait` p99 and `transaction.rpc.applyCheckout` p99;
    - commits per completed transaction, which is the `Com_commit` delta divided by `totalTransactions`;
    - background completions.
  - CP1 runs without Analytics, so its comparison with CP0 is stated with that caveat and serves only to attribute costs. If `totalTransactions` is more than 5% below the CP0 default median, record the attribution from the metrics before starting US3: mandatory commits per basket against round trips saved (R18).
  - Statement counts and latencies explain a result but never establish an improvement on their own.

**Checkpoint**: MVP. Scan and Checkout work end to end across independently owned data, every US1-owned legacy assertion has a passing equivalent, and CP1 is recorded.

---

## Phase 5: User Story 3 - Settle Interrupted Checkouts Exactly Once (Priority: P1)

**Goal**: Every checkout interrupted by a timeout, a lost reply, a duplicate request, a crash, or a restart settles exactly once:
- by forward recovery, which completes the transaction if Inventory applied the deduction and reopens it otherwise;
- with event-driven bounded waiting for concurrent requests;
- with recovery driven by an in-memory schedule, plus bounded, indexed safety scans;
- with background completions counted;
- with the reserved stock-restoration operation verified directly.

**Independent Test**: Inject faults at every Saga step, crash and restart Transaction and Inventory, and duplicate and delay internal requests. Then verify settled states, client responses, and stock reconciliation. Verify separately that the direct restoration tests pass.

### Tests for User Story 3 ⚠️ write first; they must fail before implementation

- [X] T076 [P] [US3] Write `server/tests/unit/transaction/CheckoutRecoveryWorkerTest.java` with fake stores, a fake gateway, and a controllable clock. Assert that the worker:
  - **Event-driven work**: settles handover and reconcile entries from `RecoverySchedule` as soon as they are due.
    - It first reads the attempt by key. A PENDING attempt is replayed with its stored lines under the same key.
    - An attempt that something else already settled, such as another instance, is dropped, and any local registry future for it is completed.
    - An attempt that does not exist is dropped with no write.
  - **Process-local schedule**: the worker never assumes that its schedule or its registry knows about every attempt. If another process settles an attempt during a replay, the worker's T3 or reopen guard misses, it reads the settled state, and it ends without error.
  - **Startup**: resumes every PENDING attempt with a paginated startup scan, regardless of `next_retry_at`, because nothing is owned after a restart.
  - **Safety scan**:
    - runs every `CHECKOUT_RECOVERY_SCAN_SECONDS` (default 30) with `findPending(now, …, 100)`;
    - skips attempts that a live request owns or that are already scheduled;
    - issues no other recovery query in between, so an idle schedule costs no reads, as `transaction.recovery.scanQueries` shows.
  - **Settlement**: settles definitive outcomes by T3 or reopen under the transaction's stripe lock, updates or evicts the cache, and completes the registry future.
  - **Uncertain outcomes**: increments `retry_count`, sets `next_retry_at` to now plus the smaller of 5 s and 100 ms × 2^`retry_count`, stores `last_error`, and re-enqueues the attempt in memory for that time.
  - **Alarms**: keeps FAILED_PRECONDITION and INVALID_ARGUMENT attempts pending and logs `[checkout-alarm]`.
  - **Client outcome**: never changes `client_outcome`.
- [X] T077 [P] [US3] Write `server/tests/integration/CheckoutRecoveryFaultTest.java`, using the contracts/checkout-saga.md crash matrix and covering US3 scenarios 1–4 and 8.
  - **Faults**:
    - each Transaction hook — `checkout.afterBegin`, `checkout.beforeApply`, `checkout.afterApplyReply`, and `checkout.afterRecordCompletion` — with action `halt`;
    - the Inventory hook `inventory.apply.afterCommit`, with `halt` and with `delay:3000`.
  - **Method**: run a checkout through `ServiceProcesses`, then restart the halted service without the hook.
  - **Assertions**:
    - every attempt settles without manual steps, COMPLETED exactly when Inventory recorded APPLIED and OPEN otherwise;
    - stock changes at most once;
    - a client that received 500 later sees COMPLETED through lookup and 409 on a repeated Complete;
    - no `checkout_operation` row is RESTORED or BLOCKED;
    - per-SKU reconciliation holds.
- [X] T078 [P] [US3] Write `server/tests/integration/BoundedWaitTest.java` (FR-035, US3 scenario 6). While an attempt is pending, with Inventory delayed by its hook, issue a Scan and a Complete on the same transaction and assert:
  - settlement as COMPLETED answers them with 409 `TRANSACTION_NOT_OPEN`;
  - settlement as OPEN accepts the scan, or starts attempt n+1 for the Complete;
  - waiters wake on the settlement signal: they respond within 200 ms of the settlement commit, and `transaction.wait.dbChecks` does not increase for an attempt owned in the same process;
  - still pending at the bound gives 500 with no basket change and the pending attempt untouched, after exactly one database check at the bound;
  - Lookup returns OPEN immediately;
  - with two Transaction processes on one schema (Implementation Rule 3), a request waiting in the second process observes the first process's settlement through its backstop check within `CHECKOUT_WAIT_BACKSTOP_MS` plus 500 ms. It then gives the same serial outcome as in the single-process case.
- [X] T079 [P] [US3] Write `server/tests/integration/ScanCompletionConvergenceTest.java`, migrating `racingScanAndCompletionHaveOneSerialOutcome` with every assertion: a racing Scan and Complete have one serial outcome. Replace the database-lock blocker with an Inventory delay hook. The file's other method was migrated by T059.
- [X] T080 [P] [US3] Write `server/tests/integration/BackgroundCompletionAccountingTest.java` (FR-054, R9):
  - With Inventory delayed past the budget, the client gets 500, `client_outcome=PENDING_ERROR` is committed before the response, and the later COMPLETED attempt counts as one background completion.
  - A Transaction halt before responding gives a COMPLETED attempt with `client_outcome=NONE`.
  - `db/validate-invariants.ps1` prints both counts.
- [X] T081 [P] [US3] Write `server/tests/performance/HeldConnectionDuringWaitTest.java` (FR-033, FR-035, R7). During an FR-035 wait, assert that:
  - the waiting request holds no pooled connection between its database checks, and `SELECT … FOR UPDATE NOWAIT` on the transaction row succeeds from another connection;
  - the number of database checks is at most one per `CHECKOUT_WAIT_BACKSTOP_MS` plus one at the bound, and zero before settlement for an attempt owned in the same process;
  - each check holds its connection only for one read, as `transaction.pool.holdTime` shows.
- [X] T082 [P] [US3] Write `server/tests/rpc/RestoreCheckoutDirectTest.java` (SC-012). It is a direct Inventory test; its class Javadoc states that it is not an end-to-end Saga compensation test, and its results are reported separately. Assert that:
  - restoring an APPLIED attempt restores exactly the recorded lines, once;
  - repeated, 10-way concurrent, and post-restart restores return ALREADY_RESTORED with no further change;
  - a REJECTED attempt gives NOTHING_TO_RESTORE;
  - an absent attempt becomes BLOCKED, and a later `ApplyCheckout` returns STOCK_RESTORED with no deduction;
  - halting Inventory at `inventory.restore.afterCommit` and retrying restores exactly once.
- [X] T114 [P] [US3] Write `server/tests/integration/InventoryServiceIsolationTest.java`, migrating `inventoryRestartReadsCommittedSaleWhileTransactionRunsDuringOutage`:
  - **Superseded assertion**: the legacy test expected Complete to return 200 while Inventory was down. The ledger records this as `SUPERSEDED` by FR-030, FR-037, and the spec's *Availability and Failure Behavior*. The replacement asserts that:
    - with Inventory stopped, Complete returns 500 `INTERNAL_ERROR` within the 8 s budget, lookup shows OPEN, and stock is unchanged;
    - after Inventory restarts, the attempt settles as COMPLETED without manual steps, and stock reaches 1, the value the legacy test expected.
  - **Kept assertions**: every post-restart low-stock assertion of the legacy method stays, with the same values:
    - the alert for the sold SKU;
    - `threshold=0` gives no alerts;
    - `threshold=-1` gives 400 `INVALID_THRESHOLD`;
    - POST gives 405;
    - an extra path segment gives 404.
- [X] T147 [P] [US3] Extend `server/tests/performance/AmbiguousCommitReconcileTest.java` and `server/tests/performance/MultiInstanceTransactionTest.java` for recovery (Implementation Rule 3):
  - If an ambiguous T1 commit did happen, the worker's reconcile entry settles the attempt: COMPLETED if Inventory applies it. Waiting requests then resume.
  - If an ambiguous T1 commit did not happen, there is no attempt, and the reconcile entry is dropped with no write.
  - **Worker against live request**: the second instance's worker may resume an attempt that the first instance's live request owns, for example after the second instance's startup scan. Replay plus guarded writes settle it once, with one Inventory deduction. The live request answers from the settled state and records its own `client_outcome`, so the FR-054 counts stay exact.
  - **Worker against worker**: both instances' workers recover the same attempt at once. It settles once, with one deduction, and neither worker errors.
  - **Waiters**: in both processes, waiting requests end through a signal or the database backstop, with the same serial outcome, and no local registry future is left behind.

### Implementation for User Story 3

- [X] T083 [US3] Place the `FaultHooks.hit` calls in `server/transaction/src/transaction/CheckoutOrchestrator.java` and `server/inventory/src/inventory/CheckoutStockService.java`, at the exact points listed in contracts/checkout-saga.md *Test-only fault hooks*:
  - `checkout.afterBegin`, `checkout.beforeApply`, `checkout.afterApplyReply`, and `checkout.afterRecordCompletion` in `server/transaction/src/transaction/CheckoutOrchestrator.java`;
  - `inventory.apply.afterCommit` and `inventory.restore.afterCommit` in `server/inventory/src/inventory/CheckoutStockService.java`.
- [X] T084 [P] [US3] Create `server/transaction/src/transaction/CheckoutRecoveryWorker.java` per contracts/checkout-saga.md *Recovery worker*:
  - **Driver**: a single thread takes due keys from `RecoverySchedule`. It never polls the database on a short interval.
  - **Startup scan**: loads every PENDING attempt through paginated `findPending` calls of at most 100 rows, and enqueues them due now.
  - **Safety scan**: every `CHECKOUT_RECOVERY_SCAN_SECONDS` (30), one bounded, indexed `findPending(now, …, 100)` page, repeated only while pages are full. It enqueues attempts that are neither owned nor scheduled.
  - **Settlement**: reads the attempt by key first. It drops an attempt that is absent or already settled, completing any local future. Otherwise it replays `ApplyCheckout`, then runs T3 or reopen under the stripe lock, then completes registry futures. Uncertain outcomes use `scheduleRetry`, plus an in-memory re-enqueue.
  - **Process-local**: `RecoverySchedule` and the registry's ownership marks cover only this process. Another instance may replay the same attempt, and correctness rests on Inventory's idempotency and the guarded writes (Implementation Rule 3).
  - It never sets `client_outcome`.
  - Metrics: `transaction.recovery.pending|settled|alarms|scanQueries|scanRows|scheduled`.
- [X] T085 [US3] Implement event-driven bounded waiting (FR-035, R7) in `server/transaction/src/transaction/TransactionService.java` (Scan) and `server/transaction/src/transaction/CheckoutOrchestrator.java` (Complete), replacing the temporary `CheckoutPendingFailure` path:
  - Detect a pending attempt from the cache entry, or from a `GUARD_MISS` reload, while holding the stripe lock. Get or create the attempt's `SettlementRegistry` future under that lock, then release the lock.
  - Await the future within the remaining 8 s budget. Without a signal, check the database once per `CHECKOUT_WAIT_BACKSTOP_MS` (1000), to catch settlements made elsewhere, and once more at the bound. Each check borrows a connection only for its read.
  - On settlement, restart the request from the beginning. A waiter that learns of the settlement from a database check completes and removes its local future, so settlements made by other instances leave nothing behind.
  - At the bound, if still pending, raise `CheckoutPendingFailure` with no write.
  - Lookup is unchanged.
  - Metrics: `transaction.wait` latency and `transaction.wait.signalled|dbChecks|timedOut`.
- [X] T086 [US3] Start `CheckoutRecoveryWorker` in `server/transaction/src/transaction/TransactionMain.java` after the HTTP server, reading `CHECKOUT_RECOVERY_SCAN_SECONDS` and `CHECKOUT_WAIT_BACKSTOP_MS`. Stop it in the shutdown hook before the channels and the pool are closed.
- [X] T148 [US3] Verify US3 and run its parity gate with `server/run-tests.sh`:
  - Run `PARITY_GATE=US3 server/run-tests.sh` with `SERVER_BASE_URL` set.
  - The whole suite must pass, including T076–T082, T114, and T147.
  - Record `T148` in `Verified at` for every row whose test passed.

**Checkpoint**: Interrupted checkouts settle exactly once with no manual repair, waiting and recovery are event-driven, and restoration is verified directly.

---

## Phase 6: User Story 5 - Recover Accurate Popular Items From Reliably Delivered Scans (Priority: P2)

**Goal**: The outbox relay delivers accepted scans to Analytics at least once. Analytics ingests each batch, deduplicating and assigning positions in one commit, and acknowledges only after that commit. The unchanged pipeline publishes complete windows only after persisting them, even after duplicates, reordering, and restarts.

**Independent Test**: Send a controlled stream of scans, inject duplicates, reordering, and crashes of Analytics and of the relay, then compare every published window with an independent calculation over Analytics' durable positions.

### Tests for User Story 5 ⚠️ write first; they must fail before implementation

- [X] T087 [P] [US5] Write `server/tests/rpc/AnalyticsIngestContractTest.java`. Run an in-process `AnalyticsIngestGrpcService` over `AnalyticsTestSchema`, and assert that:
  - a batch stores events with consecutive positions in batch order;
  - redelivery returns stored 0 and duplicates n;
  - an identity repeated inside one batch counts as a duplicate;
  - overlapping batches assign positions only to new identities;
  - 0 or 1,001 events, a blank SKU, `scan_sequence` 0, or a `product_name` over 100 characters returns INVALID_ARGUMENT;
  - names land in `product_name`;
  - a database outage returns UNAVAILABLE and stores no rows;
  - **acknowledgement after commit**, using `FaultingConnections`:
    - with the ingestion commit delayed, the client receives no response before the commit returns;
    - a fault before the commit gives UNAVAILABLE, with no rows and an unchanged `scan_sequence`;
    - a fault after a successful commit gives UNAVAILABLE, the rows stay durable, and a retry returns stored 0 and duplicates n with the positions unchanged.
- [X] T088 [P] [US5] Write `server/tests/database/JdbcScanIngestStoreTest.java`, migrating `server/tests-legacy/database/JdbcScanMaterializerTest.java`. Assert that:
  - positions are unique, contiguous from 1, and immutable;
  - a failure before commit leaves no rows and an unchanged `scan_sequence` (`rollbackLeavesNoPositionAndNoCheckpointAdvance`);
  - a `throw` at `analytics.ingest.beforeCommit` leaves no rows, and the retry stores the whole batch without gaps (`crashBeforeCommitRollsBackWholeBatchAndRetryKeepsItGapless`);
  - a `throw` at `analytics.ingest.afterCommit` leaves the rows durable, and concurrent retries store 0 and never reassign positions (`crashAfterCommitAndConcurrentRetriesNeverReassignPositions`);
  - an event delivered late, after higher-numbered events, receives the next position with no gap. This is the Analytics half of `lowerEventIdCommittingLateIsStillDiscovered`; T091 covers the relay half.
  - concurrent ingestion calls serialize without gaps;
  - **multiple instances** (Constitution II): two ingestion stores, each with its own pool and name map, ingest overlapping batches concurrently over one schema. Every identity is stored exactly once, and positions stay unique, contiguous, and immutable.
  - after a restart, `ProductNameMap` is reloaded and no existing name is rewritten.
- [X] T089 [P] [US5] Write `server/tests/database/JdbcAcceptedScanReaderTest.java` over `AnalyticsTestSchema`. First move the original to `server/tests-legacy/database/JdbcAcceptedScanReaderTest.java`. Insert positioned rows directly, and migrate its three methods with every assertion, including the exact 1,000-position prewarm:
  - `pollsAdjacentCommittedPositionsInBatchesAndRetainsBacklog`;
  - `freshPollDoesNotSeeConcurrentUncommittedInsert`;
  - `refusesMissingPositionDuringPollOrPrewarm`.
- [X] T090 [P] [US5] Write `server/tests/database/JdbcPopularWindowStoreTest.java` over `AnalyticsTestSchema`. First move the original to `server/tests-legacy/database/JdbcPopularWindowStoreTest.java`. Migrate its four methods with every assertion:
  - `recoversMaximumEndAndReadsLatestRanksInStoredOrderWithALimit`, with `readRanks` joining `product_name`;
  - `headerAndRanksRollBackTogetherWhenAnyRankFails`;
  - `equivalentAlreadyCommittedWindowIsRecognizedWithoutDuplicatingItsBoundary`;
  - `checkpointRejectsOutOfOrderBoundariesAndTracksOnlyCompleteWrites`;
  - a new **multiple-instance** case (Constitution II): two stores over one schema write the same sequence of windows concurrently. Each window is published exactly once, the checkpoint only moves forward, and the later writer's attempt at a published boundary is a verified no-op.
- [X] T091 [P] [US5] Write `server/tests/database/JdbcRelayStoreTest.java`. Assert that:
  - `selectUndelivered` orders by `scan_id` and skips delivered rows;
  - `markDelivered` sets `delivered_at` for exactly the given IDs in one statement;
  - a row committed late with a lower `scan_id` is selected in the next round. This is the relay half of `JdbcScanMaterializerTest#lowerEventIdCommittingLateIsStillDiscovered`.
- [X] T092 [P] [US5] Write `server/tests/performance/AnalyticsIngestStatementBudgetTest.java` (R22).
  - **Strict**: each batch's events, positions, and `scan_sequence` update share exactly one commit, and the OK reply follows that commit.
  - **Budget guard**, with keys `analytics.ingest.*`, for batches of 1, 100, 500, and 1,000 events:
    - statements per batch (design at most 5) and commits (design 1), independent of batch size;
    - `product_name` writes once the name map is warm (design 0).
- [X] T093 [P] [US5] Write `server/tests/performance/RelayStatementBudgetTest.java` (R22).
  - **Strict**:
    - `delivered_at` is written only after `DeliverScanEvents` returns OK, and never for a failed call;
    - one round makes exactly one `DeliverScanEvents` call for up to 500 events.
  - **Budget guard**, with keys `relay.round.*`: statements per round (design 2: 1 SELECT, 1 UPDATE) and commits (design 1).
- [X] T094 [P] [US5] Write `server/tests/integration/ScanEventDeliveryTest.java` covering US5 scenarios 3, 4, 6, and 8 and US2 scenario 3:
  - With Analytics stopped while scanning, scans succeed and undelivered rows grow; after Analytics restarts, every accepted scan is counted exactly once.
  - A `relay.afterAck` halt followed by a restart ignores the duplicates.
  - A Transaction halt after a scan commit but before delivery delivers the scan after restart.
  - Scans of failed or never-completed checkouts are counted.
  - With two Transaction processes on one schema (Constitution II), both relays may deliver the same rows, and every accepted scan is still counted exactly once.
- [X] T095 [P] [US5] Write `server/tests/integration/AnalyticsCrashRecoveryTest.java` over `AnalyticsTestSchema`, seeding events through ingestion. First move the original to `server/tests-legacy/integration/AnalyticsCrashRecoveryTest.java`.
  - Migrate its four methods with every assertion: `outageBeforeReadReplaysEveryEligibleWindow`, `interruptionDuringPartialAssemblyReplaysFromCheckpoint`, `failureBeforeWindowCommitReplaysCandidate`, and `failureAfterCommitBeforeAcknowledgementDoesNotDuplicateWindow`.
  - Add crashes at ingestion before and after commit.
  - Every window must appear exactly once, with no partial or duplicate window visible.
- [X] T096 [P] [US5] Write `server/tests/integration/WindowBoundaryAccuracyTest.java` over `AnalyticsTestSchema`. First move the original to `server/tests-legacy/integration/WindowBoundaryAccuracyTest.java`. Migrate both methods with every assertion:
  - `concurrentBurstsPersistExactOverlappingWindows`: windows 1–1,000, then 501–1,500; ties broken by ascending SKU; at most 10 items; the established empty response under 1,000 scans.
  - `failedRankInsertNeverMakesItsWindowVisible`.
- [X] T149 [P] [US5] Write `server/tests/integration/AnalyticsIngestCrashTest.java`, an end-to-end crash test from reception to acknowledgement (FR-041–FR-043). Use Transaction and Analytics processes over disposable schemas.
  - **Crash points**, one per run:
    - `analytics.ingest.received`, after a batch arrives and before the ingestion transaction begins;
    - `analytics.ingest.beforeCommit`;
    - `analytics.ingest.afterCommit`, after the commit and before the reply.
  - **Method**: run scans until the relay delivers a batch, halt Analytics at the hook, record the stored positions, then restart Analytics without the hook.
  - **Assertions**, after the backlog drains:
    - every accepted scan is stored exactly once;
    - positions are unique and contiguous from 1, and every position recorded before the crash keeps its event;
    - after an `afterCommit` crash, the relay's redelivery reports the batch as duplicates; after the two earlier crashes, the batch is stored fresh;
    - `delivered_at` is set only after an OK reply;
    - every published window equals a recomputation from the stored positions.
- [X] T106 [P] [US5] Write `server/tests/contract/PopularItemsEndpointTest.java`. First move the original to `server/tests-legacy/contract/PopularItemsEndpointTest.java`. Seed Analytics through `DeliverScanEvents` instead of shared tables, and migrate its three methods with every assertion:
  - `latestMeansCommittedCheckpointAndNamesComeFromCatalog`: the latest window is the committed checkpoint, and names equal the catalog names captured at scan time, now held in `product_name`.
  - `reportsPersistedWindowWithRankedTopTenAndRequestLocalLimit`.
  - `publicPortPreservesPopularItemsShapeLimitAndErrors`: the empty response under 1,000 scans, the window fields, and `limit` 0, over 10, or invalid (400 `INVALID_LIMIT`).
- [X] T112 [P] [US5] Write `server/tests/integration/IndependentServiceDeploymentTest.java`, migrating `eachArtifactRestartsWithoutRestartingItsPeers` with every assertion:
  - Run three JVMs over three disposable schemas with their own users.
  - Each service restarts while its peers keep serving.
  - Manifests and main classes are verified.
  - An OPEN basket survives the restarts and completes, with stock read through `InventoryTestSchema`.
- [X] T115 [P] [US5] Write `server/tests/integration/AnalyticsCheckoutIsolationTest.java`, migrating `failingAnalyticsPersistenceCannotCorruptConcurrentScanAndCompletionTraffic` with every assertion:
  - 20 stations of 75 scans each complete their checkouts while Analytics' window persistence fails, using the failing-store gate as before.
  - No window commits until persistence recovers; then windows 1,000 and 1,500 commit.
  - Events reach Analytics through the relay instead of a shared table.
- [X] T116 [P] [US5] Write `server/tests/integration/AnalyticsBacklogIsolationTest.java`, migrating `transactionContinuesDuringAnalyticsOutageAndPrewarmRestoresOverlap` with every assertion. Transaction keeps accepting scans and checkouts while Analytics is down. After Analytics restarts, prewarm restores the 1,000-position overlap and the windows match.

### Implementation for User Story 5

- [X] T097 [P] [US5] Create `server/analytics/src/database/ScanIngestStore.java` and `JdbcScanIngestStore.java` per contracts/scan-event-delivery.md §3:
  - steps 1–5 at READ COMMITTED;
  - `ingest` returns only after the commit has succeeded, so callers can acknowledge only durable, deduplicated batches. A commit failure, including one with `commitOutcomeUnknown`, raises `StoreFailure`.
  - hooks `analytics.ingest.beforeCommit` and `analytics.ingest.afterCommit`;
  - name-map updates applied only after commit;
  - metrics `analytics.ingest.batch|commit|duplicates`.
- [X] T098 [P] [US5] Create `server/analytics/src/analytics/ProductNameMap.java`. It loads `product_name` at startup (at most 2,000 rows), computes per-batch diffs of absent or changed names, and applies committed diffs.
- [X] T099 [US5] Create `server/analytics/src/analytics/ScanIngestionService.java` (contract validation) and `server/analytics/src/rpc/AnalyticsIngestGrpcService.java` (proto mapping):
  - After validation, the handler calls the `analytics.ingest.received` hook, then `ScanIngestStore.ingest`.
  - It calls `onNext` and `onCompleted` only after `ingest` returns, so positions and deduplication keys are durable before any OK leaves the process.
  - Failures map to INVALID_ARGUMENT, UNAVAILABLE (any `StoreFailure`, including an unknown commit outcome), or INTERNAL.
- [X] T100 [P] [US5] Remove the materializer (`server/analytics/src/database/JdbcScanMaterializer.java`) and update its callers:
  - Delete `server/analytics/src/database/JdbcScanMaterializer.java`.
  - Remove the materializer field and `materializeBatch` from `server/analytics/src/database/JdbcAcceptedScanReader.java`, and remove its call from `server/analytics/src/analytics/AcceptedScanPoller.java`.
  - Count empty polls in `server/analytics/src/analytics/AcceptedScanPoller.java` as `analytics.poller.idlePolls`, leaving its timing and constructor unchanged.
  - Make `readRanks` in `server/analytics/src/database/JdbcPopularWindowStore.java` join `product_name` instead of `catalog_item`.
  - **Legacy tests broken by this task**: move to `server/tests-legacy/` each of these still in `server/tests/`. Their ledger rows keep their status and owners:
    - `database/JdbcScanMaterializerTest` (T088, T091);
    - `database/JdbcAcceptedScanReaderTest` (T089);
    - `database/JdbcPopularWindowStoreTest` (T090);
    - `integration/AnalyticsCrashRecoveryTest` (T095);
    - `integration/WindowBoundaryAccuracyTest` (T096);
    - `contract/PopularItemsEndpointTest` (T106);
    - `support/AnalyticsDatabase`.
- [X] T101 [US5] Rewire `server/analytics/src/analytics/AnalyticsMain.java`:
  - `DatabaseConfig.fromEnvironment("ANALYTICS", "cs6510_analytics", "cs6510_analytics_svc", 10, 10)` with `new ConnectionPool(...)`.
  - `ProductNameMap`, then `AnalyticsService` (prewarm and poller), started before the gRPC server on `ANALYTICS_GRPC_PORT` (9093).
  - HTTP on `ANALYTICS_PORT` (8083) and `MetricsReporter.start("analytics")`.
  - A shutdown hook that stops HTTP, then gRPC, then calls `running.shutdown()`, then stops the reporter, then the pool.
- [X] T102 [P] [US5] Create the relay's store `server/transaction/src/database/JdbcRelayStore.java` and delivery client `server/transaction/src/rpc/GrpcAnalyticsIngestClient.java` in Transaction:
  - `server/transaction/src/database/RelayStore.java` and `JdbcRelayStore.java`, with `selectUndelivered`, `markDelivered`, and `countUndelivered` per contracts/scan-event-delivery.md §2.
  - Port `server/transaction/src/transaction/ScanEventSink.java`.
  - `server/transaction/src/rpc/GrpcAnalyticsIngestClient.java`, with a 5 s deadline and status classification.
- [X] T103 [US5] Create `server/transaction/src/transaction/ScanEventRelay.java`, a single daemon thread:
  - batches of `OUTBOX_BATCH_SIZE` (500), a 50 ms idle poll, and backoff from 100 ms to 5 s;
  - the `relay.afterAck` hook, and an alarm on INVALID_ARGUMENT;
  - metrics `transaction.relay.batch|latency`, `transaction.relay.idlePolls` (empty selects), and a backlog gauge.

  Start and stop it in `server/transaction/src/transaction/TransactionMain.java`, with a channel to `ANALYTICS_GRPC_TARGET` (127.0.0.1:9093).
- [X] T150 [US5] Verify US5 and run its parity gate with `server/run-tests.sh`:
  - With all three services on their own schemas, run `PARITY_GATE=US5 server/run-tests.sh` with `SERVER_BASE_URL` set.
  - The whole suite must pass, including T087–T096, T106, T112, T115, T116, and T149.
  - Record `T150` in `Verified at` for every row whose test passed.
- [X] T104 [US5] CP2 attribution checkpoint, recorded in `load-client/reports/microservices-architecture/checkpoints.md`:
  - Run `server/benchmark-microservices.ps1` with all services: default twice (`cp2-default-1`, `cp2-default-2`) and stress once (`cp2-stress-1`).
  - Record in `checkpoints.md`:
    - the actual client-observed `totalTransactions` of each run, and its change against the CP0 median of the same workload (Implementation Rule 6);
    - `transaction.lock.wait` p99, `transaction.lock.contended`, and `transaction.lock.collisions`;
    - relay backlog peak and Analytics unpublished-boundary peak;
    - visibility delay p99, against the 5 s target;
    - catch-up time after load stops, against the 60 s ceiling;
    - background completions;
    - recovery and waiting cost: `transaction.recovery.scanQueries`, `transaction.wait.dbChecks`, and pool hold-time p99 per service;
    - unnecessary reads: `transaction.relay.idlePolls` and `analytics.poller.idlePolls` per second of run time.
  - Compare with CP1 and the CP0 attribution runs only to attribute costs. Investigate any `totalTransactions` drop of more than 5% before continuing.
  - If idle polling or relay commits measurably slow Scans, record that as evidence for the matching R23 option.
  - Statement counts and latencies explain a result but never establish an improvement on their own.

**Checkpoint**: The full system runs, analytics is exactly-once with acknowledgement after commit, every US5-owned legacy assertion has a passing equivalent, and CP2 is recorded.

---

## Phase 7: User Story 4 - Keep One Unchanged Client Interface (Priority: P2)

**Goal**: The unchanged Load Client and the contract tests use one base URL and the seven routes. Internal failures surface only as 500 `INTERNAL_ERROR`, or as the gateway's 502 for a stopped service.

**Independent Test**: Run the contract tests and the Load Client through `http://localhost:8080`, then stop each service in turn and exercise every route.

### Tests for User Story 4 ⚠️ write first; they must fail before implementation

- [X] T107 [P] [US4] Extend `server/tests/unit/api/TransactionHttpHandlerTest.java`, keeping every existing method (ledger: `ADAPTED`):
  - `CheckoutPendingFailure`, `ProductCatalogUnavailableFailure`, `DependencyUnavailableFailure`, the INVALID_REQUEST and STOCK_RESTORED failure, and an unexpected `RuntimeException` each produce 500 `{"error":"INTERNAL_ERROR","message":"Internal server error"}` with no gRPC or SQL text.
  - The existing 400, 404, and 409 mappings are unchanged.
- [X] T108 [P] [US4] Write `server/tests/integration/GatewayServiceOutageTest.java`, which runs the real services behind the checked-in route map through `server/tests/integration/PublicRouterTestSupport.java`. `PublicRoutingIsolationTest` and `PublicRouterTestSupport` stay unchanged (ledger: `RETAINED`), because they use stand-in owners and no database. Assert that:
  - stopping each service gives 502 only on its own routes;
  - with Inventory stopped, Complete returns 500 within 10 s and lookup shows OPEN;
  - after Inventory returns, the attempt settles as COMPLETED.

### Implementation for User Story 4

- [X] T110 [US4] Verify the unchanged public interface (`deploy/nginx.conf`, `server/tests/contract/`) and run the US4 parity gate:
  - Confirm that `deploy/nginx.conf` is unchanged: seven routes, upstreams 8081, 8082, and 8083, `error_page 504 =502`, a 1 s connect timeout, and a 30 s read timeout.
  - With `SERVER_BASE_URL=http://localhost:8080`, run `PARITY_GATE=US4 server/run-tests.sh`. The whole suite must pass, including:
    - the unchanged `ItemsEndpointTest`, `TransactionLifecycleTest`, `TransactionLookupTest`, and `PublicRoutingIsolationTest`;
    - T107 and T108.
  - Confirm that the SHA-256 checks of the OpenAPI document and the Load Client in `server/tests/architecture/PipelineArchitectureTest.java` pass unchanged.
  - Record `T110` in `Verified at` for every row whose test passed.

**Checkpoint**: The public contract is unchanged.

---

## Phase 8: User Story 6 - Operate and Reset Each Service Independently (Priority: P2)

**Goal**: Each service builds, starts, stops, restarts, resets, and seeds on its own, reaches only its own data, and reports its pending recovery work (FR-039).

**Independent Test**: Build and start each service alone, attempt cross-service data access with each service's credentials, reset and seed each service through its own mechanism, and restart each service during a workload.

### Tests for User Story 6 ⚠️ write first; they must fail before implementation

- [X] T111 [P] [US6] Write `server/tests/integration/DatabaseIsolationTest.java` (SC-008). With disposable bootstrap-equivalent users, each service user's SELECT or INSERT on the other two schemas fails with MySQL error 1142, while full use of its own schema succeeds.
- [X] T113 [P] [US6] Write `server/tests/integration/ResetBaselineTest.java` for the per-service resets. T052 has already parked the legacy file. Migrate `resetProducesExactBaselineForFreshServer` with every baseline assertion. The test stays opt-in, destructive, and disabled by default. Running the per-service resets with only their own credentials must give:
  - 2,000 catalog rows, 2,000 inventory rows, and 20,000,000 units;
  - zero rows in Transaction and Analytics;
  - freshly started services that serve that baseline.
- [X] T117 [P] [US6] Write `server/tests/integration/RestartDuringWorkloadTest.java` (SC-009). During a short concurrent workload, restart each service once. After pending work drains, zero acknowledged scans are lost, and per-SKU reconciliation and window recomputation pass.
- [X] T118 [P] [US6] Extend `server/tests/architecture/CompositionRootTest.java`, keeping every existing assertion (ledger: `ADAPTED`):
  - The Inventory and Analytics mains start a gRPC server through `GrpcServers`, and `TransactionMain` starts none.
  - Every main calls `DatabaseConfig.fromEnvironment` with its own prefix and starts `MetricsReporter`.
- [X] T119 [P] [US6] Extend `server/tests/architecture/MicroserviceBoundaryTest.java`:
  - `ScanProfiler.java` and `JdbcScanMaterializer.java` no longer exist, and no source references `SCAN_PROFILE`.
  - No service reads the former shared `DB_NAME`, `DB_USER`, or `DB_PASSWORD`.

### Implementation for User Story 6

- [X] T120 [US6] Add the FR-039 status summaries in `server/transaction/src/database/TransactionStatusQueries.java`, `server/inventory/src/database/InventoryStatusQueries.java`, and `server/analytics/src/database/AnalyticsStatusQueries.java`, one line every 30 s from each main:
  - **Transaction**: pending attempts, alarmed attempts, undelivered rows, background completions, and replica version and age.
  - **Inventory**: `checkout_operation` counts by state.
  - **Analytics**: `last_position`, `last_window_end`, and the highest eligible boundary.

  Use read-only `JdbcUnitOfWork` queries in `server/transaction/src/database/TransactionStatusQueries.java`, `server/inventory/src/database/InventoryStatusQueries.java`, and `server/analytics/src/database/AnalyticsStatusQueries.java`, scheduled from each main.
- [X] T121 [US6] Verify independent builds (FR-050): delete `server/out`, then run `./build.sh transaction`, `./build.sh inventory`, and `./build.sh analytics` in separate clean runs. Each run must produce only `common.jar`, `contracts.jar`, and its own service jar. Record the commands and results for T122.
- [X] T122 [US6] Rewrite `server/README.md` for the microservice deployment, following contracts/service-deployment.md:
  - prerequisites: JDK 21, Maven 3.9+, MySQL 8.4 on 3307, and Nginx;
  - build (the Maven contracts module, then `javac`), the independent-build commands from T121, and the configuration table;
  - the bootstrap, reset, seed, and validate scripts;
  - start, stop, and restart order, and the HTTP and gRPC ports;
  - credential handling and the operational status queries;
  - the benchmark harness, `benchmark-compare.ps1`, the checkpoints, and the A/B decision rule;
  - the coverage ledger and the `PARITY_GATE` runs;
  - the statement that MySQL durability settings are retained unchanged.
- [X] T151 [US6] Run the US6 parity gate with `server/run-tests.sh`:
  - Run `PARITY_GATE=US6 server/run-tests.sh` with `SERVER_BASE_URL` set.
  - The whole suite must pass, including T111, T117, T118, and T119. T113 must pass when its opt-in property is set.
  - Record `T151` in `Verified at` for every row whose test passed.

**Checkpoint**: Independent operation and data isolation are proven.

---

## Phase 9: User Story 7 - Reproduce Benchmark Evidence (Priority: P3)

**Goal**: Measure the tuning variants under equivalent workloads, keep only measured winners, compare CP4 with CP0 in interleaved runs, and publish the two predesignated official reports with durable completion counts, invariant checks, and a documented comparison.

**Independent Test**: Reset all three services, run the default command and verify invariants after settlement, then repeat with a separate reset for the stress command. Inspect both reports and their evidence files.

- [X] T123 [US7] CP3 tuning A/B (R16, R20, R22) with `server/benchmark-compare.ps1`, recorded in `load-client/reports/microservices-architecture/checkpoints.md`:
  - **Equivalent workloads and functionality**:
    - every variant uses the same build, the same reset baseline, all three services, the exact Load Client command, and the same configuration apart from the knob under test;
    - every run must be `comparable` (T020);
    - a variant that changes behavior, or whose run fails validation, is rejected regardless of speed.
  - **Variants**, each against the current defaults, measured on its relevant workloads (R22):
    - server-side prepared statements: `DB_SERVER_PREPARED_STATEMENTS=true`, on both workloads;
    - Inventory pool: `INVENTORY_DB_POOL_SIZE` and `INVENTORY_DB_POOL_MAX_SIZE` at 10/32 and at 24/64, on stress;
    - Transaction pool: 48/80, within the MySQL connection budget (80 + 48 + 10 = 138, under 151), on stress;
    - any statement-budget alternative proposed during implementation (T135): relay coalescing, `OUTBOX_COALESCE_MS=100` (R23, from CP2's evidence), on both workloads.
    - The cache-off attribution run (`TRANSACTION_CACHE_MAX_ENTRIES=0`, R19) is skipped unless a correctness issue or a major regression needs it.
  - **Procedure** (streamlined 2026-10-22; R22 has the thresholds):
    1. **Screen** each variant with one interleaved pair (`-Pairs 1`) per relevant workload. Reject it unless Δ ≥ +5% on some workload. Within ±3% counts as practically equivalent, and below +5% is no demonstrated improvement.
    2. **Confirm** a promising variant with three fresh pairs (`-Pairs 3`) on each workload where it was promising. Keep it only if Δ ≥ +3% and it is ahead in at least two of the three pairs. Close results are not extended.
    3. **Regression check**: a confirmed variant needs at least one default-workload pair, unless the default was already measured. Reject it on any correctness failure, a Δ ≤ −3% on any measured workload, or a Scan or Complete p99 median more than 10% above the incumbent's.
    4. **Stop** once the remaining candidates are unlikely to give a meaningful gain.
    - The comparisons completed under the earlier five-pair rule stand as recorded: server prepared statements and Inventory 10/32, each on both workloads.
  - **Keep** a variant only if it passes confirmation and the regression check. Record every comparison row and each keep or reject decision. State the number of pairs, and acknowledge the reduced statistical confidence of one-pair screens and three-pair confirmations; claim no significance.
  - **Report** each variant's actual client-observed `totalTransactions` (median and range) next to the current defaults' and the CP0 baseline's (Implementation Rule 6). A variant that only lowers statement counts, latency, or resource use, without a measured `totalTransactions` gain, is not a win.
- [X] T124 [US7] Apply only the CP3 winners: change the defaults in the service mains' `DatabaseConfig.fromEnvironment` arguments or in the run scripts, and update the configuration table in `specs/005-microservices-architecture/contracts/service-deployment.md`. When a winner changes a statement count, update `server/tests/performance/statement-budgets.properties` with the new ceiling and its `.evidence` label (T135). Then rerun `server/tests/performance/**`.
- [X] T125 [US7] Run the complete suite and the final parity gate with `server/run-tests.sh`:
  - With all services and the gateway running, run `server/build-tests.sh`, then `PARITY_GATE=final SERVER_BASE_URL=http://localhost:8080 server/run-tests.sh`.
  - Require zero failures and no skipped contract, integration, rpc, or performance tests; only the opt-in `ResetBaselineTest` may abort.
  - Require a ledger with no `PENDING` row and a `Verified at` value on every row.
- [X] T152 [US7] Run the final CP0-versus-CP4 comparison (R22) with `server/benchmark-compare.ps1`, recorded in `load-client/reports/microservices-architecture/checkpoints.md`:
  - Create a worktree of commit 26c89af outside the repository with `git worktree add ../cs6510-cp0 26c89af`, and build it as in T001.
  - On the same machine and day, with nothing else running (the idle-machine gate), run three interleaved pairs per workload: default and stress, each with `-Pairs 3`. Variant A is CP0 (`-BaselineWorktree ../cs6510-cp0`). Variant B is CP4, the current build with its defaults.
  - Designate the official reports before running: the first CP4 run of each workload, `cp4-final-default-b-1` and `cp4-final-stress-b-1`. Never substitute a later or better run.
  - Record per workload:
    - the median and range of `totalTransactions`, the percentage change, and the per-pair differences;
    - the medians of Start, Scan, and Complete p95/p99.
    
    Report the measured differences without claiming statistical significance. Three pairs per workload describe the change; they do not test it.
    
    These are the only figures used for the CP0-to-CP4 comparison; CP1–CP3 serve only for attribution. Any performance claim is stated in client-observed `totalTransactions` against CP0, never in statement counts or latency alone (Implementation Rule 6).
- [X] T126 [US7] Validate and publish the official default report (FR-055, SC-010, SC-011): the predesignated run `cp4-final-default-b-1` from T152, in `load-client/reports/microservices-architecture/`. That run executed exactly `java -cp out Main` after a fresh reset of all three services.
  - Require zero errors for every operation, zero background completions, validator PASS, and evidence showing `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1`.
  - If any requirement fails, fix the cause, then rerun T125 and the whole of T152. Never substitute another run.
  - Copy the client JSON byte for byte to `load-client/reports/microservices-architecture/default.json`.
- [X] T127 [US7] Validate and publish the official stress report: the predesignated run `cp4-final-stress-b-1` from T152, in `load-client/reports/microservices-architecture/`. That run executed exactly `java -cp out Main --stations=100 --duration=120` after its own fresh reset.
  - Require validator PASS, and report and explain the background completions (FR-058). The no-substitution rule of T126 applies.
  - Copy the client JSON byte for byte to `load-client/reports/microservices-architecture/stress.json`.
- [X] T128 [US7] Write `load-client/reports/microservices-architecture/README.md` with:
  - the deployment, the JAR SHA-256 hashes, and the exact commands;
  - the final comparison, taken only from T152: CP0 against CP4 per workload with medians, ranges, percentage change, and per-pair differences, stated without claims of statistical significance. It leads with `totalTransactions`, then throughput, per-operation error rates, and p95/p99.
  - an attribution table for CP1–CP3, labeled as attribution rather than comparison: each checkpoint's actual `totalTransactions` against CP0, pool wait and hold time, `TransactionLocks` wait, contention, and collisions, Inventory lock wait, `ApplyCheckout` latency, relay and Analytics backlog, visibility delay, recovery queries, wait checks, and idle polls;
  - a statement that every performance claim rests on client-observed `totalTransactions`; statement counts and latencies appear only as explanations (Implementation Rule 6);
  - the R18 mandatory-versus-optional cost breakdown, with the measured commits per basket;
  - the FR-054 completion counts for both official runs, and the durability settings;
  - a comparison with `load-client/reports/transaction-optimization/` and `load-client/reports/service-based-architecture/`.

**Checkpoint**: The official evidence is committed.

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Cleanup and documentation only. Any runtime code change after CP4 requires rerunning T125, T152, and T126–T128.

- [X] T129 [P] Retire the single-database tooling: delete `db/init.sql`, `db/init-db.ps1`, `db/validate-invariants.sql`, `db/migrate-sql-materializer.sql`, `server/benchmark-scan.ps1`, and `server/benchmark-materializer.ps1`. Remove the CP0 worktree created by T152 with `git worktree remove ../cs6510-cp0`. Note in `load-client/reports/microservices-architecture/checkpoints.md` that CP0 is reproducible from commit 26c89af.
- [X] T130 Delete the parked legacy tests in `server/tests-legacy/`, including the legacy `CheckoutDatabase` and `AnalyticsDatabase`, only after verified parity (FR-057):
  - Before deleting anything, `PARITY_GATE=final server/run-tests.sh` must pass. Every row of every parked file must be `MIGRATED` or `SUPERSEDED`, with a replacement that passed at the recorded gate.
  - Delete file by file, then rerun `CoverageParityLedgerTest`, which checks that every deleted file had fully verified parity.
  - Keep `server/tests/COVERAGE-LEDGER.md` as the record.
- [X] T131 [P] Reconcile `specs/005-microservices-architecture/quickstart.md` and `specs/005-microservices-architecture/contracts/*.md` with any names, defaults, or commands that changed during implementation. Behavior must not change.
- [X] T132 Run quickstart.md scenarios A–H on the final build, and record the outcomes in `load-client/reports/microservices-architecture/quickstart-validation.md`.
- [X] T133 Re-verify constitution alignment before release, in `.specify/memory/constitution.md` and `specs/005-microservices-architecture/plan.md` (Implementation Rule 5):
  - The constitution reads v5.1.0 with exactly the approved text of `specs/005-microservices-architecture/constitution-amendment-proposal.md`.
  - Its temporary Sync Impact Report comment is removed before the constitution is committed, as Governance requires.
  - The plan's Constitution Check passes every gate with no deviation, and it reflects any change made during implementation.
  - The implementation still preserves durable forward recovery and every safety invariant in the spec's *Transient and Settled Guarantees*, including across crashes, restarts, and multiple Transaction instances (Principle II).
  - Any further amendment needs explicit approval of its text and goes through `/speckit-constitution`; no implementation task amends the constitution on its own initiative.

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (Phase 1)**: T001 (CP0) runs first and before any code change. T002–T007 follow it.
- **Foundational (Phase 2)**: depends on Setup and blocks every user story. It ends with the T137 gate.
- **US2 Scan (Phase 3)**: depends on Foundational.
- **US1 Checkout (Phase 4)**: depends on US2, because it needs baskets, the state cache, the locks, and the Transaction stores.
- **US3 Recovery (Phase 5)**: depends on US1, because it needs the orchestrator, the attempt store, `RecoverySchedule`, and the Inventory operations.
- **US5 Analytics (Phase 6)**:
  - The ingestion side needs only Foundational and can run in parallel with US2, US1, and US3: T087–T090, T092, T095–T101, and T106.
  - The relay side and the full-system tests need US2's `accepted_scan` writes and US3's `TransactionMain` changes (T086): T091, T093, T094, T102, T103, T112, T115, T116, and T149.
  - The gate (T150) and CP2 (T104) need US1, US3, and US5.
- **US4 Client interface (Phase 7)** and **US6 Operate and reset (Phase 8)**: both depend on CP2 (T104), and can run in parallel with each other.
- **US7 Evidence (Phase 9)**: depends on every earlier phase. T152 needs T125, and T126 and T127 need T152.
- **Polish (Phase 10)**: depends on US7. T130 needs the final gate.

### Story completion order

```text
Setup (T001 CP0 → T002–T007) → Foundational (T008–T027, T134–T136 → T137 gate)
  ├─▶ US2 Scan (T028–T040, T042–T052, T105, T138–T141 → T053 gate)
  │     ─▶ US1 Checkout (T041, T054–T074, T109, T142–T145 → T146 gate → T075 CP1)
  │           ─▶ US3 Recovery (T076–T086, T114, T147 → T148 gate)
  │                 └─▶ US5 relay side (T091, T093, T094, T102, T103, T112, T115, T116, T149)
  └─▶ US5 ingestion side (T087–T090, T092, T095–T101, T106) ──────────▶ T150 gate → CP2 (T104)
CP2 ─▶ US4 (T107, T108 → T110 gate) ┐
CP2 ─▶ US6 (T111, T113, T117–T122 → T151 gate) ┴─▶ US7 (T123 CP3 → T124 → T125 final gate
                                                        → T152 CP0-vs-CP4 → T126/T127 → T128) ─▶ Polish (T129–T133)
```

### Within each user story

- Tests come first and must fail. Then stores and adapters, then business services, then `rpc` adapters, then wiring in the main class, then the parity gate, then the checkpoint.
- Tasks that change the same file run in order:
  - `TransactionMain.java`: T052, T074, T086, T103.
  - `TransactionService.java`: T051, T073, T085.
  - `CheckoutOrchestrator.java`: T072, T083, T085.
  - `InventoryGrpcService.java`: T045, T069.
  - `GrpcInventoryGateway.java`: T047, T071.
  - `CheckoutStockService.java`: T069, T083.
- Test files extended across stories also run in order:
  - `TransactionServiceTest.java` and `TransactionBoundaryIsolationTest.java`: T036 and T037, then T058.
  - `TransactionRestartDurabilityTest.java`: T042, then T142.
  - `TransactionCacheCorrectnessTest.java`: T039, then T143.
  - `AmbiguousCommitReconcileTest.java`: T141, T143, then T147.
  - `MultiInstanceTransactionTest.java`: T140, T144, then T147.
- **Shared ledger**: every migration task and gate edits `server/tests/COVERAGE-LEDGER.md`, and parallel tasks edit only their own rows.
- **Budget file**: `statement-budgets.properties` changes only in T135 and T124.

### Parallel opportunities

- Setup: T003, T005, T006, and T007 after T002.
- Foundational:
  - T008, T009, and T010 together.
  - T013–T016 together, then T018.
  - Also in parallel: T021, T022, T023, T024, T026, T027, T134, T135, and T136.
- Every test task marked [P] within a story runs in parallel, except those that extend the same file, which are listed above.
- Implementation tasks marked [P] run in parallel:
  - US2: T044, T046, T047, T049, and T050.
  - US1: T068, T070, T071, and T145.
  - US3: T084.
  - US5: T097, T098, T100, and T102.
- US5's ingestion side can be staffed alongside US2, US1, and US3. US4 and US6 can run alongside each other.

---

## Parallel Example: User Story 2

```bash
# Tests first (all [P]):
Task: "InventoryCatalogContractTest in server/tests/rpc/InventoryCatalogContractTest.java"
Task: "ScanStatementBudgetTest in server/tests/performance/ScanStatementBudgetTest.java"
Task: "TransactionCacheCorrectnessTest in server/tests/performance/TransactionCacheCorrectnessTest.java"
Task: "CacheEvictionRaceTest in server/tests/performance/CacheEvictionRaceTest.java"
Task: "AmbiguousCommitReconcileTest in server/tests/performance/AmbiguousCommitReconcileTest.java"
Task: "LowStockEndpointTest in server/tests/contract/LowStockEndpointTest.java"
# Then independent implementation files:
Task: "CatalogCache in server/inventory/src/inventory/CatalogCache.java"
Task: "JdbcProductReplicaStore in server/transaction/src/database/JdbcProductReplicaStore.java"
Task: "TransactionStateCache and TransactionLocks in server/transaction/src/transaction/"
Task: "JdbcScanStore in server/transaction/src/database/JdbcScanStore.java"
```

## Parallel Example: User Story 1

```bash
Task: "InventoryCheckoutContractTest in server/tests/rpc/InventoryCheckoutContractTest.java"
Task: "CompleteStatementBudgetTest in server/tests/performance/CompleteStatementBudgetTest.java"
Task: "HeldConnectionDuringRpcTest in server/tests/performance/HeldConnectionDuringRpcTest.java"
Task: "DurableBasketTest in server/tests/integration/DurableBasketTest.java"
Task: "JdbcCheckoutOperationStore in server/inventory/src/database/JdbcCheckoutOperationStore.java"
Task: "JdbcCheckoutAttemptStore in server/transaction/src/database/JdbcCheckoutAttemptStore.java"
Task: "RecoverySchedule in server/transaction/src/transaction/RecoverySchedule.java"
```

## Parallel Example: User Story 3

```bash
Task: "CheckoutRecoveryFaultTest in server/tests/integration/CheckoutRecoveryFaultTest.java"
Task: "BoundedWaitTest in server/tests/integration/BoundedWaitTest.java"
Task: "RestoreCheckoutDirectTest in server/tests/rpc/RestoreCheckoutDirectTest.java"
Task: "HeldConnectionDuringWaitTest in server/tests/performance/HeldConnectionDuringWaitTest.java"
Task: "InventoryServiceIsolationTest in server/tests/integration/InventoryServiceIsolationTest.java"
```

## Parallel Example: User Story 5

```bash
Task: "AnalyticsIngestContractTest in server/tests/rpc/AnalyticsIngestContractTest.java"
Task: "AnalyticsIngestCrashTest in server/tests/integration/AnalyticsIngestCrashTest.java"
Task: "AnalyticsIngestStatementBudgetTest in server/tests/performance/AnalyticsIngestStatementBudgetTest.java"
Task: "JdbcScanIngestStore in server/analytics/src/database/JdbcScanIngestStore.java"
Task: "ProductNameMap in server/analytics/src/analytics/ProductNameMap.java"
Task: "JdbcRelayStore in server/transaction/src/database/JdbcRelayStore.java"
```

## Parallel Example: User Stories 4 and 6

```bash
Task: "TransactionHttpHandlerTest in server/tests/unit/api/TransactionHttpHandlerTest.java"
Task: "GatewayServiceOutageTest in server/tests/integration/GatewayServiceOutageTest.java"
Task: "DatabaseIsolationTest in server/tests/integration/DatabaseIsolationTest.java"
Task: "RestartDuringWorkloadTest in server/tests/integration/RestartDuringWorkloadTest.java"
```

---

## Implementation Strategy

### MVP first: Scan plus Checkout

1. Complete Phase 1, starting with the CP0 baseline (T001).
2. Complete Phase 2, ending with the foundation gate (T137).
3. Complete Phase 3 (US2: memory-first durable Scan), then Phase 4 (US1: Saga checkout).
4. **Stop and validate**: the US1 parity gate (T146), then CP1 (T075). This is the MVP: checkout across independently owned data.

### Incremental delivery

1. Add US3: exactly-once settlement, event-driven waiting and recovery, and the direct restoration tests (T148).
2. Add US5: reliable analytics (T150), then CP2 (T104).
3. Add US4 (unchanged public interface, T110) and US6 (independent operation and isolation, T151), in parallel.
4. Run US7: CP3 tuning A/B, the full suite with the final gate, the interleaved CP0-versus-CP4 comparison, then the official reports.
5. Finish with Polish: retire the old tooling, delete parked legacy tests after verified parity, and validate the quickstart.

### Performance discipline (R18, R22)

- **Strict tests gate the build and are never relaxed**:
  - acknowledgement only after commit;
  - writes that must share one commit;
  - no remote call on Scan, and exactly one `ApplyCheckout` per checkout;
  - Analytics acknowledges only after its ingestion commit, and the relay sets `delivered_at` only after OK;
  - no connection or row lock is held across `ApplyCheckout` or a wait.
- **Statement budgets are measured guards**: ceilings in `statement-budgets.properties` catch added round trips. A ceiling rises only with a T136 A/B record that shows higher `totalTransactions` beyond noise, without worse p99 latency or lock contention.
- **Benchmarks**:
  - CP0 and CP4, interleaved on the same day (T152), form the final comparison. CP1–CP3 attribute costs.
  - CP3 variants run under equivalent workloads and functionality.
  - A/B screens use one pair per relevant workload; only a variant promising about +5% gets three confirmation pairs. Close results are no demonstrated improvement, not a reason for more pairs (R22, streamlined 2026-10-22).
  - Decide on `totalTransactions` first, then tail latency, then resource use.
- **Recovery and waiting**: driven by events rather than frequent polling. Recovery queries are indexed and bounded, and CP2 measures their cost along with connection hold time and idle reads.
- **Locks**: lock wait time, contention, and stripe collisions are recorded at every checkpoint. Collisions may cost time but never change an outcome.
- **Claims**: each checkpoint reports the actual client-observed `totalTransactions` against the CP0 baseline. Fewer statements or lower latency never count as an improvement on their own.
- **Never** change MySQL durability settings, cache stock, or add Redis, Kafka, or another broker without R23 evidence.

### Test coverage discipline (FR-057)

- Keep compatible tests unchanged.
- Migrate every other assertion to an equivalent one. Record an assertion that the spec replaces as `SUPERSEDED`, citing the requirement.
- Park a legacy test only in the task that breaks it, and delete it only after verified parity (T130).
- Each story's gate fails while any legacy assertion it owns lacks a passing equivalent.

---

## Notes

- [P] tasks touch different files and depend on no incomplete task.
- Story labels map each task to spec.md for traceability. Setup, Foundational, and Polish tasks carry no label.
- Verify that each test fails before implementing; commit after each task or logical group.
- `load-client/src/**` and `spec/self-checkout-openapi.yaml` stay byte-identical, as `PipelineArchitectureTest` enforces.
- Direct `RestoreCheckout` tests (T082) are reported separately and are never presented as end-to-end Saga compensation tests.
- `server/tests/COVERAGE-LEDGER.md` is the single record of legacy coverage, from T021 through T130.
