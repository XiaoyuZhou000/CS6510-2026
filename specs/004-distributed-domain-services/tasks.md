---

description: "Implementation tasks for distributed domain services and durable analytics recovery"
---

# Tasks: Distributed Domain Services with Durable Analytics Recovery

**Input**: `specs/004-distributed-domain-services/{spec.md,plan.md,research.md,data-model.md,quickstart.md,contracts/}`

**Tests**: Required by FR-027 and constitution VIII. Keep existing assertions when adapting tests. Write each listed test before its corresponding implementation and confirm it fails for the missing behavior.

**Organization**: Tasks are grouped by user story. Paths are relative to the repository root. `[P]` means the task can proceed alongside other ready tasks because it changes different files and does not require their unfinished output.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Prepare independent Java build outputs while retaining the plain `javac`/`jar` toolchain.

- [X] T001 Create the planned source roots `server/common/src/`, `server/transaction/src/`, `server/inventory/src/`, and `server/analytics/src/`; update `server/build.sh` to build `common.jar` and separate `transaction.jar`, `inventory.jar`, and `analytics.jar` with `transaction|inventory|analytics` targets, Java 21, and Connector/J on the appropriate compile/runtime classpaths.
- [X] T002 [P] Update `server/build-tests.sh` and `server/run-tests.sh` to compile and run the existing JUnit suites against all three artifacts and support live tests using `SERVER_BASE_URL=http://localhost:8080` without skipping them silently.
- [X] T003 [P] Record the source extraction and artifact commands in `server/README.md`, identifying the old `server/src/` classes to be moved into each planned source root and the build-time-only common dependency.

---

The checked tasks below implement the plan's commit-time sequence design. They are one compliant way to assign positions; the specification permits a database-backed materializer that assigns positions after concurrent scan commits while preserving immutable, unique, gapless committed materialized positions and recovery of pending scans.

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Provide the shared schema and utility boundaries needed by all three services.

- [X] T004 Extend `db/init.sql` while preserving the 2,000-SKU/10,000-units-per-SKU reset: keep `catalog_item.sku` as PK, `price` as `DECIMAL(10,2)`, `inventory.sku` as PK/FK, and enforce `stock_quantity >= 0`; retain `transaction.status` values including `CANCELLED`, `transaction_line` PK `(transaction_id, sku)`, and existing popularity tables. Preserve the data-model constraints "2,000 unique seeded SKUs; name and price stable for one run", "stock changes only during committed completion", "Create as `OPEN` and commit before 201", and "Rows exist while OPEN and remain after completion."
- [X] T005 Add `scan_sequence` and `accepted_scan` to `db/init.sql`: initialize singleton `sequence_key=1` with `last_position` zero; make `accepted_scan.position` a `BIGINT` PK, never `AUTO_INCREMENT`, with `transaction_id` and `sku` FKs, `unit_price`, `accepted_at`, and an index on `transaction_id`; preserve the model rules "Positions are positive, contiguous, and commit-visible in order" and "Exactly one immutable event per successful scan, including scans on transactions that never complete."
- [X] T006 Add `analytics_checkpoint` and popularity constraints to `db/init.sql`: initialize singleton `checkpoint_key=1` with `last_window_end` zero; make `popular_window.window_end` UNIQUE; preserve "Complete intervals have exactly 1,000 positions", "Ends are 1,000, 1,500, 2,000, ... and start is `end-999`", and `popular_item` PK `(window_id, rank_pos)` with "Zero to ten rows per window; ranks contiguous from 1, ordered count descending and SKU ascending on ties."
- [X] T007 [P] Extract `server/src/api/HttpSupport.java`, `server/src/api/ApiErrors.java`, and `server/src/api/json/Json.java` into `server/common/src/api/` and `server/common/src/api/json/`, keeping the current JSON encoding, validation helpers, and response shapes for all seven routes.
- [X] T008 [P] Extract `server/src/database/ConnectionPool.java` and `server/src/database/StoreFailure.java` into `server/common/src/database/`; preserve configurable `DB_*` connection settings and make pool failure/cleanup usable by each independent JVM without a domain-service import.
- [X] T009 Adapt `server/tests/architecture/LayerDependencyTest.java`, `server/tests/architecture/CompositionRootTest.java`, and `server/tests/architecture/PipelineArchitectureTest.java` to enforce distinct domain entry points, no shared business-rule jar, no direct runtime domain calls, and separate Window/Ranking/Persistence stage dependencies under the new source roots.

**Checkpoint**: Schema reset, common utilities, independent build targets, and architecture tests are ready. Story work can begin.

---

## Phase 3: User Story 1 - Finish Checkout Across a Restart (Priority: P1) 🎯 MVP

**Goal**: Persist every acknowledged OPEN basket scan and complete it atomically after a Transaction Service restart.

**Independent Test**: Scan two units of one SKU and one of another, restart only Transaction Service, verify unchanged OPEN lookup, complete once, reconcile receipt/stock, and verify a repeated completion returns 409 without a second deduction.

### Tests for User Story 1

- [X] T010 [P] [US1] Add database tests for one scan's atomic line/total/sequence/event commit, rollback on invalid SKU or SQL failure, and contiguous positions after concurrent scans in `server/tests/database/JdbcAcceptedScanStoreTest.java`.
- [X] T011 [P] [US1] Add the acknowledged OPEN basket restart and post-restart completion journey, including stable `startedAt`, count, total, receipt, and repeated-completion 409, in `server/tests/integration/TransactionRestartDurabilityTest.java`.
- [X] T012 [P] [US1] Extend `server/tests/integration/ConcurrentCompletionTest.java` and `server/tests/integration/ScanCompletionConvergenceTest.java` for last-unit competing completions, nonnegative stock, and the scan-versus-completion serial outcome; keep existing assertions.
- [X] T013 [P] [US1] Adapt `server/tests/contract/TransactionLifecycleTest.java` and `server/tests/contract/TransactionLookupTest.java` to cover the unchanged public response/error shapes through port 8080, including missing station, unknown transaction/SKU, empty basket, and non-OPEN scan/completion.

### Implementation for User Story 1

- [X] T014 [US1] Extend `server/transaction/src/database/TransactionStore.java` and `server/transaction/src/database/JdbcTransactionStore.java` so `POST /transactions` commits `OPEN` before 201 and returns the MySQL `started_at`; lookup reads durable status, item count, running total, and lines rather than process memory.
- [X] T015 [US1] Add `server/transaction/src/database/JdbcAcceptedScanStore.java` with one transaction-row-locked scan operation that validates `OPEN` and SKU, upserts `transaction_line` with "quantity positive; price captured from catalog" and "Sum of line quantities and `quantity × unit_price` equals transaction item count and total", updates `transaction.total_amount` so it "reflects committed OPEN lines", locks `scan_sequence` after the transaction row, inserts exactly one positioned `accepted_scan`, and commits before HTTP 200; retry deadlocks as whole transactions within a bounded policy and never report an uncertain commit as success.
- [X] T016 [US1] Move and adapt `server/src/database/CheckoutCompletionStore.java` and `JdbcCheckoutCompletionStore.java` into `server/transaction/src/database/`; read committed lines under the transaction lock, reject missing/non-OPEN/empty baskets, conditionally decrement inventory in sorted SKU order, and commit status, `completed_at`, and every deduction together; on failed stock check roll back and leave `OPEN`.
- [X] T017 [US1] Move and refactor `server/src/transaction/TransactionService.java`, `TransactionOperations.java`, and `TransactionFailure.java` into `server/transaction/src/transaction/` to use durable store outcomes for start, scan, lookup, and completion; retire process-local `Basket` as an authority and the synchronous `AcceptedScanSink` dependency, preserving per-request physical-unit scan semantics and receipt calculations.
- [X] T018 [US1] Move and adapt `server/src/api/TransactionHttpHandler.java` and `server/src/api/ApiErrorMapper.java` into `server/transaction/src/api/`; preserve current HTTP validation, domain codes/statuses, JSON fields, and HTTP 500 `INTERNAL_ERROR` for unexpected database failures.
- [X] T019 [US1] Create `server/transaction/src/transaction/TransactionMain.java` as Transaction Service's independent entry point on `TRANSACTION_PORT` (default 8081), with its own HTTP executor, pool, shutdown hook, transaction routes only, runtime `DB_*` settings, and clear startup failure when MySQL is unavailable.

**Checkpoint**: Transaction Service alone can resume acknowledged scans and complete without Analytics or Inventory Service processes.

---

## Phase 4: User Story 2 - Query Inventory Independently (Priority: P1)

**Goal**: Serve the full catalog and committed low-stock data from an independently restartable Inventory Service.

**Independent Test**: Query 2,000 baseline products and low-stock defaults/overrides; restart Inventory Service without stopping checkout; verify it sees later committed stock.

### Tests for User Story 2

- [X] T020 [P] [US2] Adapt `server/tests/contract/ItemsEndpointTest.java` and `server/tests/contract/LowStockEndpointTest.java` to verify the unchanged catalog fields, 2,000 unique SKUs, default/override threshold, alert ordering, and error responses through port 8080.
- [X] T021 [P] [US2] Add an Inventory-only restart and committed-stock visibility test, with Transaction checkout continuing during its outage, in `server/tests/integration/InventoryServiceIsolationTest.java`.

### Implementation for User Story 2

- [X] T022 [US2] Move `server/src/transaction/StoreQueryService.java`, `CatalogOperations.java`, `InventoryOperations.java`, and `LowStockThresholds.java` into `server/inventory/src/inventory/`; retain threshold and response rules while removing Inventory business ownership from the Transaction package.
- [X] T023 [US2] Move and adapt `server/src/database/{CatalogStore,JdbcCatalogStore,InventoryStore,JdbcInventoryStore}.java` into `server/inventory/src/database/` so Inventory reads current committed catalog/stock; keep Transaction's catalog validation read as its own JDBC adapter in `server/transaction/src/database/JdbcCatalogStore.java` without importing Inventory business classes.
- [X] T024 [US2] Move and adapt `server/src/api/CatalogHttpHandler.java` and `server/src/api/InventoryHttpHandler.java` into `server/inventory/src/api/`; preserve `/items` and `/inventory/low-stock` method, query validation, JSON, ordering, and database-failure mapping.
- [X] T025 [US2] Create `server/inventory/src/inventory/InventoryMain.java` with only Inventory routes, its own HTTP executor and pool, `INVENTORY_PORT` (default 8082), runtime `DB_*` settings, shutdown hook, and database startup failure reporting.

**Checkpoint**: Inventory queries work from committed MySQL data independently of the other domain processes.

---

## Phase 5: User Story 3 - Recover Popular Items from Durable Scans (Priority: P1)

**Goal**: Consume every positioned scan from MySQL and publish exact 1,000-scan/500-hop windows once, including after crashes or long downtime.

**Independent Test**: Submit a controlled 1,500+ scan stream while Analytics stops at each specified boundary; restart it, wait for catch-up, and compare every published window/rank to independent counts over `accepted_scan`.

### Tests for User Story 3

- [X] T026 [P] [US3] Add ordered polling tests for concurrent commit visibility, adjacent positions, missing-position refusal, and retained backlog in `server/tests/database/JdbcAcceptedScanReaderTest.java`.
- [X] T027 [P] [US3] Extend `server/tests/unit/analytics/WindowFilterTest.java`, `RankingFilterTest.java`, and `PipelineMessageTest.java` for immutable positioned events, exact duplicate handling, conflicting duplicate rejection, absolute ends 1,000/1,500/2,000, 1,000-entry overlap, descending count/SKU-ascending ties, and top ten.
- [X] T028 [P] [US3] Extend `server/tests/database/JdbcPopularWindowStoreTest.java` for header+ranks+checkpoint atomicity, ordered checkpoint progression, duplicate-boundary equivalence, conflict rejection, and latest-complete-only reads.
- [X] T029 [P] [US3] Add fault-injection tests for before read, during assembly, before window commit, and after commit before worker acknowledgement in `server/tests/integration/AnalyticsCrashRecoveryTest.java`; each case must restart and compare all eligible windows against `accepted_scan` without partial or duplicate publication.
- [X] T030 [P] [US3] Add a prolonged Analytics outage and Transaction-during-outage test in `server/tests/integration/AnalyticsBacklogIsolationTest.java`; check scans continue, prewarm restores overlapping windows, and the highest eligible boundary is eventually published.
- [X] T031 [P] [US3] Adapt `server/tests/contract/PopularItemsEndpointTest.java` for the existing empty response before 1,000 scans, latest older complete window during lag, `limit` validation, fields, and rank order through port 8080.

### Implementation for User Story 3

- [X] T032 [US3] Add `server/analytics/src/database/JdbcAcceptedScanReader.java` to read `accepted_scan` by `position > cursor ORDER BY position LIMIT batch` on fresh committed reads, load exactly `W-999..W` for checkpoint prewarm, and fail visibly rather than advance across a missing position.
- [X] T033 [US3] Move and adapt `server/src/analytics/PipelineMessage.java`, `WindowMath.java`, and `WindowFilter.java` into `server/analytics/src/analytics/`; carry immutable `(position, sku)`, maintain an incremental 1,000-SKU ring, prewarm without emitting, require `position = lastApplied + 1`, and emit only at absolute 1,000 then every 500 positions.
- [X] T034 [US3] Move and adapt `server/src/analytics/RankingFilter.java` and `PersistenceFilter.java` into `server/analytics/src/analytics/`; retain separate bounded FIFO queue handoffs and immutable snapshots, rank by count descending/SKU ascending to ten, and retry the oldest window before newer ones.
- [X] T035 [US3] Move and extend `server/src/database/PopularWindowStore.java` and `JdbcPopularWindowStore.java` into `server/analytics/src/database/`; lock `analytics_checkpoint`, require next end 1,000 or prior+500 and start end-999, atomically insert complete header/ranks and advance checkpoint, verify an already committed candidate is identical, and select latest committed result only.
- [X] T036 [US3] Move and refactor `server/src/analytics/AnalyticsService.java` and `AnalyticsOperations.java` into `server/analytics/src/analytics/`, and add `server/analytics/src/analytics/AcceptedScanPoller.java`: bootstrap from the durable checkpoint, prewarm overlap, poll into bounded ingress, discard in-memory progress on restart, and keep Transaction acceptance independent of Analytics availability.
- [X] T037 [US3] Move and adapt `server/src/api/AnalyticsHttpHandler.java` into `server/analytics/src/api/`, preserving the existing popular-items request/response and `limit` semantics while querying only committed materialized windows.
- [X] T038 [US3] Create `server/analytics/src/analytics/AnalyticsMain.java` with its own HTTP executor, database pool, poller/workers, shutdown hook, `ANALYTICS_PORT` (default 8083), runtime `DB_*` settings, and clear startup failure if MySQL is unavailable; retain `AnalyticsLog` observability in `server/analytics/src/analytics/AnalyticsLog.java`.

**Checkpoint**: Analytics recovers after every specified interruption and eventually publishes each exact window once while checkout continues.

---

## Phase 6: User Story 4 - Keep One Stable Client Interface (Priority: P2)

**Goal**: Expose all seven established routes at `http://localhost:8080` through fixed routing while isolating owner outages.

**Independent Test**: Run all current contract tests and the unchanged load client through port 8080; stop each owner separately and verify unrelated routes continue while owner routes return an upstream failure.

### Tests for User Story 4

- [X] T039 [P] [US4] Add a seven-route proxy contract and owner-outage test in `server/tests/integration/PublicRoutingIsolationTest.java`, checking method/path/query/body forwarding, success and application-error status/body preservation, 502/503 for stopped owners, and no fabricated business success.
- [X] T040 [P] [US4] Add a reachable-handler database-outage contract test in `server/tests/integration/DatabaseOutageContractTest.java` that verifies HTTP 500 `INTERNAL_ERROR`, no acknowledgement of uncommitted scans/completions, and recovery of committed data after connectivity returns.

### Implementation for User Story 4

- [X] T041 [US4] Create `deploy/nginx.conf` with exact public mappings `/items` and `/inventory/low-stock` to `127.0.0.1:8082`, `/analytics/popular-items` to `127.0.0.1:8083`, and `/transactions` plus descendants to `127.0.0.1:8081`; listen on 8080, forward raw path/query, method, body, and relevant headers, and let unavailable owners return proxy 502/503.
- [X] T042 [US4] Update `server/tests/support/PipelineTestSupport.java` and `server/tests/support/CheckoutDatabase.java` for separate domain processes and the one public base URL, then run all `server/tests/contract/` tests through the router without changing `spec/self-checkout-openapi.yaml` or `load-client/src/`.

**Checkpoint**: The seven-route public contract and owner failure behavior work through one unchanged base URL.

---

## Phase 7: User Story 5 - Reproduce Distributed Evidence (Priority: P2)

**Goal**: Build, operate, restart, and measure the real distributed deployment from separate clean baseline resets.

**Independent Test**: Follow `server/README.md` and the quickstart from a reset database, restart each service alone, then produce distinct genuine default and 100-station/120-second JSON reports and reconcile database invariants.

### Tests for User Story 5

- [X] T043 [P] [US5] Add a live independent build/start/stop/restart smoke suite in `server/tests/integration/IndependentServiceDeploymentTest.java`, verifying each JAR/port, peer continuity, and recovery after its own restart.
- [X] T044 [P] [US5] Extend `db/validate-invariants.sql` to check 2,000 baseline products, nonnegative stock, completed-line stock reconciliation, transaction line/total consistency, accepted-scan count versus `scan_sequence.last_position`, contiguous positions, unique eligible window boundaries, checkpoint agreement, and ranks/counts derived independently from accepted scans.

### Implementation and evidence for User Story 5

- [X] T045 [P] [US5] Create `server/run-transaction.sh`, `server/run-inventory.sh`, and `server/run-analytics.sh` to launch each thin JAR separately with common.jar and Connector/J, documented `DB_*`, per-service port override, and independent shutdown behavior.
- [X] T046 [US5] Complete `server/README.md` with separate build/start/stop/restart commands, default 8081/8082/8083 and public 8080 ports, centralized MySQL configuration, Nginx startup, route ownership, database outage behavior, Analytics catch-up, and safe disposable-database reset sequence.
- [X] T047 [US5] Update `specs/004-distributed-domain-services/quickstart.md` with the actual verified commands for test execution, directed crash/restart checks, clean reset, Analytics catch-up, and invariant checks after implementation.
- [X] T048 [US5] From a fresh `db/init-db.ps1` reset with all domain processes restarted, run the unchanged `java -cp out Main` against the real proxy; preserve the original client-generated JSON under `load-client/reports/service-based-architecture/default.json` and record its command, baseline, service versions/ports, throughput, per-operation rates, p95/p99, and invariant/catch-up results in `load-client/reports/service-based-architecture/README.md`.
- [X] T049 [US5] After a separate fresh `db/init-db.ps1` reset and service restart, run the unchanged `java -cp out Main --stations=100 --duration=120` against the real proxy; preserve the distinct original JSON under `load-client/reports/service-based-architecture/stress.json` and add its measured rates/latencies, baseline, service versions/ports, inventory reconciliation, and Analytics catch-up to `load-client/reports/service-based-architecture/README.md`.

**Checkpoint**: Both genuine JSON files and their repeatable validation record exist in the repository.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Remove obsolete monolith wiring and verify the complete feature without weakening prior checks.

- [X] T050 Remove obsolete single-JVM `server/src/api/Main.java`, in-process Analytics scan sink wiring and migrated duplicates under `server/src/`, and old-only launch assumptions from `server/run.sh`, after confirming replacement classes are in their domain roots and no build/test classpath still references the monolith.
- [X] T051 Run `server/run-tests.sh` with zero failed and zero skipped live contract/integration tests, run `db/validate-invariants.sql`, and record the exact commands/results in `load-client/reports/service-based-architecture/README.md`; resolve any regressions in the affected `server/tests/` and service source files.
- [X] T052 Confirm `load-client/src/` and `spec/self-checkout-openapi.yaml` remain unchanged, compare both report JSON files to the client schema, and document any observed throughput or latency tradeoff from the serialized scan sequence in `load-client/reports/service-based-architecture/README.md` without inventing a numerical target.

---

## Dependencies & Execution Order

### Phase dependencies

```text
Phase 1 Setup → Phase 2 Foundation ─┬→ US1 Checkout ─────┐
                                    ├→ US2 Inventory ────┼→ US4 Public routing → US5 Evidence → Polish
                                    └→ US3 Analytics ─────┘
```

- **Setup**: Start immediately. T001 establishes the module layout; T002 and T003 can proceed beside it in their separate files.
- **Foundation**: Requires the source layout and build strategy from Setup. T004–T006 edit the same schema and run in order. T007 and T008 edit different common files and can proceed together. T009 follows the resulting ownership layout.
- **US1, US2, US3**: Each starts after Foundation and can be implemented by separate workers. US3 reads the accepted-scan schema from Foundation; its live outage test T030 requires a running Transaction Service, but the Analytics implementation has no runtime dependency on Transaction Service.
- **US4**: Integration requires all three service entry points. T041 router configuration can be prepared earlier, but T039–T042 must be validated after US1–US3 are functional.
- **US5**: Requires all domain services and the router. T048 and T049 are sequential because each requires its own fresh database reset. T049 must not reuse the default run's state.
- **Polish**: Follows the selected completed stories; T051 and T052 require both evidence runs for full feature completion.

### Within each story

- Write the story's tests first and confirm they fail for missing behavior. Then implement data adapters before business services, services before entry points/HTTP integration, and validate the story's independent test at its checkpoint.
- US1's scan lock order is transaction row then sequence row. Completion locks the transaction row and updates inventory in sorted SKU order. A scan and completion therefore have one serial result.
- US3's poll cursor is in memory only. Each complete window, all ranks, and its checkpoint commit together; persistence retries the oldest candidate before later ones.

### Parallel examples

- **US1**: T010 in `server/tests/database/JdbcAcceptedScanStoreTest.java` and T011 in `server/tests/integration/TransactionRestartDurabilityTest.java` can be written together; T012 and T013 cover different test files as well.
- **US2**: T020 in `server/tests/contract/` and T021 in `server/tests/integration/InventoryServiceIsolationTest.java` can be written together.
- **US3**: T026, T027, T028, T029, T030, and T031 use separate test files and can be prepared together; implement T032–T036 in dependency order afterward.
- **US4**: T039 in `server/tests/integration/PublicRoutingIsolationTest.java` and T040 in `server/tests/integration/DatabaseOutageContractTest.java` can be written together.
- **US5**: T043 in `server/tests/integration/IndependentServiceDeploymentTest.java`, T044 in `db/validate-invariants.sql`, and T045 in the three run scripts can proceed together; the two load runs remain sequential.

## Implementation Strategy

1. **MVP**: Complete Setup, Foundation, and US1. Demonstrate a scanned OPEN checkout surviving a Transaction Service restart, then completing once with reconciled stock.
2. **Core distributed behavior**: Add US2 and US3 independently. Demonstrate Inventory isolation and Analytics outage/replay against durable scans.
3. **Single public interface**: Add US4's fixed router and run every contract test through port 8080.
4. **Evidence**: Add US5's operation scripts and live checks, reset before each unchanged-client command, preserve both actual JSON outputs, and finish cross-cutting verification.

## Notes

- The old `server/src/` paths are migration sources; final implementation paths are the planned `server/{common,transaction,inventory,analytics}/src/` layout.
- Do not change the load client or public OpenAPI contract to make tests pass. A repeated successful HTTP scan is a new unit; replay of one persisted position is the same event.
- Preserve accepted-scan history for replay and audit during this iteration. A partial or in-flight Analytics window must never become public.
- During `$speckit-converge`, review the default and stress evidence against `load-client/reports/pipeline-architecture/`, especially per-operation Scan p99 and Analytics catch-up after an outage. Retain durable SQL scans unless measured results show a material regression; document the comparison and add a targeted optimization task only when the evidence supports one.

## Phase 9: Convergence

- [ ] T053 CRITICAL: Recreate or validate pooled JDBC connections after a temporary MySQL connection loss in `server/common/src/database/ConnectionPool.java`; add a live outage/recovery test that keeps the three service processes running, verifies failed writes receive no success, then verifies checkout, inventory queries, and Analytics backlog catch-up after MySQL returns per FR-022 and Constitution V (partial).
- [ ] T054 Profile and optimize the durable scan path in `server/transaction/src/database/JdbcAcceptedScanStore.java`, focusing on per-scan SQL round trips and scan-sequence lock hold time while preserving atomic basket/event commits and contiguous order; rerun both unchanged-client workloads from separate fresh resets, verify stock/windows/recovery, and record scan p99, throughput, errors, and Analytics catch-up against the prior pipeline and current distributed reports per plan: Complexity Tracking and FR-025 (partial).
