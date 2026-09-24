---

description: "Dependency-ordered implementation tasks for the layered architecture migration"
---

# Tasks: Migrate to Layered Architecture

**Input**: Design documents from `/specs/002-migrate-layered-architecture/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/`, `quickstart.md`, and `.specify/memory/constitution.md`

**Tests**: Tests are required by the feature specification, implementation plan, and constitution. Write each listed test before the corresponding implementation and verify that it fails for the intended reason.

**Organization**: Tasks are grouped by user story so each story has an explicit, independently verifiable outcome. The two P1 stories together form the minimum viable layered migration.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel because it changes different files and does not depend on an incomplete task in the same phase
- **[Story]**: Maps the task to a user story in `spec.md`
- Every task names the file or directory it changes or produces

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Prepare the existing plain-`javac` project for the target package layout without changing the public contract, schema, or load client.

- [X] T001 Create target source and test package directories under `server/src/api/json/`, `server/src/transaction/`, `server/src/analytics/`, `server/src/database/`, `server/tests/architecture/`, `server/tests/unit/api/`, `server/tests/unit/transaction/`, `server/tests/unit/analytics/`, and `server/tests/database/`
- [X] T002 Update recursive source and test discovery for the new package layout while retaining JDK 21, JUnit Platform Console 1.11.0, and MySQL Connector/J 9.0.0 in `server/build.sh` and `server/build-tests.sh`
- [X] T003 [P] Preserve the existing seven-route contract and add a migration test inventory comment that maps retained suites to user stories in `server/tests/contract/ItemsEndpointTest.java`, `server/tests/contract/LowStockEndpointTest.java`, `server/tests/contract/PopularItemsEndpointTest.java`, `server/tests/contract/TransactionLifecycleTest.java`, and `server/tests/contract/TransactionLookupTest.java`

---

## Phase 2: Foundational (Blocking Layer Contracts)

**Purpose**: Define transport-neutral boundaries and immutable records that all user stories depend on.

**⚠️ CRITICAL**: No implementation task for a user story should begin until these contracts compile.

- [X] T004 [P] Define `CatalogStore` and immutable catalog records in `server/src/database/CatalogStore.java`, preserving `sku` as required and unique with maximum 20 characters, `name` as required with maximum 100 characters, and `price` as required `DECIMAL(10,2)` semantics captured at scan time
- [X] T005 [P] Define `TransactionStore` and immutable durable transaction records in `server/src/database/TransactionStore.java`, preserving a generated-once transaction ID with maximum 40 characters, a non-blank station ID with maximum 40 characters, `OPEN`/`COMPLETED`/`CANCELLED` status, zero total at start, and nullable completion time set only on successful completion
- [X] T006 [P] Define `InventoryStore` and immutable low-stock projections in `server/src/database/InventoryStore.java`, preserving non-negative committed stock, a per-SKU persisted threshold, and a non-persisted request override
- [X] T007 [P] Define `CheckoutCompletionStore`, `CompletionCommand`, sorted immutable completion lines, and typed `COMPLETED`, `NOT_OPEN`, and `INSUFFICIENT_STOCK` outcomes in `server/src/database/CheckoutCompletionStore.java`; require positive aggregated quantities, one line per distinct SKU, `DECIMAL(10,2)` scan-time unit prices, and no exposed `Connection` or `SQLException`
- [X] T008 [P] Define `PopularWindowStore` and immutable snapshot/view records in `server/src/database/PopularWindowStore.java`, preserving coherent `window_start`/`window_end`, rank positions 1–10, positive scan counts, zero-to-ten deterministic ordered ranks, and atomic header-plus-rank persistence
- [X] T009 [P] Define the transport-neutral `StoreFailure` used by all database contracts in `server/src/database/StoreFailure.java`
- [X] T010 [P] Define `TransactionOperations` commands/views, `AcceptedScanSink`, and stable domain failure codes `NOT_FOUND`, `NOT_OPEN`, `EMPTY_BASKET`, `UNKNOWN_SKU`, and `INSUFFICIENT_STOCK` in `server/src/transaction/TransactionOperations.java`, `server/src/transaction/AcceptedScanSink.java`, and `server/src/transaction/TransactionFailure.java`
- [X] T011 [P] Define immutable `CatalogItemView` and `LowStockView` API-facing business contracts in `server/src/transaction/CatalogOperations.java` and `server/src/transaction/InventoryOperations.java`
- [X] T012 [P] Define `AnalyticsOperations` with accepted-scan ingestion, latest persisted result lookup, orderly shutdown, and immutable `PopularItemsView`/`RankedItemView` records in `server/src/analytics/AnalyticsOperations.java`
- [X] T013 Create the single domain/store-failure-to-HTTP mapping table for `{error,message}` responses, including sanitized `500 INTERNAL_ERROR`, in `server/src/api/ApiErrorMapper.java`

**Checkpoint**: API, transaction, analytics, and database contracts compile without reverse package dependencies or transport/persistence type leakage.

---

## Phase 3: User Story 1 - Complete Checkout Without Behavioral Change (Priority: P1) 🎯 MVP Part 1

**Goal**: Preserve start, scan, lookup, and atomic/idempotent completion behavior behind the new layer contracts.

**Independent Test**: Start a transaction, scan an available SKU, complete it, and verify the fixed response shapes, one durable completed sale, and one inventory decrement; repeat completion and run contending completions to verify no second decrement and no negative stock without using analytics or operator endpoints.

### Tests for User Story 1

- [X] T014 [P] [US1] Add basket state-machine tests for `OPEN → COMPLETING → COMPLETED`, failed-completion rollback to `OPEN`, per-basket serialization, positive aggregated quantities, and scan-time price capture in `server/tests/unit/transaction/BasketTest.java`
- [X] T015 [P] [US1] Add transaction-service tests with fake catalog, transaction, completion stores, and scan sink covering start, accepted scan, missing transaction, unknown SKU, empty basket, insufficient stock, duplicate completion, receipt-after-commit, and failure isolation in `server/tests/unit/transaction/TransactionServiceTest.java`
- [X] T016 [P] [US1] Add MySQL adapter tests proving one-connection atomic commit/rollback, ascending-SKU conditional decrements, one row per distinct SKU, durable completion, idempotency, insufficient-stock rollback, and last-unit contention in `server/tests/database/JdbcCheckoutCompletionStoreTest.java`
- [X] T017 [P] [US1] Extend lifecycle contract coverage for `{}` completion bodies, exact statuses/shapes, empty baskets, missing/not-open transactions, and duplicate completion in `server/tests/contract/TransactionLifecycleTest.java`
- [X] T018 [P] [US1] Adapt durability, duplicate-completion, concurrent-completion, stock-conservation, and scan/completion-convergence assertions to the layered server in `server/tests/integration/CompletionDurabilityTest.java`, `server/tests/integration/DuplicateCompletionTest.java`, `server/tests/integration/ConcurrentCompletionTest.java`, `server/tests/integration/StockConservationTest.java`, and `server/tests/integration/ScanCompletionConvergenceTest.java`

### Implementation for User Story 1

- [X] T019 [P] [US1] Move the synchronized in-memory basket state machine and immutable snapshots into `server/src/transaction/Basket.java`, preserving `OPEN`, temporary `COMPLETING`, successful eviction, failed-completion reopening, derived item count, and derived running total
- [X] T020 [P] [US1] Implement stable-order immutable catalog caching behind `CatalogStore`, including SKU lookup and scan-time price/name capture, in `server/src/transaction/CatalogCache.java`
- [X] T021 [P] [US1] Adapt durable open-row creation and transaction lookup to `TransactionStore`, with all SQL/JDBC mapping confined to `server/src/database/JdbcTransactionStore.java`
- [X] T022 [US1] Implement guarded `OPEN → COMPLETED`, line insertion, ascending-SKU conditional inventory decrements, rollback on any failure, and commit-before-`COMPLETED` return in `server/src/database/JdbcCheckoutCompletionStore.java`
- [X] T023 [US1] Implement basket lifecycle, UUID creation, price capture, accepted-scan callback after successful admission, typed completion outcomes, receipt construction, idempotency, and live-or-durable lookup in `server/src/transaction/TransactionService.java`
- [X] T024 [US1] Implement request validation, delegation, synchronous JSON serialization, and one-time error mapping for transaction routes in `server/src/api/TransactionHttpHandler.java`
- [X] T025 [US1] Wire `TransactionHttpHandler` only to `TransactionOperations` and register `POST /transactions`, scan, complete, and transaction lookup routes in the construction-only composition root `server/src/api/Main.java`

**Checkpoint**: User Story 1 passes independently with analytics disabled or replaced by a no-op `AcceptedScanSink`.

---

## Phase 4: User Story 2 - Change Responsibilities Safely by Layer (Priority: P1) 🎯 MVP Part 2

**Goal**: Assign every production component to exactly one required layer and enforce API → transaction/analytics → database dependency direction.

**Independent Test**: Run the architecture gate over every production `.java` file and confirm one layer assignment per file, no forbidden imports, no cycles, no SQL/JDBC outside database, no HTTP types below API, no handler-held stores, and a construction-only `api.Main`.

### Tests for User Story 2

- [X] T026 [US2] Add an exhaustive source ownership/import/dependency test that fails on unassigned files, reverse imports, transaction↔analytics imports, package cycles, `java.sql`/SQL/`ConnectionPool`/`Jdbc*` outside database except construction in `api.Main`, HTTP types below API, handler-to-store fields, or new undeclared production components in `server/tests/architecture/LayerDependencyTest.java`
- [X] T027 [P] [US2] Add a composition-root test that verifies `api.Main` only constructs/registers collaborators and every handler constructor receives a business interface rather than a database contract in `server/tests/architecture/CompositionRootTest.java`

### Implementation for User Story 2

- [X] T028 [P] [US2] Move JSON parsing/serialization into API ownership and update API imports in `server/src/api/json/Json.java` and `server/src/api/HttpSupport.java`
- [X] T029 [P] [US2] Move the JDBC connection pool unchanged into database ownership and keep connection borrowing/release internal to database adapters in `server/src/database/ConnectionPool.java`
- [X] T030 [P] [US2] Implement catalog reads behind `CatalogStore` with stable seed/load ordering in `server/src/database/JdbcCatalogStore.java`
- [X] T031 [P] [US2] Implement current committed low-stock queries behind `InventoryStore`, with no public decrement method, in `server/src/database/JdbcInventoryStore.java`
- [X] T032 [P] [US2] Replace HTTP-aware checkout exceptions with transport-neutral failure codes and messages only in `server/src/transaction/TransactionFailure.java`
- [X] T033 [US2] Complete the construction-only API bootstrap, including database adapter creation, business service injection, virtual-thread HTTP executor setup, route registration, and shutdown hooks without request-time store calls or business rules in `server/src/api/Main.java`
- [X] T034 [US2] Update the launch class from `Main` to `api.Main` while preserving positional database/server arguments and `DB_PASSWORD` handling in `server/run.sh`
- [X] T035 [US2] Remove obsolete production files after all callers compile from `server/src/Main.java`, `server/src/json/`, `server/src/catalog/`, `server/src/checkout/`, `server/src/inventory/`, and `server/src/persistence/`

**Checkpoint**: The complete source tree passes the architecture gate and the P1 checkout lifecycle still passes. Together, User Stories 1 and 2 are the minimum viable migration.

---

## Phase 5: User Story 3 - Preserve Inventory and Operational Reporting (Priority: P2)

**Goal**: Preserve catalog, low-stock, transaction lookup, and latest popular-item behavior through business-layer interfaces.

**Independent Test**: Against known catalog, inventory, transaction, and scan data, call each read-only route and verify contract-defined fields, thresholds, latest 1,000-scan window bounds, deterministic top-10 ranking, and pre-checkpoint empty behavior.

### Tests for User Story 3

- [X] T036 [P] [US3] Add store-query service tests for stable catalog order, default/per-request low-stock thresholds, current committed stock, and non-persisted overrides using fake database contracts in `server/tests/unit/transaction/StoreQueryServiceTest.java`
- [X] T037 [P] [US3] Add analytics-service tests for exact 1,000-slot ring-buffer math, 500-scan checkpoints, deterministic count-descending/SKU tie ordering, first 500-scan partial window, startup recovery, first-invalid-boundary skip, retry-in-place, ordered persistence, latest committed reads, and orderly shutdown in `server/tests/unit/analytics/AnalyticsServiceTest.java`
- [X] T038 [P] [US3] Add MySQL adapter tests for atomic popular-window header plus zero-to-ten ranks, maximum-window recovery, ordered limited reads, and absence before the first checkpoint in `server/tests/database/JdbcPopularWindowStoreTest.java`
- [X] T039 [P] [US3] Extend exact response/status/query validation for catalog, low stock, transaction lookup, and popular items in `server/tests/contract/ItemsEndpointTest.java`, `server/tests/contract/LowStockEndpointTest.java`, `server/tests/contract/TransactionLookupTest.java`, and `server/tests/contract/PopularItemsEndpointTest.java`
- [X] T040 [P] [US3] Adapt controlled concurrent scan sequences across at least two recomputation boundaries and assert exact ranges, counts, ranks, and latest-window visibility in `server/tests/integration/WindowBoundaryAccuracyTest.java`

### Implementation for User Story 3

- [X] T041 [P] [US3] Implement catalog listing and low-stock threshold resolution as transaction-layer behavior mapped from `CatalogStore` and `InventoryStore` records in `server/src/transaction/StoreQueryService.java`
- [X] T042 [P] [US3] Preserve pure ring-index, checkpoint-boundary, and window-start calculations in `server/src/analytics/WindowMath.java`
- [X] T043 [P] [US3] Implement maximum-window recovery, atomic header/rank writes, and latest ordered limited reads behind `PopularWindowStore` in `server/src/database/JdbcPopularWindowStore.java`
- [X] T044 [US3] Implement synchronized accepted-scan ingestion, the 1,000-SKU ring buffer, snapshots every 500 accepted scans, deterministic top-10 ranking, a single ordered asynchronous checkpoint worker, retry-in-place behavior, restart skip behavior, latest persisted lookup, and orderly shutdown in `server/src/analytics/AnalyticsService.java`
- [X] T045 [P] [US3] Implement `GET /items` using only `CatalogOperations` in `server/src/api/CatalogHttpHandler.java`
- [X] T046 [P] [US3] Implement `GET /inventory/low-stock` validation/serialization using only `InventoryOperations` in `server/src/api/InventoryHttpHandler.java`
- [X] T047 [P] [US3] Implement `GET /analytics/popular-items` limit validation and serialization using only `AnalyticsOperations` in `server/src/api/AnalyticsHttpHandler.java`
- [X] T048 [US3] Wire `AnalyticsOperations.recordAcceptedScan` as the transaction layer's `AcceptedScanSink`, inject read interfaces into their handlers, and register the catalog, inventory, and analytics routes in `server/src/api/Main.java`

**Checkpoint**: All operator-facing reads pass without any API handler importing or retaining a database contract.

---

## Phase 6: User Story 4 - Validate the Migrated System Under Required Loads (Priority: P2)

**Goal**: Produce reproducible default and stress evidence from separate clean baselines using the unchanged client.

**Independent Test**: Reset and restart for each workload, run 10 stations for 60 seconds and 100 stations for 120 seconds, confirm two new distinct reports identify their settings, and reconcile every SKU's stock and completed quantities.

### Tests and Validation for User Story 4

- [X] T049 [P] [US4] Update reset-baseline verification to require exactly 2,000 catalog rows, 2,000 inventory rows, 10,000 units per SKU, 20,000,000 total units, empty transactions/lines/windows, and a fresh server process in `server/tests/integration/ResetBaselineTest.java`
- [X] T050 [P] [US4] Add post-workload SQL assertions for non-negative stock, `10000 - final_stock = SUM(transaction_line.quantity)` per SKU, no partial completions, and coherent popular-window/rank bounds in `db/validate-invariants.sql`
- [X] T051 [US4] Reset with `db/init-db.ps1`, start a fresh layered server, run the unchanged client with 10 stations for 60 seconds, validate p95 start below 200 ms, scan below 100 ms, completion below 1 second as design targets, run `db/validate-invariants.sql`, and retain the new timestamped artifact in `load-client/reports/report-*.json`
- [X] T052 [US4] Stop the server, reset with `db/init-db.ps1`, start another fresh layered server, run the unchanged client with 100 stations for 120 seconds, run `db/validate-invariants.sql`, and retain a second distinct timestamped artifact with correct settings in `load-client/reports/report-*.json`
- [X] T053 [US4] Record the two new report filenames, workload settings, invariant results, and any design-target latency observations without modifying the OpenAPI or client in `specs/002-migrate-layered-architecture/quickstart.md`

**Checkpoint**: Both required workloads have distinct client-generated evidence from clean database and process state, and all correctness invariants pass.

---

## Phase 7: User Story 5 - Verify Responsibilities in Isolation (Priority: P3)

**Goal**: Demonstrate that each layer can be verified through explicit contracts without starting unrelated outer layers.

**Independent Test**: Substitute controlled collaborators at every boundary and verify API translation without a database, transaction rules without HTTP, analytics rules without HTTP, and database adapters without the server.

### Tests for User Story 5

- [X] T054 [P] [US5] Add API handler tests with fake `TransactionOperations` for validation, delegation, synchronous success serialization, domain failures, method mismatches, and sanitized infrastructure failures in `server/tests/unit/api/TransactionHttpHandlerTest.java`
- [X] T055 [P] [US5] Add API handler tests with fake `CatalogOperations` and `InventoryOperations` for response translation and query validation without database access in `server/tests/unit/api/StoreQueryHttpHandlerTest.java`
- [X] T056 [P] [US5] Add API handler tests with fake `AnalyticsOperations` for default/explicit/non-negative limits, empty/latest window serialization, and failure mapping without database access in `server/tests/unit/api/AnalyticsHttpHandlerTest.java`
- [X] T057 [P] [US5] Add focused fake-store tests proving transaction business rules expose no HTTP/JDBC types and invoke the accepted-scan sink exactly once only after successful basket admission in `server/tests/unit/transaction/TransactionBoundaryIsolationTest.java`
- [X] T058 [P] [US5] Add focused fake-window-store tests proving analytics decides bounds, counts, ranking, retry order, and visible latest results without API or JDBC collaborators in `server/tests/unit/analytics/AnalyticsBoundaryIsolationTest.java`
- [X] T059 [P] [US5] Add adapter-isolation coverage for catalog, transaction, and inventory mappings and typed store failures in `server/tests/database/JdbcStoreAdaptersTest.java`

**Checkpoint**: API, transaction, analytics, and database layers each pass their tests without booting unrelated outer layers.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Remove migration residue, document the final design, and execute the full validation gate.

- [X] T060 [P] Update build/run/test instructions, target package layout, environment variables, and live-test skip caveat in `server/README.md`
- [X] T061 [P] Update the final component ownership and dependency diagram to match implemented class names in `specs/002-migrate-layered-architecture/contracts/dependency-rules.md` and `specs/002-migrate-layered-architecture/contracts/layer-interfaces.md`
- [X] T062 Remove stale imports, obsolete compiled-output assumptions, and dead migration adapters across `server/src/` and `server/tests/` after confirming every production file has exactly one layer owner
- [X] T063 Run `server/build.sh`, `server/build-tests.sh`, and `server/run-tests.sh`; then execute every live HTTP/database check in `specs/002-migrate-layered-architecture/quickstart.md` with MySQL and the layered server available so no self-skipped test is counted as evidence
- [X] T064 Confirm `spec/self-checkout-openapi.yaml`, all files under `load-client/src/`, and `db/init.sql` are unchanged while the two new workload reports remain in `load-client/reports/`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies; start immediately.
- **Foundational (Phase 2)**: Depends on Setup and blocks all user-story implementation.
- **User Story 1 (Phase 3)**: Depends on Foundational; establishes checkout behavior through the new contracts.
- **User Story 2 (Phase 4)**: Depends on Foundational and coordinates package/file moves with User Story 1; its final gate runs after the P1 implementation compiles.
- **User Story 3 (Phase 5)**: Depends on Foundational and the package ownership established by User Story 2; transaction lookup also reuses User Story 1.
- **User Story 4 (Phase 6)**: Depends on User Stories 1–3 and a live MySQL/server environment.
- **User Story 5 (Phase 7)**: Depends on the implemented interfaces for User Stories 1–3; its test files can be developed in parallel after those contracts stabilize.
- **Polish (Phase 8)**: Depends on all selected user stories, with T063–T064 last.

### User Story Dependency Graph

```text
Setup → Foundational ─┬→ US1 ─┐
                     └→ US2 ─┼→ US3 → US4
                             └──────→ US5
US1 + US2 = P1 MVP
All implemented stories → Polish
```

### Within Each User Story

- Write the listed tests first and verify the expected failure.
- Define or stabilize records/interfaces before their implementations.
- Implement database adapters before business services that consume them.
- Implement business services before HTTP handlers.
- Complete composition-root wiring after collaborators compile.
- Pass the story's independent test before proceeding to the next priority.

### Parallel Opportunities

- T003 can proceed independently of T002 after the directories exist.
- T004–T012 are separate contract files and can run in parallel; T013 can proceed once failure types are agreed.
- US1 tests T014–T018 can run in parallel, and implementations T019–T021 can run in parallel before T022–T025.
- US2 tests T026–T027 can run in parallel; moves/adapters T028–T032 touch separate files.
- US3 tests T036–T040 can run in parallel; T041–T043 and handlers T045–T047 touch separate files.
- US5 is intentionally parallel: T054–T059 exercise different layer boundaries in different test files.
- Documentation tasks T060–T061 can run in parallel after implementation stabilizes.

---

## Parallel Examples

### User Story 1

```text
Task T014: Add basket state-machine tests in server/tests/unit/transaction/BasketTest.java
Task T015: Add fake-store transaction-service tests in server/tests/unit/transaction/TransactionServiceTest.java
Task T016: Add atomic completion adapter tests in server/tests/database/JdbcCheckoutCompletionStoreTest.java
Task T017: Extend lifecycle contract tests in server/tests/contract/TransactionLifecycleTest.java
```

### User Story 2

```text
Task T028: Move JSON support into server/src/api/json/Json.java
Task T029: Move the connection pool into server/src/database/ConnectionPool.java
Task T030: Implement server/src/database/JdbcCatalogStore.java
Task T031: Implement server/src/database/JdbcInventoryStore.java
```

### User Story 3

```text
Task T036: Add server/tests/unit/transaction/StoreQueryServiceTest.java
Task T037: Add server/tests/unit/analytics/AnalyticsServiceTest.java
Task T038: Add server/tests/database/JdbcPopularWindowStoreTest.java
Task T040: Adapt server/tests/integration/WindowBoundaryAccuracyTest.java
```

### User Story 5

```text
Task T054: Test transaction HTTP translation in server/tests/unit/api/TransactionHttpHandlerTest.java
Task T055: Test catalog/inventory HTTP translation in server/tests/unit/api/StoreQueryHttpHandlerTest.java
Task T056: Test analytics HTTP translation in server/tests/unit/api/AnalyticsHttpHandlerTest.java
Task T057: Test transaction boundary isolation in server/tests/unit/transaction/TransactionBoundaryIsolationTest.java
Task T058: Test analytics boundary isolation in server/tests/unit/analytics/AnalyticsBoundaryIsolationTest.java
Task T059: Test JDBC adapter isolation in server/tests/database/JdbcStoreAdaptersTest.java
```

---

## Implementation Strategy

### MVP First: Both P1 Stories

1. Complete Setup and Foundational phases.
2. Implement User Story 1 through its independent checkout test.
3. Complete User Story 2's package migration and architecture gate.
4. Stop and validate the combined P1 MVP: unchanged checkout behavior plus enforceable layer boundaries.

### Incremental Delivery

1. **Foundation**: Explicit contracts compile with no transport or JDBC leakage.
2. **P1 MVP**: Checkout remains correct and the four-layer dependency rules are enforced.
3. **P2 operations**: Catalog, low-stock, transaction lookup, and analytics reads work through business interfaces.
4. **P2 evidence**: Clean-baseline default and stress reports prove behavior under required loads.
5. **P3 isolation**: Each boundary is independently replaceable and testable.
6. **Polish**: Full live gate, documentation, and immutability checks complete the migration.

### Parallel Team Strategy

1. Complete Setup and agree on Foundational record/interface shapes together.
2. After Foundational, one developer can drive US1 behavior while another builds US2 architecture tests and non-overlapping file moves.
3. After the P1 composition root stabilizes, separate developers can work on US3 analytics/store-query behavior and US5 isolation tests.
4. Run US4 sequentially because each workload requires its own database reset and fresh server process.

---

## Notes

- `[P]` means separate files and no dependency on incomplete work in the same phase; tasks that touch `server/src/api/Main.java` are intentionally serialized.
- Preserve exact API methods, statuses, JSON fields, synchronous behavior, and tolerance of `{}` on completion.
- Do not modify `spec/self-checkout-openapi.yaml`, `load-client/src/`, or `db/init.sql` to make validation pass.
- A self-skipped HTTP/database test is not completion evidence.
- Commit after each task or coherent task group, and validate at every checkpoint.
