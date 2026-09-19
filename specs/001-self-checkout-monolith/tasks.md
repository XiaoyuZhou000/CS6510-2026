# Tasks: Self-Checkout Monolithic Server

**Input**: Design documents from `specs/001-self-checkout-monolith/`
**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [data-model.md](./data-model.md), [research.md](./research.md), [contracts/](./contracts/)

**Tests**: Included. `plan.md`'s Project Structure explicitly designates `server/tests/{contract,integration,unit}/` with named tests (concurrent-completion race, duplicate-completion, restart-durability, window-boundary), and Constitution Principles I/II/V are non-negotiable correctness guarantees that can only be demonstrated under concurrency via integration tests.

**Organization**: Tasks are grouped by user story (from `spec.md`) to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: Which user story this task belongs to (US1–US6)
- File paths are relative to the repository root; all new code lives under `server/`

## Path Conventions

Single project, new top-level `server/` directory (sibling to `spec/`, `load-client/`, `mockserver/`, `db/`), per `plan.md`'s Project Structure:

```text
server/
├── build.sh / run.sh / build-tests.sh / run-tests.sh
├── lib/                  # vendored jars: mysql-connector-j, junit-platform-console-standalone
├── src/
│   ├── Main.java
│   ├── api/              # routing, request/response helpers, ApiError codes
│   ├── catalog/          # GET /items
│   ├── checkout/         # transaction lifecycle + in-memory Basket
│   ├── inventory/        # stock decrement + low-stock query
│   ├── analytics/        # hopping-window scan counter + top-10
│   ├── persistence/      # JDBC pool + DAOs
│   └── json/             # hand-rolled JSON reader/writer
└── tests/
    ├── contract/
    ├── integration/
    └── unit/
```

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project scaffolding — no application logic yet

- [X] T001 Create the `server/` directory tree: `server/src/{api,catalog,checkout,inventory,analytics,persistence,json}`, `server/tests/{contract,integration,unit}`, `server/lib/` (per `plan.md` Project Structure)
- [X] T002 [P] Vendor the `mysql-connector-j` JDBC driver jar under `server/lib/` (per `plan.md` Primary Dependencies; research.md §5)
- [X] T003 [P] Vendor the `junit-platform-console-standalone` jar under `server/lib/` (per `plan.md` Testing; research.md §8)
- [X] T004 [P] Create `server/build.sh`: `javac -d out/main` over all `server/src/**/*.java` with `server/lib/*` on the classpath, mirroring `mockserver/build.sh`'s convention
- [X] T005 [P] Create `server/run.sh`: builds first if `out/main` is missing, then `java -cp out/main:lib/* Main`, forwarding CLI args `[port] [dbHost] [dbPort] [dbName] [dbUser] [dbPassword]`, mirroring `mockserver/run.sh`
- [X] T006 [P] Create `server/build-tests.sh` (`javac -cp lib/*:out/main` over `server/tests/**/*.java` into `out/test`) and `server/run-tests.sh` (`java -jar lib/junit-platform-console-standalone-*.jar -cp out/main:out/test --scan-classpath`), per research.md §8

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Infrastructure every user story's endpoints depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T007 [P] Implement a hand-rolled JSON reader/writer in `server/src/json/Json.java`, extending `mockserver/Json.java`'s parsing pattern with the object/array serialization helpers needed to write every response DTO in this feature (research.md §7)
- [X] T008 [P] Implement a bounded JDBC connection pool in `server/src/persistence/ConnectionPool.java`: a `BlockingQueue<Connection>` of ~150 connections created eagerly at startup against MySQL 8.0+ at `127.0.0.1:3307` / database `cs6510_selfcheckout`, with `borrow()`/`release()` (research.md §5)
- [X] T009 [P] Implement `server/src/api/ApiErrors.java` with the fixed `ApiError` (`error`, `message`) value type and the exact `error` string constants from [contracts/error-codes.md](./contracts/error-codes.md): `MISSING_STATION_ID` (400), `TRANSACTION_NOT_FOUND` (404), `SKU_NOT_FOUND` (404), `TRANSACTION_NOT_OPEN` (409), `EMPTY_BASKET` (409), `INSUFFICIENT_STOCK` (409)
- [X] T010 Implement `server/src/api/HttpSupport.java`: request-body reading, JSON response writing, and a method+path dispatcher supporting `/transactions/{id}/...`-style path parameters, matching `mockserver/MockServer.java`'s routing/`respond()` conventions (depends on T007, T009)
- [X] T011 [P] Implement `server/src/persistence/CatalogDao.java`: `loadAll()` running `SELECT sku, name, price FROM catalog_item` — fields `sku VARCHAR(20) PK`, `name VARCHAR(100)`, `price DECIMAL(10,2)` per data-model.md CatalogItem (depends on T008)
- [X] T012 Implement `server/src/catalog/CatalogCache.java`: an immutable `Map<sku, CatalogItem>` populated once at startup from `CatalogDao.loadAll()` (catalog is immutable during a run per data-model.md), exposing lookup-by-SKU and full-list access (depends on T011)
- [X] T013 Implement `server/src/persistence/PopularWindowDao.java` write-side and startup-read method: `writeWindow(windowStart, windowEnd, top10Entries)` — one JDBC transaction inserting a `popular_window` row (`window_id BIGINT PK AUTO_INCREMENT`, `window_start BIGINT`, `window_end BIGINT`, `computed_at TIMESTAMP(3)`) plus up to 10 `popular_item` rows (`window_id BIGINT PK,FK`, `rank_pos INT PK` 1..10, `sku VARCHAR(20) FK`, `scan_count BIGINT`) — and `readMaxWindowEnd()` running `SELECT COALESCE(MAX(window_end), 0) FROM popular_window` (depends on T008)
- [X] T014 Implement `server/src/analytics/AnalyticsRecorder.java` per data-model.md AnalyticsState: `AtomicLong globalScanCounter` seeded from `PopularWindowDao.readMaxWindowEnd()` at startup; `skipNextCheckpoint` set to `true` only when that seed value is `> 0` (research.md §9); a fixed `String[1000]` ring buffer; a single intrinsic lock guarding increment + ring-buffer write + (every 500th scan) boundary snapshot + task submission; a single-threaded `ExecutorService` whose checkpoint task tallies counts, ranks the top 10 (resolving names via `CatalogCache`), calls `PopularWindowDao.writeWindow`, and retries in place up to 2 more times (~50ms, 200ms backoff) on failure before giving up and moving to the next queued checkpoint — no scan response ever waits on this executor (research.md §4) (depends on T012, T013)
- [X] T015 Implement `server/src/Main.java`: parse CLI args `[port] [dbHost] [dbPort] [dbName] [dbUser] [dbPassword]`, construct `ConnectionPool`, load `CatalogCache`, construct `AnalyticsRecorder`, create `com.sun.net.httpserver.HttpServer` with `Executors.newVirtualThreadPerTaskExecutor()` and start listening — no endpoint routes registered yet, those are added by each user story's tasks below (depends on T008, T010, T012, T014)

**Checkpoint**: Foundation ready — user story implementation can now begin

---

## Phase 3: User Story 1 - Browse the catalog before checkout (Priority: P1) 🎯 MVP

**Goal**: A station can fetch the full catalog (SKU, name, price) before any customer scans anything.

**Independent Test**: Request the catalog with no prior state; verify every item has a SKU, name, and price, with no duplicates or omissions across all 2,000 seeded items.

- [X] T016 [P] [US1] Implement `GET /items` handler in `server/src/catalog/CatalogHandler.java`: returns every `CatalogCache` item as `{sku, name, price}` per the `CatalogResponse` schema (FR-001)
- [X] T017 [US1] Register the `/items` route on the `HttpServer` in `server/src/Main.java` (depends on T016)
- [X] T018 [P] [US1] Contract test `server/tests/contract/ItemsEndpointTest.java`: every returned item has `sku`/`name`/`price`; against the full seeded catalog, all 2,000 items are present with no duplicate SKUs (FR-001, FR-002; spec.md US1 acceptance scenarios 1–2)

**Checkpoint**: US1 is independently functional and testable.

---

## Phase 4: User Story 2 - Complete a purchase at a checkout station (Priority: P1) 🎯 MVP

**Goal**: Start a transaction, scan items into it (no stock change), complete it (stock decremented exactly once, atomically, idempotently, and durably), under any level of concurrency.

**Independent Test**: Start one transaction, scan a known set of SKUs, complete it, and confirm the returned receipt and resulting stock levels are both correct — independent of analytics or reporting.

- [X] T019 [P] [US2] Implement `server/src/checkout/Basket.java` per data-model.md: `transactionId`, `stationId`, `startedAt`, a `Map<sku, {quantity, unitPrice, name}>` mutated under a per-basket lock, with `itemCount`/`runningTotal` derived from the map (not stored redundantly)
- [X] T020 [P] [US2] Implement `server/src/persistence/TransactionDao.java` insert/complete methods: `insertOpen(transactionId, stationId)` — one INSERT into `transaction` (`transaction_id VARCHAR(40) PK`, `station_id VARCHAR(40)`, `status ENUM(OPEN, COMPLETED, CANCELLED)` = `'OPEN'`, `total_amount DECIMAL(12,2)` = `0.00`, `started_at TIMESTAMP(3)` default now); `completeIfOpen(transactionId)` — `UPDATE transaction SET status='COMPLETED', completed_at=NOW(3) WHERE transaction_id=? AND status='OPEN'`, returning the affected-row count; `insertLines(transactionId, lines)` — one `transaction_line` row per distinct SKU (`quantity INT CHECK > 0`, `unit_price DECIMAL(10,2)`) (depends on T008)
- [X] T021 [P] [US2] Implement `server/src/persistence/InventoryDao.java` conditional decrement: `decrementIfAvailable(sku, quantity)` running `UPDATE inventory SET stock_quantity = stock_quantity - ? WHERE sku=? AND stock_quantity >= ?` (`stock_quantity INT CHECK >= 0`), returning the affected-row count (FR-008; research.md §1) (depends on T008)
- [X] T022 [US2] Implement `server/src/checkout/CheckoutService.java`: `start(stationId)` — reject blank `stationId` (400 `MISSING_STATION_ID`), else create a `Basket` and `TransactionDao.insertOpen`, return the open `Transaction` (FR-003); `scan(transactionId, sku)` — reject unknown transaction (404 `TRANSACTION_NOT_FOUND`), non-open transaction (409 `TRANSACTION_NOT_OPEN`), unknown SKU via `CatalogCache` (404 `SKU_NOT_FOUND`), else mutate the `Basket` under its lock and call `AnalyticsRecorder.recordScan(sku)` — stock is untouched (FR-004, FR-005); `complete(transactionId)` — reject unknown transaction (404), non-open transaction (409 `TRANSACTION_NOT_OPEN`), empty basket checked before opening any DB transaction (409 `EMPTY_BASKET`, FR-007), else run one JDBC transaction: `completeIfOpen` idempotency guard (0 rows ⇒ rollback + 409 `TRANSACTION_NOT_OPEN`), insert lines, decrement every distinct scanned SKU **in ascending SKU order** (deadlock avoidance, research.md §1) via `decrementIfAvailable`, any 0-row decrement ⇒ rollback the whole transaction + 409 `INSUFFICIENT_STOCK` (research.md §2, no partial deductions), else commit and build the `Receipt` from the basket plus the timestamps written by the guard (FR-006, FR-008, FR-009, FR-010) (depends on T019, T020, T021, T014, T012)
- [X] T023 [US2] Implement `server/src/checkout/CheckoutHandlers.java` for `POST /transactions`, `POST /transactions/{id}/items`, `POST /transactions/{id}/complete`, mapping `CheckoutService` outcomes to the status codes and `ApiError` bodies from [contracts/error-codes.md](./contracts/error-codes.md) (depends on T022, T010)
- [X] T024 [US2] Register `/transactions`, `/transactions/{id}/items`, `/transactions/{id}/complete` routes in `server/src/Main.java` (depends on T023)
- [X] T025 [P] [US2] Contract test `server/tests/contract/TransactionLifecycleTest.java`: start/scan/complete response shapes and status codes vs. the `Transaction`/`ScanResult`/`Receipt` schemas, including every 400/404/409 case from contracts/error-codes.md (FR-003, FR-004, FR-006, FR-007)
- [X] T026 [P] [US2] Integration test `server/tests/integration/ConcurrentCompletionTest.java` against a real MySQL instance: two stations each scan the last remaining unit of the same SKU, then complete concurrently — assert exactly one completion succeeds, the other is rejected, and the SKU's stock never goes negative (Constitution I; spec.md US2 acceptance scenario 4)
- [X] T027 [P] [US2] Integration test `server/tests/integration/DuplicateCompletionTest.java`: complete an open transaction, then resubmit the identical completion request — assert stock is decremented exactly once and no second receipt charge occurs (FR-009, SC-002; spec.md US2 acceptance scenario 5)
- [X] T028 [P] [US2] Integration test `server/tests/integration/CompletionDurabilityTest.java`: complete a transaction, then read the `transaction` and `inventory` rows back over a fresh, direct JDBC connection — assert both are already committed and queryable immediately after the success response (FR-010, Constitution II; spec.md US2 acceptance scenario 6)
- [X] T029 [P] [US2] Integration test `server/tests/integration/StockConservationTest.java`: many concurrent completions across a mix of overlapping and distinct SKUs — assert for every touched SKU `initial stock − final stock == total quantity sold`, with stock never observed negative at any point (FR-008, SC-001; spec.md US2 acceptance scenario 7)

**Checkpoint**: US1 + US2 are independently functional — this is the deliverable MVP (core checkout path, correct and concurrent-safe).

---

## Phase 5: User Story 3 - Monitor low-stock inventory (Priority: P2)

**Goal**: Report which SKUs are at or below a threshold, with a per-request override.

**Independent Test**: Drive stock for a known SKU below a threshold via completed transactions, request low-stock alerts, confirm that SKU appears with correct stock and threshold.

- [X] T030 [US3] Add `listLowStock(Integer thresholdOverride)` to `server/src/persistence/InventoryDao.java`: selects `sku`, `name`, `stock_quantity`, and each row's stored `low_stock_threshold INT` (or the request's override when supplied), using an **at-or-below** comparison (`stock_quantity <= threshold`) per data-model.md's validation summary (FR-015)
- [X] T031 [US3] Implement `GET /inventory/low-stock` handler in `server/src/inventory/InventoryHandlers.java`: an optional `threshold` query param overrides the default for that request only, without changing server-side state for other callers (FR-015; spec.md US3 acceptance scenario 2)
- [X] T032 [US3] Register the `/inventory/low-stock` route in `server/src/Main.java` (depends on T031)
- [X] T033 [P] [US3] Contract test `server/tests/contract/LowStockEndpointTest.java`: response shape vs. `LowStockResponse`, default-vs-override threshold behavior, and the at-or-below boundary when stock exactly equals the threshold (FR-015; spec.md US3 acceptance scenario 1; spec.md Edge Cases)

**Checkpoint**: US1+US2+US3 independently functional.

---

## Phase 6: User Story 4 - Review popular-item analytics (Priority: P2)

**Goal**: Report the top-10 most-scanned items for the latest computed hopping window.

**Independent Test**: Drive a known sequence of scans, then request the popular-items report — independent of whether any transaction has completed.

- [X] T034 [US4] Add `readLatestWindow(limit)` to `server/src/persistence/PopularWindowDao.java`: selects the `popular_window` row with the highest `window_id BIGINT PK` and its `popular_item` rows (joined to `catalog_item` for `name`), capped at `limit` (FR-014)
- [X] T035 [US4] Implement `GET /analytics/popular-items` handler in `server/src/analytics/AnalyticsHandlers.java`: optional `limit` query param (default 10), returns `windowSize` (1000), `slideInterval` (500), `windowStart`, `windowEnd`, `computedAt`, `items` per `PopularItemsResponse` (FR-013, FR-014)
- [X] T036 [US4] Register the `/analytics/popular-items` route in `server/src/Main.java` (depends on T035)
- [X] T037 [P] [US4] Contract test `server/tests/contract/PopularItemsEndpointTest.java`: response shape vs. `PopularItemsResponse`, top-10 cap and rank ordering, and `windowStart`/`windowEnd` reflecting the latest **computed** (persisted) window rather than the very latest scan (FR-013, FR-014; spec.md US4 acceptance scenarios 1–2)
- [X] T038 [P] [US4] Integration test `server/tests/integration/WindowBoundaryAccuracyTest.java` against a real MySQL instance: fire a concurrent burst of scans from multiple threads spanning a 500-scan checkpoint boundary — assert the persisted window's counts are exact for precisely the 1,000-scan range its own `windowStart`/`windowEnd` claim to cover (Constitution V; spec.md US4 acceptance scenario 3)

**Checkpoint**: US1+US2+US3+US4 independently functional.

---

## Phase 7: User Story 5 - Look up a specific transaction for debugging (Priority: P3)

**Goal**: Retrieve a transaction's current status, item count, and running total by ID.

**Independent Test**: Start a transaction, scan a known item, request that transaction by ID, confirm status/itemCount/runningTotal.

- [X] T039 [US5] Add a completed-transaction read method to `server/src/persistence/TransactionDao.java`: fetches `status`, `started_at`, `completed_at` from the `transaction` row plus `itemCount`/`runningTotal` via `COUNT`/`SUM` over `transaction_line`, for use once a transaction is `COMPLETED` and its in-memory `Basket` has been evicted (data-model.md GET /transactions/{id} fallback)
- [X] T040 [US5] Implement `GET /transactions/{id}` handler in `server/src/checkout/CheckoutHandlers.java`: reports `itemCount`/`runningTotal` from the in-memory `Basket` while `OPEN`, falling back to the T039 DB read once `COMPLETED`; 404 `TRANSACTION_NOT_FOUND` when no such transaction exists (FR-012; spec.md US5 acceptance scenarios 1–2)
- [X] T041 [US5] Register the `GET /transactions/{id}` route in `server/src/Main.java` (depends on T040)
- [X] T042 [P] [US5] Contract test `server/tests/contract/TransactionLookupTest.java`: found (both OPEN and COMPLETED) and not-found cases vs. the `Transaction` schema (spec.md US5 acceptance scenarios 1–2)

**Checkpoint**: Every endpoint in `spec/self-checkout-openapi.yaml` is implemented.

---

## Phase 8: User Story 6 - Reset the store for a repeatable test run (Priority: P2)

**Goal**: Every load-test run starts from an identical, reproducible baseline (2,000 items, 10,000 units each), without any server or contract code changes.

**Independent Test**: Reset the store, verify the 2,000-item/10,000-unit baseline, run a short scripted sequence of transactions, reset again, confirm the baseline is restored identically.

- [X] T043 [US6] Write `server/README.md` documenting: build/run commands, CLI args (`port`, `dbHost`, `dbPort`, `dbName`, `dbUser`, `dbPassword`), and the required procedure between load-test runs — reinitialize via `db/init-db.ps1`, then (re)start the server process, since `CatalogCache` and `AnalyticsRecorder` state are loaded once at startup and are not refreshed by a database reset alone (FR-016, Constitution VI; research.md §9)
- [X] T044 [US6] Integration test `server/tests/integration/ResetBaselineTest.java`: reinitialize via `db/init.sql`, start a fresh server instance, and assert the catalog contains exactly 2,000 items with every SKU's stock at 10,000 units (FR-002, FR-016; spec.md US6 acceptance scenario 1)
- [X] T045 [US6] Execute the assignment's full validation gate end-to-end: reinitialize the database, start the server, run the unmodified `load-client` in default mode (10 stations / 60s), reinitialize again, restart the server, run `load-client` in stress mode (100 stations / 120s); confirm stock conservation (`initial − final == sold`, never negative) for both runs and save both JSON reports under `load-client/reports/` (SC-001, SC-003, SC-006; spec.md US6 acceptance scenario 2; Constitution "Validation gate") (depends on all prior phases)

**Checkpoint**: All user stories independently functional; the assignment's validation gate is satisfied end-to-end.

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Pure-logic coverage and a final full-suite confirmation

- [X] T046 [P] Unit test `server/tests/unit/RingBufferWindowMathTest.java`: ring-buffer indexing, 500-scan boundary detection, and the `skipNextCheckpoint` restart logic from research.md §9, with no database involved
- [X] T047 [P] Unit test `server/tests/unit/LowStockThresholdResolutionTest.java`: default-vs-override threshold resolution and the at-or-below boundary, in isolation from HTTP/DB
- [X] T048 [P] Unit test `server/tests/unit/JsonRoundTripTest.java`: round-trip every response DTO (`CatalogItem`, `Transaction`, `ScanResult`, `Receipt`, `LowStockAlert`, `PopularItem`, `PopularItemsResponse`, `ApiError`) through `server/src/json/Json.java`
- [X] T049 Run the full `server/tests` suite (contract + integration + unit) via `server/run-tests.sh` and confirm all tests pass before considering the feature complete

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3–8)**: All depend on Foundational completion
  - US1 (P1) and US2 (P1) can proceed in parallel once Foundational is done — they touch disjoint files except for both registering routes in `Main.java` (T017, T024 — apply sequentially, same file)
  - US3, US4, US5 (P2/P3) are independent of each other and of US1/US2's *internals*, but each registers a route into the shared `Main.java`, so route-registration tasks across stories must be applied one at a time
  - US6 (P2) is a validation/documentation story; T045 exercises every endpoint from every other story and so should run last
- **Polish (Phase 9)**: Depends on all desired user stories being complete

### User Story Dependencies

- **US1 (P1)**: No dependencies on other stories
- **US2 (P1)**: No dependencies on other stories (independent of US1 — both only need Foundational)
- **US3 (P2)**: No dependencies on other stories (reads `inventory`, which US2's completions write to, but is independently testable by seeding stock directly)
- **US4 (P2)**: No dependencies on other stories (reads `popular_window`, which the Foundational `AnalyticsRecorder` populates as soon as any scan happens — via US2 — but the read endpoint itself has no code dependency on US2's handlers)
- **US5 (P3)**: No dependencies on other stories
- **US6 (P2)**: Exercises all other stories' endpoints in T045; treat as depending on US1–US5 being complete

### Within Each User Story

- DAO/model tasks before service tasks; service tasks before HTTP handler tasks; handler tasks before route registration; route registration before that story's contract/integration tests are run
- Tests may be *written* in parallel with implementation but require the implementation tasks complete to *pass*

### Parallel Opportunities

- All Setup tasks marked [P] (T002–T006) can run in parallel once T001 exists
- Within Foundational, T007/T008/T009/T011 can run in parallel; T010/T012/T013/T014/T015 have listed dependencies
- Once Foundational completes, US1 and US2 can be staffed in parallel by different developers
- Within US2, T019/T020/T021 (Basket, TransactionDao, InventoryDao) can run in parallel; T025–T029 (all test files) can run in parallel once T024 lands
- US3, US4, US5 can each be staffed independently once Foundational completes

---

## Parallel Example: User Story 2

```bash
# Launch the three independent building blocks together:
Task: "Implement Basket in server/src/checkout/Basket.java"
Task: "Implement TransactionDao insert/complete methods in server/src/persistence/TransactionDao.java"
Task: "Implement InventoryDao conditional decrement in server/src/persistence/InventoryDao.java"

# Once CheckoutService + handlers + routes land, launch all US2 tests together:
Task: "Contract test server/tests/contract/TransactionLifecycleTest.java"
Task: "Integration test server/tests/integration/ConcurrentCompletionTest.java"
Task: "Integration test server/tests/integration/DuplicateCompletionTest.java"
Task: "Integration test server/tests/integration/CompletionDurabilityTest.java"
Task: "Integration test server/tests/integration/StockConservationTest.java"
```

---

## Implementation Strategy

### MVP First (User Stories 1 + 2 only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1 (catalog)
4. Complete Phase 4: User Story 2 (checkout core — start/scan/complete, concurrency, idempotency, durability)
5. **STOP and VALIDATE**: Run T018 and T025–T029 independently; confirm stock conservation under concurrent load
6. This is the assignment's core deliverable — everything else is additive reporting/debugging surface

### Incremental Delivery

1. Setup + Foundational → foundation ready
2. US1 + US2 → MVP: catalog + correct, concurrent-safe checkout
3. US3 → low-stock reporting
4. US4 → popular-item analytics
5. US5 → debugging lookup
6. US6 → documented reset procedure + full default/stress validation runs with saved JSON reports

### Parallel Team Strategy

With multiple developers, after Foundational completes:
- Developer A: US1 (catalog) then US5 (lookup, shares the `checkout` package)
- Developer B: US2 (checkout core) — the largest and highest-priority story
- Developer C: US3 (low-stock) then US4 (analytics read side)
- Whoever finishes first drives US6's end-to-end validation runs once all endpoints exist

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks
- [Story] label maps task to specific user story for traceability
- `Main.java` is a shared file: every route-registration task (T017, T024, T032, T036, T041) touches it and must be applied sequentially, even though the tasks are grouped under different, otherwise-parallel stories
- Tests requested explicitly per `plan.md`'s Project Structure (contract/integration/unit) — write them alongside or immediately after each story's implementation, not deferred to the end
- Verify integration tests actually exercise concurrency (real threads/executors against a real MySQL instance) — a single-threaded test cannot demonstrate Constitution I or V
- Commit after each task or logical group; stop at any checkpoint to validate a story independently
- Avoid: vague tasks, same-file conflicts marked [P], cross-story dependencies that break independent testability

---

## Phase 10: Convergence

- [X] T050 CRITICAL Make scan admission and completion finalization mutually exclusive in `server/src/checkout/Basket.java` and `server/src/checkout/CheckoutService.java`, so no scan can succeed after the completion snapshot is chosen and failed completion leaves the basket consistently retryable; add deterministic concurrent scan-vs-complete integration coverage proving every accepted scan is included in the receipt and inventory decrement per Constitution I, FR-004, FR-006, FR-008, and US2/AC7 (contradicts)
- [X] T051 CRITICAL Correct `server/src/analytics/WindowMath.java` and `server/src/analytics/AnalyticsRecorder.java` to persist the first full 1,000-scan window at scan 1,000 and subsequent full windows every 500 scans with valid inclusive boundaries, then update fresh-start, restart-skip, and concurrent boundary tests so every reported range exactly matches the 1,000 scans counted per Constitution V, FR-013, SC-004, and US4/AC1–3 (contradicts)
- [X] T052 CRITICAL Prevent computed analytics checkpoints from being discarded after bounded retries: retain and retry the oldest failed checkpoint in boundary order without blocking scan responses, ensure the latest computed window is eventually persisted and returned, and add transient/prolonged persistence-failure recovery tests per Constitution V, FR-013, and FR-014 (contradicts)
