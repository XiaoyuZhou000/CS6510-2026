---

description: "Dependency-ordered implementation tasks for Pipeline Analytics"
---

# Tasks: Pipeline Analytics

**Input**: Design documents from `/specs/003-pipeline-analytics/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/`, `quickstart.md`

**Tests**: Tests are included because the feature specification and constitution explicitly require unit, architecture, integration, contract, lifecycle, and repeatable load validation. Write each story's tests first and confirm they fail for the intended reason before implementing that story.

**Organization**: Tasks are grouped by user story so each story can be implemented and tested as an independently valuable increment.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel because it changes different files and does not depend on an incomplete task in the same phase
- **[Story]**: Maps the task to User Story 1, 2, 3, or 4
- Every task names the file or directory it changes or validates

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Establish reusable test and evidence scaffolding without changing the public API, schema, or load client.

- [X] T001 [P] Create deterministic fake stores, await helpers, controllable failure gates, and an in-memory operational-log collector for pipeline tests in `server/tests/support/PipelineTestSupport.java`
- [X] T002 [P] Define the two-report selection rules, fresh-reset requirement, exact workload settings, invariant/log evidence filenames, and prohibition on editing client-generated JSON in `load-client/reports/pipeline-architecture/README.md`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Define immutable messages, structured observability, and shared window calculations required by every story.

**⚠️ CRITICAL**: Complete this phase before any user-story implementation.

- [X] T003 [P] Create validated immutable data and typed end-of-stream records in `server/src/analytics/PipelineMessage.java`, preserving these data-model constraints verbatim: Accepted Scan `sku` is "Required, non-blank, at most 20 characters; identifies one admitted physical-unit scan"; Window Snapshot `windowStart` is "Positive inclusive first position", `windowEnd` is "Positive inclusive last position; windowEnd - windowStart + 1 = 1000", and `skus` is "Exactly 1,000 non-blank SKUs in analytics order"; Ranked Item `rank` is "1–10; contiguous in its ranked window", `sku` is "Required, non-blank, at most 20 characters, unique within a ranked window", and `scanCount` is "Positive count in the complete 1,000-event snapshot"; Ranked Window bounds are copied unchanged and `ranks` is "Zero to ten unique SKUs in deterministic contiguous rank order"
- [X] T004 [P] Create an injectable JSON-lines operational logger in `server/src/analytics/AnalyticsLog.java`, preserving these data-model constraints verbatim: `timestamp` is "Required"; `eventType` is "Required stable event name"; `stage` is `INGRESS`, `WINDOW`, `RANKING`, `PERSISTENCE`, or `PIPELINE`; `queueDepth` is "Required for backlog events" and non-negative; `retryCount` is "Required for persistence retry" and positive; `windowStart`, `windowEnd` are "Required when a retry/window is identified" and positive; `shutdownOutcome` is "Required for shutdown completion/forced events"; and `errorType`, `reason` are "Required for terminal failure/rejection as applicable" and sanitized
- [X] T005 Update hopping-window helpers in `server/src/analytics/WindowMath.java` so emission requires 1,000 fresh process events, subsequent complete windows hop by 500, restart positions seed from the persisted maximum end, and no partial window is eligible

**Checkpoint**: Immutable contracts, logging boundary, and recovery-safe window calculations are ready.

---

## Phase 3: User Story 1 - Produce Accurate Windowed Rankings (Priority: P1) 🎯 MVP

**Goal**: Flow accepted scans through independent Window, Ranking, and Persistence filters and publish exact 1,000-event, 500-hop, deterministic top-ten results.

**Independent Test**: Submit controlled sequences through the facade; prove no result at 999 scans, `[1,1000]` at scan 1,000, `[501,1500]` after another 500 scans, exact overlap membership, count-descending/SKU-ascending ties, contiguous ranks, and at most ten items read only from committed storage.

### Tests for User Story 1

> Write these tests first and verify they fail before implementation.

- [X] T006 [P] [US1] Add validation, defensive-copy, immutability, typed-end-marker, and invalid-bound tests for all pipeline records in `server/tests/unit/analytics/PipelineMessageTest.java`
- [X] T007 [P] [US1] Add Window Filter queue tests for 999/1,000/1,500 boundaries, chronological 500-event overlap, exact 1,000-SKU snapshots, partial-drain suppression, and restart warm-up `[1001,2000]` in `server/tests/unit/analytics/WindowFilterTest.java`
- [X] T008 [P] [US1] Add Ranking Filter queue tests for counts, count-descending/SKU-ascending ties, contiguous ranks, fewer-than-ten SKUs, top-ten truncation, immutable output, and end-marker forwarding in `server/tests/unit/analytics/RankingFilterTest.java`
- [X] T009 [P] [US1] Replace combined-service assumptions with end-to-end facade tests for exact windows, deterministic ranks, stage order, and store-only latest-result reads in `server/tests/unit/analytics/AnalyticsServiceTest.java`
- [X] T010 [P] [US1] Add static architecture checks for three explicit filter classes, one worker per filter, unbounded `LinkedBlockingQueue` pipes, immutable message crossing, no direct downstream filter invocation, and no HTTP/JDBC dependencies in `server/tests/architecture/PipelineArchitectureTest.java`

### Implementation for User Story 1

- [X] T011 [P] [US1] Implement the single-owner 1,000-slot ring, consumer-assigned scan positions, complete-window materialization in logical order, 500-event hops, restart warm-up, and stage-ordered end forwarding in `server/src/analytics/WindowFilter.java`
- [X] T012 [P] [US1] Implement snapshot counting, deterministic count-descending/SKU-ascending sorting, top-ten selection, contiguous ranks, and stage-ordered end forwarding with no database collaborator in `server/src/analytics/RankingFilter.java`
- [X] T013 [US1] Implement FIFO ranked-window consumption, atomic `PopularWindowStore.writeWindow` calls, and termination only after all pre-marker windows commit in `server/src/analytics/PersistenceFilter.java`
- [X] T014 [US1] Refactor `server/src/analytics/AnalyticsService.java` into the sole facade/lifecycle owner that constructs the three unbounded queues, starts named `analytics-window`, `analytics-ranking`, and `analytics-persistence` workers downstream-first, admits scans, and maps `PopularWindowStore.readLatest` without reading in-flight state
- [X] T015 [US1] Update `server/tests/architecture/CompositionRootTest.java` and `server/tests/architecture/LayerDependencyTest.java` to preserve the four-layer dependency direction while allowing only `AnalyticsService` and `PersistenceFilter` to cross the analytics-to-`PopularWindowStore` boundary

**Checkpoint**: User Story 1 publishes accurate complete windows through a genuine three-filter pipeline and is independently testable.

---

## Phase 4: User Story 2 - Keep Checkout Responsive During Analytics Work (Priority: P1)

**Goal**: Preserve checkout correctness and fast scan admission while ordered analytics work queues and retries during slow or failing persistence.

**Independent Test**: Hold persistence in a failing/slow state while concurrent stations scan and complete; prove admitted scans are serialized once without capacity waits, checkout and inventory remain correct, `[501,1500]` cannot overtake failed `[1,1000]`, and recovery commits retained windows in order.

### Tests for User Story 2

> Write these tests first and verify they fail before implementation.

- [X] T016 [P] [US2] Add Persistence Filter tests for retaining the oldest failed window, 50 ms and 200 ms fast retries followed by capped 1-second retries, one retry record per failure, no dequeue/overtaking, ordered recovery, and interruptible retry waits in `server/tests/unit/analytics/PersistenceFilterTest.java`
- [X] T017 [P] [US2] Add concurrent-producer tests proving one FIFO analytics order, exactly-once queue admission, boundary membership, no silent loss, and non-blocking unbounded-queue submission in `server/tests/unit/analytics/ConcurrentAnalyticsIngestionTest.java`
- [X] T018 [P] [US2] Extend scan tests to prove the analytics callback occurs only after basket admission and any slow/throwing analytics sink cannot roll back the scan, decrement stock, or break completion/idempotency in `server/tests/unit/transaction/TransactionServiceTest.java`
- [X] T019 [P] [US2] Add atomic popular-window rollback and equivalent-already-committed retry tests around header/rank failures in `server/tests/database/JdbcPopularWindowStoreTest.java`
- [X] T020 [P] [US2] Add an integration test that injects analytics delay/failure during concurrent scan and completion traffic and asserts inventory conservation, durable completion, and ordered retained analytics work in `server/tests/integration/AnalyticsCheckoutIsolationTest.java`

### Implementation for User Story 2

- [X] T021 [US2] Add in-place `StoreFailure` retry with 50 ms, 200 ms, then capped 1-second delays and no newer dequeue before success in `server/src/analytics/PersistenceFilter.java`
- [X] T022 [US2] Serialize the health check and unbounded ingress `offer` under the short lifecycle lock, reject rather than misreport unhealthy admission, and keep all database/ranking/window work outside the critical section in `server/src/analytics/AnalyticsService.java`
- [X] T023 [US2] Emit increasing high-water `BACKLOG` records using an injectable initial threshold that doubles after each record, plus complete `PERSISTENCE_RETRY` records for every failed attempt, from `server/src/analytics/AnalyticsService.java` and `server/src/analytics/PersistenceFilter.java`
- [X] T024 [US2] Make retry after an ambiguous write outcome recognize an equivalent already-committed window without exposing a partial result or duplicating its boundary in `server/src/database/JdbcPopularWindowStore.java`
- [X] T025 [US2] Preserve observational failure isolation after successful basket admission while preventing rejected scans from reaching analytics in `server/src/transaction/TransactionService.java`

**Checkpoint**: User Story 2 keeps checkout correct and responsive while analytics backlog and oldest-first retry remain observable.

---

## Phase 5: User Story 3 - Stop and Recover the Pipeline Predictably (Priority: P2)

**Goal**: Provide deterministic startup, drain-aware bounded shutdown, restart warm-up, ingestion rejection, and terminal worker-failure handling through structured logs.

**Independent Test**: Exercise shutdown before and exactly at window boundaries, permanent store failure, repeated shutdown, restart from persisted end 1,000, and unexpected failure in each worker; prove stage-ordered drain or explicit forced failure within one configured deadline while committed queries remain available.

### Tests for User Story 3

> Write these tests first and verify they fail before implementation.

- [X] T026 [P] [US3] Add lifecycle tests for `NEW → RUNNING → DRAINING → TERMINATED`, atomic ingestion-vs-marker ordering, complete-work drain, partial-window suppression, named-worker termination, repeated-shutdown safety, and one shared deadline in `server/tests/unit/analytics/AnalyticsLifecycleTest.java`
- [X] T027 [P] [US3] Add permanent-store-failure and caller-interruption tests proving forced worker interruption, `FORCED`/`FAILED` terminal state, `SHUTDOWN_FORCED`, and absence of `SHUTDOWN_COMPLETED` in `server/tests/unit/analytics/AnalyticsForcedShutdownTest.java`
- [X] T028 [P] [US3] Add first-failure-wins tests for unexpected Window, Ranking, and Persistence worker failures, peer interruption, later-ingestion rejection, stage identification, and continued latest-committed reads in `server/tests/unit/analytics/AnalyticsWorkerFailureTest.java`
- [X] T029 [P] [US3] Add JSON-lines schema, UTC timestamp, required event-field, escaping, credential/SQL/stack-trace/basket-data redaction, high-water, and exactly-one-terminal-outcome tests in `server/tests/unit/analytics/AnalyticsLogTest.java`
- [X] T030 [P] [US3] Add composition-root tests for `ANALYTICS_SHUTDOWN_TIMEOUT_MS`, the 5,000 ms default, invalid-value rejection, and shutdown-hook delegation in `server/tests/architecture/CompositionRootTest.java`

### Implementation for User Story 3

- [X] T031 [US3] Implement `NEW`, `RUNNING`, `DRAINING`, `TERMINATED`, `FAILED`, and `FORCED` lifecycle transitions, idempotent close, post-close/unhealthy rejection, and exactly one ingress end marker under the admission lock in `server/src/analytics/AnalyticsService.java`
- [X] T032 [US3] Complete stage-ordered end propagation so Window drains earlier scans before forwarding, Ranking drains earlier snapshots before forwarding, and Persistence exits only after earlier ranked windows commit in `server/src/analytics/WindowFilter.java`, `server/src/analytics/RankingFilter.java`, and `server/src/analytics/PersistenceFilter.java`
- [X] T033 [US3] Join all three workers against one absolute configurable deadline, interrupt them on timeout/caller interruption, and never report forced termination as a clean drain in `server/src/analytics/AnalyticsService.java`
- [X] T034 [US3] Wrap all workers with first-terminal-failure coordination that closes ingestion, records the failed stage and sanitized cause, interrupts peers, and preserves committed-result queries in `server/src/analytics/AnalyticsService.java`
- [X] T035 [US3] Emit `INGESTION_REJECTED`, `DRAIN_STARTED`, `SHUTDOWN_COMPLETED`, `SHUTDOWN_FORCED`, and first-only `WORKER_FAILED` records with all contract-required fields through `server/src/analytics/AnalyticsLog.java`
- [X] T036 [US3] Read `ANALYTICS_SHUTDOWN_TIMEOUT_MS`, default it to 5,000 ms, inject it into the analytics facade, and retain shutdown-hook ordering in `server/src/api/Main.java`

**Checkpoint**: User Story 3 drains successfully or terminates with an explicit, bounded, machine-readable failure and remains restart-safe.

---

## Phase 6: User Story 4 - Preserve External Compatibility and Evidence (Priority: P2)

**Goal**: Demonstrate that the unchanged OpenAPI contract, database baseline, and load client still work and retain one freshly reset default report plus one freshly reset stress report.

**Independent Test**: Run all contract checks and the unmodified load client against fresh server/database state for 10 stations × 60 seconds and 100 stations × 120 seconds; validate inventory/popular-window invariants and retain exactly the two selected timestamped JSON reports for this iteration.

### Tests and Evidence for User Story 4

- [X] T037 [P] [US4] Extend popular-items contract coverage for the unchanged fields, empty response, committed-only visibility, limit default/cap, negative/non-integer 400 responses, unsupported-method 405, and unmatched-path 404 in `server/tests/contract/PopularItemsEndpointTest.java`
- [X] T038 [P] [US4] Extend live controlled-window coverage for `[1,1000]`, `[501,1500]`, concurrent exact membership, deterministic ranks, and no visibility after a failed rank insert in `server/tests/integration/WindowBoundaryAccuracyTest.java`
- [X] T039 [P] [US4] Strengthen the compatibility gate so `spec/self-checkout-openapi.yaml`, `db/init.sql`, and all sources under `load-client/src/` are treated as immutable external inputs in `server/tests/architecture/PipelineArchitectureTest.java`
- [X] T040 [P] [US4] Document the Window → Ranking → Persistence stage purposes, order, `LinkedBlockingQueue` connections, lifecycle, retry behavior, and unchanged external contract in `server/README.md`
- [X] T041 [US4] Run `server/build.sh`, `server/build-tests.sh`, and `server/run-tests.sh` and save the zero-failure automated gate output in `load-client/reports/pipeline-architecture/automated-tests.txt`
- [X] T042 [US4] Reset MySQL with `db/init-db.ps1`, start a fresh server, run the live contract/integration suite with zero skips, and save its output in `load-client/reports/pipeline-architecture/live-contract-tests.txt`
- [X] T043 [US4] After a fresh database reset and server restart, run the unchanged load client with 10 stations for 60 seconds and retain the selected timestamped JSON unchanged in `load-client/reports/pipeline-architecture/`
- [X] T044 [US4] Run `db/validate-invariants.sql` after the default workload and save all `PASS` output plus relevant structured backlog/retry/shutdown records in `load-client/reports/pipeline-architecture/default-invariants-and-logs.txt`
- [X] T045 [US4] After another fresh database reset and server restart, run the unchanged load client with 100 stations for 120 seconds and retain the distinct selected timestamped JSON unchanged in `load-client/reports/pipeline-architecture/`
- [X] T046 [US4] Run `db/validate-invariants.sql` after the stress workload and save all `PASS` output plus relevant structured backlog/retry/shutdown records in `load-client/reports/pipeline-architecture/stress-invariants-and-logs.txt`
- [X] T047 [US4] Record the exact two selected JSON filenames, reset/restart provenance, settings, invariant results, and p95/p99 comparison in `load-client/reports/pipeline-architecture/README.md`, and verify that directory contains exactly one selected default JSON and one selected stress JSON

**Checkpoint**: User Story 4 proves compatibility and retains reproducible submission evidence without modifying the OpenAPI, schema baseline, or client.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Apply final quality gates across the complete feature.

- [X] T048 [P] Add cross-cutting regression assertions for log sanitization and absence of secrets, raw SQL, stack traces, or basket data in `server/tests/unit/analytics/AnalyticsLogTest.java`
- [X] T049 Run the full automated test suite from `server/run-tests.sh` and resolve all unit, architecture, database, contract, and integration regressions in the files responsible for each failure
- [X] T050 Execute every applicable step in `specs/003-pipeline-analytics/quickstart.md` and record any environment-specific command clarification without weakening its zero-skip final validation requirements
- [X] T051 Verify `spec/self-checkout-openapi.yaml`, `db/init.sql`, and `load-client/src/` have no feature changes; confirm all selected evidence is reproducible; and complete the compliance checklist in `specs/003-pipeline-analytics/checklists/requirements.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies; T001 and T002 can start immediately.
- **Foundational (Phase 2)**: Depends on Setup and blocks all user-story implementation.
- **User Story 1 (Phase 3)**: Depends on Foundational and establishes the minimum end-to-end pipeline.
- **User Story 2 (Phase 4)**: Depends on User Story 1's working queues/filters; its tests can be prepared after Foundational while US1 implementation proceeds.
- **User Story 3 (Phase 5)**: Depends on User Story 1's workers and pipes; its tests can be prepared after Foundational, and its lifecycle implementation can proceed in parallel with US2 after US1.
- **User Story 4 (Phase 6)**: Contract-test and documentation tasks can start after Foundational; live/load evidence depends on the selected US1–US3 scope being complete.
- **Polish (Phase 7)**: Depends on all stories selected for delivery.

### User Story Dependency Graph

```text
Setup → Foundational → US1 (accurate three-stage pipeline / MVP)
                          ├──→ US2 (responsive checkout + ordered retry)
                          └──→ US3 (bounded lifecycle + failure visibility)
                                  \
                         US2 ──────┴──→ US4 (compatibility + load evidence)
                                           └──→ Polish
```

### Within Each User Story

- Write the story's tests first and confirm they fail for the intended missing behavior.
- Implement immutable messages and pure stage behavior before facade coordination.
- Keep queue communication ahead of persistence/lifecycle integration.
- Complete the story's independent test before advancing to its checkpoint.
- Do not begin US4 live/load evidence until all intended implementation stories pass automated gates.

### Parallel Opportunities

- T001 and T002 can run in parallel.
- T003 and T004 can run in parallel; T005 can proceed independently once its required behavior is understood.
- US1 test tasks T006–T010 can run in parallel; T011 and T012 can run in parallel.
- US2 test tasks T016–T020 can run in parallel; T024 and T025 can run in parallel with analytics-only T021–T023.
- US3 test tasks T026–T030 can run in parallel; T035 and T036 can run in parallel with lifecycle coordination work where their files do not overlap.
- US4 tasks T037–T040 can run in parallel; the reset/load evidence tasks T042–T047 must remain sequential to preserve fresh-state provenance.

---

## Parallel Example: User Story 1

```text
Task T006: Test immutable pipeline records in server/tests/unit/analytics/PipelineMessageTest.java
Task T007: Test window boundaries in server/tests/unit/analytics/WindowFilterTest.java
Task T008: Test deterministic ranking in server/tests/unit/analytics/RankingFilterTest.java
Task T010: Test pipeline structure in server/tests/architecture/PipelineArchitectureTest.java

Then in parallel:
Task T011: Implement server/src/analytics/WindowFilter.java
Task T012: Implement server/src/analytics/RankingFilter.java
```

## Parallel Example: User Story 2

```text
Task T016: Test persistence retry in server/tests/unit/analytics/PersistenceFilterTest.java
Task T017: Test concurrent admission in server/tests/unit/analytics/ConcurrentAnalyticsIngestionTest.java
Task T018: Test checkout isolation in server/tests/unit/transaction/TransactionServiceTest.java
Task T019: Test atomic JDBC behavior in server/tests/database/JdbcPopularWindowStoreTest.java
Task T020: Test end-to-end checkout isolation in server/tests/integration/AnalyticsCheckoutIsolationTest.java
```

## Parallel Example: User Story 3

```text
Task T026: Test clean lifecycle in server/tests/unit/analytics/AnalyticsLifecycleTest.java
Task T027: Test forced shutdown in server/tests/unit/analytics/AnalyticsForcedShutdownTest.java
Task T028: Test worker failure in server/tests/unit/analytics/AnalyticsWorkerFailureTest.java
Task T029: Test JSON logs in server/tests/unit/analytics/AnalyticsLogTest.java
Task T030: Test timeout composition in server/tests/architecture/CompositionRootTest.java
```

## Parallel Example: User Story 4

```text
Task T037: Extend server/tests/contract/PopularItemsEndpointTest.java
Task T038: Extend server/tests/integration/WindowBoundaryAccuracyTest.java
Task T039: Add protected-input checks in server/tests/architecture/PipelineArchitectureTest.java
Task T040: Document the architecture in server/README.md
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Setup and Foundational phases.
2. Write and fail T006–T010.
3. Implement T011–T015.
4. Run the US1 unit and architecture tests.
5. Stop and demonstrate exact committed windows and rankings through the unchanged endpoint.

### Incremental Delivery

1. **US1**: Accurate pipeline → independently test exact windows/ranks → MVP.
2. **US2**: Add concurrency and retry guarantees → independently test checkout isolation and recovery.
3. **US3**: Add bounded lifecycle/failure behavior → independently test drain, force, restart, and logs.
4. **US4**: Run unchanged contracts/client → capture fresh default and stress evidence.
5. **Polish**: Run the full quickstart and governance checks.

### Parallel Team Strategy

After the shared foundation and US1 pipeline are complete:

- Developer A can execute US2 retry/concurrency work.
- Developer B can execute US3 lifecycle/observability work.
- Developer C can prepare US4 contract tests/documentation, then run evidence only after A and B finish.

---

## Notes

- `[P]` means different files or otherwise independent work; tasks touching the same file remain ordered.
- Story labels provide requirements traceability and are omitted only from Setup, Foundational, and Polish.
- No task authorizes changes to `spec/self-checkout-openapi.yaml`, `db/init.sql`, or `load-client/src/`.
- Queue depth is diagnostic only and must never become an accounting invariant.
- Each load run requires both a fresh database reset and a fresh server process.
- Commit after each task or coherent task group, and validate at every story checkpoint.

## Phase 8: Convergence

- [ ] T052 Remove the plain-text analytics sink failure emitted by `server/src/transaction/TransactionService.java`, keep ingestion rejection and pipeline failure reporting owned by the structured analytics logger, and add regression coverage proving no unstructured duplicate is written per FR-016 (contradicts)
- [ ] T053 Add independent increasing high-water `BACKLOG` records and deterministic slow-stage tests for the Window-to-Ranking and Ranking-to-Persistence queues so sustained ranking or persistence delay identifies the affected stage and queue depth per FR-016 and SC-010 (partial)
- [ ] T054 Re-run the current full automated and live validation gates with all prerequisites available, resolve any failures or skips, and refresh `load-client/reports/pipeline-architecture/automated-tests.txt` so its zero-failure evidence covers the currently discovered suite per SC-006 and T041/T049 (partial)
