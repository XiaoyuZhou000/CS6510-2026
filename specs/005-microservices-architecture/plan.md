# Implementation Plan: Self-Checkout Microservices with Database per Service, gRPC Contracts, and Recoverable Saga Checkout

**Branch**: `microservices-architecture` | **Date**: 2026-10-20 (revised for the memory-first performance architecture) | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/005-microservices-architecture/spec.md`. The setup script derived the name `005-microservices-architecture` from the feature directory; the checked-out Git branch and the spec header are `microservices-architecture`.

## Summary

Evolve the existing three-process, one-database service-based system into three independently deployable microservices, each owning its own MySQL schema and credentials. The design is memory-first, aiming to maximize the unchanged Load Client's `totalTransactions` without weakening any correctness, durability, or recovery guarantee (R18).

- **Transaction Service** serves Scan and Complete from in-memory projections: a catalog snapshot backed by a durable, versioned replica, and a transaction state cache. Every write is a guarded conditional UPDATE, so correctness never depends on the cache (R19).
  - **Scan** commits exactly one guarded transaction-row UPDATE plus one `accepted_scan` row. That row is the basket record and the outbox together. There is no read and no remote call.
  - **Complete** is a Transaction-orchestrated Saga with forward recovery:
    - T1 is a guarded begin-attempt with its line snapshot (2 statements and a commit).
    - Inventory's single batched, atomic, idempotent `ApplyCheckout` follows (3 statements and a commit).
    - T3 records completion (2 statements and a commit) and returns the receipt with no reads.
  - Uncertain outcomes stay pending, are reported as `500 INTERNAL_ERROR`, and are settled by a recovery worker that replays the same `(transaction_id, attempt_number)`. Concurrent requests wait for a bounded time without holding connections or locks.
- **Inventory Service** never caches stock. It caches catalog metadata and keeps a reserved, directly tested stock-restoration operation.
- **Analytics Service** ingests batched events and assigns positions in the same commit, replacing the per-event materializer. It keeps the 1,000-scan window in memory and publishes complete windows only after persisting them.
- **Tuning and measurement**: JDBC sessions stay in manual-commit mode at READ COMMITTED with Connector/J statement caching. A dependency-free metrics package and a benchmark harness measure every cost. Exact correctness tests and measured statement-budget guards catch regressions, and checkpoints CP0–CP4 judge every change by client-observed `totalTransactions`.
- **Unchanged**: the gateway, the seven REST routes, the handlers, the Load Client, and the MySQL durability settings.

## Technical Context

**Language/Version**: Java 21 (`javac --release 21`); Protocol Buffers 3; SQL for MySQL 8.0+ (8.4 locally); Bash build and run scripts; PowerShell database and benchmark tooling.

**Primary Dependencies**:
- JDK `HttpServer` for the unchanged REST handlers.
- JDBC with MySQL Connector/J 9.0.0, with tuned URL flags (R20).
- gRPC Java (`grpc-netty-shaded`, `grpc-protobuf`, `grpc-stub`; `grpc-inprocess` for tests) and `protobuf-java`, pinned through the gRPC BOM in a Maven-built contracts module (R1).
- JUnit Platform Console 1.11.0.
- Nginx as the gateway, unchanged.
- No caching or messaging infrastructure; Redis and Kafka are excluded (R23).

**Storage**:
- One local MySQL server (`127.0.0.1:3307`) with three schemas, `cs6510_transaction`, `cs6510_inventory`, and `cs6510_analytics`, each reachable only by its own user.
- Durable tables hold only business state, recovery, idempotency, delivery, and visible results.
- In-memory projections are rebuilt from those tables ([data-model.md](data-model.md#in-memory-projections-non-durable-r18-r19)).

**Testing**:
- Adapted existing suites.
- gRPC contract tests.
- Saga and delivery fault injection.
- Isolation tests.
- Direct restoration tests.
- Performance regression tests (`tests/performance`, R22): exact correctness checks, measured statement and commit budgets, no held connections, and cache staleness, eviction, collision, ambiguity, multi-instance, and restart-rebuild tests.
- Per-service and cross-service invariant validation.
- Benchmark checkpoints CP0–CP4 with the unchanged Load Client.

**Target Platform**: Windows 10 with Git Bash and PowerShell, with Unix also supported. Transaction on HTTP 8081; Inventory on HTTP 8082 and gRPC 9092; Analytics on HTTP 8083 and gRPC 9093; Nginx on 8080; MySQL on 3307.

**Project Type**: Distributed web services: three microservices behind a static HTTP gateway.

**Performance Goals**:
- **Primary**: maximize the Load Client's `totalTransactions` in the default (10 stations, 60 s) and stress (100 stations, 120 s) workloads.
- **Baseline**: the retained service-based results are about 4,750 for the default run and about 14,164 for the stress run. A same-machine CP0 is re-measured before any change (R22).
- **Also measured**: server-side Scan and Complete p95/p99, JDBC pool wait, Inventory lock contention, gRPC latency, Analytics backlog, and visibility delay (R21).
- **Hard outcomes**: zero errors and zero background completions in the default run; Analytics visibility under 5 s at p99 when healthy.
- **No numerical improvement target**: the mandatory commit-count increase is quantified in R18.

**Constraints**:
- Correctness and durability first. No acknowledgement before the local commit.
- Server durability settings unchanged: `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, binary logging on.
- Zero remote calls and zero reads on the warm Scan path.
- No connection, lock, or transaction held across RPCs or waits.
- Complete responds within an 8 s budget.
- Idempotent internal operations under stable identities.
- Per-service credentials.
- REST contract and Load Client byte-identical.
- Maximum pool sizes total 122, below MySQL's default 151.

**Scale/Scope**:
- 2,000 products at 10,000 units each.
- Up to about 280,000 accepted scans per stress run, based on the baseline.
- At most 100 concurrent baskets.
- Three services, three schemas, two internal gRPC services, seven public routes, two committed JSON reports.

## Constitution Check

*GATE: Passed before Phase 0 research and re-checked after Phase 1 design, including after the memory-first revision.*

| Gate (Constitution v5.1.0) | Plan evidence | Result |
|---|---|---|
| **I. Independent microservices and domain ownership** | Three artifacts with their own entry points, ports, configuration, pools, and schemas. `common.jar` (utilities and metrics) and `contracts.jar` (generated stubs) carry no business logic. Architecture tests confine gRPC and `contracts.*` types to `rpc` packages and composition roots, and forbid importing a peer's packages. | PASS |
| **II. Database per service** | Three schemas and three users, each granted only its own schema. No cross-schema access. Replicated data (product replica, Analytics names) has documented sources, synchronization, and consistency. Every in-memory projection accelerates access only, is updated after commit, is rebuildable after restart, and never solely holds acknowledged state ([data-model.md](data-model.md#in-memory-projections-non-durable-r18-r19)). In-process locks, caches, the settlement registry, and the recovery schedule are optimizations only. Durability, idempotency, inventory correctness, and cross-instance consistency rest on durable state, database guards, and Inventory's idempotency. Tests cover crashes, restarts, and concurrent instances of every service: Transaction (R7, R19), Inventory (R8), the relay (R10), and Analytics (R11). | PASS |
| **III. gRPC and Protocol Buffers** | Version-controlled `.proto` files. Documented validation, statuses, and outcomes. A deadline on every call. Application-level retries reuse identities. Reused channels. One batched `ApplyCheckout` per attempt. No RPC on Scan ([grpc-internal-contracts.md](contracts/grpc-internal-contracts.md)). | PASS |
| **IV. External REST API and backward compatibility** | Routes, handlers, and serialization unchanged. Gateway routing unchanged. Internal conditions map to `500 INTERNAL_ERROR`, and an unreachable owner to 502 ([public-http-and-routing.md](contracts/public-http-and-routing.md)). Byte-hash checks of the OpenAPI document and the Load Client are kept. | PASS |
| **V. Distributed transactions and Saga coordination** | Orchestrated Saga with progress persisted before and after the remote step ([checkout-saga.md](contracts/checkout-saga.md)). **Steps**: T1 is compensated by reopening the basket; Inventory's deduction is the pivot, with a reserved, durable, idempotent, directly tested restoration; T3 runs after the pivot under forward recovery and is retried until it commits under its idempotency rule, as Principle V permits. **Inventory**: atomic operations, idempotent per checkout attempt, with durable keys and at most one effective deduction. **Consistency**: judged once the Saga settles. A repeated completion returns 409 (reading for item 4). | PASS |
| **VI. Checkout and inventory consistency** | Scan never touches stock. Prices are captured on the line and stored in every `accepted_scan` row. Stock is never cached; deductions are SKU-ordered, locked, atomic, and checked by `CHECK ≥ 0`. A unique `applied_transaction_id` enforces at most one effective deduction. Guarded writes prevent lost updates from stale caches. Settled invariants are validated. Pending attempts are durable, are never reported as success, and settle automatically. An attempt with a detected integrity fault stays pending with an alarm and is never settled on an assumed outcome. | PASS |
| **VII. Reliable event delivery and analytics accuracy** | The `accepted_scan` row is written in the Scan transaction. The push relay delivers at least once. Deduplicating ingestion assigns unique, gapless, immutable positions in the same commit. Window, Ranking, and Persistence stages are unchanged, with an in-memory window. Publication is atomic before visibility. Analytics outages never block Scan or Complete ([scan-event-delivery.md](contracts/scan-event-delivery.md)). | PASS |
| **VIII. Performance and operational simplicity** | No broker, cache server, or mesh. Simple, measured optimizations come first (R18, R20, R22), and speculative ones are deferred with entry criteria (R23). Pools stay within the MySQL connection budget. Every service is documented ([service-deployment.md](contracts/service-deployment.md)). Results include throughput, error rate, and tail latency against prior iterations. | PASS |
| **IX. Verification and benchmark evidence** | The constitution's required tests plus FR-056, plus deterministic performance regression tests. End-to-end reopen compensation and direct restoration tests are reported separately, as Principle IX requires. Two genuine reports at CP4 with durable completion counts and recorded durability settings. | PASS |
| **Architectural constraints** | Incremental evolution of `server/` (code-reuse map below). Endpoint ownership unchanged. Per-service reset and seed. The 2,000 × 10,000 baseline preserved. | PASS |
| **Workflow and quality gates** | Deadlines, retry and idempotency keys, Saga states and recovery, replica consistency, event ordering and checkpoints, and visibility delay are defined. Saga steps are traced with their compensations or forward-recovery rules. Every *Planning Obligations* item is traced (table below). | PASS |

**Constitution v5.1.0** (adopted 2026-10-20 from the reviewed [amendment proposal](constitution-amendment-proposal.md)):
- **Explicit rules**: under v5.0.0, spec *Constitution Alignment* item 1 was a documented deviation, and items 2, 3, and 5 were readings. v5.1.0 states all four as explicit rules.
- **Reading**: only item 4 remains a reading: a repeated completion returns 409 `TRANSACTION_NOT_OPEN`.
- **New Principle II rule**: in-process mechanisms are never the sole authority (spec item 7). This plan satisfies it through R7 and R19 and the multi-instance tests.

**Post-design re-check**: [data-model.md](data-model.md), the five [contracts](contracts/), the [`.proto` baselines](contracts/proto/), and [quickstart.md](quickstart.md) preserve every gate under v5.1.0, including after the memory-first revision and the approved implementation constraints. No unresolved clarification remains, and no deviation remains.

## Planning Obligations Traceability

| Spec obligation | Where it is resolved |
|---|---|
| 1. Product and price replica synchronization | R4 (durable replica plus in-memory snapshot, 30 s version check); [data-model.md](data-model.md#product_replica-and-product_replica_meta) |
| 2. Saga recovery | R5–R9, R19; [checkout-saga.md](contracts/checkout-saga.md); direct deduction chosen over hold and confirm |
| 3. Analytics ordering and delivery | R10–R12; [scan-event-delivery.md](contracts/scan-event-delivery.md): positions assigned at ingestion |
| 4. Independent database resets and seeding | R13; [service-deployment.md](contracts/service-deployment.md#database-bootstrap-reset-and-seed-r13) |
| 5. Remaining decisions | Restoration: no operational use (R8). Low stock: unchanged reading. Constitution Check above. Gateway unchanged (R14). Structure and packaging below (R1). |

## Phase 0: Research

All decisions, with their rationale and the alternatives considered, are in [research.md](research.md):
- R18: memory-first model and cost table, governing the rest.
- R1–R3: build, gRPC conventions, schemas.
- R4: catalog replica and cache.
- R5: Saga statements.
- R19: transaction state cache with guarded writes.
- R6–R7: budgets and waiting.
- R8: Inventory apply in 4 round trips, plus the catalog cache.
- R9: background-completion accounting.
- R10: `accepted_scan` as scan log, basket, and outbox.
- R11–R12: ingestion assigns positions; name cache.
- R13: resets and validation.
- R20: JDBC session and driver tuning.
- R14, R16: errors and gateway; capacity.
- R21: metrics and harness.
- R22: checkpoints and regression tests.
- R15, R17: testing; constitution treatment.
- R23: deferred options with entry criteria.

## Phase 1: Design and Contracts

| Artifact | Content |
|---|---|
| [data-model.md](data-model.md) | Durable tables per schema; in-memory projections with their sources of truth and rebuild rules; state machines; validation rules |
| [contracts/public-http-and-routing.md](contracts/public-http-and-routing.md) | Preserved routes, gateway routing, internal conditions mapped to 500 and 502 |
| [contracts/grpc-internal-contracts.md](contracts/grpc-internal-contracts.md) and [contracts/proto/](contracts/proto/) | `InventoryService` and `AnalyticsIngestService`: messages, validation, statuses, deadlines, retries, compatibility, statement budgets |
| [contracts/checkout-saga.md](contracts/checkout-saga.md) | Guarded-write steps, response mapping, recovery worker, bounded waiting, crash matrix, background completions, fault hooks |
| [contracts/scan-event-delivery.md](contracts/scan-event-delivery.md) | `accepted_scan` write, relay, ingestion with positions, ordering rule, publication, visibility delay, failure behavior |
| [contracts/service-deployment.md](contracts/service-deployment.md) | Units, build, configuration (cache, metrics, JDBC session), bootstrap, reset, seed, validation, operational status, benchmark harness |
| [quickstart.md](quickstart.md) | Build, bootstrap, smoke tests, suites, scenarios A–H, checkpoints and official evidence |

### Key flows and statement budgets (R22)

The round trips below are design budgets. Tests enforce them as measured ceilings, which may rise only with A/B evidence of higher `totalTransactions`. The ordering and atomicity rules are separate, exact tests that never relax: acknowledgement after commit, shared commits, no remote call on Scan, and no resources held across remote calls or waits.

| Flow | Warm-path work | Round trips |
|---|---|---|
| Start | INSERT `transaction` with service timestamps, commit; cache entry created | 2 |
| Scan | Cache: status, pending, line price, catalog snapshot. Guarded UPDATE of the transaction row, INSERT `accepted_scan`, commit. Cache updated after commit. | 3 |
| Complete | T1 (guarded UPDATE + INSERT attempt + commit), then `ApplyCheckout` (Inventory: locking SELECT + INSERT outcome + CASE UPDATE + commit), then T3 (guarded UPDATE + UPDATE attempt + commit); receipt from cache | 3 + 1 RPC (4 in Inventory) + 3 |
| Relay, per batch of up to 500 | SELECT undelivered + `DeliverScanEvents` + batched UPDATE + commit | 3 + 1 RPC |
| Analytics ingestion, per batch | Lock sequence + duplicate SELECT + multi-row INSERT with positions (+ name upsert only for new names) + sequence UPDATE + commit | 5–6 |
| Window publication, every 500 positions | Header + ranks + checkpoint + commit (unchanged) | 4 |

## Performance Strategy

**Cost model (R18)**:
- Each durable commit costs about 7.5 ms on the benchmark machine and is mandatory wherever it remains.
- Commits on the critical path per basket are N + 4 (Start, N Scans, T1, Inventory, T3), against N + 2 in the single-database design. The two extra commits are required by C-II (Inventory has its own commit) and C-V (T1 is persisted before the remote step). Everything else is optional cost and is minimized.

**Optimizations**, ordered by expected impact per unit of complexity. Each is verified by a statement-budget test and a checkpoint measurement:
1. **Memory-first Scan** (R19, R10): catalog snapshot plus transaction state cache plus guarded write, with no line table. 10 round trips become 3, and connection hold time shrinks.
2. **JDBC sessions** (R20): manual-commit pooled connections at READ COMMITTED, statement caching, and batched rewrites. These remove the autocommit and isolation toggles.
3. **Inventory apply in 4 round trips** (R8): one locking SELECT, one outcome INSERT, one CASE UPDATE, no child table. Catalog metadata is cached; stock never is.
4. **Analytics merged ingestion** (R11, R12): one commit per batch with positions, replacing one UPDATE per event plus a separate commit. Redundant name upserts are skipped. This reduces load on the MySQL server shared with Transaction.
5. **Receipt and timestamps from memory** (R5): no read follows Start or T3.
6. **Evidence-based tuning at CP3** (R16, R20): pool sizes, server prepared statements, and cache on or off.

**Measurement (R21)**: `PERF_METRICS` histograms and counters cover server-side Scan and Complete latency, `TransactionLocks` wait, contention, and stripe collisions, pool borrow wait and hold time, Inventory locking-SELECT time and retries, `ApplyCheckout` latency and statuses, and relay and ingestion batches, backlog, and visibility delay. The harness adds `SHOW GLOBAL STATUS` row-lock counters and the recorded durability settings.

**Checkpoints (R22)**:
- **CP0**: baseline before any change, used for attribution.
- **CP1**: core checkout path, for attribution.
- **CP2**: full system, for attribution. It includes the cost of recovery and waiting, and idle reads.
- **CP3**: tuning A/B with interleaved, repeated pairs under equivalent workloads.
- **CP4**: the final comparison against fresh, interleaved CP0 runs, and the official default and stress runs, designated in advance.

A `totalTransactions` drop of more than 5% against the previous checkpoint is investigated before continuing. Decisions favor `totalTransactions` first, then tail latency, then resource use. Every checkpoint reports the actual client-observed `totalTransactions` against the CP0 baseline. An improvement is never claimed from fewer statements or lower latency alone.

**Regression tests (R22)**:
- **Exact correctness and durability tests**: acknowledgement after commit, shared commits, no remote call on Scan, no held connections or row locks during RPCs or waits, and Analytics acknowledgement after commit.
- **Measured statement and commit budgets**: ceilings that are revisable with A/B evidence.
- **Cache correctness**: stale entries, eviction races, ambiguous commits, two instances, and restart rebuilds.
- **Architecture checks**: forbid Redis and Kafka, durability changes, and missing JDBC flags.

**Deferred options (R23)**: split hot-SKU stock, an in-memory Analytics feed, a watermark relay, application-level commit batching, and new infrastructure. Each is adopted only when its entry criterion is met by measured evidence.

## Incremental Evolution Path

Each stage leaves a buildable tree. `/speckit-tasks` turns these stages into tasks.

0. **CP0 baseline** on the current implementation with the existing `server/benchmark-scan.ps1`.
1. **Contracts, build, common**:
   - `server/contracts` (Maven) with the two `.proto` files, and script classpaths.
   - The common `rpc` package and the `metrics` package, which replaces `ScanProfiler`.
   - The common JDBC unit-of-work helper with manual-commit, READ COMMITTED pools, and URL flags.
   - Test-only hooks on the pool for counting decorators.
   - Architecture gates.
2. **Inventory**: schema, seed, and `catalog_meta`; catalog cache; `JdbcCheckoutOperationStore` (4-round-trip apply, outcome, restore); `CheckoutStockService`; `InventoryGrpcService`.
3. **Transaction**:
   - schema;
   - product replica store, in-memory snapshot, and refresher;
   - transaction state cache with guarded writes, and striped per-transaction locks;
   - the `accepted_scan` Scan path;
   - `JdbcCheckoutAttemptStore`, `CheckoutOrchestrator`, `SettlementRegistry`, and `CheckoutRecoveryWorker`;
   - `GrpcInventoryGateway`;
   - removal of all catalog, line-table, and inventory SQL.

   Then the CP1 checkpoint.
4. **Analytics**: schema; `JdbcScanIngestStore` (positions at ingestion, name map); `AnalyticsIngestGrpcService`; materializer removed; `JdbcPopularWindowStore` joins `product_name`.
5. **Relay**: `ScanEventRelay` and `GrpcAnalyticsIngestClient`. Then the CP2 checkpoint.
6. **Tooling and documentation**: bootstrap, reset, seed, and validate scripts; `server/benchmark-microservices.ps1`; retire the single-database scripts; update `server/README.md`.
7. **Tests**: per-schema support; adapt existing suites; add the `rpc`, Saga, fault, waiting, delivery, isolation, direct-restoration, and performance suites.
8. **CP3 tuning A/B** with interleaved, repeated pairs. Then **CP4**: the interleaved final comparison with fresh CP0 runs, the official evidence designated in advance, and the comparison README.

### Code reuse map

| Existing code | Plan |
|---|---|
| All four HTTP handlers, `ApiErrorMapper` | Reused unchanged; the mapping test covers the new internal failure types (all 500) |
| `TransactionService` | Adapted: Start, Scan, and Complete go through the state cache, guarded writes, and the orchestrator; bounded waiting |
| `JdbcTransactionStore` | Adapted: guarded UPDATEs, service-supplied timestamps, cold-path load (row plus grouped `accepted_scan`) |
| `JdbcAcceptedScanStore` | Replaced by `JdbcScanStore`: a 2-statement Scan transaction keeping the existing deadlock-retry and ambiguous-commit rules |
| `transaction.database.JdbcCatalogStore`, `JdbcCheckoutCompletionStore`, the `transaction_line` usage | Removed: replaced by the replica store, `JdbcCheckoutAttemptStore`, and derived lines |
| `ScanProfiler` | Replaced by `common/metrics`; its phase names carry over as metric names |
| Inventory `StoreQueryService`, `JdbcCatalogStore`, `JdbcInventoryStore`, `LowStockThresholds` | Reused; `/items` is served through the catalog cache, and low stock still reads the database |
| `AnalyticsService`, `AcceptedScanPoller`, `WindowFilter`, `RankingFilter`, `PersistenceFilter`, `PipelineMessage`, `WindowMath`, `AnalyticsLog` | Reused unchanged |
| `JdbcAcceptedScanReader` | Reused; its `position`/`sku` reads stay valid |
| `JdbcScanMaterializer` | Removed: its algorithm moves into `JdbcScanIngestStore`, and its crash-boundary tests become ingestion tests |
| `JdbcPopularWindowStore` | Adapted: `readRanks` joins `product_name` |
| `ConnectionPool`, `StoreFailure`, `HttpSupport`, `Json`, `ApiErrors` | Reused. `ConnectionPool` gains manual-commit and isolation setup, borrow-wait metrics, and a test-only connection decorator. `common` gains the `rpc` and `metrics` packages. |
| `TransactionMain`, `InventoryMain`, `AnalyticsMain` | Adapted: configuration, gRPC servers and clients, background workers, metrics, ordered shutdown |
| `db/init.sql`, `db/init-db.ps1`, `db/validate-invariants.sql`, `db/migrate-sql-materializer.sql` | Retired, kept in Git history; replaced by the per-service scripts |
| `server/benchmark-scan.ps1`, `server/benchmark-materializer.ps1` | `benchmark-scan.ps1` is used once for CP0, then superseded by `benchmark-microservices.ps1`. `benchmark-materializer.ps1` is retired. |
| `deploy/nginx.conf` | Unchanged |
| `server/tests/**` | Compatible tests kept unchanged; the rest migrated without weaker assertions (FR-057), tracked per method in `server/tests/COVERAGE-LEDGER.md` and verified at each story's parity gate. Support is split per schema, and stock is read through Inventory's credentials. |

## Project Structure

### Documentation (this feature)

```text
specs/005-microservices-architecture/
├── spec.md
├── constitution-amendment-proposal.md
├── plan.md                         # this file
├── research.md                     # Phase 0
├── data-model.md                   # Phase 1
├── quickstart.md                   # Phase 1
├── contracts/                      # Phase 1
│   ├── public-http-and-routing.md
│   ├── grpc-internal-contracts.md
│   ├── checkout-saga.md
│   ├── scan-event-delivery.md
│   ├── service-deployment.md
│   └── proto/
│       ├── inventory_service.proto
│       └── analytics_ingest.proto
├── checklists/requirements.md
└── tasks.md                        # Phase 2, produced by /speckit-tasks
```

### Source code (repository root)

```text
server/
├── contracts/                      # NEW Maven module: .proto → out/contracts.jar; runtime deps → lib/grpc/ (git-ignored)
│   ├── pom.xml
│   └── src/main/proto/selfcheckout/{inventory,analytics}/v1/*.proto
├── common/src/
│   ├── api/ (+ api/json/)          # unchanged HTTP/JSON utilities
│   ├── database/                   # ConnectionPool (manual-commit, READ COMMITTED, borrow metrics, test decorator), StoreFailure, unit-of-work helper
│   ├── rpc/                        # NEW: channel factory, gRPC server bootstrap, status classification, deadlines, fault-hook registry
│   └── metrics/                    # NEW: counters, latency histograms, periodic JSON summary (replaces ScanProfiler)
├── transaction/src/
│   ├── api/                        # TransactionHttpHandler, ApiErrorMapper (unchanged)
│   ├── transaction/                # TransactionService, TransactionStateCache, ProductCatalogReplica, CheckoutOrchestrator,
│   │                               # CheckoutRecoveryWorker, SettlementRegistry, ScanEventRelay, ports, TransactionMain
│   ├── database/                   # JdbcTransactionStore, JdbcScanStore, JdbcCheckoutAttemptStore, JdbcProductReplicaStore, JdbcRelayStore
│   └── rpc/                        # GrpcInventoryGateway, GrpcAnalyticsIngestClient
├── inventory/src/
│   ├── api/                        # CatalogHttpHandler, InventoryHttpHandler (unchanged)
│   ├── inventory/                  # StoreQueryService, CatalogCache, CheckoutStockService, InventoryMain
│   ├── database/                   # JdbcCatalogStore, JdbcInventoryStore, JdbcCheckoutOperationStore, JdbcCatalogMetaStore
│   └── rpc/                        # InventoryGrpcService
├── analytics/src/
│   ├── api/                        # AnalyticsHttpHandler (unchanged)
│   ├── analytics/                  # existing pipeline (unchanged) + ScanIngestionService, ProductNameMap, AnalyticsMain
│   ├── database/                   # JdbcScanIngestStore, JdbcAcceptedScanReader, JdbcPopularWindowStore
│   └── rpc/                        # AnalyticsIngestGrpcService
├── tests/
│   ├── COVERAGE-LEDGER.md          # NEW per-method record of legacy coverage and its verified replacements (FR-057)
│   ├── unit/ architecture/ database/ contract/ integration/   # existing: compatible tests kept, others migrated
│   ├── rpc/                        # NEW gRPC contract tests
│   ├── performance/                # NEW exact correctness tests, measured statement budgets (statement-budgets.properties),
│   │                               # held-resource, cache staleness, eviction, ambiguity, and rebuild tests
│   └── support/                    # per-schema disposable databases, process launchers, fault helpers, counting and faulting decorators
├── tests-legacy/                   # legacy tests parked by the task that broke them; deleted only after verified parity
├── lib/                            # Connector/J, JUnit (committed); grpc/ (Maven-resolved, git-ignored)
├── build.sh  build-tests.sh  run-tests.sh  run-transaction.sh  run-inventory.sh  run-analytics.sh
├── benchmark-microservices.ps1     # NEW harness (R21); benchmark-scan.ps1 kept for CP0 only
└── benchmark-compare.ps1           # NEW interleaved A/B runner with the R22 decision rule

db/
├── bootstrap.ps1  bootstrap.sql
├── reset-service.ps1  reset-all.ps1  validate-invariants.ps1
├── transaction/{schema.sql,validate.sql}
├── inventory/{schema.sql,seed.sql,validate.sql}
└── analytics/{schema.sql,validate.sql}

deploy/nginx.conf                   # unchanged gateway
spec/self-checkout-openapi.yaml     # unchanged, authoritative
load-client/                        # unchanged source; reports/microservices-architecture/ for checkpoints and evidence
```

**Structure Decision**:
- **Layering**: keep the three ownership roots with their `api`, business, and `database` layers, and add an `rpc` adapter layer per service.
- **Generated stubs**: built by the only Maven module, `contracts`.
- **Shared modules**: `common` gains `rpc`, `metrics`, and the JDBC unit-of-work helper.
- **Placement**: in-memory projections live in each service's business package, behind interfaces whose database counterparts remain the source of truth. Performance tests live beside the other suites and run in `run-tests.sh`.

## Complexity Tracking

> No constitution violation remains, so the violation table is empty.

**Resolved deviation**:
- **What it was**: under v5.0.0, the final step, T3 Record completion, a retriable local commit with no compensating action, was a documented deviation from Principle V.
- **Why it was needed**: once Inventory's atomic deduction (the pivot) applies, forward recovery must end COMPLETED (FR-030, FR-032). A compensating "un-complete" step would reverse a sale that the shopper may already have been told succeeded (FR-034, FR-038).
- **How it was resolved**: constitution v5.1.0, adopted 2026-10-20, permits exactly this. Steps after a forward-recovery pivot are retried to completion under an idempotency rule instead of being compensated.

**Non-violation complexity notes**:
- **Transaction state cache (R19)**: it adds cache maintenance and 4,096 striped per-transaction locks. The locks are independent of cache entries, so eviction cannot duplicate a lock, and stripe collisions can only cost time. All of these are process-local optimizations; correctness holds with several Transaction instances. It is justified by removing a locking read and every basket read from every Scan, about 7 round trips. Correctness rests on guarded writes, on confirming error responses against the database, and on reloading after any ambiguous commit, not on the cache. `TRANSACTION_CACHE_MAX_ENTRIES=0` disables it for A/B runs and as a fallback.
- **Maven contracts module (R1)**: a second build tool, used only for stub generation and dependency resolution. Constitution VIII limits runtime infrastructure, not build tooling.
- **Gateway**: Nginx remains the only infrastructure beyond the services and MySQL.
- **Deferred options**: listed in R23 with explicit entry criteria, so speculative complexity is not added without measured evidence.
