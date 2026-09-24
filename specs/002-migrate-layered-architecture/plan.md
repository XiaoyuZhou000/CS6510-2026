# Implementation Plan: Migrate to Layered Architecture

**Branch**: `002-migrate-layered-architecture` | **Date**: 2026-09-22 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/002-migrate-layered-architecture/spec.md`

## Summary

Refactor the existing Java 21 self-checkout server into four explicit in-process layers—API,
transaction, analytics, and database access—without changing the OpenAPI contract, load client,
database reset baseline, or synchronous behavior. HTTP handlers will depend on transaction and
analytics interfaces; those business layers will depend on database-access contracts rather than
JDBC classes. Atomic checkout completion moves behind one coarse database-access operation so the
status guard, line inserts, inventory decrements, and commit remain one MySQL transaction. The
existing in-memory baskets, catalog cache, analytics ring buffer, virtual-thread HTTP executor,
and ordered asynchronous checkpoints are retained.

## Technical Context

**Language/Version**: Java 21

**Primary Dependencies**: JDK `HttpServer`, JDBC, MySQL Connector/J 9.0.0; no application framework

**Storage**: MySQL 8.0+ for catalog, inventory, transaction, completed line, and popular-window
data; process memory for open baskets, immutable catalog cache, and active analytics window

**Testing**: JUnit Platform Console 1.11.0, existing contract/integration/unit suites, new
layer-isolation and dependency-rule tests, unchanged Java load client

**Target Platform**: Single-process server on any environment with JDK 21, Bash, and MySQL 8.0+;
current scripts support Windows through Git Bash/MSYS and Unix-like hosts

**Project Type**: HTTP web service with a separate command-line load client

**Performance Goals**: Under the 10-station workload, p95 start below 200 ms, scan below 100 ms,
and completion below 1 second; correctness takes priority over latency

**Constraints**: Fixed OpenAPI and unmodified load client; synchronous HTTP endpoints; stock and
completed-sale writes commit atomically; duplicate completion is idempotent; scans do not decrement
stock; analytics uses an exact 1,000-scan window every 500 scans; no direct database access outside
the database layer; no reverse or circular layer dependencies

**Scale/Scope**: 2,000 catalog items, 10,000 starting units per SKU, 1–20 items per basket,
10 stations for 60 seconds and 100 stations for 120 seconds, one deployable process

## Constitution Check

*GATE: Passed before Phase 0 research and re-checked after Phase 1 design.*

| Gate | Design evidence | Result |
|---|---|---|
| Inventory consistency | `CheckoutCompletionStore.completeAtomically` owns one JDBC transaction for the guarded status transition, line inserts, sorted conditional inventory decrements, and commit. The transaction layer returns a receipt only for `COMPLETED`. | PASS |
| Durable completion | The database operation commits before returning success; the API cannot create a success response from a failed or uncommitted outcome. Popular windows remain durable. | PASS |
| Contract fidelity | `spec/self-checkout-openapi.yaml` and `load-client/` remain unchanged. API handlers preserve routes, exact status codes, JSON fields, and synchronous responses. | PASS |
| Performance under concurrency | In-memory baskets/catalog, virtual threads, bounded JDBC pool, per-basket locking, and non-blocking analytics checkpoint submission remain. | PASS |
| Analytics accuracy | The analytics layer exclusively owns accepted-scan ingestion, exact 1,000/500 window math, deterministic top-10 ranking, ordered checkpoint execution, and latest-result behavior. | PASS |
| Testability and repeatability | Each business/API contract accepts fakes; database adapters have MySQL integration tests; the documented reset/restart protocol precedes both required runs. | PASS |
| Layer boundary integrity | Every source component is assigned below. HTTP handlers call only transaction/analytics contracts; business layers call database contracts; JDBC/SQL types remain under `database`. The API bootstrap only constructs objects and is excluded from request-path bypass checks. | PASS |
| Validation gate | Quickstart requires a fresh reset and server restart for each workload, two distinct client-generated reports, and invariant reconciliation. | PASS (design); execution occurs after implementation |

### Post-design re-check

Phase 1 introduces no additional layer or external dependency. Boundary records are owned by the
contract they describe, public error mapping remains in API, and database transactions do not leak
through a `Connection` or generic unit-of-work abstraction. The data model preserves all existing
constraints. No constitution exception is required.

## Project Structure

### Documentation (this feature)

```text
specs/002-migrate-layered-architecture/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── dependency-rules.md
│   ├── layer-interfaces.md
│   └── public-api-compatibility.md
└── tasks.md                         # created by the later $speckit-tasks workflow
```

### Source Code (repository root)

```text
server/
├── src/
│   ├── api/
│   │   ├── Main.java               # API-layer composition/bootstrap; construction only
│   │   ├── HttpSupport.java
│   │   ├── ApiErrorMapper.java
│   │   ├── CatalogHttpHandler.java
│   │   ├── TransactionHttpHandler.java
│   │   ├── InventoryHttpHandler.java
│   │   ├── AnalyticsHttpHandler.java
│   │   └── json/Json.java
│   ├── transaction/
│   │   ├── TransactionOperations.java
│   │   ├── CatalogOperations.java
│   │   ├── InventoryOperations.java
│   │   ├── AcceptedScanSink.java
│   │   ├── TransactionService.java
│   │   ├── StoreQueryService.java
│   │   ├── CatalogCache.java
│   │   ├── Basket.java
│   │   └── TransactionFailure.java
│   ├── analytics/
│   │   ├── AnalyticsOperations.java
│   │   ├── AnalyticsService.java
│   │   └── WindowMath.java
│   └── database/
│       ├── CatalogStore.java
│       ├── TransactionStore.java
│       ├── InventoryStore.java
│       ├── CheckoutCompletionStore.java
│       ├── PopularWindowStore.java
│       ├── ConnectionPool.java
│       ├── JdbcCatalogStore.java
│       ├── JdbcTransactionStore.java
│       ├── JdbcInventoryStore.java
│       ├── JdbcCheckoutCompletionStore.java
│       └── JdbcPopularWindowStore.java
├── tests/
│   ├── architecture/                # exhaustive ownership/import/dependency checks
│   ├── unit/
│   │   ├── api/                     # handlers with controlled business collaborators
│   │   ├── transaction/             # rules with fake database contracts/scan sink
│   │   └── analytics/               # window rules with fake window store
│   ├── contract/                    # retained fixed-HTTP-contract tests
│   ├── integration/                 # retained end-to-end invariants
│   └── database/                    # JDBC adapter and atomic completion tests
├── build.sh
├── build-tests.sh
├── run.sh
└── run-tests.sh

db/
├── init.sql                         # schema and 2,000 × 10,000 baseline retained
└── init-db.ps1

load-client/                         # unchanged external validation client
└── reports/                         # two required timestamped JSON reports
```

**Structure Decision**: Keep one Java server and enforce layers through packages, small immutable
records, explicit interfaces, and automated import rules. `api.Main` is assigned to the API layer
as the composition root: it may instantiate concrete database adapters but contains no request
handling or business/database call path after startup. All runtime request collaboration follows
API → transaction/analytics → database. Catalog and low-stock use cases belong to the transaction
layer because the required architecture defines only four layers and those operations concern the
store/checkout domain, not analytics or transport.

### Current-to-target migration map

| Current component | Target ownership/change |
|---|---|
| `Main` | `api.Main`; construction-only composition root, updated launch class |
| `checkout.CheckoutHandlers`, `catalog.CatalogHandler`, `inventory.InventoryHandlers`, `analytics.AnalyticsHandlers` | API HTTP handlers; parse/validate transport, delegate, serialize, map failures |
| `api.HttpSupport`, `api.ApiErrors`, `json.Json` | API utilities; HTTP error constants replaced by `ApiErrorMapper` |
| `checkout.CheckoutService`, `checkout.Basket`, `checkout.CheckoutException` | Transaction service/domain; remove HTTP and JDBC knowledge |
| `catalog.CatalogCache`, `inventory.LowStockThresholds` | Transaction-layer store query behavior |
| `analytics.AnalyticsRecorder`, `analytics.WindowMath`, `analytics.AnalyticsWindowStore` | Analytics service/policy; query behavior moves out of HTTP handler and persistence DTO cycle is removed |
| `persistence.*` | Database contracts plus JDBC implementations; all SQL, connections, mappings, and transaction demarcation remain here |

## Implementation Strategy

1. Add architecture tests and characterization checks before moving behavior.
2. Introduce database contracts and immutable boundary records; implement them by adapting current
   DAOs without changing SQL semantics or schema.
3. Move the whole completion transaction into `JdbcCheckoutCompletionStore`; expose a typed
   outcome rather than `Connection`, SQL exceptions, or partial operations.
4. Extract transaction/catalog/inventory interfaces and domain failures; adapt existing behavior
   behind them. Inject `AcceptedScanSink`, wired by `api.Main` to the analytics service, so only a
   successfully admitted scan is counted without a transaction↔analytics package dependency.
5. Turn analytics recording and latest-window lookup into one analytics-layer interface; replace
   persistence-owned analytics DTOs with contract-owned records and preserve ordered asynchronous
   checkpoints.
6. Reduce handlers to validation, delegation, response serialization, and one-time HTTP error
   translation. Remove handler-to-DAO and business-to-API imports.
7. Move/rename packages, update `run.sh` for `api.Main`, then run architecture, unit, contract,
   database, and end-to-end suites.
8. Execute the two clean-baseline workloads and retain their distinct client-generated reports.

## Complexity Tracking

No constitution violations require justification. The single-process deployable, existing schema,
and plain `javac` build are retained; no framework, service split, generic repository abstraction,
or additional shared/model layer is introduced.
