# Implementation Plan: Pipeline Analytics

**Branch**: `pipeline-architecture` | **Date**: 2026-09-30 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/003-pipeline-analytics/spec.md`

## Summary

Replace the current analytics service's combined windowing, ranking, and background persistence
logic with three independently executing filters connected only by unbounded FIFO Java
`BlockingQueue` pipes. `AnalyticsService` remains the facade and lifecycle owner: it serializes
accepted-scan admission, serves only the latest committed database result, starts the workers, and
coordinates a configurable drain-aware shutdown. Immutable stage messages, ordered retry in the
persistence filter, terminal-failure rejection, and structured JSON log records preserve accuracy
and make backlog and lifecycle failures observable without changing the OpenAPI contract, schema,
or load client.

## Technical Context

**Language/Version**: Java 21 (`javac --release 21`)

**Primary Dependencies**: JDK HTTP server, `java.util.concurrent` (`LinkedBlockingQueue`, dedicated
worker threads), JDBC, MySQL Connector/J 9.0.0; no application framework or build tool

**Storage**: MySQL 8.0+ using the existing `popular_window`, `popular_item`, and `catalog_item`
tables through `PopularWindowStore`; no schema migration

**Testing**: JUnit Platform Console 1.11.0, repository fake-store unit tests, architecture tests,
live MySQL integration/contract tests, invariant SQL, and the unchanged Java load client

**Target Platform**: One cross-platform JVM server process; Bash scripts for build/run and
PowerShell for repeatable MySQL initialization

**Project Type**: Layered HTTP web service with an in-process analytics pipeline

**Performance Goals**: Under the default 10-station workload, p95 transaction start below 200 ms,
p95 scan below 100 ms, and p95 completion below 1 second; analytics admission performs no database
I/O and never waits for queue capacity

**Constraints**: Fixed 1,000-event window and 500-event slide; deterministic top 10; unbounded FIFO
pipes; no silent event loss; oldest-window retry blocks newer persistence; atomic window writes;
structured logs only for operational visibility; default shutdown timeout 5 seconds; unchanged
OpenAPI, load client, database reset baseline, and four-layer dependency direction

**Scale/Scope**: 2,000 catalog SKUs with 10,000 units each; default workload of 10 stations for
60 seconds and stress workload of 100 stations for 120 seconds; three analytics workers and three
FIFO queues in one server process

## Constitution Check

*GATE: Passed before Phase 0 research and passed again after Phase 1 design.*

| Gate | Plan evidence | Status |
|---|---|---|
| Inventory consistency | Analytics remains observational. Accepted-scan delivery occurs only after basket admission, and pipeline delay/failure never changes completion, stock, or idempotency logic. | PASS |
| Durable completion and reliability | Checkout durability is untouched. The persistence filter calls the existing atomic `PopularWindowStore.writeWindow` and retries the oldest ranked window in place. | PASS |
| Contract fidelity | `spec/self-checkout-openapi.yaml`, all seven routes, response mappings, and `load-client/` remain unchanged. | PASS |
| Performance under concurrency | Ingestion performs a short serialized state/health check and unbounded queue `offer`; windowing, ranking, and JDBC work run on separate workers. | PASS |
| Analytics window accuracy | The Window Filter alone assigns positions and emits exactly 1,000 immutable events at ends 1,000, 1,500, ...; Ranking applies count-descending/SKU-ascending top-10 order. | PASS |
| Testability and repeatability | Unit, architecture, live contract/integration, database invariants, and two reset/restart load runs are specified in `quickstart.md`. | PASS |
| Layer boundary integrity | All new pipeline types remain in `analytics`; only `PopularWindowStore` crosses from analytics to database; `api.Main` remains the composition root. | PASS |
| Analytics pipeline integrity | Explicit Window, Ranking, and Persistence filters each own one worker; immutable messages cross `BlockingQueue` pipes; stage-ordered end markers drain work; failure closes ingestion and is logged. | PASS |
| Validation gate | The plan requires fresh database and process state for both 10×60 and 100×120 unchanged-client reports before completion. | PASS |

Post-design re-check: the message contracts in `contracts/pipeline-messages.md`, lifecycle/log
contract in `contracts/lifecycle-observability.md`, data model, and validation guide introduce no
gate violation. No complexity exception is required.

## Project Structure

### Documentation (this feature)

```text
specs/003-pipeline-analytics/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── lifecycle-observability.md
│   ├── pipeline-messages.md
│   └── public-api-compatibility.md
└── tasks.md                         # created later by $speckit-tasks
```

### Source Code (repository root)

```text
server/
├── src/
│   ├── api/
│   │   ├── Main.java               # composition and shutdown-timeout configuration
│   │   └── AnalyticsHttpHandler.java
│   ├── transaction/
│   │   ├── AcceptedScanSink.java
│   │   └── TransactionService.java # accepted-scan producer; checkout remains authoritative
│   ├── analytics/
│   │   ├── AnalyticsOperations.java
│   │   ├── AnalyticsService.java   # facade, health, ingestion, query, lifecycle
│   │   ├── WindowFilter.java
│   │   ├── RankingFilter.java
│   │   ├── PersistenceFilter.java
│   │   ├── PipelineMessage.java     # immutable stage/control records
│   │   ├── AnalyticsLog.java        # one-line structured operational records
│   │   └── WindowMath.java
│   └── database/
│       ├── PopularWindowStore.java
│       └── JdbcPopularWindowStore.java
├── tests/
│   ├── architecture/
│   ├── unit/{api,analytics,transaction}/
│   ├── database/
│   ├── contract/
│   ├── integration/
│   └── support/
└── lib/
    ├── junit-platform-console-standalone-1.11.0.jar
    └── mysql-connector-j-9.0.0.jar

db/
├── init.sql
├── init-db.ps1
└── validate-invariants.sql

spec/self-checkout-openapi.yaml
load-client/{src,build.sh,run.sh,reports/}
```

**Structure Decision**: Retain the current four production packages and plain `javac` build. The
pipeline is an internal decomposition of `server/src/analytics/AnalyticsService.java`; no fifth
layer, framework, durable event log, public health endpoint, schema change, or client change is
introduced. Filter classes and their messages are package-owned analytics implementation details,
while `AnalyticsOperations` and `PopularWindowStore` remain the tested cross-layer boundaries.

## Complexity Tracking

No constitution violations require justification.
