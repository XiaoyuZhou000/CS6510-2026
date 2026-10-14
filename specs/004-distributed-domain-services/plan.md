# Implementation Plan: Distributed Domain Services with Durable Analytics Recovery

**Branch**: `service-based-architecture` | **Date**: 2026-10-12 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/004-distributed-domain-services/spec.md`. The setup script selected feature directory `004-distributed-domain-services`; the checked-out Git branch and the spec header are `service-based-architecture`.

## Summary

Refactor the current single JVM Pipeline Architecture into independently buildable and restartable Transaction, Inventory, and Analytics Java services over one configurable MySQL database. Keep the seven-route synchronous REST contract and unchanged load client through a static proxy on the existing base URL. A scan commits its basket update and one globally positioned event together; Analytics polls durable events, reconstructs the sliding overlap after restart, and atomically publishes each ranked window with its checkpoint. Preserve tested handlers, ranking, logging, and persistence code where practical. Validate service isolation, crash recovery, inventory invariants, and two genuine reset-based load reports.

## Technical Context

**Language/Version**: Java 21 (`javac --release 21`); SQL for MySQL 8.0+; Bash service/client scripts and existing PowerShell database reset

**Primary Dependencies**: JDK `HttpServer`, `java.util.concurrent`, JDBC, MySQL Connector/J 9.0.0, JUnit Platform Console 1.11.0; Nginx as a fixed-path HTTP reverse proxy; no RPC or broker

**Storage**: One centralized InnoDB MySQL database. Retain `catalog_item`, `inventory`, `transaction`, `transaction_line`, `popular_window`, and `popular_item`; add `scan_sequence`, `accepted_scan`, and `analytics_checkpoint`; add unique `popular_window.window_end` and required indexes

**Testing**: Adapt existing JUnit unit, architecture, contract, database, and integration suites; live distributed restart/crash tests; `db/validate-invariants.sql`; unchanged `load-client/` default and stress runs with JSON reports

**Target Platform**: Three separate Java 21 server processes plus an Nginx proxy on one or more hosts able to reach the same MySQL instance; local validation uses ports 8081/8082/8083 behind 8080

**Project Type**: Distributed HTTP services divided by checkout, inventory query, and analytics business capabilities

**Performance Goals**: Sustain the existing 10-station/60-second default and 100-station/120-second stress workloads; measure throughput, error rates, and p95/p99 latency with no invented numerical improvement threshold. Prefer short scan transactions, connection reuse, incremental in-memory windows, and bounded Analytics read-ahead.

**Constraints**: Correctness before throughput; durable accepted scans and open baskets; deterministic, durable, recoverable analytics order with immutable, unique, gapless positions for committed materialized events; atomic stock/completion and window/checkpoint commits; 1,000-scan windows every 500 positions; at most ten deterministic ranks; eventual Analytics visibility; all seven existing methods/paths and error semantics; one public base URL; no runtime domain-service calls

**Scale/Scope**: 2,000 seeded products, 10,000 units each; three domain JVMs, one static router, a single MySQL database; retained accepted-event history for recovery; two required JSON benchmark files from separate resets

## Constitution Check

*GATE: Passed before Phase 0 research and re-evaluated after Phase 1 design.*

| Constitution gate | Plan evidence | Result |
|---|---|---|
| I. Checkout and inventory consistency | Scan never decrements stock. Completion locks the transaction, uses committed lines, conditionally decrements sorted inventory rows, and commits state/stock together. Duplicate completion is rejected. | PASS |
| II. Domain service boundaries | Three separate artifacts, entry points, runtimes, ports, and local API/business/JDBC boundaries. Shared utilities have no business ownership; no service waits for another domain process. | PASS |
| III. Centralized persistence and durability | All three use one MySQL instance. The transaction/line/event/sequence scan commit and transaction/inventory completion commit are atomic. No acknowledged scan depends on a process map. | PASS |
| IV. REST contract fidelity | Exact seven public routes stay on `:8080`; existing handlers and response mappings are retained/adapted. Nginx is justified by the single URL plus independent service restart. | PASS |
| V. Recoverable event processing | Row-locked sequence defines durable order. Analytics polls persisted scans; checkpoint and complete window commit together. Startup replays after the checkpoint with overlap prewarm. Existing logging/persistence mechanisms are evaluated in [research.md](research.md). | PASS |
| VI. Pipeline and window accuracy | Separate FIFO Window, Ranking, Persistence workers receive immutable messages. Absolute 1,000/500 boundaries, deterministic top ten, ordered atomic publication, and older complete result during lag are specified. | PASS |
| VII. Performance and operational simplicity | No broker, RPC, or service mesh. Static proxy has a concrete route need. Existing plain Java build is retained; connection pools, indexes, bounded Analytics read-ahead, and independently documented start/stop are planned. | PASS |
| VIII. Verification and benchmark evidence | Adapt contract/integration tests, add concurrency and crash-point checks, compare DB-derived windows and stock, and capture two genuine unchanged-client JSON reports after separate fresh resets. | PASS |
| Architectural baseline and workflow | Incrementally extract from `pipeline-architecture`; plan traces shared tables, recovery, routing, and failure behavior. `tasks.md` and implementation follow this design. | PASS |

**Post-design re-check:** [data-model.md](data-model.md), [contracts/durable-scan-and-checkpoint.md](contracts/durable-scan-and-checkpoint.md), [contracts/public-http-and-routing.md](contracts/public-http-and-routing.md), [contracts/service-deployment.md](contracts/service-deployment.md), and [quickstart.md](quickstart.md) preserve these gates. No unresolved clarification or constitution deviation remains. The serial scan counter is this plan's implementation choice for contiguous positions at scan commit, not a requirement that analytics order equal physical commit order. A database-backed materializer could assign positions later if it preserves the same durability, immutability, uniqueness, gaplessness, and recovery guarantees. Stress measurements will quantify the selected design's cost.

## Phase 0: Research Decisions

The decision and alternative record is [research.md](research.md). Its resolved choices are:

1. For this implementation, use a transactional singleton sequence row to assign analytics positions at scan commit. The specification also permits a database-backed materializer to assign positions after concurrent scan commits; physical commit order is not an analytics requirement.
2. Move OPEN basket lines and totals to MySQL at scan time; reconstruct lookup and completion from persisted rows.
3. Use a singleton Analytics checkpoint advanced in the same transaction as each complete popular window and ranks. Replay uncheckpointed events and prewarm the 1,000-event overlap after restart.
4. Extract three Java thin artifacts with build-time shared transport/JDBC utilities and a static Nginx route proxy. No domain process communicates directly with another.
5. Reuse tested Ranking/Persistence filter behavior, `AnalyticsLog`, handler serialization, conditional stock updates, and atomic window writes, while replacing the prior skip-overlap restart rule.

## Phase 1: Design and Contracts

### Transaction boundaries and failure behavior

`POST /transactions` inserts and commits an OPEN row before HTTP 201 and returns the database timestamp. Each successful scan locks the transaction row, applies one-unit line and total changes, allocates a position through a locked `scan_sequence`, inserts its immutable event, and commits before HTTP 200. A rejected scan or failed commit leaves none of those changes. Scan and completion serialize on the transaction lock. Completion uses persisted lines, conditional SKU-sorted stock updates, and a single commit for stock plus COMPLETED status; receipt follows commit. Repeated completion returns 409 with no write. A lost response after commit retains the existing client ambiguity, described in the [public contract](contracts/public-http-and-routing.md).

### Analytics ordering, replay, and publication

In the selected commit-time design, Analytics polls committed `accepted_scan` by primary-key position using fresh reads. A later-assigned position is also valid if the poller reads only the committed, contiguous materialized prefix and can recover all unpositioned committed scans. The poller requires adjacent positions and hands immutable events through bounded FIFO queues to Window, Ranking, and Persistence stages. Window uses absolute positions, maintains a 1,000-entry ring, and emits at 1,000 then each 500 positions. At startup, checkpoint `W` loads positions `W-999..W` without publishing, then consumes `W+1` forward; zero begins at 1. The in-memory cursor may advance ahead of persistence during a run, but only a committed window advances the durable checkpoint. A persistence transaction locks checkpoint, verifies the next boundary, inserts header and ranks, updates checkpoint, and commits; unique window end and equivalence checks make retries idempotent. A query returns only the latest committed boundary or the established empty shape. The [checkpoint contract](contracts/durable-scan-and-checkpoint.md) covers each crash boundary.

### Ownership and route arrangement

Transaction owns checkout and writes accepted events, while directly reading catalog and updating inventory in the shared database. Inventory owns catalog and low-stock responses and reads committed stock. Analytics owns event consumption and popularity results, reading accepted scans and catalog names. Nginx forwards the existing public URL to the owner based on path; owner outages affect only their routes, and an unavailable proxy/database has the limits described in the [routing contract](contracts/public-http-and-routing.md). The [service deployment contract](contracts/service-deployment.md) defines ports, artifacts, configuration, and restart behavior.

## Project Structure

### Documentation (this feature)

```text
specs/004-distributed-domain-services/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── public-http-and-routing.md
│   ├── durable-scan-and-checkpoint.md
│   └── service-deployment.md
└── tasks.md                         # Phase 2, produced by $speckit-tasks
```

### Source code (planned incremental layout)

```text
server/
├── common/src/
│   ├── api/                       # transport-neutral HTTP/JSON support
│   └── database/                  # pool, store failure, shared JDBC records/utilities
├── transaction/src/
│   ├── api/                       # transaction handler and error mapping
│   ├── transaction/               # checkout rules, durable basket service, TransactionMain
│   ├── database/                  # transaction, scan, completion adapters
├── inventory/src/
│   ├── api/                       # catalog and low-stock handlers
│   ├── inventory/                 # catalog/threshold rules, InventoryMain
│   ├── database/                  # catalog/inventory read adapters
├── analytics/src/
│   ├── api/                       # popular-items handler
│   ├── analytics/                 # poller, filters, AnalyticsMain
│   ├── database/                  # durable scan reader and popular-window writer
├── tests/{unit,architecture,database,contract,integration,support}/
├── lib/                            # existing Connector/J and JUnit jars
├── build.sh                        # target-specific thin jars
├── run-transaction.sh
├── run-inventory.sh
└── run-analytics.sh

deploy/nginx.conf                   # fixed public route map on :8080
db/{init.sql,init-db.ps1,validate-invariants.sql}
spec/self-checkout-openapi.yaml    # unchanged public API
load-client/{src,build.sh,run.sh,reports/service-based-architecture/}
```

**Structure Decision:** Evolve `server/src/` into three ownership directories and a small compile-time common module. Move the existing catalog/low-stock rules out of the `transaction` package. Keep transaction-specific `ApiErrorMapper` behavior in Transaction so common does not depend on a domain. Build each service independently with plain `javac`/`jar`, adapt test classpaths, and preserve proven code instead of changing frameworks. The public Nginx process is routing infrastructure with no business API or database access.

## Complexity Tracking

No constitution violation requires an exception. The single scan-sequence row serializes accepted scan commits in this design; that cost is explicit and measured in the required reports. Gapless analytics positions do not inherently require serializing scan commits: a database-backed materialization process may assign immutable positions after commit and recover unfinished assignments. Nginx adds one operational dependency to satisfy the fixed one-URL client contract while allowing independent domain restarts.

At the `$speckit-converge` review, compare the new default and stress reports with the corresponding `load-client/reports/pipeline-architecture/` baseline runs. Focus on per-operation Scan p99 latency and Analytics catch-up after an outage, alongside throughput and error rates. Keep the durable SQL event design unless the measured results show a material regression; if they do, record the evidence and add a targeted optimization task. Do not assume a numerical regression threshold before the runs exist.
