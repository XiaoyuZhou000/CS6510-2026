# Implementation Plan: Distributed Domain Services with Durable Analytics Recovery

**Branch**: `service-based-architecture` | **Date**: 2026-10-12 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/004-distributed-domain-services/spec.md`. The setup script selected feature directory `004-distributed-domain-services`; the checked-out Git branch and the spec header are `service-based-architecture`.

## SQL Materializer redesign (2026-10-13)

The implemented ordering design is [sql-materializer-design.md](sql-materializer-design.md). It replaces the earlier commit-time sequence design. Transaction Service commits a raw event with an immutable unique ID and NULL position without locking `scan_sequence`. Analytics Materializer discovers all committed NULL-position events, assigns gapless positions in bounded transactions, and commits assignments with `scan_sequence.last_position`. The pipeline consumes only the committed positioned prefix. The prior service boundaries, REST contract, and window semantics remain in force.

## Summary

Refactor the current single JVM Pipeline Architecture into independently buildable and restartable Transaction, Inventory, and Analytics Java services over one configurable MySQL database. Keep the seven-route synchronous REST contract and unchanged load client through a static proxy on the existing base URL. A scan commits its basket update and one unpositioned raw event together; Analytics Materializer assigns positions in bounded durable batches, then the pipeline polls positioned events, reconstructs the sliding overlap after restart, and atomically publishes each ranked window with its checkpoint. Preserve tested handlers, ranking, logging, and persistence code where practical. Validate service isolation, crash recovery, inventory invariants, and two genuine reset-based load reports.

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
| III. Centralized persistence and durability | All three use one MySQL instance. The transaction/line/raw-event Scan commit and transaction/inventory completion commit are atomic; Materializer positions and checkpoint commit together. No acknowledged scan depends on a process map. | PASS |
| IV. REST contract fidelity | Exact seven public routes stay on `:8080`; existing handlers and response mappings are retained/adapted. Nginx is justified by the single URL plus independent service restart. | PASS |
| V. Recoverable event processing | SQL Materializer defines durable order without a Scan-side sequence lock. Analytics polls positioned persisted scans; checkpoint and complete window commit together. Startup replays after the checkpoint with overlap prewarm. Existing logging/persistence mechanisms are evaluated in [research.md](research.md). | PASS |
| VI. Pipeline and window accuracy | Separate FIFO Window, Ranking, Persistence workers receive immutable messages. Absolute 1,000/500 boundaries, deterministic top ten, ordered atomic publication, and older complete result during lag are specified. | PASS |
| VII. Performance and operational simplicity | No broker, RPC, or service mesh. Static proxy has a concrete route need. Existing plain Java build is retained; connection pools, indexes, bounded Analytics read-ahead, and independently documented start/stop are planned. | PASS |
| VIII. Verification and benchmark evidence | Adapt contract/integration tests, add concurrency and crash-point checks, compare DB-derived windows and stock, and capture two genuine unchanged-client JSON reports after separate fresh resets. | PASS |
| Architectural baseline and workflow | Incrementally extract from `pipeline-architecture`; plan traces shared tables, recovery, routing, and failure behavior. `tasks.md` and implementation follow this design. | PASS |

**Post-design re-check:** [data-model.md](data-model.md), [contracts/durable-scan-and-checkpoint.md](contracts/durable-scan-and-checkpoint.md), [contracts/public-http-and-routing.md](contracts/public-http-and-routing.md), [contracts/service-deployment.md](contracts/service-deployment.md), and [quickstart.md](quickstart.md) preserve these gates. No unresolved clarification or constitution deviation remains. The SQL Materializer redesign is specified in [sql-materializer-design.md](sql-materializer-design.md). Analytics order is durable and deterministic but need not equal physical commit order.

## Phase 0: Research Decisions

The decision and alternative record is [research.md](research.md). Its resolved choices are:

1. Use a SQL Materializer to assign positions in bounded transactions after concurrent Scan commits; physical commit order is not an analytics requirement.
2. Move OPEN basket lines and totals to MySQL at scan time; reconstruct lookup and completion from persisted rows.
3. Use a singleton Analytics checkpoint advanced in the same transaction as each complete popular window and ranks. Replay uncheckpointed events and prewarm the 1,000-event overlap after restart.
4. Extract three Java thin artifacts with build-time shared transport/JDBC utilities and a static Nginx route proxy. No domain process communicates directly with another.
5. Reuse tested Ranking/Persistence filter behavior, `AnalyticsLog`, handler serialization, conditional stock updates, and atomic window writes, while replacing the prior skip-overlap restart rule.

## Phase 1: Design and Contracts

### Transaction boundaries and failure behavior

`POST /transactions` commits an OPEN row before HTTP 201. A successful Scan locks its transaction row, updates the basket line and total, inserts one immutable-ID raw event with NULL position, and commits before HTTP 200. It never locks `scan_sequence`. Failure rolls back all three writes. Completion serializes with Scans on that transaction row and atomically commits inventory deductions and COMPLETED state.

### Analytics ordering, replay, and publication

Materializer locks `scan_sequence`, discovers committed events with `position IS NULL` in bounded batches, assigns consecutive positions, and commits assignments and checkpoint together. It searches all pending events, including lower IDs committed late. Analytics polls only committed positioned events in adjacent position order and passes immutable messages through the existing Window, Ranking, and Persistence queues. Window boundaries remain 1,000 then every 500. On restart, the window checkpoint `W` prewarms positions `W-999..W` and replays from `W+1`; Materializer independently resumes from pending rows. Window header, ranks, and checkpoint commit together, and duplicate results must match. Queries return the latest committed complete window. See the [durable-scan contract](contracts/durable-scan-and-checkpoint.md).

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

No constitution violation requires an exception. The `scan_sequence` row serializes only Materializer batches, not Scan commits. The pending-event index and batch update cost, Scan SQL round trips, and checkout-row contention are measured alongside throughput and lag. Nginx adds one operational dependency to satisfy the fixed one-URL client contract while allowing independent domain restarts.

At the `$speckit-converge` review, compare the new default and stress reports with the corresponding `load-client/reports/pipeline-architecture/` baseline runs. Focus on per-operation Scan p99 latency and Analytics catch-up after an outage, alongside throughput and error rates. Keep the durable SQL event design unless the measured results show a material regression; if they do, record the evidence and add a targeted optimization task. Do not assume a numerical regression threshold before the runs exist.
