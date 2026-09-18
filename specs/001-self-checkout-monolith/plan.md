# Implementation Plan: Self-Checkout Monolithic Server

**Branch**: `001-self-checkout-monolith` | **Date**: 2026-09-17 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-self-checkout-monolith/spec.md`

**Note**: This template is filled in by the `/speckit-plan` command; its definition describes the execution workflow.

## Summary

Build a single-process monolithic server that implements `spec/self-checkout-openapi.yaml`
exactly (catalog, transaction start/scan/complete/lookup, low-stock alerts, popular-item
analytics) against the existing MySQL schema in `db/init.sql`. The primary technical challenge
is Principle I (Inventory Consistency): stock must be decremented exactly once per sold unit,
atomically and idempotently, under up to 100 concurrent stations, while keeping the hot path fast
by holding in-progress baskets in memory (scanning touches no database at all; starting a
transaction does exactly one lightweight INSERT) and touching the database further only at
completion and, best-effort, at popular-item window boundaries.

## Technical Context

**Language/Version**: Java 21 (matches the JDK 21+ toolchain already required by `load-client/`
and `mockserver/`; enables virtual threads for cheap per-request concurrency)

**Primary Dependencies**: JDK built-ins only for the HTTP layer (`com.sun.net.httpserver`) and a
hand-rolled JSON reader/writer (extending the pattern already in `mockserver/Json.java`), plus the
single external dependency required to talk to MySQL: the `mysql-connector-j` JDBC driver jar
(vendored under `server/lib/`, referenced via `-cp` in `build.sh`/`run.sh`, no Maven/Gradle — this
matches the zero-build-tool convention already used by `load-client/` and `mockserver/`)

**Storage**: MySQL 8.0+, schema and seed already defined in `db/init.sql` (database
`cs6510_selfcheckout`, reachable at `127.0.0.1:3307` per `.idea/dataSources.xml`); this feature
does not modify that schema

**Testing**: JUnit 5 via the `junit-platform-console-standalone` single jar (vendored under
`server/lib/`, run with `java -jar` — keeps the no-build-tool convention while still using a
standard test framework)

**Target Platform**: A single JDK 21 server process on Linux or Windows (developer laptop or a
nearby remote host), matching the "monolithic: one deployable process" architectural constraint

**Project Type**: Single project / web-service backend (monolith) — new top-level `server/`
directory alongside the existing `spec/`, `load-client/`, `mockserver/`, `db/` directories

**Performance Goals** *(design targets, not contract-mandated — see SC-005)*: p95 < 200 ms to
start a transaction, p95 < 100 ms to scan an item, p95 < 1 s to complete a transaction, under the
default 10-station / 60 s workload

**Constraints**:
- MUST NOT modify `spec/self-checkout-openapi.yaml` or `load-client/` [FR-017, Constitution III]
- MUST NOT modify `db/init.sql`'s schema (reset/reseed behavior is reused, not changed)
- Stock decrement MUST happen only at completion, MUST be atomic and non-negative, and MUST be
  idempotent per transaction [FR-005, FR-008, FR-009, Constitution I]
- A completed transaction's record MUST be committed to MySQL before its completion response is
  returned [FR-010, Constitution II]
- Popular-item window persistence is **explicitly best-effort**: it is never awaited by, and never
  affects the success or latency of, any scan response — but computed windows MUST still commit in
  the same order their boundaries were crossed, and failed checkpoints MUST be retried in place
  (not re-queued) so ordering holds even under transient failure [FR-013, Constitution IV > V — see
  research.md §4 for the full policy, its ordering guarantee, and the resulting read/report
  behavior]
- Popular-item counts MUST be exact for every window that *is* persisted — a hopping window of the
  most recent 1,000 scans, recomputed every 500 scans — including across a server restart
  (research.md §9: restarting with a previously saved checkpoint skips one checkpoint rather than
  ever persisting an inexact window; restarting with no saved checkpoint — whether a genuinely
  fresh database or a crash before the very first checkpoint — does not skip, since there is no
  prior boundary to be inconsistent with) [FR-013, Constitution V]
- Scanning MUST touch no database at all; starting a transaction performs exactly one lightweight
  INSERT (needed so the completion-time idempotency guard has a row to target — data-model.md,
  Transaction) — both are cheap relative to the p95 targets above [Constitution IV]
- **Accepted limitations (not fully bounded — see research.md for why)**:
  1. Scan-tracking progress since the last persisted checkpoint (the in-memory counter and ring
     buffer) is lost on a server crash or restart, identically to how in-progress baskets may be
     lost (research.md §9). Resuming the counter from the last saved checkpoint keeps window
     *numbering* from appearing to rewind, but this is cosmetic only — it does **not** preserve or
     reconstruct the actual scan stream across the crash; lost scans are never recovered or
     retroactively counted by any later window.
  2. Popular-item persistence is best-effort and fully decoupled from every scan response, so
     `GET /analytics/popular-items` returns whatever window last successfully persisted — which
     **can lag the true latest checkpoint by an unbounded amount** if writes are slow or the
     database is unavailable across several consecutive checkpoints, not just a "usually
     near-immediate" delay (research.md §4). This is an accepted risk given the assignment's
     actual conditions (a healthy, local, co-located MySQL instance for a 60–120 s run), not a
     structural guarantee, and would need an explicit bound (backlog cap, circuit breaker, or
     surfaced staleness) if that assumption changes.

  Neither limitation affects inventory correctness or completed-transaction durability.

**Scale/Scope**: 2,000-SKU catalog, 10,000 units of stock per SKU at baseline; 10 concurrent
stations / 60 s (default) and 100 concurrent stations / 120 s (stress); basket size 1–20 items per
transaction

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Plan approach | Status |
|---|---|---|
| I. Inventory Consistency (NON-NEGOTIABLE) | Completion runs inside one JDBC transaction: an idempotency guard (`UPDATE transaction SET status='COMPLETED' ... WHERE status='OPEN'`, checking the affected-row count) gates a single-statement conditional decrement per SKU (`UPDATE inventory SET stock_quantity = stock_quantity - ? WHERE sku=? AND stock_quantity >= ?`), with SKUs locked in a stable sorted order to avoid deadlocks; any failed row aborts the whole transaction with no partial writes. See [research.md](./research.md) §1–2. | PASS |
| II. Durable Completion & Reliability | The completion response waits on one JDBC transaction that commits inventory, the completed-transaction row, and its line items together — this is the only response any of this feature's endpoints wait on a commit for. Popular-item persistence is explicitly best-effort and asynchronous (research.md §4); it never gates a response, so it is not treated as part of this principle's "success response" guarantee. In-progress baskets, not-yet-checkpointed scan tracking, and any in-flight/failed checkpoint stay non-durable, per the constitution's accepted trade-off (research.md §9, and the read-behavior note in §4). | PASS |
| III. Contract Fidelity | Implementation targets `spec/self-checkout-openapi.yaml` as fixed input; no changes to the spec or to `load-client/`. New `error` codes (e.g. `INSUFFICIENT_STOCK`) are added only as string values inside the existing, already-generic `ApiError.error` field — no schema change. See [research.md](./research.md) §3. | PASS |
| IV. Performance Under Concurrency | Scanning is purely in-memory (`ConcurrentHashMap` of open baskets), no DB access at all. Starting a transaction performs exactly one lightweight INSERT (data-model.md, Transaction). Completion is the one operation that always waits on a DB transaction. Popular-item checkpoints are submitted to a background executor without any scan ever waiting on them (research.md §4), so this principle is never traded off against analytics accuracy. Virtual threads (Java 21) back the HTTP server's executor so 100 concurrent stations don't exhaust a fixed platform-thread pool. | PASS |
| V. Analytics Window Accuracy | A single global lock guards the scan counter and a fixed-size (1,000) ring buffer; the multiple-of-500 boundary check and the snapshot copy happen inside that same lock so every window that *is* persisted is exact for the exact scan range it claims, even under bursts. A single-threaded executor persists checkpoints strictly in boundary order — retrying failed ones in place rather than re-queuing — so the highest-`window_id` row that exists is always the most recently *successfully computed* window, never an out-of-order or partially-tallied one. This exactness guarantee holds across a restart that finds a previously saved checkpoint (one checkpoint is skipped rather than persisting an inexact one); a restart that finds no saved checkpoint — fresh database or a crash before the first checkpoint, indistinguishable to the server — does not skip, since there is no prior boundary to protect. Note this principle's PASS covers *exactness of what is persisted*, not *freshness of what is readable* — see the best-effort read-lag limitation above and research.md §4, which is an explicitly accepted, unbounded-in-the-worst-case gap, not a second guarantee. See [research.md](./research.md) §4, §9. | PASS (exactness); best-effort (freshness) |
| VI. Testability & Repeatability | Server takes the already-existing `db/init.sql` / `db/init-db.ps1` reinitialization as an external precondition (unchanged); it performs no migrations of its own and assumes the schema is already present at startup. | PASS |

No violations identified; **Complexity Tracking is not needed**.

## Project Structure

### Documentation (this feature)

```text
specs/001-self-checkout-monolith/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output (/speckit-plan command)
├── data-model.md        # Phase 1 output (/speckit-plan command)
├── quickstart.md        # Phase 1 output (/speckit-plan command)
├── contracts/           # Phase 1 output (/speckit-plan command)
└── tasks.md             # Phase 2 output (/speckit-tasks command - NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
server/                       # NEW — the monolithic server (this feature)
├── build.sh                  # javac, matching load-client/ and mockserver/ conventions
├── run.sh                    # java -cp, forwards CLI args (port, DB connection info)
├── lib/                      # vendored jars: mysql-connector-j, junit-platform-console-standalone
├── src/
│   ├── Main.java             # entrypoint: wires HTTP routes, DB pool, in-memory state, starts HttpServer
│   ├── api/                  # HttpServer route handlers; request parsing, response writing, status codes
│   ├── catalog/               # GET /items — read-only catalog access
│   ├── checkout/              # transaction lifecycle: start, scan, complete, get-by-id; in-memory basket state
│   ├── inventory/             # stock decrement logic, GET /inventory/low-stock
│   ├── analytics/             # hopping-window scan counter, ring buffer, top-10 computation
│   ├── persistence/           # JDBC connection pool + DAOs (catalog, inventory, transaction, popular_window)
│   └── json/                  # hand-rolled JSON reader/writer (extends mockserver's Json.java pattern)
└── tests/
    ├── contract/              # one test class per endpoint, asserting response shape/status vs the OpenAPI spec
    ├── integration/           # concurrent-completion race, duplicate-completion, restart-durability, window-boundary tests against a real MySQL instance
    └── unit/                  # pure logic: ring buffer/window math, low-stock threshold resolution, JSON round-trips
```

**Structure Decision**: New top-level `server/` directory, sibling to `spec/`, `load-client/`,
`mockserver/`, and `db/` — consistent with how this repository already separates concerns, and
satisfying the constitution's "module boundaries SHOULD be kept separable" guidance (API handling,
checkout/inventory logic, persistence, and analytics are distinct packages) so later weeks can
lift pieces of this code into different architectural styles.

## Complexity Tracking

> Not applicable — the Constitution Check above reported no violations.
