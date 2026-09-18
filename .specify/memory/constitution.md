<!--
Sync Impact Report
==================
Version change: [initial template] → 1.0.0
Rationale: Initial ratification. First concrete constitution replacing the unfilled
scaffold; MAJOR baseline (1.0.0) per semantic versioning for a first adopted governance
document.

Modified principles: none (initial adoption)

Added sections:
  - Core Principles I–VI (Inventory Consistency; Durable Completion & Reliability;
    Contract Fidelity; Performance Under Concurrency; Analytics Window Accuracy;
    Testability & Repeatability)
  - Architectural Constraints (Section 2)
  - Development Workflow & Quality Gates (Section 3)
  - Governance

Removed sections: none

Templates / dependent artifacts requiring review:
  - .specify/templates/* (plan/spec/tasks) read this file at runtime; no edits required now.

Follow-up TODOs: none. All placeholders resolved.

NOTE: This report is scratch material for amendment review and should be removed before
the amended constitution is committed.
-->

# Self-Checkout System Constitution

## Core Principles

The principles below are ordered by priority. When two principles conflict, the
higher-numbered priority yields to the lower-numbered one. Consistency (I) is the
highest priority; Reliability (II) and Performance (IV) follow, consistent with the
project's Architectural Characteristics Analysis.

### I. Inventory Consistency (NON-NEGOTIABLE)

Correct inventory is the highest priority; a fast checkout that produces wrong stock
counts is a defect, not a trade-off.

- Stock MUST be decremented only when a transaction completes, never when an item is
  scanned.
- For every SKU, `initial_stock - final_stock` MUST equal the total quantity sold in
  completed transactions, and stock MUST never become negative.
- Completion MUST be atomic: stock checks and decrements for all basket items, plus the
  completed-transaction record, MUST commit together or not at all. Partial deductions
  are forbidden.
- Completion MUST be idempotent: repeating a completion for the same transaction MUST
  NOT decrement stock a second time.
- Concurrent completions MUST NOT sell the same last unit.

Rationale: Concurrent checkout stations contend for the same SKUs; correctness under
contention is the product's core promise.

### II. Durable Completion & Reliability

A successful completion MUST mean the result is durable.

- Inventory changes and the completed-transaction record MUST be committed to the
  database before a success/receipt response is returned.
- Persisted popular-item results MUST be written to the database as required by the
  contract.
- A failure at one checkout station MUST NOT corrupt another station's basket or the
  shared inventory.
- In-progress (unpaid) baskets MAY be held in server memory to reduce write load; this
  is an accepted trade-off that weakens recovery of unfinished work, and a server crash
  MAY lose in-progress baskets. Durability guarantees apply to completed transactions
  only.

Rationale: Durability is scoped to committed sales so that acknowledged purchases are
never lost, while unfinished baskets stay cheap.

### III. Contract Fidelity

The implementation exists to satisfy one fixed contract exercised by an unmodified
client.

- The server MUST conform to the OpenAPI specification in `spec/` for every endpoint,
  including request/response shapes and status codes.
- The unmodified load client MUST run against the server without changes.
- The API contract and the load client are external inputs and MUST NOT be modified to
  make an implementation pass.

Rationale: Week-to-week comparisons are only meaningful when the contract and test tool
are held constant.

### IV. Performance Under Concurrency

Performance is prioritized after consistency and reliability, and is measured under
concurrent load.

- Latency targets are p95 goals under the default 10-station workload: below 200 ms to
  start a transaction, 100 ms to scan an item, and 1 s to complete a transaction. These
  are design targets, not contract-mandated thresholds.
- Frequent operations (start, scan) SHOULD avoid unnecessary database access; holding
  in-progress baskets in memory is the sanctioned technique for this.
- Performance work MUST NOT weaken the guarantees in Principles I and II.

Rationale: Customers repeat start and scan throughout checkout, so those paths must stay
fast without trading away correctness or durable completion.

### V. Analytics Window Accuracy

Popular-item analytics MUST be accurate for the defined window, even though the ranking
is not refreshed on every scan.

- Scans MUST be counted over a hopping window: the most recent 1,000 scans, recomputed
  every 500 scans.
- The top 10 items for the latest computed window MUST be persisted and returned by
  `/analytics/popular-items` together with the corresponding window boundaries.
- Counts MUST be correct for the window they describe.

Rationale: The contract specifies a windowed aggregate, not a continuously exact
ranking; accuracy is defined relative to that window.

### VI. Testability & Repeatability

Every load run MUST start from the same, easily reproduced state.

- The database MUST be reinitializable before each run to 2,000 catalog items with
  10,000 units of stock per item.
- The unchanged load client MUST be used to produce results.
- JSON reports from the default run and the stress run MUST be saved so results can be
  compared from identical starting conditions.

Rationale: Comparable measurements require identical initial conditions and a fixed
measurement tool.

## Architectural Constraints

- **Current architecture (this iteration): monolithic.** The API MUST be delivered as a
  single deployable server process (e.g., Spring Boot, Flask, Node.js). Implementation
  language is free.
- **Persistence:** Stock items and popular-item results MUST be stored in a database
  that can be easily reinitialized between runs. In-memory state is permitted only for
  in-progress baskets, subject to Principle II.
- **Scale envelope:** The system MUST correctly handle both the default run (10
  concurrent stations for 60 s) and the stress run (100 stations for 120 s), preserving
  inventory correctness in both. Throughput, error rate, and p95/p99 latency SHOULD be
  captured for both runs to characterize behavior under increased load.
- **Forward compatibility:** Because later iterations reuse parts of this codebase under
  different architectures, module boundaries (API handling, transaction/inventory logic,
  persistence, analytics) SHOULD be kept separable to ease future re-architecture.

## Development Workflow & Quality Gates

- Work follows the Spec Kit flow: constitution → specify → plan → tasks → implement.
  Feature specs and plans MUST be consistent with this constitution.
- **Contract gate:** A change MUST NOT be considered done if it breaks conformance to the
  OpenAPI spec or requires modifying the load client.
- **Consistency gate:** Changes touching scan, completion, or inventory MUST preserve the
  invariants in Principle I, including behavior under concurrent completion and duplicate
  completion.
- **Validation gate:** Both the default and stress load runs MUST be executed with the
  unmodified client from a freshly reinitialized database, and their JSON reports saved,
  before results are reported or submitted.
- Trade-offs that touch prioritized characteristics (consistency, reliability,
  performance) MUST be made explicit and MUST respect the priority order in Core
  Principles.

## Governance

- This constitution supersedes other project practices for the Self-Checkout System.
  Where a plan, task, or implementation conflicts with it, the constitution prevails.
- **Amendments** MUST be documented in this file, include a rationale, and update the
  version and amendment date. The Sync Impact Report at the top records each change and
  is expected to be removed before the amended file is committed.
- **Versioning policy** (semantic):
  - MAJOR: backward-incompatible governance or principle removals/redefinitions.
  - MINOR: a new principle/section or materially expanded guidance.
  - PATCH: clarifications, wording, and non-semantic refinements.
- **Compliance review:** Specs, plans, and implementation changes MUST be checked against
  the Core Principles and Quality Gates before work is considered complete. Any accepted
  deviation MUST be justified in writing in the relevant spec or plan.

**Version**: 1.0.0 | **Ratified**: 2026-09-17 | **Last Amended**: 2026-09-17
