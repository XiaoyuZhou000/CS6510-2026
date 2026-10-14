<!--
Sync Impact Report
==================
Version change: 3.0.0 → 4.0.0
Rationale: MAJOR. Independent domain-service deployment and durable scan-event recovery
replace the single-process architecture and its allowance for non-durable accepted scans.

Modified principles:
  - I. Inventory Consistency → I. Checkout and Inventory Consistency
  - II. Durable Completion & Reliability → III. Centralized Persistence and Durability
  - III. Contract Fidelity → IV. REST Contract Fidelity
  - IV. Performance Under Concurrency → VII. Performance and Operational Simplicity
  - V. Analytics Window Accuracy → VI. Analytics Pipeline and Window Accuracy
  - VI. Testability & Repeatability → VIII. Verification and Benchmark Evidence
  - VII. Layer Boundary Integrity → II. Domain Service Boundaries
  - VIII. Analytics Pipeline Integrity → VI. Analytics Pipeline and Window Accuracy
Added sections: none; Core Principles now includes V. Recoverable Event Processing.
Removed sections: none.
Follow-up TODOs: none.

This report is temporary review material and should be removed before commit.
-->

# Self-Checkout System Constitution

## Core Principles

When requirements compete, correctness and recoverability take precedence over
throughput. Explicit assignment requirements take precedence over optional design
preferences.

### I. Checkout and Inventory Consistency (NON-NEGOTIABLE)

- Scanning MUST NOT decrement stock; stock changes only when a transaction completes.
- Completion MUST atomically commit the completed-transaction state and all required
  inventory checks and decrements, or commit none of them. Repeated completion MUST NOT
  decrement stock again.
- Concurrent completions MUST prevent overselling and lost updates. For each SKU,
  `initial_stock - final_stock` MUST equal the quantity sold in completed transactions,
  and final stock MUST NOT be negative.
- A success response and receipt MUST follow the successful commit of the required
  transaction and inventory changes.

Rationale: A sale is valid only when its inventory and transaction records agree under
concurrent checkout.

### II. Domain Service Boundaries (NON-NEGOTIABLE)

- The system MUST be decomposed by business capability into independently buildable,
  runnable, configurable, and deployable services, each with its own runtime and entry
  point. The preferred boundaries are Transaction (checkout lifecycle, scans, completion,
  transaction state), Inventory (catalog, inventory queries, low-stock reporting), and
  Analytics (popular-item processing and ranking). A different boundary MUST be justified
  by business responsibility rather than a technical layer or table.
- Each service MUST retain clear API, business, and persistence boundaries internally.
  Business rules MUST NOT be placed in HTTP handlers or generic database adapters.
- The Analytics Service MUST retain the existing Window, Ranking, and Persistence filter
  pipeline. Services MUST NOT depend on another service's process or startup to perform
  their own assigned responsibility, except for shared database availability.
- Additional services MUST have a documented business need; decomposition solely to add
  layers, tables, or infrastructure is insufficient.

Rationale: Capability ownership makes independent deployment useful while preserving
the separation of concerns gained in the layered and pipeline iterations.

### III. Centralized Persistence and Durability (NON-NEGOTIABLE)

- All domain services MUST use one centralized MySQL database. Persisted database state
  MUST be the primary means of cross-service coordination. Services MAY access the same
  tables when their responsibilities require it; for example, Transaction MAY update
  inventory through JDBC within a checkout transaction.
- Direct service-to-service REST, gRPC, or other RPC MUST NOT be introduced unless a
  specific need is documented and the database-based approach is insufficient. gRPC is
  not part of the required external interface.
- Shared JDBC or persistence code SHOULD be factored into reusable components when it
  prevents duplication. Shared components MUST NOT contain cross-domain business logic
  or create runtime dependencies between services.
- Accepted business events needed for analytics MUST be durably stored in MySQL. An event
  MUST NOT be acknowledged as durably accepted before its required database transaction
  commits. Transaction Service MUST persist the event information within the appropriate
  transactional workflow. In-memory baskets or caches MAY accelerate access but MUST NOT
  be the sole record of accepted events or committed transactions.
- Acknowledged transaction state MUST be recoverable after Transaction Service restart
  without manual reconstruction.
- Database access MUST preserve transaction integrity and concurrency safety.

Rationale: The shared database is the assignment's coordination boundary and the source
of truth after a service crash.

### IV. REST Contract Fidelity (NON-NEGOTIABLE)

- The existing HTTP/REST endpoints MUST retain their request and response shapes,
  status codes, and error semantics as specified in `spec/self-checkout-openapi.yaml`.
  The provided load client MUST work without modification.
- Any routing needed to expose multiple services through the existing client-facing
  contract MUST be documented and justified. OpenAPI MAY document or validate that
  contract without adding unnecessary runtime machinery.
- External communication MUST remain HTTP/REST. gRPC MUST NOT be introduced without a
  demonstrated technical requirement and a documented compatibility decision.

Rationale: A stable contract keeps the architecture iterations comparable and protects
existing clients.

### V. Recoverable Event Processing (NON-NEGOTIABLE)

- Analytics Service MUST consume persisted database events, not direct in-process calls
  from Transaction Service. It MUST eventually reflect every relevant committed scan
  event, including events accepted before an unexpected Analytics Service termination.
- Processing MUST tolerate duplicate delivery, interrupted work, and restart without
  losing or double-counting accepted events. Durable event order, checkpoint semantics,
  and the boundary between processing and checkpoint commits MUST be defined in the
  technical plan and tested against crash points.
- Restart MUST reconstruct correct sliding-window and ranking state from durable data
  without manual reconstruction of accepted transactions. Rebuilding from event history
  MAY occur during initialization or recovery; normal queries SHOULD use the latest
  complete materialized result.
- Existing `AnalyticsLog`, `PersistenceFilter`, and related persistence mechanisms MUST
  be evaluated for reuse before replacement.

Rationale: A process-local queue cannot provide a recovery guarantee once Analytics runs
separately from Transaction.

### VI. Analytics Pipeline and Window Accuracy

- Analytics Service MUST preserve separate Window, Ranking, and Persistence filter
  responsibilities, ordered asynchronous queue-based pipes rather than direct downstream
  calls, and immutable data passed between stages. In-memory sliding-window and ranking
  structures SHOULD support incremental processing during normal operation; database
  history remains the recoverable source.
- Popularity MUST count accepted scans, not completed sales. The established hopping
  window of the most recent 1,000 scans, recomputed every 500 scans, MUST retain its
  current boundaries and business rules. Ranking MUST be deterministic, with SKU-ascending
  tie breaking and the top 10 items for each complete window.
- The endpoint MUST return only a complete, successfully persisted window and its
  boundaries; it MUST NOT expose partial or in-flight state. Processing and persistence
  MUST preserve event and window order across retry and recovery.
- Analytics MAY be eventually consistent with committed scans. The plan MUST define the
  visibility and failure behavior, and no accepted event may be silently discarded.

Rationale: Durable input and incremental in-memory filters preserve both recoverability
and the prior pipeline's efficient query path.

### VII. Performance and Operational Simplicity

- Correctness and recovery guarantees MUST NOT be weakened for throughput. Services
  SHOULD avoid redundant queries, unnecessary synchronous writes, and excessive polling;
  required durable event writes are not optional.
- JDBC access, indexing, connection reuse, and incremental event consumption SHOULD be
  chosen to sustain the existing default and stress workloads. Performance results MUST
  include throughput, error rate, and tail latency for comparison with prior iterations.
- Message brokers, service meshes, API gateways, and other infrastructure MUST NOT be
  added without a concrete requirement and documented benefit. Simple Java JARs are
  acceptable; Docker MAY be used when it simplifies deployment.
- Each service MUST document its build, startup, port, configuration, database connection,
  and dependencies. Independent restart MUST preserve shared state correctness.

Rationale: This academic system needs measurable performance and repeatable operation,
without infrastructure that obscures the required service boundaries.

### VIII. Verification and Benchmark Evidence

- Existing functional behavior and relevant automated tests MUST be preserved. Tests MUST
  cover service isolation, API compatibility, concurrent checkout and inventory
  invariants, durable event consumption, duplicate handling, and Analytics Service crash
  and restart at persistence, processing, and checkpoint boundaries.
- Recovery tests MUST verify that persisted events reconstruct the correct window and
  ranking without loss or double counting. Concurrent completion tests MUST reconcile
  final stock with completed sales.
- The unchanged load client MUST run against the actual distributed deployment and
  centralized MySQL database from a reproducible baseline. Two genuine JSON reports MUST
  be committed: one with default parameters and one using
  `java -cp out Main --stations=100 --duration=120`. Results MUST NOT be fabricated.

Rationale: Architecture and recovery claims require executable evidence under the same
client and workloads used for earlier iterations.

## Architectural Constraints

- This iteration MUST begin from the `pipeline-architecture` implementation and proceed
  through incremental refactoring. Tested business logic, persistence adapters, HTTP
  handlers, and pipeline filters SHOULD be reused where practical. Unrelated feature
  changes SHOULD be avoided.
- Transaction Service owns checkout lifecycle and atomic completion; Inventory Service
  owns catalog and low-stock queries; Analytics Service owns persisted-event consumption,
  windowing, ranking, and popular-item queries. Shared table access does not transfer
  ownership of those business responsibilities.
- Every service MUST connect to the same configurable MySQL instance. The existing REST
  routes MUST remain reachable to the unchanged client; any routing arrangement MUST be
  explicit in deployment documentation.
- The system MUST preserve the established 2,000-item, 10,000-units-per-item reset
  baseline for comparable runs. Default and stress reports MUST identify their settings
  and the distributed deployment used.

## Development Workflow & Quality Gates

- Work MUST follow constitution → specification → plan → tasks → implementation. The
  specification and plan MUST trace service ownership, shared tables, event durability,
  recovery, routing, and deployment back to these principles.
- Plans MUST define event ordering, checkpoint and restart rules, transaction boundaries,
  failure behavior, and the consistency delay visible to analytics users. Significant
  changes to API behavior, consistency, or analytics semantics MUST be justified.
- Reviews MUST verify independent service build and restart, no unapproved runtime
  service coupling, transactional inventory invariants, unchanged REST behavior, and
  crash-safe analytics without loss or double counting.
- Completion requires passing relevant automated tests and producing both reproducible
  benchmark JSON files from the distributed deployment and centralized database.

## Governance

- This constitution governs subsequent specifications, plans, tasks, implementation,
  and code reviews. A conflicting artifact MUST be corrected or an explicit deviation
  from a MUST requirement MUST be documented and justified before acceptance.
- Amendments MUST record their rationale, affected principles, version, and amendment
  date in this file. The Sync Impact Report is review material and MUST be removed before
  the amended constitution is committed.
- Semantic versioning applies: MAJOR for incompatible principle or governance changes,
  MINOR for new principles or material additions, and PATCH for non-semantic corrections.
- Compliance reviews MUST prioritize simplicity, correctness, compatibility, durability,
  and independent deployability. Implementation choices belong in the relevant
  specification or plan unless a principle requires them.

**Version**: 4.0.0 | **Ratified**: 2026-09-17 | **Last Amended**: 2026-10-11
