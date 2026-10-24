# Self-Checkout System Constitution

## Core Principles

When requirements compete, correctness and recoverability take precedence over
throughput. Explicit assignment requirements take precedence over optional design
preferences. Business rules, external API contracts, and correctness guarantees from
prior iterations remain in force unless a principle below explicitly replaces them.

### I. Independent Microservices and Domain Ownership (NON-NEGOTIABLE)

- The system MUST consist of three independently deployable microservices:
  - **Transaction Service** owns the transaction lifecycle, shopping baskets, accepted
    scans, captured prices, transaction states, and checkout orchestration. It
    coordinates checkout completion with Inventory Service and owns the durable records
    required for transaction recovery and idempotency.
  - **Inventory Service** owns the product catalog, product availability, stock
    quantities, and inventory reservations or deductions. It is the sole authority for
    inventory mutations, MUST prevent overselling, and MUST apply inventory operations
    atomically.
  - **Analytics Service** owns accepted-scan analytics, event consumption progress,
    sliding/hopping window computation, ranking, and persisted analytics results. It
    MUST operate independently of synchronous checkout processing.
- Each service MUST be independently buildable, startable, stoppable, and deployable,
  with its own runtime, entry point, configuration, and port.
- Service responsibilities MUST remain clearly separated. Business logic and persistence
  ownership MUST NOT leak across service boundaries. Within each service, API, business,
  and persistence layers MUST remain distinct; business rules MUST NOT live in HTTP/gRPC
  handlers or generic database adapters.
- Shared libraries (e.g., generated gRPC stubs, utilities) MAY be used but MUST NOT
  contain cross-domain business logic or create runtime dependencies between services.
- Additional services (other than an API Gateway) MUST have a documented business need.

Rationale: Capability ownership is what makes independent deployment and independent
data stores meaningful.

### II. Database per Service (NON-NEGOTIABLE)

- Each microservice MUST own an independent data store. For local development, separate
  MySQL databases or schemas within one MySQL instance MAY be used, provided ownership
  and access isolation are strictly enforced (e.g., separate schemas and per-service
  credentials limited to the owning schema).
- Each service MUST access only its own database. Direct SQL queries, joins, foreign
  keys, or transactions spanning databases owned by different services are prohibited.
  No service may directly read or modify another service's tables.
- Cross-service data access MUST occur through explicitly defined service interfaces or
  reliable event delivery. The architecture MUST NOT rely on a shared database as an
  implicit communication mechanism.
- Each service MUST manage its own local database transactions.
- Data duplication across services MAY be used when justified by performance or
  availability (e.g., a product/price replica in Transaction Service), but replicated
  data MUST have a documented source of truth, synchronization mechanism, and
  consistency model.
- Acknowledged state MUST be durable in the owning service's database and recoverable
  after that service restarts without manual reconstruction. In-memory caches MAY
  accelerate access but MUST NOT be the sole record of acknowledged state.
- In-process mechanisms such as locks, caches, wait registries, and retry schedules MAY
  be used as performance and coordination optimizations, but MUST NOT be the sole
  authority for durability, idempotency, inventory correctness, or cross-instance
  consistency; durable state, database guards, and idempotent operations remain
  authoritative. All safety guarantees MUST hold across process crashes, restarts, and
  multiple concurrent instances of a service.

Rationale: Independent data ownership is the defining property of this iteration and
removes hidden coupling through shared tables.

### III. gRPC and Protocol Buffers for Internal Communication (NON-NEGOTIABLE)

- Internal synchronous service-to-service communication MUST use gRPC with Protocol
  Buffers. Service interfaces MUST be defined in version-controlled `.proto` files.
- RPC request and response messages MUST have clear validation and error semantics,
  using defined gRPC status codes and documented business error details.
- gRPC channels SHOULD be created once and reused rather than created per request.
- Every remote call MUST have a defined deadline/timeout and retry policy. Retries MUST
  NOT introduce duplicate business operations; operations that may be retried MUST be
  idempotent, keyed by a stable identifier (e.g., transaction ID or event ID).
- Internal interfaces MUST NOT expose database implementation details (table names,
  SQL, row identifiers not part of the domain).
- Unnecessary synchronous RPC MUST be avoided, particularly inside the high-frequency
  Scan operation. Asynchronous communication MAY use a reliable event-delivery mechanism
  when appropriate.
- Performance optimizations MUST NOT compromise service ownership or consistency
  guarantees.

Rationale: Explicit, typed contracts make cross-service coupling visible, testable, and
safe to retry.

### IV. External REST API and Backward Compatibility (NON-NEGOTIABLE)

- The existing Load Client and its REST contract MUST remain unchanged. Existing HTTP
  endpoints, methods, request and response formats, status codes, and observable
  business behavior as specified in `spec/self-checkout-openapi.yaml` MUST be preserved.
  That OpenAPI specification remains authoritative for external compatibility.
- The Load Client MUST run without source modifications. A microservices redesign MUST
  NOT require any client-side change.
- An API Gateway such as Nginx MAY expose the existing REST interface and route requests
  to the owning service. Routing MUST be documented in deployment configuration.
- Internal gRPC details MUST remain invisible to the external client. Internal errors,
  timeouts, and unavailability MUST be translated into the established external HTTP
  error contract.

Rationale: A stable external contract keeps architecture iterations comparable and
protects existing clients.

### V. Distributed Transactions and Saga Coordination (NON-NEGOTIABLE)

- Cross-service business operations MUST NOT assume a single ACID transaction across
  independent service databases; two-phase commit across service databases is not used.
- Checkout completion MUST use an explicitly designed orchestration-based Saga
  coordinated by Transaction Service. The plan MUST define each Saga step, its local
  transaction, its compensating action or, for a step after a forward-recovery pivot,
  its retry and idempotency rule, and the persisted Saga/transaction states.
- A Saga MAY use forward recovery after its pivot step. Once the pivot commits (for
  checkout, Inventory Service's atomic stock deduction), the Saga MUST run to completion
  by retrying its remaining steps; those steps MUST be idempotent and need no
  compensating action. Steps before the pivot MUST have compensating actions. A
  compensating action for the pivot MAY be kept as a reserved capability that no normal
  path invokes; it MUST still be durable, idempotent, and directly tested.
- Saga progress MUST be persisted in Transaction Service's database before and after
  each remote step so an interrupted Saga can be resumed or compensated after restart
  without manual intervention.
- Inventory operations invoked by the Saga MUST be atomic within Inventory Service and
  idempotent per checkout attempt, where each attempt has a stable identifier associated
  with its transaction: a retried or duplicated request MUST NOT deduct or restore stock
  more than once. Inventory Service MUST durably record processed operation keys. A
  transaction MAY make a new attempt after a definitive rejection or a confirmed
  restoration, but MUST NOT have more than one effective stock deduction.
- Insufficient stock or any failed step MUST leave the system consistent once the Saga
  settles: no partial stock deduction may remain for a transaction that is not
  completed, and compensations MUST themselves be idempotent and retried until they
  succeed.
- The client MUST receive a success response only after the Saga reaches a committed
  completed state. Repeated completion requests MUST return the established result
  without re-executing inventory effects.
- Choreography MAY be used only for non-critical asynchronous propagation (e.g.,
  analytics events); it MUST NOT replace orchestration for checkout completion without
  a documented amendment.

Rationale: With separate databases, correctness depends on explicit, recoverable
coordination rather than an implicit shared transaction.

### VI. Checkout and Inventory Consistency (NON-NEGOTIABLE)

- Scanning MUST NOT decrement stock; stock changes only through checkout completion.
- The price captured at scan time MUST be the price recorded for that line; later
  catalog changes MUST NOT alter accepted scans.
- Concurrent completions MUST prevent overselling and lost updates. For each SKU,
  `initial_stock - final_stock` MUST equal the quantity sold in completed transactions,
  and final stock MUST NOT be negative, including after retries, crashes, and
  compensations.
- Each transaction MUST end in exactly one terminal outcome as observed by clients, and
  that outcome MUST agree with Inventory Service's recorded effects.
- Guarantees about completed transactions, final stock, and agreement between services
  apply to settled Sagas. While a Saga is pending, its state MUST be durable, MUST NOT be
  reported as success, and MUST settle automatically, without manual intervention, once
  the required services are reachable. The only exception is a detected integrity fault,
  meaning a contract violation that cannot occur in correct operation: the Saga MUST then
  remain pending, MUST NOT be settled on an assumed outcome, and MUST raise an alarm
  visible to operators. A server error returned while a Saga is pending is not a
  terminal outcome; the settled state is.

Rationale: A sale is valid only when transaction records and inventory agree, regardless
of how the work is distributed.

### VII. Reliable Event Delivery and Analytics Accuracy

- Accepted-scan events MUST be durably recorded by Transaction Service within the same
  local transaction that accepts the scan (e.g., a transactional outbox), and delivered
  to Analytics Service through gRPC or another reliable mechanism. Analytics Service MUST
  NOT read Transaction Service's database.
- Delivery MAY be at-least-once; Analytics Service MUST deduplicate by stable event
  identity and durably record its consumption progress in its own database so that
  duplicates, interrupted work, and restarts of either service neither lose nor
  double-count events. Event order and checkpoint semantics MUST be defined in the plan.
- Analytics Service MUST preserve separate Window, Ranking, and Persistence
  responsibilities with ordered, asynchronous hand-off between stages and immutable
  data between them.
- Popularity MUST count accepted scans, not completed sales. The established hopping
  window of the most recent 1,000 scans, recomputed every 500 scans, MUST retain its
  boundaries and rules. Ranking MUST be deterministic, with SKU-ascending tie breaking
  and the top 10 items per complete window.
- The popular-items endpoint MUST return only a complete, successfully persisted window
  and its boundaries; it MUST NOT expose partial or in-flight state. Restart MUST
  reconstruct correct window and ranking state from Analytics Service's durable data.
- Analytics MAY be eventually consistent with accepted scans; the plan MUST define the
  visibility delay and failure behavior. Analytics unavailability MUST NOT block Scan or
  checkout.

Rationale: Reliable, idempotent delivery preserves analytics correctness without coupling
analytics to the checkout path.

### VIII. Performance and Operational Simplicity

- Correctness and recovery guarantees MUST NOT be weakened for throughput. Services
  SHOULD avoid redundant queries, unnecessary synchronous writes and RPCs, and excessive
  polling; required durable writes are not optional.
- Connection pooling, gRPC channel reuse, indexing, batching, and local replicas SHOULD
  be chosen to sustain the default and stress workloads. Performance results MUST
  include throughput, error rate, and tail latency for comparison with prior iterations.
- Infrastructure beyond the three services, their databases, and an optional API
  Gateway (e.g., message brokers, service meshes) MUST NOT be added without a concrete
  requirement and documented benefit. Simple Java JARs are acceptable; Docker MAY be
  used when it simplifies deployment.
- Each service MUST document its build, startup, ports (HTTP and gRPC), configuration,
  database connection, and dependencies. Restarting any single service MUST preserve
  correctness of the overall system.

Rationale: The system needs measurable performance and repeatable operation without
infrastructure that obscures service boundaries.

### IX. Verification and Benchmark Evidence

- Existing functional behavior and relevant automated tests MUST be preserved. Tests MUST
  cover database isolation, `.proto` contract behavior, external API compatibility,
  concurrent checkout and inventory invariants, Saga success, insufficient stock,
  idempotent retries, and crash/restart at Saga steps. Tests MUST also cover every
  compensation that a Saga path can trigger, tested end to end, and every reserved
  compensation capability, tested directly; direct tests are not reported as end-to-end
  Saga compensation tests.
- Tests MUST verify reliable analytics delivery: no loss or double counting across
  duplicate delivery and restart of Transaction or Analytics Service.
- Concurrent completion tests MUST reconcile final stock with completed sales.
- The unchanged Load Client MUST run against the actual microservice deployment from a
  reproducible baseline. Two genuine JSON reports MUST be committed: one with default
  parameters and one using `java -cp out Main --stations=100 --duration=120`. Results
  MUST NOT be fabricated.

Rationale: Distributed correctness claims require executable evidence under the same
client and workloads used for earlier iterations.

## Architectural Constraints

- This iteration MUST evolve from the existing service-based implementation through
  incremental refactoring. Tested business logic, HTTP handlers, persistence adapters,
  and analytics filters SHOULD be reused where practical. Unrelated feature changes
  SHOULD be avoided.
- Endpoint ownership: Transaction Service serves `/transactions/**`; Inventory Service
  serves `/items` and `/inventory/low-stock`; Analytics Service serves
  `/analytics/popular-items`. Each owning service's database is the source of truth for
  that data.
- Each service MUST use its own configurable database or schema and credentials. Any
  data reset or seeding used for benchmarks MUST be performed per service through its
  own interfaces or scripts, never by one service writing another's data.
- The established 2,000-item, 10,000-units-per-item baseline MUST be preserved for
  comparable runs. Default and stress reports MUST identify their settings and the
  microservice deployment used.

## Development Workflow & Quality Gates

- Work MUST follow constitution → specification → plan → tasks → implementation. The
  specification and plan MUST trace service ownership, data stores, `.proto` contracts,
  Saga steps with their compensations or forward-recovery rules, event delivery, gateway
  routing, and deployment back to these principles.
- Plans MUST define deadlines, retry and idempotency keys, Saga states and recovery
  rules, replicated-data consistency models, event ordering and checkpoints, and the
  analytics visibility delay.
- Reviews MUST verify independent build and restart, no cross-database access, gRPC-only
  internal synchronous calls, idempotent retries, inventory invariants, unchanged REST
  behavior, and loss-free, duplicate-free analytics.
- Completion requires passing relevant automated tests and producing both reproducible
  benchmark JSON files from the microservice deployment.

## Governance

- This constitution governs subsequent specifications, plans, tasks, implementation,
  and code reviews. A conflicting artifact MUST be corrected, or an explicit deviation
  from a MUST requirement MUST be documented and justified before acceptance.
- Amendments MUST record their rationale, affected principles, version, and amendment
  date in this file. The Sync Impact Report is review material and MUST be removed before
  the amended constitution is committed.
- Semantic versioning applies: MAJOR for incompatible principle or governance changes,
  MINOR for new principles or material additions, and PATCH for non-semantic corrections.
- Compliance reviews MUST prioritize correctness, data ownership, compatibility,
  recoverability, and independent deployability. Implementation choices belong in the
  relevant specification or plan unless a principle requires them.

**Version**: 5.1.0 | **Ratified**: 2026-09-17 | **Last Amended**: 2026-10-20
