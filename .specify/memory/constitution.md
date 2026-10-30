# Self-Checkout System Constitution

## Core Principles

When requirements compete, correctness and recoverability take precedence over
throughput. Explicit assignment requirements take precedence over optional design
preferences. Business rules, external API contracts, and correctness guarantees from
prior iterations remain in force unless a principle below explicitly replaces them.

### I. Independent Microservices and Domain Ownership (NON-NEGOTIABLE)

- The system MUST consist of three independently deployable microservices:
  - **Transaction Service** owns the transaction lifecycle, shopping baskets, accepted
    scans, captured prices, transaction states, and checkout Saga state. It owns the
    durable records required for transaction recovery and idempotency.
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
  messaging, and persistence layers MUST remain distinct. Business rules MUST NOT live in
  HTTP handlers, message listeners, or generic database adapters.
- Shared libraries (e.g., event schemas, generated message classes, utilities) MAY be
  used but MUST NOT contain cross-domain business logic or create runtime dependencies
  between services.
- Additional services (other than an API Gateway and the message broker) MUST have a
  documented business need.

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
- Cross-service data access MUST occur through events published to the message broker
  (Principle III). The architecture MUST NOT rely on a shared database as an implicit
  communication mechanism.
- Each service MUST manage its own local database transactions.
- Data duplication across services MAY be used when justified by performance or
  availability (e.g., a product/price replica in Transaction Service), but replicated
  data MUST have a documented source of truth, an event-based synchronization mechanism,
  and a consistency model.
- Acknowledged state MUST be durable in the owning service's database and recoverable
  after that service restarts without manual reconstruction. In-memory caches MAY
  accelerate access but MUST NOT be the sole record of acknowledged state. The message
  broker MUST NOT be the sole record of any service's business state.
- In-process mechanisms such as locks, caches, wait registries, and retry schedules MAY
  be used as performance and coordination optimizations, but MUST NOT be the sole
  authority for durability, idempotency, inventory correctness, or cross-instance
  consistency; durable state, database guards, and idempotent operations remain
  authoritative. All safety guarantees MUST hold across process crashes, restarts, and
  multiple concurrent instances of a service.

Rationale: Independent data ownership is the defining property of the microservice
design and removes hidden coupling through shared tables.

### III. Event-Based Inter-Service Communication (NON-NEGOTIABLE)

- All communication between microservices MUST be asynchronous messages carried by a
  message broker. Synchronous service-to-service calls (gRPC, REST, or direct sockets)
  are prohibited. External requests that reach a service through the API Gateway are not
  service-to-service communication.
- Every message type MUST have a version-controlled schema (e.g., Protocol Buffers or
  JSON Schema). Each message MUST carry a globally unique event ID, type, schema version,
  aggregate or routing key, occurrence time, and a correlation ID that links it to the
  originating transaction or request.
- Schema changes MUST be backward compatible for consumers already deployed, or MUST be
  introduced as a new versioned type. Messages MUST NOT expose database implementation
  details (table names, SQL, row identifiers not part of the domain).
- Each message type MUST have exactly one owning producer service. Topics, queues, or
  exchanges, their routing keys, and their consumer groups MUST be documented.
- Ordering MUST be guaranteed per routing key where business rules depend on order
  (e.g., per transaction for Saga messages, and one defined order for analytics scans).
  The plan MUST state each ordering guarantee and how the chosen broker provides it.
- Delivery MAY be at-least-once. Every consumer MUST be idempotent, keyed by the event
  ID or a stable business key, and MUST acknowledge a message only after its local
  database effects have committed.
- Messages that cannot be processed after a bounded number of retries MUST be moved to a
  dead-letter destination or an equivalent parked state. They MUST be observable, MUST
  NOT be silently dropped, and MUST NOT block unrelated routing keys indefinitely.
- Performance optimizations MUST NOT compromise service ownership or consistency
  guarantees.

Rationale: Events through a broker decouple services in time and availability. Explicit
schemas, keys, and idempotent consumers keep that decoupling safe to retry.

### IV. External REST API and Backward Compatibility (NON-NEGOTIABLE)

- The existing Load Client and its REST contract MUST remain unchanged. Existing HTTP
  endpoints, methods, request and response formats, status codes, and observable
  business behavior as specified in `spec/self-checkout-openapi.yaml` MUST be preserved.
  That OpenAPI specification remains authoritative for external compatibility.
- Every external endpoint MUST remain synchronous. A service MAY wait for the events it
  needs before responding, but the client MUST NOT observe asynchronous internals.
- The Load Client MUST run without source modifications. The event-based redesign MUST
  NOT require any client-side change.
- An API Gateway such as Nginx MAY expose the existing REST interface and route requests
  to the owning service. Routing MUST be documented in deployment configuration.
- Broker details MUST remain invisible to the external client. Internal errors,
  timeouts, broker unavailability, and pending events MUST be translated into the
  established external HTTP error contract.

Rationale: A stable external contract keeps architecture iterations comparable and
protects existing clients.

### V. Distributed Transactions and Saga Coordination (NON-NEGOTIABLE)

- Cross-service business operations MUST NOT assume a single ACID transaction across
  independent service databases. Two-phase commit across service databases or between a
  database and the broker is not used.
- Checkout completion MUST be an explicitly designed Saga whose steps are carried by
  broker messages. The plan MUST choose orchestration (command and reply messages) or
  choreography (domain events) and justify the choice. Whichever is chosen, Transaction
  Service MUST durably own the checkout's Saga state, and the plan MUST define each
  step, its message types, its local transaction, its compensating action or, for a
  step after a forward-recovery pivot, its retry and idempotency rule, and the persisted
  Saga/transaction states.
- A Saga MAY use forward recovery after its pivot step. Once the pivot commits (for
  checkout, Inventory Service's atomic stock deduction), the Saga MUST run to completion
  by retrying its remaining steps; those steps MUST be idempotent and need no
  compensating action. Steps before the pivot MUST have compensating actions. A
  compensating action for the pivot MAY be kept as a reserved capability that no normal
  path invokes; it MUST still be durable, idempotent, and directly tested.
- Saga progress MUST be persisted in Transaction Service's database together with the
  outgoing message for each step (Principle VII outbox) so an interrupted Saga can be
  resumed or compensated after a restart of any service or the broker, without manual
  intervention.
- Inventory operations triggered by Saga messages MUST be atomic within Inventory
  Service and idempotent per checkout attempt, where each attempt has a stable
  identifier associated with its transaction: a redelivered or duplicated message MUST
  NOT deduct or restore stock more than once. Inventory Service MUST durably record
  processed operation keys in the same local transaction as the stock change. A
  transaction MAY make a new attempt after a definitive rejection or a confirmed
  restoration, but MUST NOT have more than one effective stock deduction.
- Insufficient stock or any failed step MUST leave the system consistent once the Saga
  settles: no partial stock deduction may remain for a transaction that is not
  completed, and compensations MUST themselves be idempotent and retried until they
  succeed.
- The client MUST receive a success response only after the Saga reaches a committed
  completed state in Transaction Service. Repeated completion requests MUST return the
  established result without re-publishing effective inventory operations.

Rationale: With separate databases and asynchronous messages, correctness depends on
explicit, recoverable coordination rather than an implicit shared transaction.

### VI. Checkout and Inventory Consistency (NON-NEGOTIABLE)

- Scanning MUST NOT decrement stock; stock changes only through checkout completion.
- The price captured at scan time MUST be the price recorded for that line; later
  catalog changes MUST NOT alter accepted scans.
- Concurrent completions MUST prevent overselling and lost updates. For each SKU,
  `initial_stock - final_stock` MUST equal the quantity sold in completed transactions,
  and final stock MUST NOT be negative, including after redelivery, crashes, broker
  restarts, and compensations.
- Each transaction MUST end in exactly one terminal outcome as observed by clients, and
  that outcome MUST agree with Inventory Service's recorded effects.
- Guarantees about completed transactions, final stock, and agreement between services
  apply to settled Sagas. While a Saga is pending, its state MUST be durable, MUST NOT be
  reported as success, and MUST settle automatically, without manual intervention, once
  the required services and the broker are reachable. The only exception is a detected
  integrity fault, meaning a contract violation that cannot occur in correct operation:
  the Saga MUST then remain pending, MUST NOT be settled on an assumed outcome, and MUST
  raise an alarm visible to operators. A server error returned while a Saga is pending is
  not a terminal outcome; the settled state is.

Rationale: A sale is valid only when transaction records and inventory agree, regardless
of how the work is distributed.

### VII. Reliable Event Delivery and Analytics Accuracy

- Every message a service publishes MUST first be recorded in that service's database
  within the same local transaction as the state change that causes it (a transactional
  outbox). A relay MUST publish outbox entries to the broker with retries and MUST mark
  them published only after the broker confirms receipt. Publishing directly to the
  broker without a durable record is prohibited for messages that affect business state
  or analytics.
- Accepted-scan events MUST be recorded by Transaction Service within the local
  transaction that accepts the scan and delivered to Analytics Service only through the
  broker. Analytics Service MUST NOT read Transaction Service's database.
- Analytics Service MUST deduplicate by stable event identity and durably record its
  consumption progress in its own database, so that duplicates, interrupted work, and
  restarts of either service or the broker neither lose nor double-count events. Broker
  offsets MAY be used as an optimization but MUST NOT be the sole record of progress.
  Event order and checkpoint semantics MUST be defined in the plan.
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
  visibility delay and failure behavior. Analytics or broker unavailability MUST NOT
  block Scan, and Analytics unavailability MUST NOT block checkout.

Rationale: Outbox publishing and idempotent consumption preserve correctness without
coupling the checkout path to the availability of any other component.

### VIII. Performance and Operational Simplicity

- Correctness and recovery guarantees MUST NOT be weakened for throughput. Services
  SHOULD avoid redundant queries, unnecessary synchronous writes, excessive polling of
  outboxes, and chatty per-item messages where batching preserves the guarantees above.
  Required durable writes are not optional.
- The system MUST use exactly one message broker product (e.g., Kafka, RabbitMQ, or
  ActiveMQ). The choice, its delivery and ordering guarantees, persistence settings, and
  rejected alternatives MUST be recorded in the feature's research. The broker MUST be
  configured for durable message storage so that acknowledged publishes survive a broker
  restart.
- Connection pooling, producer and consumer reuse, indexing, batching, and local
  replicas SHOULD be chosen to sustain the default and stress workloads. Performance
  results MUST include throughput, error rate, and tail latency for comparison with
  prior iterations.
- Infrastructure beyond the three services, their databases, the message broker, and an
  optional API Gateway (e.g., caches, service meshes, a second broker) MUST NOT be added
  without a concrete requirement and documented benefit. Simple Java JARs are
  acceptable; Docker MAY be used when it simplifies deployment.
- Each service and the broker MUST document build or installation, startup, ports,
  configuration, database or storage connection, topics or queues, and dependencies.
  Restarting any single service or the broker MUST preserve correctness of the overall
  system.

Rationale: The broker is now required infrastructure. Everything else must still earn
its place, and operation must stay repeatable.

### IX. Verification and Benchmark Evidence

- Existing functional behavior and relevant automated tests MUST be preserved. Tests MUST
  cover database isolation, event schema compatibility, external API compatibility,
  concurrent checkout and inventory invariants, Saga success, insufficient stock,
  duplicate and redelivered messages, and crash/restart at Saga steps. Tests MUST also
  cover every compensation that a Saga path can trigger, tested end to end, and every
  reserved compensation capability, tested directly; direct tests are not reported as
  end-to-end Saga compensation tests.
- Tests MUST verify that no synchronous service-to-service call remains, that every
  published message passes through an outbox, and that broker restart or temporary
  unavailability neither loses nor duplicates business effects.
- Tests MUST verify reliable analytics delivery: no loss or double counting across
  duplicate delivery and restart of Transaction Service, Analytics Service, or the
  broker.
- Concurrent completion tests MUST reconcile final stock with completed sales.
- The unchanged Load Client MUST run against the actual event-based deployment from a
  reproducible baseline. Two genuine JSON reports MUST be committed: one with default
  parameters and one using `java -cp out Main --stations=100 --duration=120`. Results
  MUST NOT be fabricated.
- An architecture diagram MUST be committed. It MUST show the API Gateway, the three
  microservices, each service's database, the message broker, and the topics or queues
  with their producers and consumers.

Rationale: Distributed correctness claims require executable evidence under the same
client and workloads used for earlier iterations.

## Architectural Constraints

- This iteration MUST evolve from the existing microservices implementation (feature
  005) through incremental refactoring. Tested business logic, HTTP handlers,
  persistence adapters, Saga state handling, and analytics filters SHOULD be reused
  where practical. gRPC interfaces between services MUST be replaced by broker messages
  and then removed. Unrelated feature changes SHOULD be avoided.
- Endpoint ownership: Transaction Service serves `/transactions/**`; Inventory Service
  serves `/items` and `/inventory/low-stock`; Analytics Service serves
  `/analytics/popular-items`. Each owning service's database is the source of truth for
  that data.
- Any catalog or price replica in Transaction Service MUST be populated and kept current
  through events published by Inventory Service, including its initial load.
- Each service MUST use its own configurable database or schema and credentials. Any
  data reset or seeding used for benchmarks MUST be performed per service through its
  own interfaces or scripts, never by one service writing another's data. The broker's
  topics or queues MUST be reset or drained as part of the same reproducible baseline.
- The established 2,000-item, 10,000-units-per-item baseline MUST be preserved for
  comparable runs. Default and stress reports MUST identify their settings and the
  event-based deployment used.

## Development Workflow & Quality Gates

- Work MUST follow constitution → specification → plan → tasks → implementation. The
  specification and plan MUST trace service ownership, data stores, broker choice,
  message schemas, topics or queues, Saga steps with their compensations or
  forward-recovery rules, outbox relays, gateway routing, and deployment back to these
  principles.
- Plans MUST define message ordering keys, idempotency keys, retry and dead-letter
  policies, Saga states and recovery rules, the wait and timeout behavior of synchronous
  endpoints that depend on events, replicated-data consistency models, consumption
  checkpoints, and the analytics visibility delay.
- Reviews MUST verify independent build and restart, no cross-database access, no
  synchronous service-to-service calls, outbox use for every published message,
  idempotent consumers, inventory invariants, unchanged REST behavior, and loss-free,
  duplicate-free analytics.
- Completion requires passing relevant automated tests, producing both reproducible
  benchmark JSON files from the event-based deployment, and committing the architecture
  diagram.

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

**Version**: 6.0.0 | **Ratified**: 2026-09-17 | **Last Amended**: 2026-10-28
