# Feature Specification: Self-Checkout Microservices with Database per Service, gRPC Contracts, and Recoverable Saga Checkout

**Feature Branch**: `microservices-architecture`

**Created**: 2026-10-19

**Status**: Draft

**Input**: User description: "Self-Checkout Microservices Redesign with Independent Databases, gRPC Communication, and Recoverable Distributed Checkout. Redesign the existing service-based architecture into independently deployable Transaction, Inventory, and Analytics microservices in full compliance with Constitution v5.0.0: database per service with documented replicated data; gRPC with Protocol Buffers for synchronous internal operations with deadlines, error handling, stable idempotency keys, safe retries, and no unnecessary remote calls on Scan; the unchanged REST contract and Load Client, optionally behind an API Gateway; a recoverable Transaction-orchestrated Saga for Complete; accepted-scan events recorded with the basket update and delivered at least once with idempotent consumption and deterministic, gapless analytics positions; independent build, start, stop, restart, reset, and seed; and two genuine default and stress benchmark reports, preserving correctness and durability even if performance decreases. Identify unresolved business semantics for clarification instead of inventing behavior."

## Baseline and Sources

- **[C-I]–[C-IX]** refer to the principles of `.specify/memory/constitution.md` v5.1.0, amended on 2026-10-20. The principle numbers are unchanged from v5.0.0.
- **[Contract]** refers to `spec/self-checkout-openapi.yaml`, which stays authoritative, together with the current HTTP handlers and contract tests that define error details the OpenAPI document does not enumerate.
- **[Preserved]** refers to the current `server/` implementation, `db/init.sql`, the existing automated tests, and specifications `specs/001-*` through `specs/004-*`.
- **[Client]** refers to the unchanged Load Client in `load-client/src/`.
- **[Assignment]** refers to `Microservices Architecture Requirement.txt`.

Today, three service processes sit behind one gateway URL but share one MySQL database. Transaction Service validates and prices scans by reading catalog rows, commits each accepted scan's basket update and analytics event together, and completes checkout by deducting stock and marking the transaction completed in one database transaction. Analytics Service reads accepted scans directly, assigns gapless positions in batches, and persists complete windows with a checkpoint. No service calls another. [Preserved]

This feature gives each service its own database. Transaction Service can no longer read catalog data or change stock, and Analytics Service can no longer read accepted scans. Cross-service work moves to explicit internal interfaces and reliable event delivery, and checkout completion becomes a Saga coordinated by Transaction Service. Everything a client can observe stays the same. [C-I][C-II][C-IV][C-V]

All user stories are required for acceptance. Priorities set delivery order, not optionality.

## Clarifications

### Session 2026-10-20

- Q: How does a checkout attempt settle when it cannot finish before its response deadline, or when a crash interrupts it? → A: Finish later. The attempt stays durably pending until Inventory's outcome is reconciled under the same attempt identity, then becomes COMPLETED if Inventory applied the stock effect or OPEN if Inventory definitively rejected it. A timeout or lost reply never triggers compensation by itself, a client-facing server error may represent an uncertain outcome, and lookup shows the settled state. (FR-030)
- Q: How are a Scan or Complete answered when they arrive while a checkout for the same transaction is pending? → A: They wait for a bounded time and are then applied in serial order against the settled state. If the attempt has not settled in time, they receive a server error, and nothing changes. Waiting holds no database connection or lock, and a duplicate Complete keeps the established status codes, including `409 TRANSACTION_NOT_OPEN`. (FR-035)
- Q: What role does stock restoration play under forward recovery? → A: Normal checkout uses forward recovery: an applied Inventory deduction always leads to COMPLETED, and a definitive insufficient-stock rejection reopens the basket. Inventory Service keeps a durable, idempotent stock-restoration operation as a reserved capability that normal checkout never invokes. It is tested directly against Inventory, and those tests are not reported as end-to-end Saga compensation tests. (FR-032)
- Q: Which response represents a pending or uncertain outcome? → A: A reachable service reuses the existing `500 INTERNAL_ERROR` response. The 409 business errors are unchanged, and the gateway keeps its existing 502 for an unreachable service. A 500 from an unsettled Complete is never a definitive cancellation. (FR-017)
- Q: What identity keys Inventory operations? → A: A stable checkout-attempt identifier associated with the transaction ID. A new attempt is allowed after a definitive insufficient-stock rejection, and no transaction may have more than one effective inventory deduction. (FR-028, FR-029)
- Q: How are completions that the client did not see as successful measured? → A: `totalTransactions` remains the primary metric. Client-observed successful Completes are recorded separately from background completions after client errors, and healthy workloads minimize background completions. (FR-054, FR-058)
- Q: How is regression coverage preserved while existing tests are migrated? → A: Compatible tests stay in place. Migrated tests keep equivalent assertions. An assertion of behavior that this specification replaces is replaced by the specified behavior and recorded with its governing requirement. Legacy tests are deleted only after coverage parity is verified. (FR-057)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Complete Checkout Across Independently Owned Data (Priority: P1)

As a shopper, I start a transaction, scan items, and complete payment, and I receive the same receipt and errors as before, even though Transaction Service records my basket and Inventory Service separately owns the stock. [Contract][C-V][C-VI]

**Why this priority**: Checkout is the primary business operation. With separate databases, its correctness now depends on explicit cross-service coordination.

**Independent Test**: With Transaction and Inventory Services running on separate databases, seed controlled stock, scan known baskets, and complete them, including concurrently and with insufficient stock. Compare receipts, transaction status, and Inventory's stock with expected values.

**Acceptance Scenarios**:

1. **Given** an OPEN transaction with two units of SKU A and one unit of SKU B, **When** completion succeeds, **Then** the response is HTTP 200 with the established receipt fields, each line carries the unit price captured at scan time, Inventory shows A reduced by exactly 2 and B by exactly 1, and lookup shows COMPLETED.
2. **Given** an OPEN basket in which at least one SKU exceeds available stock, **When** completion is requested, **Then** the response is `409 INSUFFICIENT_STOCK` naming a SKU without enough stock, no SKU's stock changes, and the transaction stays OPEN with its basket, so further scans and a later completion attempt behave as they do today. This is the end-to-end compensation path of the checkout Saga.
3. **Given** two OPEN transactions that each hold the last available unit of a SKU, **When** both complete concurrently, **Then** at most one succeeds, the other receives `409 INSUFFICIENT_STOCK`, and stock never becomes negative.
4. **Given** a completed transaction, **When** completion is requested again, including after restarts of any service, **Then** the response is `409 TRANSACTION_NOT_OPEN` and Inventory records no further effect.
5. **Given** an unknown transaction, or an OPEN transaction with an empty basket, **When** completion is requested, **Then** the response is `404 TRANSACTION_NOT_FOUND` or `409 EMPTY_BASKET` respectively, and Inventory records no effect.
6. **Given** a SKU's catalog price changes in Inventory Service after a unit was scanned, **When** the transaction completes, **Then** the receipt and totals use the price captured at scan time.

---

### User Story 2 - Scan Durably Without Per-Scan Remote Dependencies (Priority: P1)

As a station, each scan is validated, priced, and durably recorded by Transaction Service alone, so scanning stays fast and keeps working while Inventory or Analytics is unavailable. [C-II][C-III][C-VI][C-VII]

**Why this priority**: Scan is the highest-frequency operation. A remote call or peer dependency on it would cut throughput and availability.

**Independent Test**: Stop Inventory and Analytics Services, then start transactions, scan, and look them up. Restart Transaction Service and continue scanning. Restart the peers and verify that stock is unchanged and that every accepted scan reaches Analytics exactly once.

**Acceptance Scenarios**:

1. **Given** Transaction Service holds product information, **When** a valid scan is accepted, **Then** the response returns the established fields, item count, and running total, and no remote call is made for that scan.
2. **Given** Transaction Service has obtained product information and Inventory Service is then stopped, **When** stations start new transactions, scan known SKUs into them, and look them up, including after a Transaction Service restart, **Then** all requests succeed with correct names and prices.
3. **Given** Analytics Service is stopped, **When** scans are accepted, **Then** they succeed, and after Analytics restarts, each of them is counted exactly once.
4. **Given** scans were acknowledged, **When** Transaction Service terminates unexpectedly and restarts, **Then** lookup shows the same status, item count, total, and start time; completion uses every acknowledged unit; and every acknowledged scan is still delivered to Analytics.
5. **Given** any number of accepted scans, **When** stock is inspected, **Then** no scan has changed stock.
6. **Given** a scan is rejected for any reason, **When** state is inspected, **Then** neither the basket nor the stream of accepted-scan events has changed.

---

### User Story 3 - Settle Interrupted Checkouts Exactly Once (Priority: P1)

As the store, every checkout interrupted by a timeout, lost response, duplicate request, crash, or restart ends in exactly one settled outcome that agrees with Inventory, with no double deduction, no double restoration, and no manual repair. [C-V][C-VI]

**Why this priority**: Without a shared database transaction, partial failure between services is the main new threat to correctness.

**Independent Test**: Inject faults at each checkout step: before Inventory is contacted, after Inventory applies the stock change but before its reply arrives, after the reply but before Transaction Service records it, and after completion is recorded but before the client response. Crash and restart Transaction and Inventory at those points, keep Inventory unresponsive past the response deadline, and duplicate and delay internal requests. Verify each checkout's settled state, the client's responses, and stock reconciliation, and confirm that no timeout or lost reply triggered stock restoration. Separately, exercise the reserved stock-restoration operation directly against Inventory Service with duplicated, repeated, crash-interrupted, and out-of-order requests, and report those results apart from the end-to-end Saga results.

**Acceptance Scenarios**:

1. **Given** Inventory applied a checkout's stock change but its reply was lost, **When** Transaction Service reconciles under the same attempt identifier, **Then** stock is reduced exactly once and the transaction becomes COMPLETED without stock restoration. [FR-030]
2. **Given** Transaction Service crashes at any checkout step, **When** it restarts, **Then** it resumes the pending attempt from its durable progress, learns Inventory's actual outcome, and settles the checkout without manual intervention.
3. **Given** Inventory Service crashes after committing a stock change but before replying, **When** the request is repeated with the same identifier after restart, **Then** Inventory returns the original outcome and changes no further stock.
4. **Given** a checkout does not settle before its response deadline, **When** the deadline passes, **Then** the client receives `500 INTERNAL_ERROR`, never success, within the client's 10-second request timeout, and that response does not mean the checkout was cancelled. The attempt stays pending and later settles as COMPLETED if Inventory applied the stock effect, or as OPEN with its basket if Inventory definitively rejected it, which lookup then shows. [FR-017][FR-030]
5. **Given** a checkout completed after its client received `500 INTERNAL_ERROR`, **When** the client repeats Complete, **Then** the response is `409 TRANSACTION_NOT_OPEN`, stock does not change, and lookup shows COMPLETED. [FR-036]
6. **Given** a checkout is pending, **When** a Scan or Complete for the same transaction arrives, **Then** it waits up to the bound and is then applied in serial order: it receives `409 TRANSACTION_NOT_OPEN` if the transaction completed, is processed normally if the transaction reopened, or receives `500 INTERNAL_ERROR` with no change if the attempt is still pending. [FR-035]
7. **Given** the reserved stock-restoration operation is invoked directly against Inventory Service, **When** the request is repeated, duplicated, interrupted by an Inventory crash, retried after restart, or arrives before a delayed stock request for the same attempt, **Then** units are restored at most once and only if they were deducted, and the delayed request has no effect. This is a direct Inventory test, not an end-to-end Saga compensation test. [FR-032]
8. **Given** any combination of the faults above, **When** all checkouts have settled, **Then** for every SKU, initial stock minus final stock equals the units in COMPLETED transactions, and no stock is negative.

---

### User Story 4 - Keep One Unchanged Client Interface (Priority: P2)

As a station or evaluator, I use the existing base URL and seven routes without changing the Load Client or knowing where each service runs. [Contract][Client][C-IV]

**Why this priority**: Architecture iterations are comparable only when the public contract and the client stay unchanged.

**Independent Test**: Run the existing contract tests and the unchanged Load Client through the single base URL, then stop each service in turn and exercise every route.

**Acceptance Scenarios**:

1. **Given** all services are running, **When** the unchanged Load Client and the contract tests use one base URL, **Then** paths, methods, request and response fields, success statuses, validation errors, and domain errors match the established contract.
2. **Given** one service is stopped, **When** any route is requested, **Then** it behaves as specified in *Availability and Failure Behavior*.
3. **Given** an internal timeout, unavailable dependency, or internal error affects a client request, **When** the client receives the response, **Then** it is a non-success response as defined in FR-017, reveals no internal interface details, and is never a fabricated success or a misclassified validation error.

---

### User Story 5 - Recover Accurate Popular Items From Reliably Delivered Scans (Priority: P2)

As an operator, I see popular items that count every accepted scan exactly once in deterministic windows, despite duplicate or out-of-order delivery and restarts of either service. [C-VII]

**Why this priority**: Analytics now receives scans across a service boundary, so delivery reliability determines its correctness.

**Independent Test**: Submit a controlled stream of scans with known SKUs. Inject duplicate deliveries and reordering, crash Analytics at receipt, position assignment, window computation, and persistence, and crash Transaction Service during delivery. Compare every published window with an independent calculation over Analytics Service's durable positions.

**Acceptance Scenarios**:

1. **Given** fewer than 1,000 accepted scans, **When** popular items are queried, **Then** the established empty response is returned.
2. **Given** 1,000 accepted scans, **When** Analytics catches up, **Then** the first published window covers positions 1–1,000; after 500 more scans, the next covers 501–1,500.
3. **Given** an event is delivered more than once, **When** Analytics processes it, **Then** it is counted once and holds one position.
4. **Given** events arrive out of order or late, **When** positions are assigned, **Then** every accepted scan holds exactly one immutable position, and positions are unique and contiguous from 1.
5. **Given** Analytics crashes at any processing boundary, **When** it restarts, **Then** every eligible window eventually appears once with correct counts, and no partial or duplicate window is ever visible.
6. **Given** Analytics was unavailable while scanning continued, **When** it returns, **Then** it processes the backlog and publishes windows in order with correct overlap.
7. **Given** tied counts across more than ten SKUs, **When** a window is published, **Then** at most ten items appear, ordered by descending count and then ascending SKU.
8. **Given** a transaction's checkout failed or was never attempted, **When** popularity is computed, **Then** its accepted scans count like any others.

---

### User Story 6 - Operate and Reset Each Service Independently (Priority: P2)

As a maintainer, I build, start, stop, restart, reset, and seed each service on its own, and each service can reach only its own data. [C-I][C-II][C-VIII]

**Why this priority**: Independent deployment and data ownership are what distinguish microservices from a distributed monolith.

**Independent Test**: Build and start each service alone. Attempt cross-service data access with each service's credentials. Reset and seed each service through its own mechanism. Restart each service during a workload while the others keep running.

**Acceptance Scenarios**:

1. **Given** only one service's artifacts and configuration, **When** it is built and started, **Then** it runs without building or starting the other services.
2. **Given** each service's database credentials, **When** they are used to read or change another service's data, **Then** access is denied.
3. **Given** each service's reset mechanism, **When** all three services are reset and seeded, **Then** Inventory holds the 2,000-product, 10,000-units-per-product baseline, Transaction and Analytics hold no business data, and no service wrote another service's data.
4. **Given** a workload is running, **When** any single service is restarted, **Then** the others keep serving as specified in *Availability and Failure Behavior*, and after the restart every invariant in Success Criteria holds.

---

### User Story 7 - Reproduce Benchmark Evidence (Priority: P3)

As an evaluator, I reproduce two genuine Load Client reports from fresh baselines against the actual microservice deployment and compare them with the prior service-based iteration. [Assignment][C-IX]

**Why this priority**: Benchmark evidence is required for completion and is meaningful only after correctness is established.

**Independent Test**: Reset all three services, run the default command, and verify invariants after all work settles. Repeat with a separate reset for the stress command. Inspect both reports and the completion counts recorded with them.

**Acceptance Scenarios**:

1. **Given** a fresh baseline, **When** `java -cp out Main` runs with its defaults of 10 stations for 60 seconds, **Then** a genuine JSON report is produced and retained.
2. **Given** a separate fresh baseline, **When** `java -cp out Main --stations=100 --duration=120` runs, **Then** a distinct genuine JSON report is produced and retained.
3. **Given** either run has finished and all work has settled, **When** results are reviewed, **Then** `totalTransactions`, throughput, per-operation error rates, and p95/p99 latency are reported and compared with the prior iteration; durable COMPLETED transactions and background completions are recorded alongside the report; and the stock, transaction, and analytics invariants are verified.

### Edge Cases

**Checkout and Saga**

- A stock deduction is applied but its reply is lost. A repeated request under the same attempt identifier returns the original outcome, stock changes once, and the transaction completes. [FR-028][FR-030]
- A delayed or duplicated stock request reaches Inventory after Inventory recorded that attempt's outcome. Inventory returns the recorded outcome and changes no stock. If the attempt's stock was restored through the reserved operation, the request has no effect. [FR-028][FR-032]
- Inventory has no record of an attempt when Transaction Service reconciles it, because the request never arrived. The attempt has not failed; Transaction Service sends it again under the same identifier. [FR-030][FR-031]
- A reserved stock-restoration request is duplicated, retried after a restart, or arrives before the deduction it reverses. Units are restored at most once, and only if they were deducted. [FR-032]
- The reserved stock-restoration operation is applied to a pending attempt through a documented operational procedure. Transaction Service then treats the attempt as definitively not applied and reopens the basket. The operation is never applied to an attempt whose transaction is COMPLETED. [FR-032]
- Transaction Service crashes after recording its intent but before calling Inventory, after calling but before recording the reply, after recording the deduction but before recording COMPLETED, or after COMPLETED but before responding. On restart, it resumes the pending attempt from durable progress. A client whose connection dropped learns the outcome through lookup. [FR-033]
- Inventory Service crashes before or after committing a deduction or a stock restoration. Its replies after restart reflect only what it committed. [FR-028]
- One SKU in a multi-SKU basket lacks stock. No SKU is deducted. [FR-026][FR-027]
- Concurrent attempts compete for the same SKUs, with basket lines in any order. Stock is never oversold or negative, and contention is never reported as insufficient stock. [FR-026]
- A basket contains a SKU that Inventory does not recognize, for example because a replica is out of sync. Inventory rejects the attempt definitively, nothing is deducted, the client receives `500 INTERNAL_ERROR` rather than success or `INSUFFICIENT_STOCK`, and the transaction returns to OPEN. [FR-030]
- A price changes in Inventory after a scan. The captured price stands. [FR-019][FR-034]
- A Scan races with a Complete, or two Complete requests race, for the same transaction. At most one attempt runs, no unit joins a basket that is being completed, and each response matches a serial order. [FR-035]
- Completion is requested again after an attempt settled without a standing deduction. It is a new attempt with a new identifier, evaluated against current stock. [FR-029]
- A checkout completes after its client received `500 INTERNAL_ERROR`. Lookup shows COMPLETED with its item count and total, a repeated Complete returns `409 TRANSACTION_NOT_OPEN`, and the run's evidence counts it as a background completion. The receipt lines and completion time cannot be retrieved through the existing contract. [FR-030][FR-036][FR-054]
- Units returned by the reserved stock-restoration operation let another transaction's later attempt succeed. This is correct and is not overselling.
- Inventory stays unavailable for a long time. Pending attempts keep their baskets frozen and settle when it returns, each under its own identifier. An attempt that Inventory never processed is evaluated against the stock at that time, so it may be rejected and reopen. Meanwhile, lookup shows OPEN, and Scan and Complete for those transactions wait up to the bound and then return `500 INTERNAL_ERROR`. [FR-030][FR-035]
- Service clocks differ. Each timestamp comes from the service that owns it, and no correctness decision depends on comparing clocks across services.

**Scans and transactions**

- Transaction Service has never obtained product information. Start and Lookup work, and Scan returns `500 INTERNAL_ERROR`. [FR-023]
- A request names both an unknown transaction and an unknown SKU. The response is `404 TRANSACTION_NOT_FOUND`. For a completed transaction and an unknown SKU, it is `409 TRANSACTION_NOT_OPEN`. [FR-020]
- A database failure occurs before commit. Nothing is acknowledged, and neither the basket nor the event stream changes. A commit whose outcome is unknown returns 500, and persisted state is authoritative on lookup.
- A scan response is lost after commit. The client cannot tell whether the unit was added, and a retry adds another unit, as today. Internal redelivery of the same event does not count again.
- The same SKU is scanned again after its catalog price changed. The new unit uses the price already recorded on that SKU's basket line, as today. [FR-019]
- Inventory is reseeded with a different catalog. Replicas are refreshed per FR-008 before scans rely on them. The benchmark catalog is identical across resets, so its replica stays valid.

**Analytics**

- An event is delivered twice, or redelivered after a lost acknowledgement. It counts once. [FR-042]
- Events arrive out of order. Positions follow the documented ordering rule, without gaps, duplicates, or reassignment. [FR-043]
- Transaction Service crashes after committing a scan but before delivering its event. The event is delivered after restart. [FR-041]
- Analytics Service crashes after persisting a window but before recording its progress. The boundary is not published twice, and recomputing it gives the same result. [FR-047][FR-048]
- Analytics Service is down for a whole run. Afterward it processes the backlog in order and publishes every eligible window. [FR-041][FR-044]
- `limit` is 0, greater than 10, or invalid. As today, the response contains no items, at most ten items, or `400 INVALID_LIMIT`, respectively. [FR-047]

**Isolation, operations, and benchmarks**

- A service's credentials are used against another service's data. Access is denied. [FR-004]
- Only one service is reset. This is allowed for testing, but invariants are judged from that reset point. Benchmark baselines reset all three services. [FR-052]
- Requests time out at the client under stress. Each timed-out request's effect is all or nothing, and invariants are checked against durable state rather than client counts.
- A checkout settles after its client timed out. Stock reconciliation uses durably completed transactions, not `totalTransactions`, and the evidence explains the difference. [FR-054]

## Requirements *(mandatory)*

### Functional Requirements

**Service boundaries**

- **FR-001 [C-I]**: The system MUST consist of three independently deployable services, Transaction, Inventory, and Analytics, each with its own runtime, entry point, configuration, ports, and database. No other service may be added without a documented business need. A client-facing gateway is permitted.
- **FR-002 [C-I]**: Transaction Service MUST own the transaction lifecycle, baskets, accepted scans and their captured prices, checkout coordination and its durable progress, and accepted-scan events awaiting delivery. Inventory Service MUST own the catalog, prices, stock, low-stock thresholds, and the durable record of inventory operations. Analytics Service MUST own received scan events, analytics positions, windows, rankings, and consumption progress.
- **FR-003 [C-I]**: Transaction Service MUST serve `/transactions/**`, Inventory Service MUST serve `/items` and `/inventory/low-stock`, and Analytics Service MUST serve `/analytics/popular-items`. Business rules MUST stay in their owning service, and shared code MUST NOT contain cross-domain business logic or create runtime coupling between services.

**Database per service and replicated data**

- **FR-004 [C-II]**: Each service MUST keep its data in its own database or schema, accessed through credentials limited to it. No query, join, reference, or transaction may span two services' data, and no service may read or change another service's data.
- **FR-005 [C-II]**: Information MUST cross service boundaries only through defined internal interfaces or reliable event delivery, never through shared storage.
- **FR-006 [C-II][C-III]**: Transaction Service MUST keep a local replica of the product information needed to validate scans and to capture names and prices, with Inventory Service as the source of truth, so that Scan never requires a remote call. Once populated, the replica MUST keep Scan available during Inventory outages, including across a Transaction Service restart. The replica MUST NOT be used for any stock decision.
- **FR-007 [C-II][C-VII]**: Popular items MUST show each SKU's catalog name without Analytics Service reading another service's data. The plan chooses whether names travel with events or come from a replica of Inventory's catalog.
- **FR-008 [C-II]**: Each replica MUST have a documented source of truth, synchronization path, staleness bound, and behavior when its source is unavailable. Before a benchmark run begins, replicas MUST reflect Inventory's catalog as it stands after the latest Inventory reset or reseed.

**Internal communication**

- **FR-009 [C-III]**: Synchronous service-to-service operations MUST use gRPC with Protocol Buffers interfaces kept under version control. Each operation MUST document its request validation, business outcomes, and error statuses, and MUST NOT expose storage details.
- **FR-010 [C-III][C-V]**: Internal interfaces MUST support at least the interactions in *Required Cross-Service Interactions*, with the outcomes listed there.
- **FR-011 [C-III]**: Every remote call MUST have a deadline. A caller MUST distinguish definitive business outcomes from transient or uncertain ones, and MUST retry only operations that are idempotent under a stable identity. Retries MUST NOT create duplicate business effects.
- **FR-012 [C-III]**: Start, Scan, and Lookup MUST NOT make synchronous calls to another service.
- **FR-013 [C-III][C-IX]**: Internal interface definitions MUST evolve compatibly: a change MUST NOT break a consumer built against the previous version. A provider and a consumer built separately from the same version MUST interoperate, as shown by contract tests against the real provider.

**External compatibility**

- **FR-014 [Contract][C-IV]**: All seven existing routes MUST keep their paths, methods, request and response fields, success statuses, error statuses and codes, validation precedence, and business semantics, as summarized in *Established External Behavior*. `spec/self-checkout-openapi.yaml` remains authoritative and unchanged.
- **FR-015 [C-IV]**: One client-facing base URL, by default `http://localhost:8080`, MUST route each request to its owning service. The routing MUST be documented in deployment configuration.
- **FR-016 [Client][C-IV]**: The Load Client MUST run without any change to its source, defaults, or behavior.
- **FR-017 [C-IV]**: When a reachable service cannot give a definitive result because an outcome is pending, a dependency is unavailable or slow, or an internal error occurred, it MUST return the existing `500 INTERNAL_ERROR` response with the established `{error, message}` body. Established 4xx business errors, including every 409, keep their current meaning and precedence. When an owning service is unreachable, the gateway's existing 502 response applies. A 500 from an unsettled Complete means only that the outcome is not yet known: it MUST NOT be interpreted, documented, or implemented as a definitive cancellation, and lookup shows the settled state. None of these conditions may produce a success response or a 4xx business error, and no response may reveal internal interface details.

**Transactions and scans**

- **FR-018 [Contract][C-II]**: Start MUST durably create an OPEN transaction before returning HTTP 201. Lookup MUST return the current durable status, item count, running total, and start time, including after restarts.
- **FR-019 [Contract][C-VI]**: Each successful Scan MUST add exactly one unit at the unit price recorded on that SKU's basket line, which is captured from Transaction Service's product information when the SKU is first accepted into the transaction. The basket change and its accepted-scan event MUST commit in one local transaction before HTTP 200. A rejected Scan MUST commit neither. A repeated identical Scan request adds another unit, as today.
- **FR-020 [Contract]**: Scan validation MUST keep its established precedence: request validation (400), then unknown transaction (`404 TRANSACTION_NOT_FOUND`), then a transaction that is not open (`409 TRANSACTION_NOT_OPEN`), then an unknown SKU (`404 SKU_NOT_FOUND`).
- **FR-021 [C-VI]**: Scanning MUST NOT change stock.
- **FR-022 [C-II]**: Acknowledged transactions, basket lines, captured prices, and accepted-scan events MUST survive a Transaction Service crash and restart without manual reconstruction.
- **FR-023 [C-II]**: If Transaction Service has no usable product information, for example on first start while Inventory has never been reachable, Scan MUST return `500 INTERNAL_ERROR` (FR-017) rather than `404 SKU_NOT_FOUND`.

**Distributed checkout**

- **FR-024 [C-V]**: Complete MUST run as a Saga orchestrated by Transaction Service, without any transaction or two-phase commit that spans service databases. The plan MUST define every step, its local transaction, its compensating action or the reason it needs none, and the persisted states, consistent with *Checkout Recovery Model* and *Constitution Alignment*.
- **FR-025 [Contract][C-V]**: Complete MUST check, in this order, for an unknown transaction (`404 TRANSACTION_NOT_FOUND`), a transaction that is not open (`409 TRANSACTION_NOT_OPEN`), and an empty basket (`409 EMPTY_BASKET`) before requesting any stock effect.
- **FR-026 [C-V][C-VI]**: Inventory Service MUST apply a checkout attempt's stock effect for all basket lines atomically: all lines or none. Concurrent attempts MUST NOT oversell, lose updates, or drive stock negative. Contention or transient faults inside Inventory MUST NOT be reported as insufficient stock.
- **FR-027 [Contract][C-V]**: If any line lacks stock, Inventory MUST reject the attempt definitively and change no stock, and the transaction MUST return to OPEN with its basket, so that further scans and later completion attempts proceed as they do today. When the rejection arrives before the response deadline, Complete MUST return `409 INSUFFICIENT_STOCK` naming such a SKU.
- **FR-028 [C-V]**: Every Inventory operation for a checkout MUST carry a stable checkout-attempt identifier associated with the transaction ID. Inventory Service MUST durably record each attempt's outcome and MUST answer any repeated request for the same attempt with the original outcome. Each attempt's units MUST be deducted at most once and restored at most once.
- **FR-029 [C-V][C-VI]**: A transaction may make successive checkout attempts, but a new attempt is allowed only after the previous one settled without a standing deduction: definitively rejected, for example for insufficient stock, or restored through the reserved operation (FR-032). A transaction MUST have at most one pending attempt at a time and MUST NOT have more than one effective inventory deduction, meaning a deduction that was applied and not restored. A completion request after a definitive rejection is a new attempt, not a retry.
- **FR-030 [C-V][C-VI]**: If a checkout attempt does not settle before its response deadline, or a crash interrupts it, the client MUST NOT receive success, and a reachable Transaction Service MUST return `500 INTERNAL_ERROR` (FR-017), which may represent an uncertain outcome and is not a cancellation. The attempt MUST remain durably pending. A timeout, lost reply, or crash MUST NOT by itself be treated as an inventory failure or trigger stock restoration. Transaction Service MUST resume the attempt automatically under the same attempt identifier until Inventory gives a definitive answer, and then settle it: COMPLETED if Inventory applied the stock effect, or OPEN with the basket intact if Inventory definitively rejected the attempt for insufficient stock or an invalid request, or reports that its stock was restored (FR-032). An attempt that Inventory has not processed has not failed; Transaction Service sends it again under the same identifier. Lookup MUST show the settled state through the existing contract.
- **FR-031 [C-V]**: When a remote outcome is uncertain because of a timeout, lost reply, or crash, Transaction Service MUST NOT finalize or reopen the attempt on an assumed outcome. It MUST learn Inventory's actual outcome, either by asking for it or by repeating the request under the same attempt identifier, which returns the recorded outcome if Inventory already processed the attempt.
- **FR-032 [C-V]**: Normal checkout MUST use forward recovery. Once Inventory applies an attempt's stock effect, the attempt MUST end COMPLETED, and no timeout, lost reply, or crash triggers stock restoration. A definitive rejection MUST reopen the basket, which is the compensating action for the attempt's first step. Inventory Service MUST also keep a durable, idempotent stock-restoration operation as a reserved capability that no normal checkout path invokes:
  - It MUST restore exactly the units the attempt deducted, at most once, across duplicates, crashes, and retries.
  - When used, it MUST be retried until confirmed, and it MUST prevent any later stock request for the same attempt from taking effect.
  - It MUST NOT be used for an attempt whose transaction is COMPLETED. If it is used for a pending attempt, Transaction Service MUST treat that attempt as definitively not applied and reopen the basket.
  - The plan MUST document any operational use.
  - It MUST be verified by direct tests against Inventory Service, and those tests MUST NOT be reported as end-to-end Saga compensation tests.
- **FR-033 [C-V][C-VIII]**: Saga progress MUST be persisted in Transaction Service's database before and after each remote step, in local transactions that commit before the remote call begins and after its result is known. No database connection, lock, or open database transaction may be held while a remote call is outstanding. Recovery MUST begin automatically after a restart.
- **FR-034 [C-V][C-VI]**: Complete MUST return HTTP 200 with the established receipt only after Inventory has durably committed the attempt's deduction and Transaction Service has durably committed COMPLETED. Receipt lines and totals MUST use the captured prices.
- **FR-035 [Contract][C-V]**: While a checkout attempt is pending, a Scan or Complete for the same transaction MUST wait for the attempt to settle, for a bounded time that ends within the Load Client's default 10-second request timeout. It MUST then be applied in serial order against the settled state: if the transaction is COMPLETED, the request receives `409 TRANSACTION_NOT_OPEN`; if the transaction is OPEN again, the request is processed normally, as a scan or as a new completion attempt. If the attempt has not settled when the wait ends, the request MUST receive `500 INTERNAL_ERROR` (FR-017) and MUST NOT change the basket, start another attempt, or cancel the pending attempt. Waiting MUST NOT hold a database connection, lock, or open database transaction. No request may add units to the basket being completed. Lookup MUST answer without waiting and MUST report OPEN until COMPLETED is durable.
- **FR-036 [Contract][C-V]**: Repeating Complete after a transaction is COMPLETED MUST return `409 TRANSACTION_NOT_OPEN` with no further stock effect. This includes completions that settled after the client received `500 INTERNAL_ERROR`, restarts of any service, and duplicated or delayed internal messages.
- **FR-037 [C-V][Client]**: Waiting on Inventory, and waiting under FR-035, MUST be bounded so that, at the default benchmark load or lighter, every Scan and Complete response arrives within the Load Client's default 10-second request timeout, even when Inventory is unavailable or unresponsive.
- **FR-038 [C-VI]**: Each transaction MUST end with exactly one client-observable outcome that agrees with Inventory's recorded effects. A `500 INTERNAL_ERROR` from an unsettled Complete is not an outcome; the settled state shown by lookup is. After all attempts settle, for every SKU, initial stock minus final stock MUST equal the units in COMPLETED transactions, and no stock may be negative, including after retries, crashes, and any use of the reserved stock-restoration operation. These are settled guarantees, as defined in *Transient and Settled Guarantees*.
- **FR-039 [C-VIII][C-IX]**: Each service MUST provide documented operational means, outside the public REST contract, to tell whether recovery work is pending: pending checkout attempts, undelivered events, or eligible windows not yet published. Transaction Service MUST also be able to report the background completions that FR-054 counts.

**Reliable analytics**

- **FR-040 [C-VII]**: Each accepted-scan event MUST have a stable, unique identity and MUST be recorded in the same local transaction as its basket change (FR-019).
- **FR-041 [C-VII]**: Events MUST reach Analytics Service asynchronously and at least once. Scan and Complete MUST NOT wait for Analytics. Undelivered events MUST remain durable until Analytics confirms durable receipt, and delivery MUST resume automatically after either service restarts.
- **FR-042 [C-VII]**: Analytics Service MUST deduplicate events by identity, so duplicates and replays never change counts, positions, windows, or rankings.
- **FR-043 [C-VII]**: Each distinct accepted scan MUST receive exactly one analytics position, stored durably by Analytics Service. Positions MUST be unique, contiguous from 1, and immutable. The order MUST be fixed once established and need not match physical commit order, acceptance time, or delivery order; the plan documents the ordering rule. Late or out-of-order delivery MUST NOT create a gap, a duplicate, or a reassignment. An event that has been received but not yet positioned is pending, not a gap.
- **FR-044 [C-VII]**: The first complete window MUST cover positions 1–1,000, and later windows MUST end every 500 positions (1,500, 2,000, and so on), each containing exactly the latest 1,000 positions. Boundaries MUST NOT shift after restart or replay.
- **FR-045 [C-VII]**: Each window MUST rank SKUs by descending accepted-scan count, break ties by ascending SKU, number ranks contiguously from 1, and keep at most ten. Popularity counts accepted scans, not completed sales.
- **FR-046 [C-VII]**: Window computation, ranking, and persistence MUST remain separate stages with ordered, asynchronous hand-off and immutable data between them.
- **FR-047 [Contract][C-VII]**: A window and all its ranks MUST become visible together, only after durable persistence, and in window order. Popular items MUST return the latest such window, or the established empty response, with the established `limit` behavior.
- **FR-048 [C-VII]**: After an Analytics crash before durable receipt, after receipt but before positioning, during computation, before or after result persistence, or before acknowledging delivery, every accepted scan MUST appear exactly once in every eligible window, with no duplicate publication, using only Analytics Service's own durable data.
- **FR-049 [C-VII]**: Analytics may lag behind accepted scans. The plan MUST define the visibility delay under healthy operation, and lag MUST appear only as an older complete window.

**Operations**

- **FR-050 [C-VIII]**: Each service MUST document its build, startup, shutdown, restart, ports, configuration, database connection, credential handling, and dependencies, and MUST be buildable, startable, stoppable, and restartable on its own.
- **FR-051 [C-VIII]**: Restarting any single service MUST preserve overall correctness. The restarted service MUST resume from its own durable state without a peer restart.
- **FR-052 [C-II][C-VIII]**: Each service MUST provide its own reset and seed mechanism, and none may write another service's data. The benchmark baseline MUST consist of Inventory seeded with the current deterministic 2,000-product catalog at 10,000 units per product and a low-stock threshold of 50, and of Transaction and Analytics with no business data.
- **FR-053 [C-VIII]**: Unavailability of a service or of its database MUST produce the behavior in *Availability and Failure Behavior*.

**Performance and verification**

- **FR-054 [C-VIII][C-IX]**: The primary benchmark metric MUST remain the unchanged Load Client's `totalTransactions`, the count of client-observed successful Completes. Each run MUST also report throughput, per-operation success and error rates, and p95/p99 latency, compared with the most recent retained service-based reports. Alongside each report, and without modifying the Load Client's JSON, the evidence MUST record from durable state:
  - the number of COMPLETED transactions;
  - the number of background completions, meaning attempts that completed after Transaction Service answered their Complete with `500 INTERNAL_ERROR`;
  - any remaining difference from `totalTransactions`, which reflects successful responses that the client did not receive in time.

  No numerical improvement target applies, and correctness and durability MUST NOT be traded for throughput.
- **FR-055 [Assignment][C-IX]**: Two genuine JSON reports MUST be produced by the unchanged Load Client against the actual microservice deployment and committed: one from `java -cp out Main` (10 stations, 60 seconds) and one from `java -cp out Main --stations=100 --duration=120`, each after a separate fresh reset of all three services. Each report MUST identify its settings and the deployment used, and invariant checks MUST follow each run after all work settles. Fabricated reports and reports from earlier architectures are not evidence.
- **FR-056 [C-IX]**: Automated tests MUST cover:
  - concurrent checkouts with stock reconciliation;
  - Saga success;
  - insufficient stock with the basket reopened, which is the end-to-end compensation path;
  - forward recovery after timeouts, lost replies, and crashes of Transaction and Inventory at each Saga step;
  - idempotent retries and duplicated or delayed internal requests;
  - reconciliation of uncertain outcomes;
  - bounded waiting during a pending attempt;
  - the reserved stock-restoration operation, tested directly against Inventory Service with duplicates, crashes, and retries, and reported separately from end-to-end Saga tests;
  - database isolation;
  - internal interface contracts and compatibility;
  - recovery of undelivered events;
  - analytics windows, rankings, ties, positions, duplicates, reordering, and restarts;
  - independent failure of each service;
  - external API compatibility;
  - the unchanged Load Client.
- **FR-057 [C-IX]**: Existing tests MUST be kept or adapted without weakening their assertions. Tests that rely on cross-service database access MUST be adapted to use each service's own data or interfaces.
  - Where an existing assertion encodes behavior that this specification replaces, it MUST be replaced by an assertion of the specified behavior, and the replacement MUST be recorded together with the governing requirement. Example: a checkout that succeeds while Inventory Service is unavailable, which *Availability and Failure Behavior* now defines as `500 INTERNAL_ERROR` with the attempt pending.
  - No existing test may be deleted until each of its assertions has been migrated or replaced in this way and verified.
- **FR-058 [C-VIII]**: The plan MUST choose response deadlines and capacity that minimize background completions under healthy workloads. The default benchmark MUST produce none, and the stress benchmark's count MUST be reported and explained.

### Established External Behavior

Preserved from [Contract] and [Preserved]; contract tests remain the oracle.

| Route | Owner | Success | Established errors, in precedence order |
|---|---|---|---|
| `GET /items` | Inventory | 200, the full catalog in ascending SKU order (2,000 items at baseline) | — |
| `POST /transactions` | Transaction | 201, an OPEN transaction with zero items | 400 `MISSING_STATION_ID`, 400 `INVALID_REQUEST` |
| `POST /transactions/{id}/items` | Transaction | 200, the scan result | 400 `MISSING_SKU` or `INVALID_REQUEST`; 404 `TRANSACTION_NOT_FOUND`; 409 `TRANSACTION_NOT_OPEN`; 404 `SKU_NOT_FOUND` |
| `POST /transactions/{id}/complete` | Transaction | 200, the receipt | 404 `TRANSACTION_NOT_FOUND`; 409 `TRANSACTION_NOT_OPEN`; 409 `EMPTY_BASKET`; 409 `INSUFFICIENT_STOCK` |
| `GET /transactions/{id}` | Transaction | 200, the transaction | 404 `TRANSACTION_NOT_FOUND` |
| `GET /inventory/low-stock` | Inventory | 200; default threshold 50; alerts where stock is at or below the threshold, in ascending SKU order | 400 `INVALID_THRESHOLD` |
| `GET /analytics/popular-items` | Analytics | 200; default `limit` 10 and at most ten ranks; the empty response before the first complete window | 400 `INVALID_LIMIT` |

A reachable service returns 500 `INTERNAL_ERROR` for an unexpected failure, a pending outcome, or an unavailable dependency (FR-017). An unreachable owning service yields the gateway's existing 502.

### Data Ownership and Replicated Information

| Information | Source of truth | Copies elsewhere | Consistency expectation |
|---|---|---|---|
| Catalog: SKU, name, price | Inventory | Transaction's product replica, used for validation, names, and captured prices; Analytics' names, carried by events or held in a replica | Eventually consistent with Inventory; the catalog is fixed during benchmark runs; refreshed after an Inventory reseed; never used for stock decisions |
| Stock, thresholds, and inventory operation records | Inventory | None | Atomic and immediately consistent within Inventory |
| Transactions, basket lines with captured prices, checkout attempts and their progress | Transaction | Inventory keeps only the per-attempt quantities it needs to apply and restore stock effects | Durable before acknowledgement |
| Accepted-scan events | Transaction, until delivered | Analytics' received events | At-least-once delivery; Analytics converges to counting each event exactly once |
| Positions, windows, ranks, and consumption progress | Analytics | None | Eventually consistent with accepted scans; only complete, persisted windows are visible |

### Required Cross-Service Interactions

| Interaction | Direction | Timing | Required outcomes | Identity for safe retry |
|---|---|---|---|---|
| Provide catalog information | Inventory to a requesting service | Synchronous, never per scan | Current catalog entries; if Inventory is unavailable, the caller keeps using its last replica | Read-only |
| Apply checkout stock effect | Transaction to Inventory | Synchronous, during Complete and when resuming a pending attempt | Applied to all lines; insufficient stock, naming a SKU, with nothing applied; invalid, for example an unknown SKU, with nothing applied; refused because the attempt's stock was already restored. A repeated request returns the recorded outcome. | Checkout-attempt identifier |
| Report attempt outcome | Transaction to Inventory | Synchronous, during reconciliation; needed only if reconciliation asks for the outcome instead of repeating the apply request (FR-031) | Applied; rejected, with the reason; not processed; restored | Read-only |
| Restore an attempt's stock (reserved) | Caller documented by the plan, to Inventory | Synchronous; never invoked by normal checkout; exercised directly in tests and by any operational procedure the plan documents; retried until confirmed | Units restored; nothing to restore, with the attempt now blocked; already restored | Checkout-attempt identifier |
| Deliver accepted-scan events | Between Transaction and Analytics | Asynchronous to Scan, at least once | Durable receipt acknowledged; unacknowledged events redelivered | Event identity |

If the plan splits the stock effect into a hold and a later confirmation, the hold becomes the pivot step and the confirmation a retriable step. Both carry the same identifier and reconciliation obligations.

### Checkout Recovery Model

Normal checkout uses forward recovery. The plan defines the local transactions, persisted states, and mechanisms behind each step.

| Step | Service | Effect | Definitive failure | Uncertain outcome |
|---|---|---|---|---|
| 1. Begin attempt | Transaction | Durably records a pending attempt with its identifier and freezes the basket | If the local commit fails, the transaction stays OPEN with no attempt | Recovery finds any committed attempt and resumes it |
| 2. Apply stock effect (pivot) | Inventory | Deducts all lines atomically under the attempt identifier, or rejects the attempt with nothing applied | A rejection reopens the basket, which compensates step 1. A waiting client receives `409 INSUFFICIENT_STOCK`, or `500 INTERNAL_ERROR` for an invalid request. | Forward recovery: ask, or repeat under the same identifier, until the answer is definitive; never restore stock |
| 3. Record completion (retriable) | Transaction | Marks the transaction COMPLETED and returns the receipt to a waiting client | Cannot fail definitively; retried until committed | Retried |
| Reserved: restore stock | Inventory | Restores exactly the attempt's units, at most once, and blocks later stock requests for that attempt | — | Idempotent; retried until confirmed |

Only steps 1–3 run during normal checkout. Stock restoration is a reserved capability. No normal path invokes it, it is verified by direct tests (FR-032, SC-012), and the plan documents any operational use.

### Transient and Settled Guarantees

An attempt is **pending** from the moment Transaction Service records it until Transaction Service records its settlement. It is **settled** once it is COMPLETED, meaning Inventory applied the stock effect and Transaction Service recorded completion, or once it is rejected, meaning Inventory applied nothing and the basket is OPEN again.

| Guarantee | When it holds | Requirements |
|---|---|---|
| No success response before durable completion in both services | At all times | FR-034 |
| Stock is never negative or oversold; each attempt is deducted at most once and restored at most once | At all times | FR-026, FR-028 |
| Acknowledged scans and baskets are durable, and no unit joins a basket that is being completed | At all times | FR-019, FR-022, FR-035 |
| At most one pending attempt and at most one effective deduction per transaction | At all times | FR-029 |
| Pending attempts are durable and resume automatically after restarts | At all times | FR-030, FR-033 |
| Lookup reports OPEN even if Inventory has already applied the attempt's stock effect | Transient, while pending | FR-035 |
| A `500 INTERNAL_ERROR` from Complete can precede the attempt's completion | Transient, while pending | FR-017, FR-030 |
| Low-stock reports treat units taken by the attempt as unavailable | Transient, while pending | Assumptions |
| The transaction is COMPLETED exactly when Inventory's deduction for it stands | Once settled | FR-030, FR-038 |
| No deduction stands for a transaction that is not COMPLETED | Once settled | FR-030, FR-038 |
| Initial stock minus final stock equals the units in COMPLETED transactions | Once all attempts settle | FR-038, SC-002 |
| Exactly one terminal outcome per transaction, as shown by lookup | Once settled | FR-038 |

### Availability and Failure Behavior

| Condition | Required observable behavior |
|---|---|
| Inventory Service unavailable | `/items` and `/inventory/low-stock` return the gateway's 502. Start, Scan, and Lookup continue for transactions without a pending checkout. Complete still returns `404 TRANSACTION_NOT_FOUND`, `409 TRANSACTION_NOT_OPEN`, or `409 EMPTY_BASKET` where they apply. Otherwise it returns `500 INTERNAL_ERROR` within 10 seconds, and the attempt stays pending, with its basket frozen, until Inventory returns and the attempt settles (FR-030). Scan and Complete for a transaction with a pending checkout wait up to the bound and then return `500 INTERNAL_ERROR` (FR-035). Analytics is unaffected. A newly started Load Client cannot begin, because it fetches `/items` first. |
| Analytics Service unavailable | `/analytics/popular-items` returns the gateway's 502. All other routes are unaffected. Events are retained and delivered after restart, and windows catch up in order. |
| Transaction Service unavailable | `/transactions/**` return the gateway's 502. Inventory and Analytics routes continue, and Analytics keeps serving its latest window. On restart, pending checkouts resume and settle, and undelivered events are delivered. |
| A service's own database unavailable | That service's database-dependent operations return 500 `INTERNAL_ERROR` without acknowledging uncommitted writes. Other services keep working, except that Complete behaves as it does when Inventory is unavailable if Inventory's database is the one that is down. When databases share one server for local development, a server outage affects all services at once. |
| Any single service restarts | Peers keep running. The restarted service resumes from its own durable state. |

### Key Entities

- **Transaction**: Identity, station, status (OPEN or COMPLETED; CANCELLED remains unused by any public operation), start and completion times, item count, and running total.
- **Basket Line**: SKU, quantity, and the unit price captured when the SKU was first accepted into the transaction.
- **Accepted-Scan Event**: Stable identity, transaction, SKU, captured price, and acceptance time. It waits durably in Transaction Service (an outbox) until Analytics confirms receipt, and it counts toward popularity whether or not its transaction completes.
- **Product Replica**: Transaction Service's copy of each SKU's name and price; never authoritative for stock.
- **Checkout Attempt**: One Saga instance: its stable identifier, associated with the transaction ID; the line quantities being completed; and persisted progress. It stays pending until Inventory's outcome is known, then settles as completed, or as rejected with the transaction reopened. Normal checkout never restores its stock.
- **Product and Stock**: Inventory Service's SKU, name, price, current stock, and low-stock threshold.
- **Inventory Operation Record**: Inventory Service's durable record of each checkout-attempt identifier, its quantities, its outcome, and its restoration status, used to answer retries and reconciliation.
- **Received Scan Event**: Analytics Service's deduplicated copy of an accepted-scan event and its immutable position once assigned.
- **Popular Window**: A complete 1,000-position interval at a 500-position hop, its computation time, and up to ten ranked SKU counts; public only once fully persisted.
- **Recovery State**: Each service's durable progress: undelivered events, pending checkout attempts, background completions, and analytics consumption and publication progress.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001 [C-IV]**: 100% of applicable contract tests for the seven routes pass through the single base URL, and the Load Client source and the OpenAPI document are byte-identical to those of the prior iteration.
- **SC-002 [C-VI]**: In concurrent completion tests and after both benchmark runs settle, every SKU has nonnegative stock, and initial stock minus final stock equals the units in COMPLETED transactions. Repeated completions cause zero additional stock changes.
- **SC-003 [C-V]**: Across fault-injection tests that cover every checkout step, crashes of Transaction and Inventory, lost replies, timeouts, and duplicated or delayed internal requests, 100% of checkouts settle without manual intervention: COMPLETED exactly when Inventory applied the stock effect, and OPEN otherwise. Zero clients receive success for a checkout that is not COMPLETED, zero timeouts or lost replies trigger stock restoration, and zero attempts are deducted or restored more than once.
- **SC-004 [C-II][C-III]**: With Inventory Service stopped, 100% of valid Start and Lookup requests succeed, and so do 100% of valid Scan requests for transactions without a pending checkout, including after a Transaction Service restart. 100% of Complete requests that pass the pre-checks return `500 INTERNAL_ERROR` within 10 seconds, and after Inventory returns, 100% of those attempts settle without manual intervention.
- **SC-005 [C-VII]**: With Analytics Service stopped during a workload, zero Scan or Complete failures are attributable to it. After it restarts, published windows reach the highest eligible boundary with zero lost or double-counted scans.
- **SC-006 [C-VII]**: Across controlled streams with duplicates, reordering, and crashes at every analytics boundary, 100% of accepted scans hold exactly one position, positions run from 1 to N without gaps, 100% of eligible windows match an independent calculation, and zero partial or duplicate windows are ever visible.
- **SC-007 [C-VII]**: With all services healthy, the published window reaches the highest eligible boundary within 60 seconds after the last accepted scan of either benchmark workload.
- **SC-008 [C-II]**: 100% of attempts to read or change another service's data with a service's credentials are denied, and review finds no cross-service data access path.
- **SC-009 [C-I][C-VIII]**: Each service builds, starts, stops, and restarts on its own. Restarting any one service during a workload causes zero loss of acknowledged scans and zero violations of SC-002 or SC-006.
- **SC-010 [C-IX]**: The default benchmark run, with all services healthy, records zero errors for each of the three operations and zero background completions.
- **SC-011 [C-IX]**: Two distinct, genuine, committed JSON reports, one from the default run and one from the 100-station, 120-second run, each made after a fresh reset, report `totalTransactions`, throughput, per-operation error rates, and p95/p99 latency. They include a documented comparison with the prior service-based reports, are accompanied by the durable completion counts required by FR-054, and pass the invariant checks.
- **SC-012 [C-V][C-IX]**: In direct tests against Inventory Service, 100% of duplicated, repeated, crash-interrupted, and out-of-order stock-restoration requests restore units at most once and only for deducted attempts, and every later stock request for a restored attempt has no effect. These results are reported separately from end-to-end Saga results.

## Assumptions

- The OpenAPI document and tested handler behavior govern endpoint details. The public contract has no idempotency key for scans, so a lost scan response leaves the client uncertain, and lookup reveals only aggregate state.
- Payment always succeeds. There is no cancellation, restock, price-change, or catalog-change endpoint, and no public operation produces the CANCELLED status.
- The catalog and prices stay fixed during a benchmark run. They change only through Inventory's reset and seed or through test fixtures.
- The Load Client's default per-request timeout is 10 seconds. It never retries and counts only HTTP 200 completions in `totalTransactions`. A client-side timeout does not imply that the server rolled back. Because an attempt answered with `500 INTERNAL_ERROR` may still complete later, `totalTransactions` can undercount durable completions. Reconciliation therefore uses durable state, and the difference is recorded under FR-054.
- The external contract has no pending status and no route that returns a receipt again. While a checkout is pending, lookup shows OPEN. A checkout that completes after a `500 INTERNAL_ERROR` exposes its status, item count, and total through lookup, but not its receipt lines or completion time.
- The OpenAPI statement that every endpoint is synchronous request/response is read as governing the interaction pattern: every request receives a response, and a server error does not promise that nothing happened, as with today's rare 500 after a durable commit. Background completion after a `500 INTERNAL_ERROR` relies on this reading.
- Services, databases, and the gateway run on one machine for local development. Separate schemas in one MySQL server with per-service credentials satisfy database per service [C-II]. Internal interfaces are reachable only on the local host or a private network, so transport security and service-to-service authentication are out of scope.
- The existing gateway arrangement may be retained. No message broker, service mesh, or other infrastructure is added without a documented need [C-VIII].
- Low-stock alerts report Inventory's authoritative stock. Units taken by a pending attempt are unavailable; under forward recovery they are sold once the attempt settles, and they return only if the reserved restoration operation is used for that attempt. After all attempts settle, reported stock equals initial stock minus units sold. This is a provisional default to confirm during clarification.
- The 60-second analytics visibility ceiling in SC-007 is a provisional default. The plan may set a tighter bound under FR-049.
- Performance may decrease relative to the prior iteration. Correctness and durability are pass conditions, and throughput is measured, not targeted.

## Constitution Alignment

This specification weakens no MUST requirement of the constitution. When it was written against v5.0.0, several Saga clauses did not say how they apply to forward recovery or to transient states, and one clause had no direct counterpart in forward recovery. The MINOR amendment v5.1.0, reviewed and approved on 2026-10-20 ([constitution-amendment-proposal.md](constitution-amendment-proposal.md)), states those rules explicitly. Items 1–3 and 5 are therefore compliant with explicit clauses, item 4 remains a reading, and the plan records no deviation.

| # | Clause (as worded in v5.0.0, unless marked) | Specification position | Status under v5.1.0 |
|---|---|---|---|
| 1 | V: the plan defines each Saga step's compensating action | Begin attempt is compensated by reopening the basket. Apply stock effect, the pivot, has a reserved restoration operation that normal checkout never invokes. Record completion is a retriable final step with no compensating action. | Compliant: Principle V lets a step after a forward-recovery pivot carry a retry and idempotency rule instead of a compensating action. This was a documented deviation under v5.0.0. |
| 2 | V: Inventory operations are idempotent per transaction | Operations are keyed per checkout attempt, with an identifier associated with the transaction, and a transaction has at most one effective deduction (FR-028, FR-029). A literal single key per transaction would break the existing test that retries Complete after insufficient stock. | Compliant: Principle V keys idempotency per checkout attempt, with at most one effective deduction |
| 3 | V: no partial deduction may remain for a transaction that is not completed. VI: final-stock reconciliation, and exactly one terminal outcome that agrees with Inventory | These are settled guarantees. While an attempt is pending, Inventory may hold its deduction while lookup shows OPEN, and a client may hold a 500 (*Transient and Settled Guarantees*, FR-038). | Compliant: Principle V's consistency rule applies once the Saga settles, and Principle VI separates settled guarantees from pending-state rules |
| 4 | V: repeated completion requests return the established result | `409 TRANSACTION_NOT_OPEN`, as the existing tests require (FR-036) | Reading |
| 5 | IX: tests cover compensation | End to end: reopening the basket after insufficient stock (User Story 1, scenario 2). Directly: the reserved stock-restoration operation against Inventory (SC-012), which is not reported as an end-to-end Saga compensation test. | Compliant: Principle IX requires exactly this split |
| 6 | IV: internal errors, timeouts, and unavailability use the established external error contract | Reachable services reuse `500 INTERNAL_ERROR`, the gateway keeps its 502, and the 409s are unchanged (FR-017) | Compliant |
| 7 | II (added in v5.1.0): in-process locks, caches, wait registries, and retry schedules are never the sole authority, and safety guarantees hold across crashes, restarts, and multiple concurrent instances | Acknowledged state and idempotency records are durable (FR-019, FR-022, FR-028). No requirement assumes a single Transaction Service process; the guarantees in *Transient and Settled Guarantees* are stated for the service, not for one process. | Compliant |

## Planning Obligations

The plan MUST address at least the following and trace each item to the listed requirements:

1. **Product and price replica synchronization**: initial load, refresh, staleness bound, durability across restart, behavior while Inventory is unavailable, and refresh after an Inventory reseed (FR-006, FR-007, FR-008, FR-023).
2. **Saga recovery**: steps and local transactions, persisted states, whether the stock effect is a direct deduction or a hold followed by confirmation, attempt identifiers, deadlines, retry and backoff policy, the reconciliation method, recovery after a crash of either service, bounded waiting without holding connections or locks, and how background completions are counted and minimized (FR-024–FR-039, FR-054, FR-058).
3. **Analytics ordering and delivery**: event identity, delivery direction and acknowledgement, deduplication, the position-assignment rule, checkpoints, and the visibility delay (FR-040–FR-049).
4. **Independent database resets and seeding**: per-service reset and seed tooling and credentials, the benchmark reset sequence, and the replica refresh that follows an Inventory reseed (FR-004, FR-008, FR-052).
5. **Remaining decisions**: any operational use of the reserved stock-restoration operation (FR-032); the low-stock reading during pending attempts (Assumptions); the Constitution Check treatment in *Constitution Alignment*; gateway configuration; module layout; and packaging.
