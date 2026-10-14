# Feature Specification: Distributed Domain Services with Durable Analytics Recovery

**Feature Branch**: `service-based-architecture`

**Created**: 2026-10-11

**Status**: Draft

**Input**: User description: "Evolve the existing Pipeline Architecture into independently deployable Transaction, Inventory, and Analytics domain services using one centralized MySQL database, durable accepted scans, recoverable analytics, the unchanged REST contract and load client, and reproducible default and stress evidence."

## Baseline and Sources

- **[C-I]–[C-VIII]** refer to the corresponding principles in `.specify/memory/constitution.md` v4.0.0.
- **[Contract]** refers to `spec/self-checkout-openapi.yaml`, the current HTTP handlers, and contract tests. Current tests and handlers clarify error details not enumerated by OpenAPI.
- **[Preserved]** refers to the current `server/` implementation, `db/init.sql`, the existing automated tests, and the prior specifications under `specs/001-*`, `specs/002-*`, and `specs/003-*`.
- **[Assignment]** refers to `Service Based Architecture Requirement.txt` and the requested default and stress submissions.

Today one process serves all seven routes. It creates durable transaction records and commits completed sales atomically, but keeps open baskets only in memory until completion. A scan can be acknowledged after an in-memory basket update even if delivery to the in-process Analytics Pipeline fails. The pipeline publishes complete windows, but its previous restart rule skips overlap because it cannot recover lost scans. This feature changes those recovery properties while retaining the established checkout, REST, and analytics business semantics. Constitution v4.0.0 supersedes the earlier pipeline specification's restart skip. [C-I][C-III][C-V][C-VI][Preserved]

The technical plan must assess reuse of the existing analytics logging and persistence mechanisms and retain tested behavior where practical; this specification does not select the replacement or reuse strategy. [C-V][Preserved]

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Finish Checkout Across a Restart (Priority: P1)

As a shopper, I can start checkout, scan items, and complete payment with the same public responses even if Transaction Service stops after acknowledging scans. [Contract][C-I][C-III]

**Why this priority**: Checkout is the primary business operation; an acknowledged item must remain in its transaction and eventual sale.

**Independent Test**: Start a transaction, scan a controlled basket, stop and restart only Transaction Service, then look up and complete the transaction and reconcile its receipt and stock.

**Acceptance Scenarios**:

1. **Given** a new transaction, **When** a station scans one SKU twice and another SKU once, **Then** lookup shows three accepted units and the correct running total while stock is unchanged.
2. **Given** those three scans returned success, **When** Transaction Service terminates unexpectedly and restarts, **Then** lookup still shows OPEN with the same count, total, and start time, and completion uses all three units.
3. **Given** an open nonempty basket, **When** completion succeeds, **Then** a receipt follows the atomic commit of completed state and every inventory deduction; each SKU decreases by exactly its scanned quantity.
4. **Given** a completed transaction, **When** completion is requested again, including after restart, **Then** the existing `409 TRANSACTION_NOT_OPEN` response is returned and stock remains unchanged.
5. **Given** two shoppers scanned the last unit of one SKU, **When** they complete concurrently, **Then** at most one succeeds, the other changes no stock or completed state, and stock never becomes negative.

---

### User Story 2 - Query Inventory Independently (Priority: P1)

As a checkout station or operator, I can retrieve the catalog and committed low-stock alerts while another domain service is stopped or restarting, provided the shared database and client-facing path are available. [Contract][C-II][C-III]

**Why this priority**: The unchanged load client needs the catalog before checkout, and the operator needs accurate stock information.

**Independent Test**: Run Inventory Service against the central database, query catalog and low stock, restart it without restarting peers, then verify that committed sales appear in its results.

**Acceptance Scenarios**:

1. **Given** the reset baseline, **When** `/items` is requested, **Then** 2,000 unique products with the established SKU, name, and price fields are returned.
2. **Given** committed inventory and an optional threshold, **When** `/inventory/low-stock` is requested, **Then** alerts, threshold behavior, ordering, and response fields match existing behavior.
3. **Given** Inventory Service restarts while Transaction Service runs, **When** checkout requests are made, **Then** checkout continues; Inventory Service resumes current queries after restart.

---

### User Story 3 - Recover Popular Items from Durable Scans (Priority: P1)

As an operator, I can query accurate popular items after Analytics Service crashes, restarts, or falls behind while checkout continues. [C-V][C-VI]

**Why this priority**: Popularity must count every accepted scan, including those accepted during an analytics outage, without repeated counts after replay.

**Independent Test**: Submit a known scan sequence, interrupt Analytics Service at position assignment (if deferred), consumption, and persistence boundaries, scan during the outage, restart it, and compare every completed window with an independent calculation from the durable assigned analytics positions.

**Acceptance Scenarios**:

1. **Given** fewer than 1,000 accepted scans and no prior complete window, **When** popular items are queried, **Then** the established empty response appears; no partial window is shown.
2. **Given** 1,000 accepted scans, **When** analytics catches up, **Then** the first complete persisted window covers positions 1–1,000; after 500 more scans the next covers 501–1,500.
3. **Given** Analytics Service stops before consuming, during processing, before persisting, or just after persisting a complete window, **When** it restarts against intact shared data, **Then** all scans eventually appear in their assigned-position windows exactly once, with no duplicate or partial window visible.
4. **Given** Analytics Service is unavailable but the database works, **When** Transaction Service accepts valid scans, **Then** they succeed and persist independently; after analytics returns, all resulting complete windows appear in order.
5. **Given** tied counts and more than ten SKUs, **When** a complete window is queried, **Then** at most ten ranks appear, ordered by descending count and SKU ascending for ties.
6. **Given** concurrently committed scans, including scans assigned positions after commit if that design is used, **When** ordering and analytics catch up, **Then** each committed scan has one immutable position, assigned positions are gapless, and windows use assigned order even if it differs from physical database commit order.

---

### User Story 4 - Keep One Stable Client Interface (Priority: P2)

As a station or evaluator, I use the existing base URL and seven established routes without changing the load client or knowing service locations. [Contract][C-IV]

**Why this priority**: Architecture iterations are comparable only when the public client contract is stable.

**Independent Test**: Start services separately, run current contract tests and the unchanged load client through the documented base URL, and stop one service at a time while checking unrelated routes.

**Acceptance Scenarios**:

1. **Given** all services and the database are available, **When** the unchanged client uses one base URL, **Then** existing paths, methods, request/response structures, statuses, validation errors, and domain errors remain compatible.
2. **Given** one domain service stops, **When** a route owned by another running service is requested, **Then** it remains usable if the shared database and client-facing path are available.
3. **Given** a route's owner is unavailable, **When** that route is requested, **Then** it does not return a fabricated business success or misclassify the outage as a validation failure.

---

### User Story 5 - Reproduce Distributed Evidence (Priority: P2)

As a maintainer or evaluator, I can build, start, restart, and measure each service, and reproduce two real load-client reports from fresh baseline data. [Assignment][C-VII][C-VIII]

**Why this priority**: Separate source directories alone do not demonstrate an independent distributed deployment.

**Independent Test**: Follow startup instructions, reset the database before each run, execute the unchanged default and stress commands, inspect their JSON and database invariants, and restart a single service without restarting peers.

**Acceptance Scenarios**:

1. **Given** documented configuration, **When** each service is built, started, stopped, and restarted independently, **Then** peers continue operating subject to shared database availability.
2. **Given** the 2,000-product, 10,000-unit-per-product reset baseline, **When** `java -cp out Main` runs against the distributed deployment, **Then** a genuine JSON report is produced and retained in the repository.
3. **Given** a separate fresh reset, **When** `java -cp out Main --stations=100 --duration=120` runs, **Then** a distinct genuine JSON report is produced and retained in the repository.
4. **Given** either run has finished, **When** results are reviewed, **Then** throughput, request success/failure rates, p95/p99 latency, stock reconciliation, analytics correctness, and recovery checks are recorded or reproducibly verifiable.

### Edge Cases

- Repeating the same valid scan request is another physical-unit scan under the existing contract, which has no request idempotency key. A retry after a lost response can therefore be ambiguous to the client. Internal event replay is different: it must not add another count. [Contract][C-V]
- A scan racing with completion has one serial outcome: accepted and included in the completed receipt and event stream, or rejected as not open with neither state nor event change. [C-I][C-III]
- Invalid station IDs, missing SKUs, unknown transactions/SKUs, empty baskets, already completed transactions, invalid query values, and insufficient stock retain current statuses and error meanings; rejected scans create no event. [Contract]
- A database failure before commit produces no success acknowledgement and leaves no partial scan state/event or completion/inventory change. If commit succeeds but the response is lost, persisted state is authoritative on lookup or retry. [C-I][C-III]
- A database outage cannot make an uncommitted scan appear acknowledged or expose a partial analytics result. Services resume from persisted state when connectivity returns. [C-III][C-V]
- Concurrent scans at a window boundary and delayed position assignment preserve one deterministic durable global order and exact 1,000-scan membership. A committed scan awaiting materialization is not a missing event or a permanent gap. [C-V][C-VI]
- While a newer window is incomplete or uncommitted, the endpoint returns the latest older complete window, or the established empty response. Older boundaries represent temporary lag, not permanent event loss. [Contract][C-VI]
- Duplicate event delivery, interrupted processing, and a crash between completed-window persistence and consumption progress cannot create duplicate counts, divergent results for a boundary, or progress past missing events. [C-V]
- Prolonged Analytics Service downtime leaves a recoverable backlog; after restart, windows catch up in original order with correct overlap. [C-V][C-VI]

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001 [C-II]**: Provide Transaction, Inventory, and Analytics business-domain services as independently buildable, configurable, runnable, deployable, and restartable units, each with its own runtime and entry point. An additional service requires a documented business need.
- **FR-002 [C-II]**: Transaction Service owns creation, scans, completion, and lookup; Inventory Service owns catalog and low-stock queries; Analytics Service owns persisted-scan consumption, windowing, ranking, result persistence, and popular-item queries. Each keeps request handling, business rules, and persistence access distinct internally.
- **FR-003 [C-III]**: All services use the same configurable centralized MySQL database as their primary coordination mechanism. No domain operation requires a direct runtime call to another domain service. Shared persistence components may be reused without transferring business-rule ownership.
- **FR-004 [C-III]**: Transaction Service may read catalog data to validate and price scans and must update inventory directly within atomic completion. Inventory Service reads catalog and committed inventory. Analytics Service reads committed accepted scans, catalog display details, and committed popularity results. The plan documents these shared-data workflows.
- **FR-005 [Contract][C-IV]**: Expose all seven current paths and methods at one documented client-facing base URL usable by the unchanged load client. Preserve request/response fields, success statuses, current error statuses and codes, and synchronous REST behavior without gRPC or unnecessary new public APIs.
- **FR-006 [Contract][C-III]**: `POST /transactions` creates a durable OPEN transaction before HTTP 201. `GET /transactions/{id}` returns its current durable status, item count, running total, and start time after restart as well as during normal operation.
- **FR-007 [Contract][C-III]**: Each successful `POST /transactions/{id}/items` represents one physical unit and atomically commits the corresponding accepted transaction/basket state and required scan-event information before HTTP 200. Rejected scans commit neither. Scanning never decrements inventory.
- **FR-008 [C-III]**: Previously acknowledged open transaction state and scans survive unexpected Transaction Service termination; after restart, lookup, further valid scans, and completion use all acknowledged units without manual reconstruction.
- **FR-009 [Contract][C-I]**: A repeated valid scan of an OPEN transaction retains the existing meaning of one more unit. Repeated completion after success retains `409 TRANSACTION_NOT_OPEN` and deducts no inventory again.
- **FR-010 [C-I]**: Completion atomically verifies sufficient stock, commits all required deductions and completed transaction state, or commits none. Concurrent completions prevent overselling and lost updates. The success receipt follows commit.
- **FR-011 [Contract]**: Preserve receipt line quantities, price/total calculations, timestamps, transaction statuses, empty-basket rejection, unknown transaction/SKU behavior, and current invalid-request semantics.
- **FR-012 [Contract][C-II]**: Inventory Service serves the complete catalog and low-stock alerts from current committed data, retaining the existing threshold default/override, response fields, and ordering.
- **FR-013 [C-V]**: Analytics event ordering MUST be deterministic, durable, and recoverable; it need not match physical database transaction commit order. Concurrently committed accepted scans MAY receive analytics positions through a database-backed materialization process. Once assigned, positions MUST be immutable, unique, and gapless across committed materialized events. Concurrent acceptance, delayed assignment or visibility, restart, and replay cannot skip an accepted event, change its assigned window membership, or count it twice.
- **FR-014 [C-V]**: Analytics Service obtains input from persisted database state, including scans committed while it was stopped, without Transaction Service invoking its pipeline. Normal processing uses efficient incremental in-memory state; durable data suffices for complete reconstruction.
- **FR-015 [C-VI]**: Analytics retains distinct Window, Ranking, and Persistence stages in that order, with asynchronous ordered queue-based handoffs and immutable interstage data. Popularity counts accepted scans, not completed sales.
- **FR-016 [C-VI]**: The first complete window covers global scan positions 1–1,000; subsequent windows end at 1,500, 2,000, and every later 500-scan step, each containing exactly the most recent 1,000 accepted scans. Boundaries do not shift after restart or replay.
- **FR-017 [C-VI]**: Each complete window ranks by descending accepted-scan count, uses ascending SKU for ties, assigns contiguous ranks from 1, and retains at most ten items.
- **FR-018 [C-V][C-VI]**: A complete window and all ranks become visible atomically only after successful persistence. Results appear in window order; incomplete, in-flight, failed, or partial results are invisible. The popular-items endpoint returns the latest complete persisted window or the established empty response, with existing `limit` behavior.
- **FR-019 [C-V]**: Analytics recovery handles a crash before position assignment, before event consumption, during processing, before result persistence, and after result persistence but before consumption progress is recorded. It eventually assigns and reflects every durable accepted scan in every eligible window exactly once, with no duplicate publication or incorrect rank.
- **FR-020 [C-V]**: Reprocessing a persisted event or window boundary yields the same visible result. Recovery after prolonged downtime processes all retained scans in original order and preserves overlapping windows.
- **FR-021 [C-II][C-V]**: With the shared database available, Analytics Service failure/restart does not inherently block valid Transaction Service scans or completions. Failure/restart of Transaction or Inventory Service likewise leaves unrelated running services able to fulfill their responsibilities.
- **FR-022 [C-III][C-IV]**: When a service is unavailable, its routes fail or indicate service unavailability rather than report false business success; other available routes continue. A temporary central database outage prevents acknowledgement of uncommitted writes and preserves committed data. A reachable handler encountering an unexpected database failure retains the existing HTTP 500 `INTERNAL_ERROR` JSON behavior rather than returning an incorrect business result.
- **FR-023 [C-I][C-III]**: Database failure, interrupted writes, unexpected shutdown, and restart preserve all-or-nothing effects for each accepted scan and completion. A lost response after a successful commit does not undo committed state; lookup and retry reflect the existing contract.
- **FR-024 [C-VII]**: Document each service's build, startup, port, database configuration, dependencies, shutdown, and independent restart. Document the client-facing route arrangement without requiring a particular gateway, packaging format, or service mesh.
- **FR-025 [C-VII][C-VIII]**: Verify and measure throughput, per-operation success/failure rates, p95/p99 tail latency, inventory reconciliation, analytics windows/rankings, and recovery under default and 100-station/120-second workloads. Durability and consistency take priority over throughput; no numerical improvement target is imposed without a baseline.
- **FR-026 [Assignment][C-VIII]**: Commit two genuine JSON reports produced by the unchanged load client against the actual distributed deployment and centralized database using `java -cp out Main` and `java -cp out Main --stations=100 --duration=120` after separate fresh baseline resets. Prior-architecture reports and fabricated data are not evidence.
- **FR-027 [C-VIII]**: Automated verification covers public compatibility, independent startup/restart, checkout and inventory invariants, durable acknowledged scans, duplicate handling, analytics interruption at all specified boundaries, replay after downtime, and result visibility. Preserve or adapt relevant existing tests without weakening their assertions.

### Service Ownership and Shared Persisted Data

| Service | Business capabilities | Persisted data read | Persisted data changed |
|---|---|---|---|
| Transaction | Start, scan, complete, lookup | Transaction and accepted-scan state; product identity/price; stock at completion | Open/completed transaction state, accepted scans, stock deductions upon successful completion |
| Inventory | Catalog and low-stock queries | Product identity/price and committed stock/thresholds | None in required HTTP workflows |
| Analytics | Recover/process scans; serve popular items | Accepted scans and their durable analytics order, product display details, complete popularity results and recovery state | Complete popularity results and durable progress needed for recovery; assigned positions if database-backed materialization is selected |

Transaction validation and pricing require catalog reads; atomic checkout requires direct inventory updates. Analytics reads accepted scans written by Transaction Service and persists derived results without changing checkout state. These are shared-database workflows, not direct service calls. The plan determines exact tables, transaction boundaries, recovery representation, event consumption, and reusable persistence code. [C-II][C-III][C-V]

### Availability and Failure Behavior

| Condition | Required observable behavior |
|---|---|
| Transaction Service unavailable | Transaction routes fail or indicate service unavailability; Inventory and Analytics routes remain usable when their services, database, and client-facing path work. Committed transactions/scans remain intact. |
| Inventory Service unavailable | Catalog/low-stock routes fail or indicate service unavailability; running Transaction checkout and Analytics processing remain usable through direct database access. A newly started load client may be unable to begin because it fetches `/items` first. |
| Analytics Service unavailable | Popular-items route fails or indicates service unavailability; Transaction accepts and durably records scans and completions, while Inventory queries continue. On restart, Analytics catches up. |
| One service restarts | Other services continue their work; the restarted service reconstructs correct state from committed data without a peer restart. |
| Central database unavailable | No service acknowledges an uncommitted write or claims fresh durable state. Database-dependent operations fail until connectivity returns; committed state stays recoverable and Analytics resumes without gaps or double counting. |

### Key Entities

- **Transaction**: Checkout identity and station, OPEN or completed state, start/completion times, accepted unit count, prices, running total, and eventual receipt lines.
- **Accepted Scan**: One successful one-unit SKU scan associated with a transaction. Its unique durable analytics position may be assigned after scan commit; it counts even if the transaction never completes.
- **Product and Inventory State**: SKU, display name, price, committed stock, and low-stock threshold; stock changes only on successful completion.
- **Popular Window**: A complete 1,000-position accepted-scan interval at a 500-position hop, its computation time, and up to ten ranked SKU counts; only fully persisted windows are public.
- **Analytics Recovery State**: Durable information sufficient to find unprocessed scans, reconstruct overlapping windows, and avoid publishing a boundary twice; its representation is a planning choice.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001 [C-I][C-III]**: In restart tests, 100% of acknowledged scans remain in open transaction lookup and final receipt calculations; zero acknowledged scans disappear, and scan-only actions change stock by zero.
- **SC-002 [C-I]**: In concurrent completion tests and both workloads, every SKU has nonnegative final stock and `initial stock − final stock = quantity in completed transactions`; repeated completion changes inventory zero additional times.
- **SC-003 [C-IV]**: All seven routes pass applicable contract tests through one documented base URL; the unchanged load client runs default and stress commands against the distributed deployment.
- **SC-004 [C-II]**: Each of the three services can be built, started, stopped, and restarted separately; in every single-service interruption test, unrelated running operations continue when shared database and client-facing access work.
- **SC-005 [C-V][C-VI]**: Across controlled concurrent streams and every specified crash point, 100% of committed materialized scans have unique, immutable, gapless analytics positions and 100% of eligible complete windows match an independent calculation in assigned-position order for 1,000-scan membership, 500-scan advancement, counts, top-ten membership, and ties; zero partial or duplicate windows appear.
- **SC-006 [C-V]**: After short and prolonged Analytics outages, the highest published complete window eventually reaches the highest eligible boundary of all durably accepted scans, with zero permanently missing or double-counted events. Temporary lag appears as an older published boundary.
- **SC-007 [C-VIII]**: Two distinct committed JSON reports identify actual settings and distributed target: one default run and one 100-station, 120-second run, each after a fresh 2,000-product, 10,000-units-per-product reset. They include measured throughput, success/failure rates, and p95/p99 latency; associated checks establish inventory, analytics, and recovery correctness.

## Assumptions

- The OpenAPI document and tested handler behavior govern endpoint details. The public contract has no scan idempotency key: identical successful scan calls each mean another unit. Internal replay of one durable event cannot count again.
- A lost HTTP response after commit can leave a caller uncertain whether a scan was accepted. Lookup reveals aggregate state, but this feature does not promise client-level exactly-once retries or add a public request identifier.
- The mandated shared MySQL system survives service-process failures and temporary database outages without losing committed data. Catastrophic database data loss is outside scope.
- Payment remains assumed successful by the simulation. No payment processor, cancellation endpoint, new business feature, UI, or load-client change is in scope.
- Catalog data and prices remain stable during a baseline run, as in the current reset script. The baseline has 2,000 products with 10,000 stock units each.
- There is no established numerical throughput or latency improvement target. Default and stress runs provide measured evidence; correctness is a pass condition even if performance changes.
- Event-table design, the choice of commit-time position assignment or database-backed materialization, event consumption, checkpoint representation, exact database transaction design, routing mechanism, Java module layout, packaging, and deployment topology are reserved for `$speckit-plan`.
