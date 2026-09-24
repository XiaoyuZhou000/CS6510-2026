# Feature Specification: Migrate to Layered Architecture

**Feature Branch**: `layered-architecture`

**Created**: 2026-09-22

**Status**: Draft

**Input**: User description: "the migration from monolithic to layered architecture"

## Requirement Sourcing

Requirements in this specification use the following source tags so assignment obligations are
distinguished from project quality goals:

- **[Assignment]** — required by `Layered Architecture Requirement.txt`.
- **[Contract]** — fixed externally by `spec/self-checkout-openapi.yaml` and the unmodified load
  client.
- **[Constitution]** — required by `.specify/memory/constitution.md`, version 2.0.0.
- **[Migration]** — necessary to preserve the working self-checkout behavior while changing its
  internal organization.
- **[Design target]** — a measurable project goal rather than a contract-mandated threshold.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Complete Checkout Without Behavioral Change (Priority: P1)

A shopper uses a checkout station to start a transaction, scan items, and complete payment with
the same responses and inventory outcomes as before the migration.

**Why this priority**: The architectural change has no value if it breaks the store's primary
customer journey or changes the fixed client contract.

**Independent Test**: Run the transaction lifecycle through the existing public interface and
verify the receipt, transaction state, and inventory changes without relying on analytics or
operator reports.

**Acceptance Scenarios**:

1. **Given** a freshly initialized store and an available item, **When** a shopper starts a
   transaction, scans that item, and completes payment, **Then** every public response conforms
   to the existing contract and stock is decremented exactly once at completion. [Contract]
2. **Given** concurrent shoppers attempt to buy overlapping inventory, **When** their
   transactions complete, **Then** no stock becomes negative and the decrease in stock equals
   the quantities recorded in completed sales. [Constitution]
3. **Given** a completion request is repeated for a transaction that already completed, **When**
   the repeated request is processed, **Then** no second stock decrement or second completed sale
   is created. [Constitution]
4. **Given** one shopper's request fails, **When** other stations continue checking out, **Then**
   their baskets and the shared inventory remain correct. [Constitution]

---

### User Story 2 - Change Responsibilities Safely by Layer (Priority: P1)

A maintainer can identify where request handling, transaction behavior, analytics behavior, and
data access belong, and can change one responsibility without needing to understand or alter
unrelated concerns.

**Why this priority**: Explicit separation of responsibilities is the purpose of the migration
and the core deliverable for this assignment.

**Independent Test**: Classify every server component and every cross-component dependency,
then verify that each component has one declared layer, each responsibility is owned by the
correct layer, and every dependency follows the permitted direction.

**Acceptance Scenarios**:

1. **Given** the migrated server, **When** a maintainer traces any incoming request, **Then**
   request translation is confined to the API layer and business behavior is delegated to the
   transaction or analytics layer. [Assignment] [Constitution]
2. **Given** any inventory, completed-sale, catalog, or analytics persistence operation, **When**
   its call path is inspected, **Then** database interaction is confined to the database-access
   layer. [Constitution]
3. **Given** the server's dependency map, **When** dependencies are reviewed, **Then** they flow
   from the API layer to a transaction or analytics layer and from those layers to the
   database-access layer, with no reverse or circular dependency. [Constitution]
4. **Given** files or modules have been moved or renamed, **When** their behavior is reviewed,
   **Then** the migration is accepted only if responsibilities and runtime delegation also obey
   the layer boundaries. [Constitution]

---

### User Story 3 - Preserve Inventory and Operational Reporting (Priority: P2)

A store operator can retrieve the catalog, inspect low-stock items, look up a transaction, and
view the latest popular-item results exactly as before the migration.

**Why this priority**: These required capabilities support store operations and demonstrate that
the migration preserves the complete public contract, not only the checkout happy path.

**Independent Test**: Exercise each read-only public capability against known catalog,
transaction, inventory, and scan data and compare the responses to the contract.

**Acceptance Scenarios**:

1. **Given** a known initialized catalog, **When** it is requested, **Then** every item is returned
   with the contract-defined identity, name, and price. [Contract]
2. **Given** stock at or below an applicable threshold, **When** low-stock information is
   requested, **Then** the qualifying items and their current stock are returned correctly.
   [Contract]
3. **Given** a known transaction, **When** it is retrieved by identifier, **Then** its current
   status, item count, and total are returned correctly. [Contract]
4. **Given** at least 1,000 scans with a known distribution, **When** the latest popular-item
   result is requested after a recomputation boundary, **Then** it contains the correct top 10
   ranking and boundaries for the most recent computed 1,000-scan window. [Constitution]

---

### User Story 4 - Validate the Migrated System Under Required Loads (Priority: P2)

A grader or maintainer can initialize the store, run the unchanged load client in default and
stress modes, and retain evidence that the layered version remains correct and usable.

**Why this priority**: The assignment requires both reports, and load validation exposes boundary
or concurrency regressions that isolated behavior checks can miss.

**Independent Test**: From a clean baseline, run the unchanged client with 10 stations for 60
seconds, reset, run it with 100 stations for 120 seconds, and verify that each run produces a
distinct timestamped report while preserving data invariants.

**Acceptance Scenarios**:

1. **Given** a fresh baseline of 2,000 items with 10,000 units each, **When** the default workload
   runs, **Then** it completes using the unmodified client and produces a timestamped JSON report.
   [Assignment] [Constitution]
2. **Given** the store has been reset to the same fresh baseline, **When** the stress workload
   runs, **Then** it completes using the unmodified client and produces a separate timestamped
   JSON report. [Assignment] [Constitution]
3. **Given** either load run, **When** its completed sales and final inventory are reconciled,
   **Then** the stock conservation, non-negative stock, durable completion, and analytics-window
   guarantees still hold. [Constitution]

---

### User Story 5 - Verify Responsibilities in Isolation (Priority: P3)

A maintainer can validate request translation, transaction rules, analytics rules, and data
access independently through explicit layer contracts.

**Why this priority**: Independent verification reduces regression risk and is evidence that the
new boundaries are real, but it follows preservation of user-facing behavior and required load
validation.

**Independent Test**: Substitute a controlled collaborator at each layer boundary and verify the
layer's outcomes without starting unrelated layers.

**Acceptance Scenarios**:

1. **Given** controlled transaction and analytics collaborators, **When** public requests are
   presented to the API layer, **Then** request validation, delegation, response translation, and
   error mapping can be verified without database access. [Constitution]
2. **Given** controlled data-access behavior, **When** transaction rules are exercised, **Then**
   basket lifecycle, pricing, completion, idempotency, and inventory coordination can be verified
   without serving public requests. [Constitution]
3. **Given** a controlled scan sequence and controlled persistence behavior, **When** analytics
   rules are exercised, **Then** window boundaries, counts, ranking, and result persistence can be
   verified without serving public requests. [Constitution]

### Edge Cases

- A request fails validation before reaching a business layer; no transaction, analytics, or
  database state changes occur.
- A data-access operation fails during transaction completion; neither inventory nor the
  completed-sale record is partially committed, and the API returns no false success.
- Two completions contend for the final unit of a SKU; at most one succeeds and no layer caches or
  assumes a stale inventory result.
- A duplicate completion arrives after a successful response was lost; the original durable
  result remains authoritative and stock is unchanged by the retry.
- Concurrent scans cross an analytics recomputation boundary; exactly one coherent result is
  exposed for the claimed scan range.
- The store is reset between load runs while prior in-progress state exists; the next run begins
  from the documented clean baseline with no state leaking from the previous run.
- A lower layer encounters invalid or unavailable data; the failure is communicated through an
  explicit layer contract and translated once at the public boundary.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The migrated system MUST preserve every operation, request and response shape,
  status outcome, and synchronous interaction defined by the existing public contract.
  [Contract] [Migration]
- **FR-002**: The migrated system MUST continue to work with the unmodified load client.
  [Assignment] [Contract]
- **FR-003**: Every server component MUST be assigned to exactly one of four responsibility
  layers: API, transaction, analytics, or database access. [Assignment] [Constitution]
- **FR-004**: The API layer MUST own public-request validation, request and response translation,
  and public status/error mapping, and MUST delegate all business behavior. [Constitution]
- **FR-005**: The API layer MUST NOT implement inventory, transaction, analytics, pricing, or
  persistence rules. [Constitution]
- **FR-006**: The transaction layer MUST own transaction and basket lifecycle, scan processing,
  pricing, completion, idempotency, and coordination of inventory consistency. [Constitution]
- **FR-007**: The analytics layer MUST own scan-window state, recomputation after every 500 scans,
  ranking of the most recent computed 1,000-scan window, and the popular-item result exposed to
  callers. [Constitution]
- **FR-008**: The database-access layer MUST own all durable catalog, inventory, completed-sale,
  idempotency, and popular-item reads and writes, including transaction boundaries and persistence
  mappings. [Constitution]
- **FR-009**: No layer other than the database-access layer may execute database operations
  directly. [Constitution]
- **FR-010**: Dependencies MUST flow from the API layer into the transaction or analytics layer
  and from those layers into the database-access layer; reverse and circular dependencies MUST
  NOT exist. [Constitution]
- **FR-011**: Cross-layer collaboration MUST use explicit contracts whose inputs, outputs, and
  failure outcomes can be verified independently. [Constitution]
- **FR-012**: Moving or renaming existing monolithic components without changing responsibility
  ownership and runtime delegation MUST NOT be considered completion of the migration.
  [Constitution]
- **FR-013**: The system MUST decrement stock only when a transaction completes, and the stock
  checks, decrements, idempotency outcome, and completed-sale record MUST succeed atomically or
  have no effect. [Constitution]
- **FR-014**: A repeated completion of the same transaction MUST NOT decrement stock or create a
  completed sale more than once. [Constitution]
- **FR-015**: A successful completion response MUST be returned only after inventory and the
  completed-sale record are durable. [Constitution]
- **FR-016**: Concurrent completions MUST preserve non-negative stock and the invariant that the
  baseline stock minus final stock equals the quantity sold in completed transactions for every
  SKU. [Constitution]
- **FR-017**: Popular-item results MUST remain accurate for the claimed 1,000-scan window, MUST be
  recomputed every 500 scans, and MUST persist the top 10 items with window boundaries.
  [Contract] [Constitution]
- **FR-018**: The database MUST be reinitializable to exactly 2,000 catalog items with 10,000 units
  of stock per item before each validation run. [Constitution]
- **FR-019**: Validation MUST include one default run with 10 concurrent stations for 60 seconds
  and one stress run with 100 concurrent stations for 120 seconds, each from a fresh baseline.
  [Assignment] [Constitution]
- **FR-020**: The migrated feature MUST retain two distinct timestamped JSON reports produced by
  the unmodified client, one for each required workload, and each report MUST identify its
  workload settings. [Assignment] [Constitution]
- **FR-021**: Layer-boundary verification MUST cover all public call paths and MUST detect direct
  data-access bypasses, reverse dependencies, circular dependencies, and business rules placed in
  the API layer. [Constitution]

### Key Entities

- **Transaction**: A shopper's checkout session, identified uniquely and carrying lifecycle
  status, station identity, basket contents, totals, and start/completion times.
- **Basket Line**: The quantity and price contribution of a SKU within an open or completed
  transaction.
- **Catalog Item**: A sellable product identified by SKU with a name and unit price.
- **Inventory Record**: The durable available stock for one SKU, changed only by successful
  transaction completion.
- **Scan Event**: One unit scan associated with a transaction and ordered within the analytics
  stream.
- **Popular-Item Window**: A durable ranked result for a claimed scan range, including boundaries,
  computation time, and up to 10 ranked items with counts.
- **Layer Contract**: The explicit inputs, outputs, failure outcomes, and permitted direction of a
  collaboration between adjacent responsibility layers.
- **Load Report**: Timestamped evidence from one validation workload, including workload settings,
  success/error counts, throughput, and latency distributions.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of public contract checks that passed before migration also pass against the
  layered version without modifying the contract or load client.
- **SC-002**: A responsibility and dependency audit assigns 100% of server components to exactly
  one required layer and finds zero direct database-access bypasses, reverse dependencies,
  circular dependencies, or business rules owned by the API layer.
- **SC-003**: Across both required load runs, every SKU finishes with non-negative stock and 100%
  of stock changes reconcile with quantities in completed transactions.
- **SC-004**: Repeating a completion request in every duplicate-completion validation case causes
  zero additional stock decrements and zero additional completed-sale records.
- **SC-005**: For controlled scan sequences that cross at least two recomputation boundaries,
  100% of returned popular-item counts, ranks, and window boundaries match the expected
  1,000-scan ranges.
- **SC-006**: A maintainer can validate each of the four layers independently through its explicit
  contracts without starting unrelated outer layers.
- **SC-007**: The unchanged client completes a 10-station, 60-second run and a 100-station,
  120-second run from separate clean baselines, producing two distinct timestamped JSON reports
  that record the correct settings.
- **SC-008** *(design target)*: Under the default workload, at least 95% of start operations finish
  within 200 ms, scans within 100 ms, and completions within 1 second, without weakening any
  consistency or durability outcome.
- **SC-009** *(qualitative)*: In review, a maintainer can explain where a requested change belongs
  and trace its permitted collaborators from the layer definitions without inspecting unrelated
  business concerns.

## Assumptions

- This feature migrates the existing self-checkout server rather than creating a second public
  service; it may remain a single deployable process.
- Existing customer and operator behavior is the compatibility baseline. The public contract,
  load-client behavior, catalog size, initial stock, analytics semantics, and correctness
  guarantees remain unchanged.
- The four layers named by the assignment and constitution are the complete set for this
  migration. Internal subdivisions are acceptable only when every component still has one clear
  owner among those layers and all dependency rules remain intact.
- In-progress baskets may remain non-durable and may be lost on process failure; completed sales,
  inventory changes, and computed popular-item results retain their existing durability
  guarantees.
- Performance numbers in SC-008 are project design targets, not assignment-mandated pass/fail
  thresholds. Correctness and durability take priority over latency.
- The existing OpenAPI specification and load client are external dependencies and are not
  changed by this feature.
- The existing architectural characteristics analysis remains the source for priority and
  trade-off rationale; this migration does not recreate it.
