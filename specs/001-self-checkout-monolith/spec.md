# Feature Specification: Self-Checkout Monolithic Server

**Feature Branch**: `001-self-checkout-monolith`

**Created**: 2026-09-17

**Status**: Draft

**Input**: User description: "Create a specification for the supermarket self-checkout monolithic architecture assignment. Cover catalog retrieval, starting a transaction, scanning items, completing payment, transaction lookup, low-stock reporting, and popular-item analytics. Include acceptance scenarios for concurrent checkout, atomic inventory updates, duplicate completion, database persistence, analytics window accuracy, and repeatable test initialization. Preserve the API contract and load client. Reference the existing architectural characteristics analysis without recreating it. Distinguish assignment requirements from our chosen design targets. Flag ambiguities or conflicts between the source documents."

## Requirement Sourcing

Every requirement and success criterion below is tagged with its source, so assignment-mandated
behavior is never confused with our own design targets:

- **[Contract]** — fixed by `spec/self-checkout-openapi.yaml`; any implementation must satisfy it.
- **[Assignment]** — stated in the assignment brief (`Monolithic Architecture Requirement.txt`).
- **[Constitution]** — a non-negotiable rule adopted in `.specify/memory/constitution.md`.
- **[Design target]** — a goal we chose ourselves (see `Characteristics-Analysis.md`), not graded
  pass/fail by the assignment. Numeric performance figures fall in this category.

This spec does not restate the rationale or trade-off discussion already recorded in
`Characteristics-Analysis.md`; it references that document instead of duplicating it.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Browse the catalog before checkout (Priority: P1)

A checkout station needs to know what items exist, their names, and their prices before a
customer can scan anything, so it fetches the full catalog once when it starts up.

**Why this priority**: Nothing else in the system is reachable without this — the load-testing
client itself calls this endpoint once at startup to drive every simulated customer.

**Independent Test**: Can be fully tested by requesting the catalog with no prior state and
verifying every item has a SKU, name, and price, delivering value on its own even with no
transactions yet in the system.

**Acceptance Scenarios**:

1. **Given** the store has been initialized with its baseline catalog, **When** a station
   requests the catalog, **Then** it receives every item's SKU, name, and price. [Contract]
2. **Given** the catalog has 2,000 items as required for a test run, **When** it is requested,
   **Then** all 2,000 items are returned with no duplicates or omissions. [Assignment]

---

### User Story 2 - Complete a purchase at a checkout station (Priority: P1)

A customer starts a transaction at a station, scans each item they intend to buy one unit at a
time, and then completes the transaction to pay; the store's stock accurately reflects every
completed sale, even when many stations are doing this at once.

**Why this priority**: This is the core purpose of the system — every other capability exists to
support, monitor, or report on this flow.

**Independent Test**: Can be fully tested by starting one transaction, scanning a known set of
SKUs, completing it, and confirming the returned receipt and the resulting stock levels are both
correct — independent of any analytics or reporting feature.

**Acceptance Scenarios**:

1. **Given** an item is in stock, **When** a station starts a transaction and scans one unit of
   that item, **Then** the transaction's item count and running total increase by that item's
   price, and the item's stock is *not yet* decremented. [Contract] [Constitution: Principle I]
2. **Given** an open transaction with one or more scanned units, **When** the station completes
   the transaction, **Then** the response is a receipt listing every scanned line, the total
   amount, and timestamps, and the store's stock for each scanned SKU decreases by the scanned
   quantity. [Contract]
3. **Given** a transaction with an empty basket, or a transaction that is already completed or
   cancelled, **When** completion is requested, **Then** the request is rejected and no stock
   changes occur. [Contract]
4. **Given** two different stations each hold an open transaction that has scanned the last
   remaining unit of the same SKU, **When** both stations complete their transactions at
   effectively the same time, **Then** at most one completion succeeds in decrementing that
   unit, the other is rejected or fails without decrementing, and the SKU's stock never goes
   negative. [Constitution: Principle I — concurrent checkout / atomic inventory update]
5. **Given** a transaction has already been completed successfully, **When** a completion
   request for that same transaction is submitted again (e.g., a retried client call), **Then**
   the second request does not decrement stock a second time and does not produce a second
   receipt charge. [Constitution: Principle I — duplicate completion]
6. **Given** a transaction has just been completed, **When** the completed-transaction record
   and the decremented stock are checked immediately after the success response is returned,
   **Then** both are already durably recorded (queryable even after a restart of the server
   process), not merely held in memory. [Constitution: Principle II — database persistence]
7. **Given** many stations are completing transactions concurrently for a mix of overlapping and
   distinct SKUs, **When** all completions finish, **Then** for every SKU, `initial stock − final
   stock` equals the total quantity of that SKU sold across all completed transactions, with no
   negative stock at any point. [Constitution: Principle I; Assignment]

---

### User Story 3 - Monitor low-stock inventory (Priority: P2)

A store operator (or the grading harness, standing in for one) checks which items are running low
so restocking or reporting decisions can be made.

**Why this priority**: Useful and required by the contract, but it is a read-only report that
depends on User Story 2 having already changed stock — it adds no value in isolation before any
sales have happened.

**Independent Test**: Can be fully tested by driving stock for a known SKU below a threshold
through completed transactions, then requesting low-stock alerts and confirming that SKU appears
with correct current stock and threshold.

**Acceptance Scenarios**:

1. **Given** a SKU's stock has fallen to or below the applicable threshold, **When** low-stock
   alerts are requested, **Then** that SKU appears in the results with its current stock,
   threshold, and the time the alert was generated. [Contract]
2. **Given** a caller supplies an explicit threshold override, **When** low-stock alerts are
   requested, **Then** the override is used for that request only, without changing the server's
   default threshold for other callers. [Contract]

---

### User Story 4 - Review popular-item analytics (Priority: P2)

A store analyst (or the grading harness) checks which items have been most frequently scanned
recently, to understand purchasing trends.

**Why this priority**: Required by the contract and meaningful once scanning activity exists, but
it is a downstream reporting feature, not part of the critical checkout path.

**Independent Test**: Can be fully tested by driving a known sequence of scans and then requesting
the popular-items report, independent of whether any transaction has been completed yet (scans,
not completions, drive this count).

**Acceptance Scenarios**:

1. **Given** scans are counted using a hopping window of the most recent 1,000 scans that is
   recomputed every 500 scans, **When** the popular-items report is requested at any time,
   **Then** it reflects the most recently *computed* window (not necessarily the very latest
   scan) together with that window's start and end boundaries. [Contract] [Assignment]
2. **Given** a computed window, **When** the popular-items report is requested, **Then** it
   returns at most the top 10 items ranked by scan count within that window, each with its SKU,
   name, scan count, and rank. [Contract]
3. **Given** a burst of concurrent scans arrives from multiple stations spanning a window
   boundary, **When** the next window is computed, **Then** its counts are accurate for the exact
   1,000-scan range they claim to cover. [Constitution: Principle V — analytics window accuracy]

---

### User Story 5 - Look up a specific transaction for debugging (Priority: P3)

A developer or instructor inspects a single transaction's current status while diagnosing
behavior.

**Why this priority**: Explicitly not exercised by the load-testing client's main workload;
provided for manual debugging only, so it carries the least priority.

**Independent Test**: Can be fully tested by starting a transaction, scanning a known item, and
requesting that transaction by ID to confirm its status, item count, and running total.

**Acceptance Scenarios**:

1. **Given** a transaction exists, **When** it is looked up by ID, **Then** its current status
   (open, completed, or cancelled), item count, and running total are returned. [Contract]
2. **Given** no transaction exists for a given ID, **When** it is looked up, **Then** a not-found
   result is returned. [Contract]

---

### User Story 6 - Reset the store for a repeatable test run (Priority: P2)

Before each load-test run (default or stress), the store's catalog and stock are brought back to
the same known baseline so that runs are comparable and repeatable.

**Why this priority**: Not part of the customer-facing checkout flow, but required for the
assignment's own evaluation method — without it, the two submitted load-client runs would not be
comparable to each other or reproducible.

**Independent Test**: Can be fully tested by resetting the store, verifying the catalog has the
required baseline (2,000 items, 10,000 units of stock each), running a short scripted sequence of
transactions, resetting again, and confirming the baseline is restored identically.

**Acceptance Scenarios**:

1. **Given** the store has been used by a prior test run, **When** it is reinitialized, **Then**
   its catalog contains exactly 2,000 items and every item's stock is reset to 10,000 units.
   [Assignment] [Constitution: Principle VI]
2. **Given** the store has just been reinitialized, **When** the unmodified load-testing client is
   run in default mode and then, after another reinitialization, in stress mode, **Then** both
   runs start from an identical baseline and each produces its own comparable JSON report.
   [Assignment] [Constitution: Principle VI]

---

### Edge Cases

- A scan references a SKU that does not exist in the catalog, or a transaction ID that does not
  exist, is already completed, or is cancelled. [Contract: 404 / 409 responses]
- Two stations complete transactions for the same SKU when only one unit of stock remains — only
  one may succeed; the other must fail cleanly without leaving stock negative or partially
  decremented. [Constitution: Principle I]
- A completion request is retried by a client (e.g., after a timeout) after the first attempt
  already succeeded — the retry must not double-charge or double-decrement. [Constitution:
  Principle I]
- A popular-items window boundary falls in the middle of a burst of concurrent scans from
  multiple stations — the next computed window's counts must still be exact for the scan range it
  reports. [Constitution: Principle V]
- A low-stock query threshold exactly equals a SKU's current stock — the boundary condition
  (at-or-below vs. strictly-below) must be applied consistently.
- The stress workload (100 stations, 120 seconds) must preserve every consistency and durability
  guarantee that holds under the default workload (10 stations, 60 seconds) — it must not fall
  back to weaker guarantees under higher concurrency. [Assignment] [Constitution: Architectural
  Constraints]

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST return the full catalog (SKU, name, price) for every item on request.
  [Contract]
- **FR-002**: System MUST support a baseline catalog of 2,000 items with 10,000 units of stock
  each for test runs. [Assignment]
- **FR-003**: System MUST allow starting a new transaction for a given station, returning a
  transaction identifier and its initial (open, zero-item) state. [Contract]
- **FR-004**: System MUST allow scanning one physical unit of a SKU into an open transaction,
  updating that transaction's item count and running total, and MUST reject scans against a
  transaction or SKU that does not exist, or a transaction that is not open. [Contract]
- **FR-005**: System MUST NOT decrement a SKU's stock at scan time; stock is decremented only at
  completion. [Contract] [Constitution: Principle I]
- **FR-006**: System MUST allow completing an open transaction with a non-empty basket, producing
  a receipt (line items, item count, total amount, start/completion timestamps) and decrementing
  stock by one unit for every scanned unit. [Contract]
- **FR-007**: System MUST reject completion of a transaction that is already completed, cancelled,
  or has an empty basket, without changing any stock. [Contract]
- **FR-008**: System MUST guarantee, for every SKU and under any level of concurrent completion,
  that stock never goes negative and that `initial stock − final stock` equals the exact total
  quantity sold across completed transactions for that SKU. [Constitution: Principle I,
  non-negotiable]
- **FR-009**: System MUST treat completion as idempotent per transaction: resubmitting a
  completion request for a transaction that has already been completed MUST NOT decrement stock
  again or alter the original receipt. [Constitution: Principle I]
- **FR-010**: System MUST durably persist a completed transaction's record and the resulting stock
  levels before returning a successful completion response. [Constitution: Principle II]
- **FR-011**: System MUST ensure a failure affecting one station's transaction does not corrupt
  another station's basket or the shared inventory. [Constitution: Principle II]
- **FR-012**: System MUST allow retrieving a single transaction's current status, item count, and
  running total by its ID, independent of the main checkout flow, for debugging use. [Contract]
- **FR-013**: System MUST count item scans using a hopping window of the most recent 1,000 scans,
  recomputed at every 500 scans, and MUST persist the top 10 items (SKU, name, scan count,
  rank) for the latest computed window together with its window boundaries and computation time.
  [Contract] [Assignment]
- **FR-014**: System MUST make the latest computed popular-items window retrievable on request,
  optionally limited to fewer than 10 items if requested. [Contract]
- **FR-015**: System MUST determine low-stock status per SKU against a threshold (an optional
  per-request override, or a server-side default when omitted) and MUST make the resulting list of
  alerts (SKU, name, current stock, threshold, triggered time) retrievable on request. [Contract]
- **FR-016**: System MUST support resetting its catalog and stock data to the required baseline
  between test runs without any change to the API contract or the load-testing client. [Assignment]
  [Constitution: Principle VI]
- **FR-017**: System MUST NOT require any modification to `spec/self-checkout-openapi.yaml` or to
  the load-testing client to satisfy any requirement in this spec. [Constitution: Principle III]
- **FR-018**: System MUST preserve every consistency, durability, and correctness guarantee above
  under both the default load (10 concurrent stations, 60 seconds) and the stress load (100
  concurrent stations, 120 seconds). [Assignment] [Constitution: Architectural Constraints]

### Key Entities

- **Catalog Item**: A sellable SKU with a name and a price; the catalog is the fixed universe of
  items a station can scan.
- **Stock Level**: The current on-hand unit count for a SKU; decremented only by completed
  transactions and never negative.
- **Transaction**: One customer's checkout session at one station — identifies the station,
  tracks status (open, completed, cancelled), item count, and running total from the moment it is
  started until it is completed.
- **Scanned Line / Basket**: The set of scanned units accumulated within an open transaction,
  carried through to the receipt at completion.
- **Receipt**: The durable record produced at completion — the final line items, quantities, total
  amount, and start/completion timestamps for one transaction.
- **Low-Stock Alert**: A derived, on-demand judgment that a SKU's current stock is at or below a
  threshold, with the threshold and the time it was evaluated.
- **Popular-Items Window**: A persisted snapshot of the top-ranked SKUs by scan count over a
  specific range of the most recent scans, together with that range's boundaries and when it was
  computed.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001** *(assignment-mandated)*: After both a default run (10 stations, 60 seconds) and a
  stress run (100 stations, 120 seconds), every SKU's stock satisfies `initial stock − final
  stock = total quantity sold in completed transactions`, with no SKU ever observed negative.
- **SC-002** *(assignment-mandated)*: When the same transaction's completion is submitted more
  than once, stock changes exactly once and only one receipt's worth of value is ever charged
  against inventory.
- **SC-003** *(assignment-mandated)*: The catalog and stock can be reset to an identical baseline
  (2,000 items, 10,000 units each) before each run, and both the default and stress runs can be
  repeated from that same baseline with independently comparable results.
- **SC-004** *(assignment-mandated)*: On every request, the popular-items report reflects exactly
  the 1,000 most-recently-scanned items as of the latest multiple-of-500 scans, with correctly
  stated window boundaries.
- **SC-005** *(design target, not graded pass/fail by the assignment)*: Under the default
  10-station load, 95% of start-transaction requests complete in under 200 ms, 95% of scans in
  under 100 ms, and 95% of completions in under 1 second — see `Characteristics-Analysis.md` for
  the full rationale and trade-offs behind these targets.
- **SC-006** *(qualitative)*: A grader or developer can run the unmodified load-testing client
  twice — once with default parameters, once in stress mode — against a freshly reset store, and
  obtain two independent JSON reports without any manual data cleanup between runs.

## Assumptions

- **Reasonable defaults chosen** (no reasonable default was contradicted by the source documents):
  - The exact numeric low-stock threshold is not fixed by the assignment, the OpenAPI contract, or
    `Characteristics-Analysis.md`; this spec only requires that a threshold (overridable per
    request) exists and drives alerts. The specific default value is an implementation/planning
    decision, not a specification constraint.
  - Payment always succeeds in this simulation, per the contract; there is no payment-failure path
    to specify.
  - "Popular items" are counted by item scans (one unit = one scan event), not by completed
    transactions, per the contract's domain model.

- **Ambiguities/conflicts noted between source documents** (flagged per request, resolved using
  the more precise / more authoritative source):
  - The assignment brief and `Characteristics-Analysis.md` both describe the popular-items window
    loosely as a "sliding window," while the OpenAPI contract and the ratified constitution use
    the more precise term "hopping window" (fixed-size window recomputed every 500 scans, not
    recomputed on every scan). This spec follows the contract/constitution's terminology and
    semantics as authoritative.
  - The invoking command referenced a file at `../Monolithic Architecture.txt`, which does not
    exist relative to this project. The assignment brief actually present in this repository is
    `Monolithic Architecture Requirement.txt` (project root); this spec was written from that file.
  - `Characteristics-Analysis.md` explicitly notes its latency figures are "proposed targets, not
    measured results or thresholds specified by the assignment." This spec preserves that
    distinction (see SC-005) rather than treating those numbers as contract-mandated pass/fail
    criteria.

- **Dependencies on existing artifacts**:
  - `spec/self-checkout-openapi.yaml` is the authoritative API contract and is not modified by this
    feature.
  - The existing load-testing client (`load-client/`) is used unmodified to produce the two
    required JSON reports; this spec does not alter its behavior.
  - `Characteristics-Analysis.md` remains the single source for the full architectural
    characteristics analysis and priority trade-off rationale; this spec references it rather than
    reproducing it.
