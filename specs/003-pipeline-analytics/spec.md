# Feature Specification: Pipeline Analytics

**Feature Branch**: `pipeline-architecture`

**Created**: 2026-09-29

**Status**: Draft

**Input**: User description: "Implement a three-stage asynchronous analytics pipeline within the existing layered architecture: Window Filter → Ranking Filter → Persistence Filter, connected by Java BlockingQueue pipes. Preserve the OpenAPI contract and load client."

This specification uses the following source labels:

- **[Assignment]** — required by `Pipeline Architecture Requirement.txt`.
- **[Constitution]** — required by Self-Checkout System Constitution v3.0.0.
- **[Preserved]** — existing externally observable behavior that this migration must retain.
- **[Design]** — a project decision that makes the required behavior testable.

## Clarifications

### Session 2026-09-30

- Q: What should happen when an analytics pipeline queue fills during a load spike? → A: Use unbounded FIFO queues; accept every event immediately and monitor backlog growth.
- Q: How long should orderly shutdown wait for the analytics pipeline to drain before reporting failure and forcing termination? → A: Use a configurable timeout with a 5-second default.
- Q: How should operators and tests observe pipeline backlog, retries, shutdown state, and worker failure without changing the public API? → A: Use structured server logs only.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Produce Accurate Windowed Rankings (Priority: P1)

As a store operator, I want accepted item scans to flow through independent windowing,
ranking, and persistence stages so that the popular-items endpoint reports the correct
top items for each completed analytics window. [Assignment][Constitution]

**Why this priority**: Accurate windowed analytics is the functionality selected by the
assignment for pipeline refactoring. Without an accurate end-to-end result, the new
architecture provides no usable value.

**Independent Test**: Submit a controlled sequence of accepted scans, wait for each
complete window to be published, and verify its boundaries, counts, deterministic rank
order, and overlap with the preceding window.

**Acceptance Scenarios**:

1. **Given** a fresh analytics state, **When** 999 accepted scans have entered the
   pipeline, **Then** no completed popular-items window is published.
2. **Given** a fresh analytics state, **When** the 1,000th accepted scan is processed,
   **Then** the system publishes a ranking for scans 1 through 1,000.
3. **Given** the first completed window, **When** another 500 scans are processed,
   **Then** the next published ranking covers scans 501 through 1,500.
4. **Given** multiple SKUs with equal counts in a completed window, **When** the ranking
   is produced, **Then** ties are ordered by SKU in ascending order and rank positions
   remain contiguous from 1.
5. **Given** more than ten distinct SKUs in a completed window, **When** its ranking is
   published, **Then** only the first ten deterministically ranked items are returned.

---

### User Story 2 - Keep Checkout Responsive During Analytics Work (Priority: P1)

As a shopper using a checkout station, I want scanning and payment correctness to remain
independent of analytics computation and persistence so that analytics work cannot
corrupt or roll back my checkout. [Constitution][Preserved]

**Why this priority**: Inventory consistency and durable completion outrank analytics and
must remain correct while the analytics stages operate asynchronously or recover from a
failure.

**Independent Test**: Introduce slow or temporarily failing analytics persistence while
multiple stations scan and complete transactions, then verify checkout outcomes and
inventory invariants while the analytics pipeline retains ordered work.

**Acceptance Scenarios**:

1. **Given** analytics persistence is temporarily unavailable, **When** shoppers scan
   valid items and complete transactions, **Then** checkout correctness and inventory
   consistency are preserved.
2. **Given** an older completed analytics window cannot be persisted, **When** newer
   windows reach the persistence stage, **Then** no newer window becomes visible before
   the older window succeeds.
3. **Given** persistence recovers after a transient failure, **When** queued windows are
   processed, **Then** every retained complete window is persisted once in window order.
4. **Given** concurrent checkout stations submit accepted scans, **When** the events are
   processed, **Then** each accepted event occupies exactly one position in a single
   analytics order and none is silently discarded.

---

### User Story 3 - Stop and Recover the Pipeline Predictably (Priority: P2)

As an operator, I want pipeline startup, shutdown, and failure behavior to be predictable
so that accepted analytics work is either completed or its failure is clearly visible.
[Constitution][Design]

**Why this priority**: An asynchronous design introduces worker and queue lifecycle risks
that did not exist in the same form when all analytics responsibilities were concentrated
in one service.

**Independent Test**: Start and stop the server with events at different pipeline stages,
verify that accepted complete windows drain in order, verify workers terminate, and then
restart from an existing persisted recovery point.

**Acceptance Scenarios**:

1. **Given** accepted events and complete windows remain in flight, **When** an orderly
   shutdown begins, **Then** new ingestion stops, already accepted work drains in stage
   order, and all pipeline workers terminate within the configured shutdown timeout,
   which defaults to five seconds.
2. **Given** shutdown cannot drain because persistence remains unavailable, **When** the
   shutdown bound expires, **Then** forced termination is reported as a failure rather
   than as a clean shutdown through a structured server log record.
3. **Given** the latest persisted window ended at scan 1,000 and prior ring-buffer content
   is unavailable after restart, **When** the pipeline restarts, **Then** it waits for a
   full 1,000 new accepted scans before publishing window 1,001 through 2,000.
4. **Given** a pipeline worker terminates unexpectedly, **When** the system continues to
   operate, **Then** a structured server log identifies the failed stage and new events
   are not silently reported as successfully accepted by a non-functioning pipeline.

---

### User Story 4 - Preserve External Compatibility and Evidence (Priority: P2)

As a maintainer or evaluator, I want the server contract and workload driver to remain
unchanged so that pipeline results can be compared fairly with earlier architectures.
[Assignment][Constitution]

**Why this priority**: The assignment evaluates internal architecture using the same API
and load client. Changing either would invalidate the comparison.

**Independent Test**: Run the existing contract suite and unchanged load client against
the pipeline server, then retain one default and one stress report from freshly reset
databases.

**Acceptance Scenarios**:

1. **Given** the existing OpenAPI contract, **When** all endpoint contract tests run,
   **Then** request shapes, response shapes, status codes, and routes remain unchanged.
2. **Given** the existing load client, **When** it runs with default parameters against a
   freshly initialized database, **Then** it completes without client modification and
   produces a timestamped JSON report.
3. **Given** the existing load client, **When** it runs with 100 stations for 120 seconds
   against another freshly initialized database, **Then** it completes without client
   modification and produces a distinct timestamped JSON report.
4. **Given** either workload has completed, **When** database invariants are checked,
   **Then** stock remains non-negative and completed quantities reconcile with inventory
   changes.

### Edge Cases

- The pipeline receives zero scans or stops before the first 1,000-scan window is
  complete; no partial result is published.
- A shutdown occurs exactly as scan 1,000 or a later 500-scan boundary is accepted; the
  resulting complete window is processed once.
- Multiple producers submit events concurrently at a window boundary; every event is
  assigned one unique position and exactly one window membership pattern.
- A completed window contains fewer than ten distinct SKUs; every distinct SKU is
  returned with contiguous ranks and no placeholder rows.
- A completed window contains equal counts for several SKUs; repeated runs over the same
  ordered input produce the same ranking.
- Queue backlog grows during a load spike; submission remains independent of queue
  capacity, no event is silently dropped, and structured logs expose backlog growth.
- Ranking or persistence is slower than ingestion for an extended period; backlog remains
  ordered and is observable.
- Persistence fails after a window header or some rank data would otherwise be written;
  no partial window becomes visible.
- The latest persisted result is queried while a newer window is in flight; the endpoint
  returns the older complete result.
- A worker is interrupted or terminates unexpectedly; the pipeline does not continue to
  advertise healthy ingestion without surfacing the failure.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001 [Assignment]**: The system MUST refactor only windowed popular-item analytics
  into a pipeline; transaction, catalog, inventory, and HTTP responsibilities MUST remain
  within their existing layered boundaries.
- **FR-002 [Assignment][Constitution]**: The analytics pipeline MUST contain three
  independently executing stages in this order: window formation, ranking, and
  persistence.
- **FR-003 [Constitution][Design]**: Stages MUST exchange work through unbounded,
  asynchronous FIFO pipes and MUST NOT invoke a downstream stage directly.
- **FR-004 [Constitution]**: Data passed between stages MUST be immutable and MUST contain
  all event or window information needed by the receiving stage.
- **FR-005 [Constitution]**: The system MUST establish one serial analytics order across
  accepted scan events from all concurrent checkout stations.
- **FR-006 [Constitution][Design]**: While ingestion is open and pipeline workers are
  healthy, submitting an accepted scan event MUST NOT wait for pipe capacity, and every
  submitted event MUST be processed exactly once. Events MUST NOT be silently discarded.
- **FR-007 [Preserved]**: The window stage MUST maintain a 1,000-scan hopping window and
  emit the first complete snapshot at scan 1,000 and each subsequent snapshot after 500
  additional scans.
- **FR-008 [Constitution]**: Each emitted snapshot MUST identify inclusive window start and
  end positions and MUST contain exactly the accepted scans assigned to that window.
- **FR-009 [Preserved]**: The ranking stage MUST count occurrences by SKU, order counts
  descending, break equal-count ties by SKU ascending, assign contiguous ranks beginning
  at 1, and retain at most ten results.
- **FR-010 [Constitution]**: The persistence stage MUST commit each window and its ranks as
  one complete durable result so partial windows never become visible.
- **FR-011 [Constitution]**: Complete windows MUST become visible in ascending window order.
  A failed older window MUST be retained and retried before a newer window is published.
- **FR-012 [Preserved]**: The popular-items query MUST return the latest successfully
  persisted complete window and MUST NOT read partial or in-flight stage state.
- **FR-013 [Constitution]**: The analytics facade MUST be the only pipeline boundary
  exposed to other layers and MUST coordinate ingestion, persisted-result queries,
  startup, health, and shutdown.
- **FR-014 [Constitution]**: An orderly shutdown MUST reject new analytics ingestion,
  drain already accepted work through all stages in order, and wait for all stage workers
  to terminate within a configurable timeout that defaults to five seconds.
- **FR-015 [Constitution]**: A shutdown that cannot drain within its bound and an unexpected
  worker termination MUST be observable as failures.
- **FR-016 [Constitution][Design]**: Analytics backlog, persistence retries, ingestion
  rejection, shutdown outcome, and terminal worker failure MUST be reported through
  structured server logs only. Records MUST identify the event type, affected stage,
  timestamp, and relevant queue depth, retry count, window boundary, or shutdown outcome.
  The feature MUST NOT add a public health endpoint or persist pipeline health state.
- **FR-017 [Constitution]**: Analytics delay or failure MUST NOT alter transaction state,
  decrement inventory early, weaken atomic completion, or compromise idempotency.
- **FR-018 [Preserved]**: After restart from an existing persisted window without recoverable
  in-memory window content, the system MUST collect a full 1,000 new scans before
  publishing the next complete non-overlapping recovery window.
- **FR-019 [Assignment][Constitution]**: The OpenAPI document, endpoint behavior, database
  initialization baseline, and load client MUST remain unchanged by this feature.
- **FR-020 [Assignment]**: The repository MUST retain one timestamped JSON report from a
  default load run and one distinct report from a 100-station, 120-second stress run,
  each performed after a fresh database initialization.
- **FR-021 [Assignment]**: Submission documentation MUST name the three pipeline stages,
  state each stage's purpose, describe their order, and identify the asynchronous pipe
  mechanism connecting them.

### Key Entities

- **Accepted Scan Event**: One successfully admitted physical-unit scan, identified by SKU
  and assigned one unique position in the global analytics order.
- **Window Snapshot**: An immutable, complete 1,000-event view with inclusive start and end
  positions, emitted at each eligible 500-event boundary.
- **Ranked Window**: An immutable result derived from one window snapshot, containing its
  boundaries and no more than ten deterministically ordered SKU counts.
- **Persisted Popular Window**: The durable, atomically visible representation of one
  ranked window and the source of responses from the popular-items endpoint.
- **Pipeline Operational Record**: A structured server log record describing backlog,
  retry, ingestion rejection, worker termination, draining, or shutdown outcome, with the
  stage and event-specific diagnostic values needed for operators and automated tests.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For controlled sequences at and beyond 1,000 scans, 100% of published windows
  have the expected 1,000-scan membership and advance by exactly 500 scans.
- **SC-002**: Across repeated and concurrent test runs, 100% of accepted scan events are
  assigned exactly once to the serial analytics stream, with no silent loss or duplicate
  processing.
- **SC-003**: For every completed test window, reported counts, top-ten membership, rank
  positions, and tie ordering match an independently calculated expected result.
- **SC-004**: During injected persistence failures, zero newer windows become visible ahead
  of the oldest failed window, and all retained windows become visible in order after
  recovery.
- **SC-005**: Orderly shutdown tests account for 100% of work accepted before shutdown and
  terminate all analytics workers within the configured timeout, which defaults to five
  seconds; any forced termination is reported as unsuccessful.
- **SC-006**: The unchanged contract suite passes with zero externally observable API
  regressions, and the unchanged load client completes both required workloads.
- **SC-007**: Under the default 10-station workload, p95 transaction-start latency remains
  below 200 ms, p95 scan latency remains below 100 ms, and p95 completion latency remains
  below 1 second without weakening correctness guarantees.
- **SC-008**: Both default and stress runs finish with non-negative stock and exact
  reconciliation between completed sales and inventory reduction.
- **SC-009**: The repository contains exactly the two selected submission reports for this
  iteration—one default and one 100-station, 120-second stress report—and documentation
  identifies the three analytics stages and their connections.
- **SC-010**: Captured server logs contain machine-readable records for every injected
  retry, ingestion rejection, terminal worker failure, and forced shutdown, and backlog
  records identify the affected stage and queue depth without exposing those details
  through the public API.

## Assumptions

- The pipeline is an internal refactoring of popular-item analytics only; a full-system
  pipeline conversion is explicitly out of scope.
- The existing four-layer server remains the baseline, and the pipeline is contained
  within the analytics layer except for its existing database abstraction.
- The assignment-mandated in-process asynchronous pipe mechanism and worker arrangement
  are architectural constraints. All stage pipes are unbounded FIFO queues so ingestion
  does not block on queue capacity; thread lifecycle mechanics and health representation
  will be selected during planning.
- An "accepted scan" means a valid SKU that has been successfully admitted to an open
  in-memory basket. Rejected scans do not enter analytics.
- Analytics is observational: a pipeline failure does not undo an already admitted basket
  scan, but the failure must be visible and must not be mislabeled as successful analytics
  ingestion.
- Existing persisted popular-window tables and atomic write semantics remain suitable;
  no public response or database initialization change is required.
- In-memory partial windows are not durable. After restart, the next result is published
  only after a full new 1,000-scan window, preserving the current recovery behavior.
- A partial window remaining at a clean shutdown is not published because it does not
  satisfy the 1,000-scan definition; complete windows already emitted must drain.
- The shutdown timeout is configurable for operations and deterministic failure-path
  testing; when no value is supplied, the server uses five seconds.
- Pipeline operational visibility is provided only through structured server logs; no
  public health route, persisted health record, or separate metrics interface is added.
- Existing catalog size, initial stock, default workload, stress workload, and latency
  targets remain unchanged.
