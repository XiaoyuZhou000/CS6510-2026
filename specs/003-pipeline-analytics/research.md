# Phase 0 Research: Pipeline Analytics

All technical-context questions are resolved. Decisions are constrained by the Java 21 layered
server, the existing MySQL schema, and Constitution v3.0.0.

## 1. Pipeline decomposition

**Decision**: Keep `AnalyticsService` as the only application-facing facade and lifecycle owner,
but move processing into explicit `WindowFilter`, `RankingFilter`, and `PersistenceFilter` workers.

**Rationale**: The current service combines ordering, ring-buffer mutation, ranking, retry, and
persistence. Three independently executing filters are constitutionally required, while retaining
the facade preserves the existing transaction and API boundaries.

**Alternatives considered**: Keeping one service with a background checkpoint task was rejected
because it is not a three-stage pipeline. Refactoring transaction or HTTP responsibilities into
filters was rejected as out of scope and harmful to layer ownership.

## 2. Pipe implementation and workers

**Decision**: Use three default-unbounded `LinkedBlockingQueue` instances: facade-to-Window,
Window-to-Ranking, and Ranking-to-Persistence. Run one named platform thread per filter and start
the downstream workers before opening ingestion.

**Rationale**: `LinkedBlockingQueue` is a JDK-only asynchronous FIFO pipe; its unbounded form makes
`offer` non-blocking with respect to capacity, and `take` lets idle workers wait efficiently.
Explicit threads make worker identity, termination, and a shared shutdown deadline straightforward.

**Alternatives considered**: `ArrayBlockingQueue` can fill and block/reject; `SynchronousQueue` has
no buffering; `PriorityBlockingQueue` is not FIFO; polling a concurrent collection is not a
blocking pipe; an embedded broker and three executors add dependencies or hidden queues without
benefit.

## 3. Global ordering and ingestion/shutdown race

**Decision**: Serialize the facade's health check and ingress `offer` under a short lifecycle lock.
The single Window consumer assigns monotonically increasing scan positions after dequeue. Shutdown
uses the same lock to change `RUNNING` to `DRAINING` and enqueue an end marker after the last
accepted event.

**Rationale**: This defines one total FIFO order across concurrent producers, prevents an event
from being enqueued behind the drain marker, and keeps mutable window state out of producers. The
critical section contains no database work and cannot wait for queue capacity.

**Alternatives considered**: Producer-side `AtomicLong` positions were rejected because concurrent
enqueue order can differ from counter order. Unsynchronized close and enqueue were rejected because
an accepted event could land behind the drain marker.

## 4. Immutable message contracts

**Decision**: Use validated immutable Java records for `AcceptedScan`, `WindowSnapshot`,
`RankedItem`, and `RankedWindow`, plus typed end-of-stream control messages. Snapshot and rank lists
are defensively copied with `List.copyOf`; the Window Filter materializes the ring in logical scan
order rather than sharing or cloning mutable storage blindly.

**Rationale**: Each receiver gets all required data without shared mutable state. Typed control
messages avoid `null` (which queues forbid) and allow completion to flow in stage order.

**Alternatives considered**: Arrays and shared ring/count maps were rejected as mutable. Direct
downstream method calls and data records overloaded with sentinel values were rejected as ambiguous
or non-pipelined.

## 5. Window formation and restart recovery

**Decision**: The Window Filter owns the position counter and 1,000-slot ring. It seeds the counter
from `PopularWindowStore.readMaxWindowEnd()`, tracks events accepted since this process started,
and emits only at a 500-position boundary after at least 1,000 fresh events are available. It emits
no partial snapshot on shutdown.

**Rationale**: A fresh process emits `[1,1000]`, then `[501,1500]`. After a persisted end `N` with
no recoverable ring content, the first new result is `[N+1,N+1000]`, preserving current recovery
tests and avoiding a fabricated overlap.

**Alternatives considered**: Publishing at 500 events was rejected because the feature requires a
complete 1,000-event window. Persisting the ring or adding a durable event log was rejected as a
schema and scope expansion.

## 6. Deterministic ranking

**Decision**: The Ranking Filter alone counts snapshot SKUs, sorts by count descending and SKU
ascending, selects at most ten, and assigns contiguous ranks from one.

**Rationale**: This preserves the public result and existing validation while giving the filter one
clear responsibility and no database dependency.

**Alternatives considered**: Ranking in Window collapses two required filters; ranking in the
database or Persistence Filter mixes computation with durable mapping and would change existing
layer behavior.

## 7. Ordered persistence and retry

**Decision**: The Persistence Filter consumes one ranked window at a time and retains that exact
head item until `PopularWindowStore.writeWindow` succeeds. Preserve short retry delays of 50 ms and
200 ms, then retry at a capped 1-second interval, logging every retry. It never dequeues a newer
window while the older one is failing.

**Rationale**: A single FIFO consumer provides strict commit order and keeps later windows from
becoming visible first. The store already writes the header and all ranks in one JDBC transaction
and exposes only committed data.

**Alternatives considered**: Parallel writers, maximum-attempt dropping, dead-lettering, and moving
failed work to the queue tail were rejected because they permit overtaking or loss. A new uniqueness
constraint was rejected because the feature preserves `db/init.sql`; ambiguous post-commit driver
failure remains a documented single-process/schema limitation to be covered by adapter tests and
application-side duplicate verification if implementation evidence shows it is reachable.

## 8. Orderly and forced shutdown

**Decision**: A single end marker travels downstream. Window consumes all earlier scans and forwards
the marker after its snapshots; Ranking forwards it after its rankings; Persistence exits after all
earlier results commit. The facade joins all three workers against one absolute configurable
deadline, default 5,000 ms. On expiry or caller interruption it interrupts workers, enters a failed/
forced terminal state, and logs failure rather than clean completion. Repeated shutdown is
idempotent.

**Rationale**: Stage-ordered completion drains accepted complete work exactly once without a marker
overtaking output still being produced upstream. One deadline ensures the configured five seconds
does not accidentally become five seconds per worker.

**Alternatives considered**: Immediate interruption can discard work. Placing markers into every
queue at once can terminate downstream before upstream output arrives. An unbounded shutdown wait
can hang forever during database failure.

## 9. Health and terminal worker failure

**Decision**: Track lifecycle as `NEW`, `RUNNING`, `DRAINING`, `TERMINATED`, `FAILED`, or `FORCED`.
A common worker wrapper records the first unexpected failure, closes ingestion, emits a terminal
structured record, and interrupts peer workers. Later ingestion logs and throws; latest committed
queries remain available.

**Rationale**: A dead worker must not leave the facade advertising healthy acceptance. Keeping reads
available preserves useful durable results, and the transaction layer's existing sink-failure
isolation prevents analytics failure from undoing basket admission.

**Alternatives considered**: Relying on daemon exit or an uncaught stack trace was rejected as
silent operational failure. Rolling back the checkout scan was rejected because analytics is
observational. In-memory queues deliberately do not promise crash durability.

## 10. Structured operational logging

**Decision**: Add an analytics-owned injectable logger whose default emits one JSON object per line
to stderr. Stable fields are `timestamp`, `eventType`, and `stage`, plus event-specific queue depth,
retry count, window bounds, outcome, or sanitized failure type/reason. Log backlog only when a new
high-water threshold is crossed (starting at a configurable testable threshold and doubling) to
avoid per-event noise.

**Rationale**: JSON lines are machine-readable without a new dependency, endpoint, or database
table. Injection makes failure and shape assertions deterministic. Analytics-local serialization
does not create a dependency on the API package.

**Alternatives considered**: Free-text stderr is not reliably testable; per-event logging would
damage throughput; a health endpoint, persisted state, metrics service, or logging framework would
change scope or public behavior.

## 11. Schema, API, and toolchain preservation

**Decision**: Retain Java 21, plain `javac`/Bash scripts, JUnit Platform 1.11.0, MySQL Connector/J
9.0.0, `PopularWindowStore`, all existing tables, all seven HTTP routes, the OpenAPI file, and the
load client. Add `ANALYTICS_SHUTDOWN_TIMEOUT_MS` configuration in the composition root with a 5,000
ms default and constructor injection for deterministic tests.

**Rationale**: No external dependency or persistent model change is needed. Fixed contracts and
repeatable workloads are required for architectural comparison.

**Alternatives considered**: Maven/Gradle, dependency-injection or logging frameworks, schema
migrations, new endpoints, and client changes were rejected as unnecessary and prohibited scope.

## 12. Verification strategy

**Decision**: Retain existing tests and add filter-level queue tests, concurrent-ingestion and
boundary/shutdown race tests, retry/no-overtaking tests, worker-failure and JSON-log tests, bounded
forced-shutdown tests, and architecture checks for three filters and queue-only data flow. Finish
with live contract/integration tests and fresh-reset default/stress load runs plus invariant SQL.

**Rationale**: End-to-end output alone cannot prove independent workers, immutable boundaries,
no direct stage calls, deterministic failure handling, or a true bounded drain.

**Alternatives considered**: Load testing alone was rejected because it cannot deterministically
exercise retry, crash, or exact boundary races. Unit testing alone was rejected because it cannot
prove unchanged HTTP/database behavior and load-client compatibility.

## Residual risks

- Unbounded queues trade producer responsiveness for heap-growth risk under sustained downstream
  outage; high-water logs expose but do not cap this risk.
- Permanent persistence failure intentionally prevents a clean drain; forced shutdown is bounded
  and reported unsuccessful rather than dropping the oldest result silently.
- A process crash can lose partial and queued in-memory analytics work; only committed windows are
  durable, consistent with the feature assumptions.
- Without a schema uniqueness constraint, a rare ambiguous commit acknowledgement can duplicate a
  boundary unless the database adapter verifies an already-committed equivalent before retry.
