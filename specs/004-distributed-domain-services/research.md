# Research: Distributed Domain Services with Durable Analytics Recovery

## R1. Accepted scan durability and ordering

**Decision:** Use one InnoDB transaction for each successful scan. Lock the transaction row, verify `OPEN`, read the catalog item, upsert its `transaction_line`, increment durable `transaction.total_amount`, then lock a singleton `scan_sequence` row, allocate the next position, insert one `accepted_scan`, and commit before returning HTTP 200. Rejected scans roll back all changes. The sequence row starts at zero and is updated in the same transaction as the event, so aborted scans leave no position gap. All scan writers take locks in the same transaction-row → sequence-row order. Retry deadlocks as an entire transaction, within a bounded policy; never acknowledge an ambiguous failed commit.

**Rationale:** The existing `TransactionService` acknowledges an in-memory basket before its optional `AcceptedScanSink` succeeds. This selected InnoDB counter design gives one contiguous, commit-visible analytics order across concurrent station threads. The product requirement does not require positions to follow physical database commit order. A consumer can require the next position and detect a data-integrity gap. [MySQL locking reads](https://dev.mysql.com/doc/refman/8.0/en/innodb-locks-set.html) and [transaction/rollback semantics](https://dev.mysql.com/doc/refman/8.0/en/commit.html) support this design.

**Alternatives considered:** An `AUTO_INCREMENT` event ID is unique but may have rollback gaps and allocation order can differ from commit visibility; see [MySQL's auto-increment handling](https://dev.mysql.com/doc/refman/8.4/en/innodb-auto-increment-handling.html). A timestamp cannot uniquely order concurrent scans. A database-backed materializer is also compliant: it may assign positions after concurrent commits if it durably records the work, assigns each committed event once, commits an immutable unique gapless prefix, and resumes pending work after a crash. It would reduce scan-path sequence contention but add materialization lag and recovery logic. Assigning positions only in the Analytics JVM loses scans during outages. Scanning `MAX(position)+1` without a locked sequence row races under concurrency.

## R2. Transaction recovery and completion

**Decision:** Treat `transaction` and `transaction_line` as the authoritative open basket. Preserve one line per `(transaction_id, sku)` with quantity and captured unit price. On scan, update the line and running total in the same transaction as the event. On lookup and completion, reconstruct from committed rows, including the database `started_at`. Completion locks the transaction row, rejects missing/non-OPEN/empty transactions, reads lines in SKU order, conditionally decrements inventory in the same database transaction, changes status and completion time, and commits before returning the receipt. Sort inventory updates by SKU and retry deadlocks as whole operations.

**Rationale:** The existing `JdbcCheckoutCompletionStore` already has conditional stock decrements and a guarded status update, but it currently inserts lines only at completion and `TransactionService` keeps OPEN baskets solely in a process map. Retaining the same tables and completion logic minimizes the change while making acknowledged scans recoverable.

**Alternatives considered:** Rebuilding a basket only from accepted events duplicates transaction state and can change price/receipt semantics. Retaining process-local baskets as the source of truth fails an unexpected restart. Calling Inventory Service during completion would break the required one-transaction stock commit.

## R3. Analytics checkpoint and crash recovery

**Decision:** Add singleton `analytics_checkpoint(last_window_end)` initialized to zero and a unique key on `popular_window.window_end`. A poller reads committed `accepted_scan` rows in ascending position on fresh autocommit/READ COMMITTED statements, checks contiguity, and enqueues immutable positioned events. The Window Filter maintains a 1,000-entry in-memory ring; it emits on absolute ends 1,000, 1,500, 2,000, and so on. Ranking remains deterministic. Persistence locks the checkpoint row and atomically inserts one complete header, all ranks, and the next checkpoint value. It accepts only end 1,000 after zero or the previous end plus 500. On an ambiguous retry, it verifies an existing boundary and ranks for equivalence; conflicting results fail visibly. The latest endpoint reads only committed materialized windows.

On restart, read checkpoint `W`. If `W>0`, prefill the ring from accepted positions `W-999..W` without emitting. Then poll from `W+1`; if `W=0`, start at position 1. Positions processed since `W` but without a persisted complete window are replayed. Keep an in-memory poll cursor only for the current run; never persist it independently of the window. Use bounded queues or demand-based polling to bound memory while the database holds the full backlog. Consumer queries use a fresh read view so a long-lived snapshot cannot hide newer commits; see [MySQL isolation levels](https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html).

**Rationale:** The existing `WindowMath` restart rule explicitly skips overlap because scans were not durable. Persisting a boundary and its ranks together makes that boundary the recovery checkpoint. Recovery rebuilds exactly the overlap needed for the next window; a crash before commit replays the candidate, while a crash after commit starts after it. Existing `AnalyticsLog`, `RankingFilter`, immutable `PipelineMessage`, FIFO `PersistenceFilter`, and atomic `JdbcPopularWindowStore` are reusable with revised input and checkpoint semantics.

**Alternatives considered:** Keeping the current process-local counter and skip rule omits overlap. Persisting a per-event consumption cursor ahead of the result can lose a window after a crash. Recomputing all history on every startup is correct but needlessly slow. A broker adds infrastructure and still requires transactionally durable accepted scans.

## R4. Service boundaries and client routing

**Decision:** Build three independent thin Java 21 service artifacts with separate `main` entry points and configurable ports: Transaction on 8081, Inventory on 8082, Analytics on 8083 by default. A static Nginx reverse proxy on 8080 sends `/transactions` and descendants to Transaction, `/items` and `/inventory/low-stock` to Inventory, and `/analytics/popular-items` to Analytics. Preserve request method, path, query, body, upstream status, and response body. An unavailable upstream returns proxy 502/503; a reachable service experiencing a database failure retains the existing JSON 500 `INTERNAL_ERROR` mapping. The proxy is routing infrastructure required by the one-base-URL contract, not a fourth domain service.

**Rationale:** The unchanged load client defaults to `http://localhost:8080` and preloads `/items`. A router outside the three domain processes permits unrelated paths to continue while a service restarts. Domain services coordinate through MySQL, not direct runtime calls.

**Alternatives considered:** Hosting all handlers in one JVM defeats independent restart. Putting the router in Transaction Service makes every public route unavailable during its restart. Direct service ports require client changes or multiple base URLs. A service mesh or general API gateway is unnecessary for these fixed routes.

## R5. Shared code and build layout

**Decision:** Keep plain `javac`/`jar` scripts and extract a build-time `common` artifact for transport-neutral HTTP/JSON helpers, JDBC connection management, and shared persistence records/interfaces. Each domain artifact contains only its own handlers, business rules, adapters, and entry point, plus a dependency on common and MySQL Connector/J. Move catalog and low-stock business classes currently named under `transaction` into Inventory ownership. Keep transaction-specific error mapping with Transaction; common must not import Transaction. Adapt tests and their classpaths to the three outputs.

**Rationale:** The present `server/build.sh` builds a single `out/main`, and `api.Main` wires the domain process to analytics through `AcceptedScanSink`. Extraction keeps tested handler and adapter behavior where practical without a runtime service dependency.

**Alternatives considered:** Duplicating the JDBC pool and JSON code across three services increases drift. A shared business-rule jar transfers domain ownership across boundaries. Introducing Maven/Gradle is possible but adds a build migration unrelated to the feature.

## R6. Validation and performance evidence

**Decision:** Run existing unit/contract/integration tests adapted to the distributed runtime, plus crash-point and reconciliation tests, through the proxy where public behavior is under test. Reset the single MySQL database before each unchanged client command. Save two new genuine JSON reports under `load-client/reports/service-based-architecture/` with a manifest recording service versions/ports, reset, commands, and invariant results. Wait for Analytics to reach the highest eligible boundary before asserting final windows, because its endpoint is eventually consistent.

**Rationale:** The assignment and constitution require default and 100-station/120-second evidence from the actual distributed deployment. Prior pipeline reports are baselines, not substitutes. There is no new numerical latency target; correctness and measured p95/p99 behavior are the gates.

**Alternatives considered:** Synthetic reports cannot establish service behavior. A single run without a fresh reset makes stock and popularity totals incomparable. Treating temporary analytics lag as permanent loss would produce false failures.
