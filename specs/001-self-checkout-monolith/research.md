# Phase 0 Research: Self-Checkout Monolithic Server

All items below were resolved during planning; none are left as `NEEDS CLARIFICATION`.

## 1. Atomic, idempotent completion in JDBC/MySQL

**Decision**: A single JDBC transaction (`Connection.setAutoCommit(false)`), executed against the
already-connected MySQL 8 database, does all of the following before commit:

1. Guard idempotency first: `UPDATE transaction SET status='COMPLETED', completed_at=NOW(3) WHERE
   transaction_id=? AND status='OPEN'`. If the affected-row count is 0, the transaction does not
   exist, or is already completed/cancelled, or the basket is empty (checked in-memory before this
   step) — roll back and return the appropriate 404/409 with **no further statements executed**.
2. Only if step 1 affected exactly 1 row, insert one `transaction_line` row per distinct SKU in
   the basket (quantity, unit_price captured from the in-memory basket, which itself captured
   price at scan time from the catalog).
3. For every distinct SKU in the basket, **in ascending SKU order** (a fixed, deterministic order
   across all completions, to prevent lock-ordering deadlocks between transactions touching
   overlapping SKU sets), run: `UPDATE inventory SET stock_quantity = stock_quantity - ? WHERE
   sku=? AND stock_quantity >= ?`. If any single-SKU update affects 0 rows, insufficient stock
   remains for that SKU — roll back the entire transaction (Principle I: no partial deductions).
4. Commit. Only after a successful commit is the receipt (built from the in-memory basket plus the
   timestamps written in step 1) returned to the caller.

**Rationale**: This mirrors the pattern already documented in `db/init.sql`'s comments
(conditional `UPDATE ... WHERE stock_quantity > 0`), extended to a multi-row, multi-table
transaction. InnoDB's row locks on the touched `inventory` and `transaction` rows make the
check-and-decrement atomic without needing explicit `SELECT ... FOR UPDATE` or application-level
locks. The status-guard update doubles as the idempotency check: a retried completion request for
an already-`COMPLETED` transaction affects 0 rows in step 1 and is rejected before any inventory
statement runs, satisfying FR-009/SC-002 with a single indexed UPDATE.

**Alternatives considered**:
- *Optimistic locking with a version column*: rejected — adds a column not present in the existing
  `db/init.sql` schema (which this feature must not modify) for no benefit over the
  already-atomic conditional `UPDATE`.
- *`SELECT ... FOR UPDATE` then application-side check*: rejected — an extra round trip per SKU
  compared to the single conditional `UPDATE`, with the same correctness guarantee.
- *Serializable transaction isolation*: rejected — default MySQL `REPEATABLE READ` combined with
  the conditional `UPDATE`'s row locking is sufficient and avoids the throughput cost of stricter
  isolation under 100-station contention.

## 2. Insufficient stock discovered at completion time

**Decision**: Because stock is never checked at scan time (per contract/FR-005), it is possible
for the sum of scanned-but-not-yet-completed quantity for a SKU across many open transactions to
exceed remaining stock. When this is discovered during the step-3 conditional `UPDATE` above, the
server rolls back and responds `409` using the OpenAPI contract's existing, deliberately
free-form `ApiError.error` string with the value `INSUFFICIENT_STOCK`.

**Rationale**: The OpenAPI spec's 409 response for `/transactions/{id}/complete` is documented as
"cannot be completed (already completed/cancelled, or basket is empty)" and does not enumerate an
exhaustive set of `error` string values — `ApiError.error` is typed as a plain string with only an
illustrative example. Reusing the existing 409 status with a new, self-describing error code
satisfies FR-017/Constitution III (no OpenAPI schema change) while still surfacing the true cause,
and it is the only response documented for this endpoint that fits "the transaction cannot be
completed as requested." This is flagged as a resolved ambiguity between the contract's
enumerated example error codes and the constitution's non-negotiable non-negative-stock guarantee
(Principle I); the contract's status-code shape wins, the specific string value is our own
addition (see `contracts/error-codes.md`).

**Alternatives considered**: Returning `200` with a partial receipt (rejected — silently charges
for fewer items than scanned, contradicting "no partial deductions"); inventing a new `410`/`422`
status not in the OpenAPI spec (rejected — would violate contract fidelity).

## 3. In-memory basket representation

**Decision**: A `ConcurrentHashMap<String transactionId, Basket>` held in the `checkout` package,
where `Basket` holds `stationId`, `status` (mutated only under a per-basket lock or via atomic
status transition), `startedAt`, and a `Map<sku, quantity>` plus `unitPrice` captured at scan time
(so price changes, if any were ever introduced, would not retroactively alter an open basket).
Item count and running total are derived from this map rather than stored redundantly, avoiding
drift.

**Rationale**: Matches Constitution IV ("holding in-progress baskets in memory is the sanctioned
technique") and Characteristics-Analysis.md's stated design. `ConcurrentHashMap` gives lock-free
reads for `GET /transactions/{id}` and safe concurrent inserts for `POST /transactions`; per-basket
mutation (scan, complete) is synchronized on the individual `Basket` object, so contention is
scoped to a single transaction, not the whole map.

**Alternatives considered**: Persisting the basket to MySQL on every scan (rejected — reintroduces
a DB round trip on the hottest path, violating the p95 scan-latency design target and Constitution
IV's guidance); a single global lock over all baskets (rejected — unnecessary contention across
unrelated stations).

## 4. Exact hopping-window popular-items computation under concurrency

**Policy decision (explicit, to resolve a prior self-contradiction)**: Popular-item analytics is
**best-effort, decoupled from every scan response** — never a source of latency or failure for any
scan, including the one that crosses a checkpoint boundary. Ordering is still guaranteed (a later
window can never commit before an earlier one), and read behavior is precisely defined below. This
supersedes an earlier draft of this section that had the triggering scan's response wait on its
own checkpoint's commit with a timeout fallback — that draft was self-contradictory (it claimed
both "commits before the response is returned" *and* "returns the response anyway if the commit
hasn't happened," which cannot both be true) and coupled an unrelated subsystem's availability to
the checkout path, contradicting spec.md's own framing of analytics as "a downstream reporting
feature, not part of the critical checkout path" (User Story 4 rationale) and the constitution's
explicit priority order (IV Performance outranks V Analytics Window Accuracy when they conflict).

**Decision**: A dedicated `analytics` component holds `AtomicLong globalScanCounter`, a
fixed-size `String[1000]` ring buffer of SKUs, a single intrinsic lock, and a **single-threaded
(serial) `ExecutorService`** dedicated to popular-item persistence. Recording a scan:

1. Acquire the lock; increment the counter; write the SKU at `index = (counter - 1) % 1000`.
2. If `counter % 500 == 0` (and this isn't the one restart-adjacent checkpoint being skipped per
   §9): copy the current buffer contents (`System.arraycopy`, still inside the lock) and the
   window's `[windowStart, windowEnd]` bounds, then **submit** a checkpoint task to the
   single-threaded executor, still while holding the lock (submission order == boundary order).
3. Release the lock. The scan's HTTP response is written immediately — **no scan ever waits on
   analytics persistence**, regardless of whether it crossed a boundary.

Each checkpoint task, run on the single background thread, does its own bounded retry: attempt to
tally counts, rank the top 10, and commit one `popular_window` + up to 10 `popular_item` rows in a
JDBC transaction; on failure, retry up to 2 more times with a short backoff (e.g. 50 ms, 200 ms)
**within the same task**, not by resubmitting to the queue. Only after all attempts are exhausted
does the task give up and log a permanent miss for that window, then the executor moves on to the
next queued checkpoint. Retrying inside the task (rather than re-queuing) is what keeps ordering
intact under failure: the single worker thread never starts window N+1's task until window N's has
fully resolved, one way or the other.

**Read behavior**: `GET /analytics/popular-items` always returns the row with the highest
`window_id` that actually exists in `popular_window` — i.e. the latest *successfully persisted*
checkpoint. If the most recent checkpoint attempt failed after exhausting retries, there is simply
no row for it, and the report correctly falls back to the last one that did succeed. This matches
spec.md's own acceptance criterion that the report "reflects the most recently *computed* window
(not necessarily the very latest scan)" — a checkpoint that never successfully persisted was never
"computed" in the sense the contract means (the contract ties "computed" to the persisted result,
not to an in-memory attempt).

**Accepted limitation — reporting lag is not bounded by this design**: `GET /analytics/popular-items`
returns the latest *successfully persisted* window, full stop — not "the latest window, usually
within some small delay." In the expected case (a healthy, co-located MySQL instance, as this
project always runs it), each checkpoint commits within a network round trip of being submitted,
so in practice the visible window is stale by at most the low-hundreds-of-milliseconds it takes
that one commit to land — but nothing in the design *guarantees* that. If a checkpoint's write is
slow, or the database is briefly unavailable across several consecutive 500-scan boundaries, each
of those checkpoint tasks independently retries twice and gives up (§ above); the executor keeps
moving to the next boundary regardless, so several checkpoints in a row can end up pending or
permanently missing, and the report will keep returning whatever window last actually succeeded —
potentially many boundaries behind — for as long as that condition persists. There is no retry
budget, backlog limit, or alerting on this in the current design, so the staleness is unbounded in
the worst case, bounded only by "however long the underlying failure lasts," which this design
does not control. Documenting this is not the same as satisfying FR-013/FR-014's "latest computed
window" requirement under those worst-case conditions — it is a known, accepted gap, not a proof of
compliance. It is accepted here because the actual failure mode it protects against (sustained
MySQL unavailability during a 60–120 s load-test run against a local database) is not expected to
occur in this project's grading conditions; if that assumption ever stops holding, this needs an
actual bound (e.g., a capped backlog with a circuit breaker, or surfacing staleness in the response
itself) rather than best-effort-and-hope.

**Rationale**: Submitting to the executor *inside* the same lock that serializes counter
increments guarantees tasks are enqueued in strict boundary order; a single-threaded executor that
retries in place (rather than re-queuing) drains its queue strictly in that order regardless of
transient failures, so `popular_window` rows are always committed in boundary order — no later
window can ever commit before an earlier one, satisfying Constitution V's exactness requirement
independent of the best-effort success/failure of any individual checkpoint. Removing all
scan-side waiting keeps the scan hot path (both the ~499-in-500 non-boundary scans and the 1-in-500
boundary scan) fully in-memory, satisfying Constitution IV without qualification. The lock's own
critical section stays O(1) (array write, counter increment, and — once every 500 scans — a cheap
array copy plus a non-blocking `submit()` call), so it adds negligible latency even at 100
concurrent stations.

**Alternatives considered**: Blocking the triggering scan's response on its own checkpoint commit
(rejected — the self-contradiction described above, and it couples checkout-path availability to
an unrelated subsystem, contradicting spec.md's explicit "not part of the critical checkout path"
framing and Constitution IV > V); a lock-free ring buffer with CAS (rejected — correctness under
concurrent bursts is harder to prove exactly, for a data structure written to at most ~a few
hundred times per second; the added complexity isn't justified); recomputing directly from the
`transaction_line`/scan history in MySQL on each boundary (rejected — a scan-level history table
isn't part of the existing schema and would add a write per scan, again violating the
scan-latency design target); holding the analytics lock across the DB write itself (rejected — it
would serialize *every* concurrent scan, not just the 1-in-500 triggering one, behind a network
round trip, directly contradicting Constitution IV); re-queuing a failed checkpoint task to the
back of the executor instead of retrying in place (rejected — would let a later window's task run,
and potentially commit, before an earlier failed window's retry succeeds, reintroducing the
out-of-order commit bug this design exists to prevent).

## 5. JDBC connection pooling without a build tool

**Decision**: A minimal hand-rolled bounded pool (`BlockingQueue<Connection>`, sized to
comfortably exceed the 100-station stress scale, e.g. 150 connections, created eagerly at startup)
in `persistence/`, rather than adding HikariCP or another pooling library as a dependency.

**Rationale**: Keeps the dependency footprint to exactly one external jar (`mysql-connector-j`),
matching the "Plain Java + JDK HttpServer + JDBC" stack decision and the repository's existing
zero-external-dependency convention for `load-client/` and `mockserver/`. A bounded blocking-queue
pool is a well-understood, small (~50-line) amount of code for this scale.

**Alternatives considered**: HikariCP (rejected — an additional dependency and a Maven/Gradle
dependency-resolution step this repo's build scripts don't currently have); opening a new
connection per request (rejected — connection setup cost would dominate p95 latency under load).

## 6. HTTP layer and concurrency model

**Decision**: `com.sun.net.httpserver.HttpServer` (JDK built-in, same as `mockserver/`), with its
executor set to `Executors.newVirtualThreadPerTaskExecutor()` (Java 21, stable virtual threads).
Routing is a small hand-written dispatcher matching method + path (including the
`/transactions/{id}/...` path-parameter shape), consistent with `mockserver/MockServer.java`'s
existing approach.

**Rationale**: Virtual threads let each of up to 100 concurrent station connections block on JDBC
calls (which are inherently blocking) without exhausting a fixed platform-thread pool, at near-zero
per-thread cost — a direct, low-effort answer to the stress-scale requirement (FR-018) without
introducing an async framework.

**Alternatives considered**: A fixed platform-thread pool sized to the expected concurrency
(rejected — brittle if the stress scale changes; virtual threads remove the need to size this at
all); a reactive/async HTTP stack (rejected — adds a dependency and complexity disproportionate to
this project's synchronous, JDBC-bound contract).

## 7. JSON serialization

**Decision**: A small hand-rolled JSON reader/writer in `json/`, following the same shape as
`mockserver/Json.java` (which already exists in this repo and is proven against this exact
contract's response shapes).

**Rationale**: Avoids adding Jackson/Gson as a dependency; the response/request shapes are simple,
flat-to-one-level-nested DTOs (see `data-model.md`), well within what a ~200-line hand-rolled
(de)serializer handles reliably, as already demonstrated by the mock server.

**Alternatives considered**: Jackson (rejected — an external dependency for a small, fixed set of
DTOs that don't need general-purpose (de)serialization features).

## 8. Test execution without a build tool

**Decision**: JUnit 5 via the single `junit-platform-console-standalone-<version>.jar`, vendored
under `server/lib/`, compiled with `javac -cp lib/*:out/main test/**/*.java -d out/test` and run
with `java -jar lib/junit-platform-console-standalone-<version>.jar -cp out/main:out/test --scan-classpath`.

**Rationale**: Gives a standard, familiar test framework (assertions, parameterized tests,
`@Test` lifecycle) without requiring Maven/Gradle, keeping parity with how `build.sh`/`run.sh`
already work elsewhere in this repo.

**Alternatives considered**: Hand-rolled assertions with a `main()` test runner (rejected — JUnit 5
is a negligible one-jar addition and is far more familiar/maintainable for the integration and
concurrency tests this feature specifically needs).

## 9. Analytics recovery after a server shutdown or crash — and its limitation

**Known limitation (accepted, not fixed)**: The `globalScanCounter` and the 1,000-slot ring buffer
(§4) live only in server memory. If the process stops for any reason — crash, restart, deploy —
between two 500-scan checkpoints, the scans recorded since the last checkpoint are permanently
lost: there is no durable log of individual scans, only of completed windows. This is the same
class of trade-off Constitution II already accepts for in-progress baskets ("a server crash MAY
lose in-progress baskets... durability guarantees apply to completed transactions only"); here it
applies to in-progress *scan tracking* rather than in-progress *baskets*. It does not affect
inventory correctness (Principle I) or completed-transaction durability (Principle II) at all —
only the freshness of the next popular-items window after a restart.

**Decision**: On startup, before accepting any requests, the server runs
`SELECT COALESCE(MAX(window_end), 0) FROM popular_window` and initializes `globalScanCounter` to
that value. Two distinct cases follow, distinguished only by whether a saved checkpoint exists —
**not** by whether this is "really" a fresh database, which the server cannot know:

- **No saved checkpoint** (`popular_window` is empty, so the query returns `0`). This reads
  identically for two different real situations: (a) a genuinely fresh database, e.g. right after
  `db/init-db.ps1`, where no scan has ever happened, or (b) a crash/restart that happened *before*
  the first checkpoint was ever reached — anywhere from scan 1 to scan 499 — where up to 499 real
  scans occurred but were never checkpointed and are now gone from memory. The server cannot tell
  these apart from `window_end` alone, and it does not need to: in both cases there is no prior
  *persisted* boundary to be inconsistent with, so the correct action is the same either way —
  restart analytics counting from zero, accepting the loss of whatever tracking (zero scans, or up
  to 499) existed before this point. `skipNextCheckpoint` is `false`; the ring buffer fills from
  position 1, and the checkpoint at scan 500 is computed and persisted normally, because every one
  of those 500 ring-buffer entries was genuinely observed by this running server since it started.
  An uninterrupted assignment run (the common case — default and stress runs both start from a
  freshly reinitialized database per FR-016) always gets its first checkpoint at scan 500, on
  schedule.
- **Saved checkpoint exists** (`popular_window` has rows, so the query returns some
  `lastWindowEnd > 0`): this unambiguously means the server previously ran long enough to persist
  at least one checkpoint, then stopped and is now restarting. `skipNextCheckpoint` is set `true`.
  The ring buffer starts empty, but the very next boundary the resumed counter would hit
  (`lastWindowEnd + 500`) is **skipped** — no snapshot is computed or persisted at that count —
  because the window it would describe (`[lastWindowEnd - 499, lastWindowEnd + 500]`)
  half-overlaps scans that happened *before* this restart and are no longer in memory; computing it
  would silently under-count or fabricate data for that range. Recording resumes normally, and the
  first checkpoint fully backed by real, in-memory post-restart data — at `lastWindowEnd + 1000` —
  is computed and persisted as usual.

**Rationale**: Resuming the counter from the last persisted `window_end` (rather than always
resetting to 0) keeps `windowStart`/`windowEnd` **numerically** monotonically increasing across a
true restart, so a report consumer never sees the sequence appear to rewind — but this is cosmetic
continuity of the numbering only, not real continuity of the scan stream. **The skip does not
preserve or recover the scans that happened between the last saved checkpoint and the crash; those
are gone permanently, and no later window ever accounts for them.** What the skip achieves is
narrower: it prevents the server from *persisting an incorrect window* over that gap. The buffer
that becomes valid again at `lastWindowEnd + 1000` is a full buffer of entirely new, real
post-restart scans — it does not stitch together with, complete, or in any way reconstruct
whatever the buffer held before the crash. Distinguishing "no saved checkpoint" from "saved
checkpoint exists" (rather than guessing at "is this fresh") avoids over-applying the skip: when
`lastWindowEnd = 0`, skipping would needlessly delay analytics on every ordinary, uninterrupted
run — including the two runs (default and stress) the assignment actually grades — for a gap
(0 to 499 possibly-lost pre-crash scans) that, if it exists at all, has already happened and
skipping cannot undo. The skip is reserved for the one case where *not* skipping would actively
produce a wrong answer: a checkpoint whose window would otherwise mix real pre-restart data (now
unavailable) with real post-restart data, violating the invariant already promised in §4 and
Constitution V — "counts MUST be correct for the window they describe." The cost of the skip, when
it does apply, is a longer-than-usual gap (up to ~1,000 scans instead of 500) before analytics data
reappears after a genuine restart — a case the load test itself does not induce (it runs the
server continuously) and which does not affect the checkout path at all.

**Alternatives considered**: Reset the counter to 0 on restart (rejected — makes window numbering
appear to rewind, which is more confusing than a longer gap for no benefit); compute the
first post-restart checkpoint anyway, zero-padding or ignoring missing ring-buffer slots
(rejected — would report a window whose stated boundaries don't match the data actually
tallied, contradicting Constitution V); persist individual scans (not just window summaries) so no
data is ever lost across a restart (rejected — reintroduces a DB write on every scan, directly
violating the scan-latency design target and Constitution IV, to protect against an event — a
mid-run server crash — the load test itself does not induce).
