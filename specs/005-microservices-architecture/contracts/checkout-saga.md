# Checkout Saga Contract

**Requirements**: FR-024–FR-039, FR-054, FR-058 | **Decisions**: R5–R9 | **Data**: [data-model.md](../data-model.md#transaction-and-checkout-attempt-transaction-service) | **RPCs**: [grpc-internal-contracts.md](grpc-internal-contracts.md#applycheckout)

Transaction Service orchestrates `POST /transactions/{id}/complete` as a Saga with forward recovery. No database transaction, lock, or connection spans the remote call (FR-033).

## Steps

Every step runs under the transaction's striped in-process lock from `TransactionLocks` (R19).
- **Locking**: the lock is independent of cache entries, so evicting an entry never creates a second lock. It is released before the remote call and before any wait.
- **Guarded writes**: every write to the `transaction` row is a guarded conditional UPDATE whose `WHERE` clause repeats the cached expectation. Zero affected rows means the request rolls back, reloads from the database, and re-evaluates.
- **Ambiguous commits**: a commit whose outcome is unknown invalidates the cache entry before the lock is released, so the next request reloads before any mutation.

Correctness never depends on the cache.

| Step | Service | Local transaction (design statement budget, R22) | Outcome handling |
|---|---|---|---|
| **Pre-checks** | Transaction | None on the warm path: existence, OPEN, pending, and empty basket come from the cache entry, loaded on a miss. A 404 or 409 is confirmed against the database before it is sent (R19). | Not found gives 404 `TRANSACTION_NOT_FOUND`; not open gives 409 `TRANSACTION_NOT_OPEN`; pending means bounded wait, then restart; no units gives 409 `EMPTY_BASKET` |
| **T1 Begin attempt** | Transaction | Guarded `UPDATE transaction SET attempt_count=n, active_attempt_number=n WHERE transaction_id=? AND status='OPEN' AND active_attempt_number IS NULL AND item_count=<cached> AND attempt_count=n-1`, then INSERT `checkout_attempt` with the canonical `lines` snapshot, totals from the cache, and `next_retry_at` at the end of the response budget, then commit: **2 statements + commit** | Cache entry marked pending `n`; attempt registered in `SettlementRegistry` as owned by this request. If the commit outcome is unknown: invalidate the entry, queue a reconcile entry in `RecoverySchedule`, and answer 500 without calling Inventory. |
| **T2 Apply stock effect** (pivot) | Inventory | `ApplyCheckout((tx, n), lines)`: locking SELECT, INSERT outcome, CASE UPDATE if applied, commit (**3 + commit**; [contract](grpc-internal-contracts.md#applycheckout)) | APPLIED → T3. INSUFFICIENT_STOCK, INVALID_REQUEST, or STOCK_RESTORED → reopen. Uncertain → retry within the budget, then hand over to recovery. |
| **T3 Record completion** (retriable) | Transaction | Guarded `UPDATE transaction SET status='COMPLETED', completed_at=<service time>, active_attempt_number=NULL WHERE transaction_id=? AND active_attempt_number=n`, plus UPDATE of attempt `n` to COMPLETED with `settled_at` and `client_outcome='SUCCESS'` (if still answering), then commit: **2 + commit** | Return 200 with the receipt built from the cached snapshot and the supplied `completed_at`, with no read. Evict the cache entry. |
| **Reopen** (compensates T1) | Transaction | Guarded UPDATE clearing `active_attempt_number=n`, plus UPDATE of attempt `n` to REJECTED with its reason, `rejected_sku`, and `client_outcome`, then commit: **2 + commit** | INSUFFICIENT_STOCK gives 409 `INSUFFICIENT_STOCK` naming the SKU. INVALID_REQUEST and STOCK_RESTORED give 500 `INTERNAL_ERROR`. The cache entry becomes OPEN again. |
| **Reserved: restore stock** | Inventory | `RestoreCheckout((tx, n))` | Not part of normal checkout. See [grpc-internal-contracts.md](grpc-internal-contracts.md#restorecheckout-reserved-fr-032). |

- **Compensation**: T1 is the only compensatable step, compensated by reopening. T2 is the pivot, and once it reports APPLIED the Saga always runs T3.
- **T3**: has no compensating action. It is retried until it commits, as Principle V of constitution v5.1.0 permits for a step after a forward-recovery pivot.
- **Guard races**: if a T3 or reopen guard finds the attempt already settled, for example because recovery won the race, it reads the settled state and responds from it. Both paths are idempotent.
  - A live request answering from an attempt that another actor settled first records its own `client_outcome` (`SUCCESS` or `REJECTED`) with a guarded UPDATE (`client_outcome = 'NONE'`), then answers.
  - If that write fails, it answers 500 `INTERNAL_ERROR`, so the background-completion counts stay exact.

## Multiple instances

The deployment runs one Transaction instance, but no guarantee depends on that. The state cache, `TransactionLocks`, `SettlementRegistry`, and `RecoverySchedule` are process-local optimizations, not distributed coordination. Database guards and Inventory's idempotency are the authority.

| Concurrent action on one transaction from two instances | Why it stays correct |
|---|---|
| Scans | The guarded UPDATE serializes them on the row: one guard succeeds per expected `item_count`, and the other instance reloads and retries |
| Completes | T1's guard (`active_attempt_number IS NULL`, expected `item_count` and `attempt_count`) and the unique pending column allow one attempt. The other request waits and answers from the settled state. |
| Scan during the other instance's pending attempt | The guard's `active_attempt_number IS NULL` keeps every unit out of the frozen basket |
| Recovery of the same attempt, by a live request and a worker or by two workers | `ApplyCheckout` replays the recorded outcome under the same key, so stock changes once. One T3 or reopen guard wins, and the rest read the settled state. |
| Waiting for the other instance's settlement | The waiter's database backstop sees it within `CHECKOUT_WAIT_BACKSTOP_MS`, and the waiter then removes its local future |

Tests run two instances over one schema for each row (T140, T144, T147, T078).

## Response budget and mapping (FR-017, FR-030, FR-037)

The handler starts an 8 s budget (`CHECKOUT_RESPONSE_BUDGET_MS`) on entry. That budget covers pre-checks, T1, `ApplyCheckout` calls of 2 s each with retries, and T3 or reopen.

| Situation, within the budget unless stated | HTTP response | Durable effect |
|---|---|---|
| Unknown transaction, not open, or empty basket | 404 / 409 / 409, as today | None |
| APPLIED and T3 committed | 200 receipt | COMPLETED, with `client_outcome = SUCCESS` |
| INSUFFICIENT_STOCK and reopen committed | 409 `INSUFFICIENT_STOCK` | Attempt REJECTED; transaction OPEN with its basket |
| INVALID_REQUEST or STOCK_RESTORED and reopen committed | 500 `INTERNAL_ERROR` | Attempt REJECTED; transaction OPEN |
| Budget expires while the attempt is pending (Inventory slow or down, lost reply, uncertain status, `FAILED_PRECONDITION`) | 500 `INTERNAL_ERROR`, sent after `client_outcome = PENDING_ERROR` and `next_retry_at = now` are committed | Attempt stays PENDING. The request queues it in `RecoverySchedule` even if that write failed, and recovery takes over at once. |
| T1 commit fails or its outcome is unknown | 500 `INTERNAL_ERROR` | No attempt, or a committed attempt. The reconcile entry lets recovery resume it or drop the entry, and the next request reloads first. |
| T3 commit fails transiently, or its outcome is unknown | Retried within the budget. If a retry's guard misses because the earlier commit succeeded, the request responds from the settled attempt: 200 with the durable `completed_at`. Otherwise 500 `INTERNAL_ERROR`. | Recovery repeats `ApplyCheckout` (replayed APPLIED), then T3 |

A 500 from an unsettled Complete is never a cancellation (FR-017). The attempt settles later as COMPLETED or OPEN, and lookup shows which.

## Recovery worker (FR-030, FR-031, FR-033)

- **Driver**: one worker thread in Transaction Service takes due attempt keys from the in-memory `RecoverySchedule` (R7). It does not poll the database on a short interval. The schedule is fed by:
  - request handovers at budget expiry, due at once;
  - reconcile entries after ambiguous T1 commits, due at once;
  - the worker's own backoff retries.
- **Startup scan**: at startup nothing is owned, so the worker enqueues every PENDING attempt, reading them in pages of at most 100 rows.
- **Safety scan**: every `CHECKOUT_RECOVERY_SCAN_SECONDS` (30), one query `WHERE state='PENDING' AND next_retry_at <= now ORDER BY next_retry_at LIMIT 100` on the `(state, next_retry_at)` index, repeated only while pages are full.
  - It enqueues attempts that no live request owns and that are not already scheduled.
  - It catches anything the schedule missed, such as an attempt handed over by another process.
  - T1 sets `next_retry_at` to the end of the response budget, so the scan leaves live attempts alone.
- **Action**: the worker reads the attempt by key first. An attempt that is absent, or that was settled meanwhile, perhaps by another instance, is dropped, and any local future is completed. Otherwise the worker re-sends `ApplyCheckout` with the stored `lines` snapshot under the same `(transaction_id, attempt_number)`.
  - A definitive outcome leads to T3 or reopen, which the worker performs under the same striped lock so the cache stays coherent. It then updates or evicts the cache entry and completes the registry future so waiting requests resume.
  - A reconcile entry whose attempt does not exist is dropped with no write.
- **Scheduling**: uncertain statuses increment `retry_count`, store `last_error`, and set `next_retry_at` with exponential backoff from 100 ms to a 5 s cap. The attempt is re-enqueued in memory for that time. There is no retry limit.
- **Measurement**: recovery queries, rows read, schedule size, and settlements are counted (R21) and reported at checkpoint CP2.
- **Alarms**: `FAILED_PRECONDITION` and `INVALID_ARGUMENT` keep the attempt pending, are logged at error level, and are counted in the operational status (FR-039).
- **Client outcome**: recovery never sets `client_outcome`. An attempt with no recorded client response keeps `NONE`.

## Bounded waiting for concurrent requests (FR-035)

| Request during a pending attempt | Behavior |
|---|---|
| Scan or Complete | 1. Detect the pending attempt from the cache entry, or from the guarded write's miss followed by a reload. 2. Get or create the attempt's registry future under the stripe lock, then release the lock, which is the only thing held at that point. 3. Wait up to the request's remaining 8 s budget on the future. Settlements in the same process complete it under the same lock, so the wait ends without any database read. 4. As a backstop for settlements made elsewhere, check the database once per `CHECKOUT_WAIT_BACKSTOP_MS` (1,000 ms) and once at the bound, borrowing a connection only for each read. 5. On settlement, restart the request from the beginning: COMPLETED gives 409 `TRANSACTION_NOT_OPEN`; OPEN means normal processing (scan accepted, or a new attempt `n + 1`). Unsettled at the bound gives 500 `INTERNAL_ERROR` with no change and no effect on the pending attempt. |
| Lookup | Never waits. It returns the durable row: OPEN until T3 commits, then COMPLETED. |
| Repeated Complete after COMPLETED | 409 `TRANSACTION_NOT_OPEN`, with no Inventory call (FR-036) |

## Crash and fault matrix

| Fault point | Durable state afterwards | Settlement |
|---|---|---|
| Transaction crash before the T1 commit | No attempt | None needed; the transaction is OPEN; the client may retry |
| Transaction crash after T1, before `ApplyCheckout` is sent | PENDING; Inventory has no record | Recovery sends; Inventory processes; T3 or reopen |
| Inventory crash before its commit | PENDING; Inventory has no record | Recovery repeats; Inventory processes fresh |
| Inventory commits, then the reply is lost or delayed past the deadline | PENDING; Inventory APPLIED or REJECTED | Live retry or recovery repeats; replay returns the recorded outcome; T3 or reopen. Stock changes once. |
| Transaction crash after the reply, before T3 | PENDING; Inventory APPLIED | Recovery replays APPLIED, then T3 |
| Transaction crash after T3, before the HTTP response | COMPLETED | None; lookup shows COMPLETED; a repeat gives 409 |
| Inventory down for a long time | PENDING, basket frozen | Settles when Inventory returns. An attempt Inventory never processed is evaluated against the stock at that time. |
| Duplicate or delayed `ApplyCheckout` | Any | Recorded outcome replayed; no second deduction (FR-028) |

## Background-completion accounting (FR-054, FR-058; R9)

| Count, from Transaction's own schema | Definition |
|---|---|
| Durable COMPLETED transactions | `status = 'COMPLETED'` |
| Client-observed successes | The Load Client's `totalTransactions` |
| Background completions | Attempts with `state = 'COMPLETED' AND client_outcome = 'PENDING_ERROR'` |
| Completions without a recorded response | Attempts with `state = 'COMPLETED' AND client_outcome = 'NONE'`, for example after a crash |
| Remaining difference | Durable COMPLETED transactions minus `totalTransactions`, minus the two counts above: 200s the client did not receive in time |

- **Recording**: these counts are recorded beside each benchmark report, and the Load Client's JSON stays unmodified.
- **Targets**: the default run must show zero background completions (SC-010). The stress run reports and explains its count.

## Invariants

| Invariant | Holds |
|---|---|
| No 200 before Inventory's APPLIED and T3 are both durable (FR-034) | Always |
| At most one pending and at most one completed attempt per transaction; at most one effective deduction (FR-029) | Always |
| No unit joins a frozen basket (FR-035) | Always |
| No database connection, lock, or transaction is held across `ApplyCheckout` or a wait (FR-033, FR-035) | Always |
| The transaction is COMPLETED exactly when Inventory's deduction for it stands (FR-038) | Once settled |
| Per SKU: initial stock minus final stock equals COMPLETED line quantities (FR-038) | Once all attempts settle |

## Test-only fault hooks (R15)

These hooks are named points in production code. They are inert unless the JVM property `selfcheckout.faults` lists them, and production scripts never set it.

| Hook | Location |
|---|---|
| `checkout.afterBegin` | After the T1 commit |
| `checkout.beforeApply` | Before `ApplyCheckout` is sent |
| `checkout.afterApplyReply` | After a reply arrives, before T3 or reopen |
| `checkout.afterRecordCompletion` | After the T3 commit, before the response |
| `inventory.apply.afterCommit` | Inventory, after its commit, before the reply |
| `inventory.restore.afterCommit` | Inventory, after a reserved-restoration commit, before the reply; used only by the direct restoration tests (SC-012) |

Supported actions are `halt` (`Runtime.halt`), `delay:<ms>`, and `throw`.
