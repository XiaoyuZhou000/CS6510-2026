# Checkout Saga Contract (Choreographed)

**Requirements**: FR-031–FR-046, FR-061, FR-066 | **Decisions**: D5–D8, D11, D12 | **Events**: [event-catalog.md](event-catalog.md) | **Data**: [data-model.md](../data-model.md#checkout_attempt-changed)

`POST /transactions/{id}/complete` runs a Saga choreographed through domain events. Transaction Service owns the Saga state in `checkout_attempt`. No database transaction, lock, or connection is held while a request waits for an event (FR-040). Feature 005's guarded writes, striped `TransactionLocks`, state cache, and ambiguous-commit rules ([005 checkout-saga.md](../../005-microservices-architecture/contracts/checkout-saga.md)) still apply. Only the remote step changes.

## Steps

| Step | Service | Local transaction | Event |
|---|---|---|---|
| **Pre-checks** | Transaction | None on the warm path (005) | — |
| **T1 Begin attempt** | Transaction | Guarded UPDATE of `transaction`, INSERT `checkout_attempt` (with `request_event_id` and `next_retry_at = now + 2 s`), INSERT `outbox_event` (`CheckoutRequested`), commit: **3 statements + commit** | After the commit, the event ID is handed to the dispatcher. The request never waits for the broker. |
| **T2 Apply stock effect** (pivot) | Inventory | On consuming `CheckoutRequested`: locking SELECT in SKU order, INSERT `checkout_operation` (with `outcome_event_id`), CASE UPDATE if applied, INSERT `outbox_event` (outcome), commit: **4 + commit** | `StockDeducted`, or `StockRejected` (`INSUFFICIENT_STOCK` or `INVALID_REQUEST`, with the SKU), or `StockDeductionRefused` |
| **T3 Record completion** (retriable) | Transaction | On consuming `StockDeducted` for the active attempt: guarded UPDATE of `transaction` to COMPLETED and UPDATE of the attempt to COMPLETED, including `client_outcome` when the responder was claimed, commit: **2 + commit** | — |
| **Reopen** (compensates T1) | Transaction | On consuming `StockRejected`, `StockDeductionRefused`, or `StockRestored` for the active attempt: guarded UPDATE clearing the active attempt, UPDATE of the attempt to REJECTED with its reason and SKU, commit: **2 + commit** | — |
| **Reserved: restore stock** | Inventory | On consuming `StockRestorationRequested`: the 005 restore operation plus INSERT `outbox_event`, commit | `StockRestored` or `RestorationNotNeeded`. Never part of normal checkout (D8). |

T1 is the only compensatable step. T2 is the pivot: once `StockDeducted` exists, the attempt always ends COMPLETED. T3 has no compensating action. It is retried through redelivery and republish until it commits, as Principle V allows after a forward-recovery pivot.

## Outcome handler rules (Transaction, `CheckoutOutcomeHandler`)

| Event received | Attempt state in Transaction's database | Action |
|---|---|---|
| `StockDeducted` | PENDING and active | T3, then complete the settlement future |
| `StockRejected` or `StockDeductionRefused` | PENDING and active | Reopen with the reason and SKU, then complete the future |
| `StockRestored` | PENDING and active | Reopen with reason `STOCK_RESTORED` |
| Any outcome | Already COMPLETED or REJECTED with the same result | No-op; commit the offset |
| `StockDeducted` | REJECTED | Integrity fault: park the record, record `INTEGRITY:deduction-after-reject`, raise an alarm |
| `StockRestored` | COMPLETED | Integrity fault: park and alarm. No state change, because a completed sale is never reversed. |
| Any outcome | Attempt unknown | Park as `UNKNOWN_ATTEMPT` and alarm |
| `RestorationNotNeeded` | Any | No state change |

Each action runs under the transaction's stripe lock, so the state cache stays coherent with the database (005 R19).

## Response budget and claiming the response

The handler starts an 8 s budget on entry (`CHECKOUT_RESPONSE_BUDGET_MS`). After T1, the request registers as the attempt's responder (state WAITING) in `SettlementRegistry` and waits on the attempt's future. It holds nothing else.

| Situation | Who answers | HTTP | Durable effect |
|---|---|---|---|
| Unknown transaction, not open, or empty basket | Request | 404 / 409 / 409 | None; no event is published |
| The handler claims WAITING → ANSWERING, then T3 commits | Request, from the settlement | 200 receipt (from the cached snapshot and the supplied `completed_at`) | COMPLETED, `client_outcome = SUCCESS` |
| The handler claims, then reopens for `INSUFFICIENT_STOCK` | Request | 409 `INSUFFICIENT_STOCK` naming the SKU | REJECTED, `client_outcome = REJECTED`, transaction OPEN |
| The handler claims, then reopens with reason `INVALID_REQUEST` (from `StockRejected`) or `STOCK_RESTORED` (from `StockDeductionRefused` or `StockRestored`) | Request | 500 `INTERNAL_ERROR` | REJECTED, transaction OPEN |
| The budget ends first: the request claims WAITING → TIMED_OUT | Request | 500 `INTERNAL_ERROR`, after committing `client_outcome = PENDING_ERROR` and `next_retry_at = now` | Attempt PENDING; settled later by an outcome event (background completion if it completes) |
| The handler claimed, but its commit fails transiently | Handler retries T3 under its lock (005 rule). If retries are exhausted, the request answers 500 from a failed settlement. | 500 `INTERNAL_ERROR` | Attempt PENDING; the offset is not committed, so the event is redelivered and T3 retried |
| The outcome is consumed by another Transaction process | That process settles durably. The waiter's database backstop (1 s) sees it and answers from it; otherwise 500 at the bound. | As above | No reply routing (D11) |

The claim is a compare-and-set on the registry entry, so exactly one of the handler and the request writes `client_outcome`. A 500 from an unsettled Complete is never a cancellation (FR-024).

## Republish and recovery (FR-037, FR-038)

- **Schedule**: `CheckoutRecoveryWorker` and `RecoverySchedule` (005 R7) drive republishing. An attempt is due at `next_retry_at`. The worker reads the attempt by key, drops it if settled, and otherwise **re-sends the recorded `CheckoutRequested` outbox row** (same `event_id`, same bytes; D5). It then sets `next_retry_at` with backoff from 2 s to a 5 s cap, with no limit, increments `retry_count`, and sets `last_error = REPUBLISHED`. A send failure sets `PUBLISH_FAILED`.
- **Sources of due attempts**: request handovers at budget end (due at once), the startup scan of every PENDING attempt, the 30 s safety scan, and the worker's own backoff. Unchanged from 005.
- **No local settlement**: the worker never settles an attempt. Only an outcome event does.
- **Stuck alarm**: an attempt still PENDING after `CHECKOUT_STUCK_ALARM_SECONDS` (60) is counted as alarmed, with its age, in the status line. This covers outcomes parked in Inventory, such as a payload mismatch, or parked in Transaction, without assuming an outcome.

## Bounded waiting for concurrent requests (FR-042)

Unchanged from 005: a Scan or Complete for a transaction with a pending attempt waits on the attempt's future for the rest of its budget. It checks the database once per `CHECKOUT_WAIT_BACKSTOP_MS` (1,000 ms) and at the bound, and then restarts against the settled state: COMPLETED gives 409 `TRANSACTION_NOT_OPEN`; OPEN means normal processing; still pending gives 500 with no change. Lookup never waits.

## Crash, message, and broker fault matrix

| Fault point | Durable state afterwards | Settlement |
|---|---|---|
| Transaction crash before the T1 commit | No attempt, no outbox row | None needed; the client may retry |
| Transaction crash after T1, before the dispatcher sends | PENDING; outbox row unpublished | The startup sweep publishes the row; Inventory applies; the outcome settles the attempt |
| Broker down at T1 | PENDING; outbox row unpublished | The client gets 500 at the budget. The sweep publishes when the broker returns. |
| Broker restarts with `CheckoutRequested` confirmed but not consumed | PENDING; the record is in the log | Inventory consumes it after the restart |
| Inventory crash before its commit | PENDING; no operation | The offset is not committed, so the event is redelivered and processed fresh |
| Inventory commits, then crashes before sending the outcome | Operation recorded; outcome row unpublished | Inventory's startup sweep sends the outcome |
| Inventory commits and sends, then crashes before committing its consumer offset | Operation recorded, outcome sent | Redelivered request: Inventory re-sends the recorded outcome (D8); stock changes once |
| Outcome lost, delayed, or duplicated | PENDING, or settled | Republish produces a re-sent outcome; duplicates are no-ops |
| Transaction crash after consuming `StockDeducted`, before T3 commits | PENDING; Inventory APPLIED | The offset is not committed, so the outcome is redelivered after restart, then T3 |
| Transaction crash after T3, before the HTTP response | COMPLETED | Lookup shows COMPLETED; a repeat gives 409 |
| Delayed `CheckoutRequested` after a restoration | Operation RESTORED or BLOCKED | `StockDeductionRefused`; reopen if still pending |
| Poison `CheckoutRequested` (payload mismatch, undecodable) | PENDING | Parked in Inventory. The stuck alarm is raised at 60 s, and the attempt is never settled on an assumed outcome. |

## Background-completion accounting (FR-061, FR-066)

Unchanged definitions from 005 R9: durable COMPLETED, `totalTransactions`, background completions (`state = COMPLETED AND client_outcome = PENDING_ERROR`), completions without a recorded response (`client_outcome = NONE`), and the remaining difference. The harness also records parked events per service and the final consumer lag. The default run must show zero background completions and zero parked events (SC-013).

## Invariants

All of 005's Saga invariants hold. In addition:

| Invariant | Holds |
|---|---|
| `CheckoutRequested` is durable in the outbox whenever an attempt is PENDING | Always (one local transaction) |
| Each `checkout_operation` row has exactly one outcome event recorded with it | Always (one local transaction) |
| No attempt is settled except by an outcome event | Always |
| No connection, lock, or transaction is held while a request waits for an event | Always |

## Test-only fault hooks

These are named points in production code. They are inert unless the JVM property `selfcheckout.faults` lists them. Actions are `halt`, `delay:<ms>`, and `throw`.

| Hook | Location |
|---|---|
| `checkout.afterBegin` | After the T1 commit, before the dispatcher signal |
| `outbox.beforeSend`, `outbox.afterConfirm` | Dispatcher, in every producing service, around a send |
| `outbox.beforeMark` | After confirmation, before the marker commit |
| `inventory.apply.afterCommit` | Inventory, after the apply commit, before the dispatcher signal |
| `inventory.restore.afterCommit` | Inventory, after a reserved-restoration commit (direct tests only) |
| `consumer.afterHandlerCommit` | Every consumer, after the handler's local commit, before the offset commit |
| `checkout.outcome.beforeCommit` | Transaction outcome handler, before the T3 or reopen commit |
| `checkout.afterRecordCompletion` | After the T3 commit, before the settlement future completes |
