# Constitution Amendment: v5.0.0 → v5.1.0

**Status**: **Adopted on 2026-10-20.** It was reviewed and revised, then approved explicitly, including the separately approved wording of change 8. It was applied with `/speckit-constitution`, and `.specify/memory/constitution.md` now reads v5.1.0. This file remains the record of the amendment's rationale and review.

**Date**: 2026-10-20

**Source**: [spec.md](spec.md), section *Constitution Alignment*

**Version bump**: MINOR. The amendment adds explicit rules for forward recovery and for transient Saga states, and relaxes nothing that a v5.0.0-compliant design relies on. No principle is removed. Every design that complies with v5.0.0 still complies.

## Why

Feature 005 completes checkout with forward recovery. Once Inventory Service applies an attempt's stock deduction, the Saga always finishes, and a definitive insufficient-stock rejection reopens the basket.

Constitution v5.0.0 does not say how its Saga clauses apply to such a design. Read strictly, some clauses would forbid states that every Saga passes through, or would require an end-to-end stock-compensation path that forward recovery never triggers. This amendment states the intended reading explicitly instead of silently weakening a MUST.

## Review (2026-10-20)

The first draft's direction was sound, but six gaps needed fixing before adoption.

| # | Finding in the first draft | Revision |
|---|---|---|
| 1 | Principle V still said "the plan MUST define each Saga step, its local transaction, its compensating action". Adding a forward-recovery bullet beside it left that literal MUST in conflict with a final step that has no compensating action. | Change 1 amends that bullet directly |
| 2 | The replacement for "idempotent per transaction" omitted the sentence "Inventory Service MUST durably record processed operation keys". Applying it as written could have dropped a safety rule. It also allowed a new attempt only after a rejection, although FR-029 also allows one after a confirmed restoration. | Change 3 restates the whole bullet, keeping that sentence, and adds the restoration case |
| 3 | The transient-state bullet required every pending Saga to "settle automatically once the required services are reachable". The design deliberately keeps an attempt pending with an alarm when it detects an integrity fault, such as a payload mismatch, because only a guess could settle it. The draft would have made that safe behavior non-compliant. | Change 5 adds a narrow integrity-fault exception: stay pending, never settle on an assumed outcome, raise an alarm |
| 4 | Principle V's "no partial stock deduction may remain" rule was qualified from inside Principle VI, which split one rule across two principles | Change 4 qualifies Principle V's bullet in place. Change 5 keeps the general transient-versus-settled rule in VI. |
| 5 | The Development Workflow section still required plans to trace "Saga steps and compensations" | Change 7 adds forward-recovery rules |
| 6 | Nothing stated that in-process coordination must never carry correctness. The approved implementation constraints require this, so the guarantees hold with several instances of a service. | Change 8, approved separately with refined wording |

**Safety invariants**: the revised text preserves durable forward recovery and every existing safety invariant:
- a success response only after the Saga reaches a committed completed state (V, unchanged);
- atomic Inventory operations, idempotent under retries and duplicates, with durably recorded operation keys (V, restated in full);
- at most one effective stock deduction per transaction (V, made explicit);
- Saga progress persisted before and after each remote step, with automatic resumption after restart (V, unchanged);
- compensating actions for every step before the pivot, retried until they succeed (V, made explicit);
- no overselling, no negative stock, and per-SKU reconciliation, including after retries, crashes, and compensations (VI, unchanged);
- exactly one client-observed terminal outcome that agrees with Inventory's recorded effects (VI, unchanged; a pending Saga's server error is not that outcome).

The amendment relaxes exactly two literal readings, both required by any Saga that uses forward recovery:
- a step after the pivot needs no compensating action, because it must instead be retried to completion;
- settled-state guarantees apply once the Saga settles, not while it is pending.

## Proposed text

### 1. Principle V: amend the bullet that lists what the plan must define

Current text:

> Checkout completion MUST use an explicitly designed orchestration-based Saga coordinated by Transaction Service. The plan MUST define each Saga step, its local transaction, its compensating action, and the persisted Saga/transaction states.

Proposed text:

> Checkout completion MUST use an explicitly designed orchestration-based Saga coordinated by Transaction Service. The plan MUST define each Saga step, its local transaction, its compensating action or, for a step after a forward-recovery pivot, its retry and idempotency rule, and the persisted Saga/transaction states.

### 2. Principle V: add after the bullet amended by change 1

> A Saga MAY use forward recovery after its pivot step. Once the pivot commits (for checkout, Inventory Service's atomic stock deduction), the Saga MUST run to completion by retrying its remaining steps; those steps MUST be idempotent and need no compensating action. Steps before the pivot MUST have compensating actions. A compensating action for the pivot MAY be kept as a reserved capability that no normal path invokes; it MUST still be durable, idempotent, and directly tested.

### 3. Principle V: replace the Inventory idempotency bullet

Current text:

> Inventory operations invoked by the Saga MUST be atomic within Inventory Service and idempotent per transaction: a retried or duplicated request MUST NOT deduct or restore stock more than once. Inventory Service MUST durably record processed operation keys.

Proposed text:

> Inventory operations invoked by the Saga MUST be atomic within Inventory Service and idempotent per checkout attempt, where each attempt has a stable identifier associated with its transaction: a retried or duplicated request MUST NOT deduct or restore stock more than once. Inventory Service MUST durably record processed operation keys. A transaction MAY make a new attempt after a definitive rejection or a confirmed restoration, but MUST NOT have more than one effective stock deduction.

### 4. Principle V: qualify the consistency bullet

Current text:

> Insufficient stock or any failed step MUST leave the system consistent: no partial stock deduction may remain for a transaction that is not completed, and compensations MUST themselves be idempotent and retried until they succeed.

Proposed text:

> Insufficient stock or any failed step MUST leave the system consistent once the Saga settles: no partial stock deduction may remain for a transaction that is not completed, and compensations MUST themselves be idempotent and retried until they succeed.

### 5. Principle VI: add a bullet

> Guarantees about completed transactions, final stock, and agreement between services apply to settled Sagas. While a Saga is pending, its state MUST be durable, MUST NOT be reported as success, and MUST settle automatically, without manual intervention, once the required services are reachable. The only exception is a detected integrity fault, meaning a contract violation that cannot occur in correct operation: the Saga MUST then remain pending, MUST NOT be settled on an assumed outcome, and MUST raise an alarm visible to operators. A server error returned while a Saga is pending is not a terminal outcome; the settled state is.

### 6. Principle IX: replace "compensation" in the list of required tests

> every compensation that a Saga path can trigger, tested end to end, and every reserved compensation capability, tested directly; direct tests are not reported as end-to-end Saga compensation tests

### 7. Development Workflow & Quality Gates: amend the tracing bullet

Replace "Saga steps and compensations" with "Saga steps with their compensations or forward-recovery rules".

### 8. Principle II: add a bullet (approved separately, with this wording)

> In-process mechanisms such as locks, caches, wait registries, and retry schedules MAY be used as performance and coordination optimizations, but MUST NOT be the sole authority for durability, idempotency, inventory correctness, or cross-instance consistency; durable state, database guards, and idempotent operations remain authoritative. All safety guarantees MUST hold across process crashes, restarts, and multiple concurrent instances of a service.

## Before adoption

Until adoption, the feature 005 plan recorded the subject of changes 1 and 2, a retriable final step with no compensating action, as a documented, justified deviation in its Complexity Tracking table. Changes 3–7 were readings of the v5.0.0 text. Adoption removed the deviation.

## Adoption record (2026-10-20)

- `.specify/memory/constitution.md`:
  - received exactly changes 1–8, with change 6 placed as its own sentence after the test list so the list stays grammatical;
  - version 5.1.0, Last Amended 2026-10-20;
  - the old v4.0.0 → v5.0.0 Sync Impact Report was replaced by one for this amendment. Governance requires that report to be removed before the amended constitution is committed (task T133).
- **Templates and commands**: none of `.specify/templates/plan-template.md`, `spec-template.md`, `tasks-template.md`, `checklist-template.md`, or the `speckit-*` skills restates a changed clause, so none needed a change.
- **Feature 005 updates**:
  - spec: the [C-I]–[C-IX] reference and the *Constitution Alignment* statuses, with new item 7;
  - plan: the Constitution Check, with every gate passing and no deviation, and Complexity Tracking, with the deviation row resolved;
  - research R5 and R17;
  - the checkout Saga contract;
  - tasks: Implementation Rule 5 and T133;
  - the requirements checklist.
- **Invariants re-confirmed**: durable forward recovery and every safety invariant listed under *Review* still hold as written. Principle II now also requires them across crashes, restarts, and multiple instances, which the design and its tests already cover (R7, R19; tasks T140, T144, T147, T078).
