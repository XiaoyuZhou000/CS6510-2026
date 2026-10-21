# Specification Quality Checklist: Self-Checkout Microservices with Database per Service, gRPC Contracts, and Recoverable Saga Checkout

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-10-19
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs)
- [x] Focused on user value and business needs
- [x] Written for non-technical stakeholders
- [x] All mandatory sections completed

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Success criteria are technology-agnostic (no implementation details)
- [x] All acceptance scenarios are defined
- [x] Edge cases are identified
- [x] Scope is clearly bounded
- [x] Dependencies and assumptions identified

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria
- [x] User scenarios cover primary flows
- [x] Feature meets measurable outcomes defined in Success Criteria
- [x] No implementation details leak into specification

## Notes

- **Iteration 1 (2026-10-19)**: FR-037's "absent server overload" was not measurable and became "at the default benchmark load or lighter". FR-030 and FR-035 were left open for clarification.
- **Iteration 2 (2026-10-20)**: Both markers were resolved. FR-030 became "finish later", and FR-035 became "wait for a bounded time, then apply in serial order". Dependent sections were aligned.
- **Iteration 3 (2026-10-20)**: Six follow-up decisions were applied and recorded under Clarifications.
  1. **Stock restoration**: Inventory keeps it as a reserved, durable, idempotent capability that normal checkout never invokes (FR-032, SC-012, *Checkout Recovery Model*). Reopening the basket after insufficient stock is the end-to-end compensation path (User Story 1, scenario 2). Direct Inventory tests are not reported as end-to-end Saga compensation tests (FR-056).
  2. **Uncertain outcomes**: a reachable service returns `500 INTERNAL_ERROR`, the 409s are unchanged, and an unreachable service gets the gateway's 502. A 500 from an unsettled Complete is never a cancellation (FR-017). The old "temporary server error" term was replaced throughout.
  3. **Idempotency**: a stable checkout-attempt identifier associated with the transaction ID. New attempts are allowed after a definitive rejection, with at most one effective deduction per transaction (FR-028, FR-029).
  4. **Constitution alignment**: the new *Transient and Settled Guarantees* section, the *Constitution Alignment* table, and a proposed v5.1.0 amendment in [constitution-amendment-proposal.md](../constitution-amendment-proposal.md).
  5. **Benchmark metrics**: `totalTransactions` stays primary. Durable COMPLETED transactions and background completions are recorded alongside each report without changing the Load Client's JSON (FR-054). Background completions are minimized, with none allowed in the default run (FR-058, SC-010).
  6. **Planning obligations**: replica synchronization, Saga recovery, analytics ordering, and independent database resets are now required plan topics (*Planning Obligations*).
- **Iteration 4 (2026-10-20, task review)**: FR-057 now states how regression coverage is preserved during migration:
  - an existing assertion of behavior that this specification replaces is replaced by the specified behavior and recorded with its governing requirement;
  - no existing test is deleted before its assertions are migrated or replaced and verified.

  The clarification is recorded under Clarifications. Every checklist item still passes. No requirement changed meaning, and no new behavior was introduced.
- **Iteration 5 (2026-10-20, constitution v5.1.0)**: [C-I]–[C-IX] now cite v5.1.0. *Constitution Alignment* statuses reflect the adopted clauses, and item 7 covers the new Principle II rule. No requirement changed. Every checklist item still passes.
- **Consistency sweep**: no stale "temporary server error" or gateway-error wording remains. Every use of "compensation" refers either to reopening the basket or to the reserved restoration operation. All cross-references between FRs, edge cases, tables, and success criteria were checked.
- **Named technologies**: gRPC with Protocol Buffers, MySQL, the REST/OpenAPI contract, and the benchmark commands appear only because the constitution and the request mandate them. The Saga steps are described at the business level, as the constitution's workflow section requires the specification to trace them. Local transactions, states, and mechanisms are left to the plan.
- **Audience**: the readers are evaluators and maintainers of an architecture assignment, so architecture terms are part of the requested scope. Requirements are stated as observable outcomes.
- **Provisional defaults that clarification may still revisit**: low-stock reporting during pending attempts, and the 60-second analytics visibility ceiling in SC-007.
- **Governance item, resolved on 2026-10-20**: constitution v5.1.0 adopted the reviewed amendment. *Constitution Alignment* items 1–3 and 5 now comply with explicit clauses, item 4 remains a reading, and new item 7 records compliance with Principle II's rule on in-process mechanisms. The plan records no deviation.
