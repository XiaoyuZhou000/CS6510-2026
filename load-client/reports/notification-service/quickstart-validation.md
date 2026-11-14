# Quickstart validation (T128)

A full pass of [quickstart.md](../../../specs/007-notification-service/quickstart.md) as updated in phase 11. Every scenario was run with the committed script named in the quickstart. Each was run when its user story was finished, and the pass ended with the final release gate on 2026-11-13. Each row links its evidence. Dates are the machine's local time (UTC−8).

| Section | Run | Result | Evidence |
|---|---|---|---|
| §1 Build and start | 2026-11-13: `server/build.sh all`, `subscriber-client/build.sh`, then `us7-operations/quickstart-1.ps1` | **PASS**: the reset removed the run directory; `ops/status` showed `catalogLoaded: true`, `subscriptionVersion: 0`; five topics and four groups; all seven 006 routes answered | [us7-operations.md](us7-operations.md) |
| A: sale events without Notification | 2026-11-12, `us1-scenario-a/scenario-a.ps1` | **PASS**: one `SaleCompleted` per COMPLETED transaction, none otherwise; `saleEvents` 0; verdict FAIL with `selectionMismatch` equal to the sales, as specified | [us1-scenario-a.md](us1-scenario-a.md) |
| B: notify once per sold SKU | 2026-11-11/12, `us3-smoke/b/scenario-b.ps1` | **PASS** | [us3-smoke/](us3-smoke/README.md) |
| C: reconciliation detects every mismatch | 2026-11-13, `subscriber-client/run-tests.sh` with `KAFKA_TEST_BOOTSTRAP` (final gate) | **PASS**: 67 of 67, `ReconcileFaultInjectionTest` and the live resume test included | [final-gate-tests.txt](final-gate-tests.txt) |
| D: main load test, twice with one seed | 2026-11-11/12, `server/benchmark-notification.ps1` | **PASS**, both runs; snapshots byte-identical | [us3-smoke/](us3-smoke/README.md) |
| E: subscription changes | 2026-11-12, `change-scenario/scenario-e.ps1`, alone and with load | **PASS**, both | [change-scenario/](change-scenario/README.md) |
| F: duplicates, failures, and restarts | 2026-11-12, `fault-matrix/run-fault-matrix.ps1` | **PASS** for items 1–5; item 6 reports the malformed record as parked, by design | [fault-matrix/](fault-matrix/README.md) |
| G: existing behavior unchanged | Strict gate: 2026-11-13 (final gate). Notification-stopped run: 2026-11-13, `us6-unchanged/run-scenario-g.ps1` | **PASS**: 646 tests, 0 failed, 3 skipped by design, no live suite skipped; 0 Load Client errors with Notification stopped, and reconciliation passed after it started | [final-gate-tests.txt](final-gate-tests.txt), [us6-unchanged/](us6-unchanged/README.md) |
| H: evidence | 2026-11-13, `run-evidence.ps1`, `IsolationSummary`, Mermaid CLI 11.17.0 | **PASS**: every run reconciled; SC-006 PASS (ratios 1.01 and 0.97); the diagram test passed | [README.md](README.md) |

## Notes

- **Changes found by the pass**:
  - `server/build.sh all` used to be rejected. It is accepted since phase 9.
  - Scenario F's crash faults are `halt`, not `crash`.
  - The harnesses take `-NginxBin`.
  - Scenario G's strict gate needs a running deployment and the live-test variables.

  The quickstart now says all of this.
- **Bugs found by the final gate**, fixed before its last run (recorded in plan.md, *Implementation Notes*):
  - Notification marked a new channel ready before its partition could take writes.
  - The test topic helper had the same gap. It was the cause of the long-known flaky Kafka ordering test.
  - `SaleEventParkingLiveTest` sometimes built one sale twice.
- **What reruns would show**: the scenarios before H were run before phase 10 and phase 11's fixes. Phase 10 changed only the harness's `-Fault` option and the report's `endToEndExcluding`. Phase 11 changed only channel provisioning. Neither affects what scenarios A–G check, and the final gate reruns their automated parts.
