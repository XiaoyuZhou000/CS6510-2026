# Quickstart Validation (T132)

**Run**: from 2026-10-22 23:53 to 2026-10-23 05:38 local time, on the final build:
- the reset baseline test ran at 23:53;
- the final gate ran 05:23–05:29;
- the scenarios ran 05:33–05:38.

The build is commit `eb1e9af` plus the phase 10 working copy, with no runtime code changed since CP4. Every step follows [quickstart.md](../../../specs/005-microservices-architecture/quickstart.md).

**Environment**:
- Windows 10, JDK 21, MySQL 8.4.4 on `127.0.0.1:3307`, and Nginx 1.31.6 with the unchanged `deploy/nginx.conf`.
- The services ran on their standard ports: Transaction 8081, Inventory 8082 and 9092, Analytics 8083 and 9093, and the gateway on 8080.
- This was not a benchmark. The machine was shared and not idle, so the latencies below are for orientation only.

**Logs**: the raw logs are git-ignored, under `server/out/t130/`, `server/out/t132/`, and `server/out/t132-acd/`, so the figures are recorded here.

## Sections 1–4

| Step | Outcome |
|---|---|
| 1. Build | `./build-tests.sh` (a full `./build.sh`) succeeded and produced `contracts.jar`, `common.jar`, `transaction.jar`, `inventory.jar`, `analytics.jar`, and 22 jars in `lib/grpc/`. `./build.sh inventory` alone also succeeded (FR-050). |
| 2. Bootstrap and reset | `db/bootstrap.ps1 -AdminUser <admin> -Port 3307` reported "Service schemas and users are ready", with each user granted only its own schema. `db/reset-all.ps1` reported 2,000 catalog and 2,000 inventory rows, 20,000,000 stock units, 0 transactions, 0 `accepted_scan` rows in Transaction and in Analytics, and Analytics `last_position` 0: "All three service schemas are at the baseline." |
| 3. Smoke test through the gateway | `/items`: 200 with 2,000 items in SKU order. Start: 201 with an `OPEN` transaction. Scan `SKU-000001`: 200 at the captured `unitPrice` 0.85. Lookup: 200, still `OPEN`. Complete: 200 with the receipt (SC-001). |
| 4. Automated suites | **Final gate**: `PARITY_GATE=final SERVER_BASE_URL=http://localhost:8080 ./run-tests.sh` ran against a freshly reset deployment (T130, 2026-10-23 05:23–05:29). It found 378 tests: 377 passed, 0 failed, and 0 were skipped. Only the opt-in, destructive `integration.ResetBaselineTest` aborted. **Reset baseline test**: run separately with `-DRUN_RESET_BASELINE_TEST=true` and the deployment stopped, it passed (1 of 1). |

## Section 5: scenarios A–H

| # | How it was run | Outcome | Result |
|---|---|---|---|
| A | Live, through the gateway: Start; scan `SKU-000101` twice and `SKU-000102` once; then Complete | **Complete**: 200 with the receipt, totalling 9.20: 2 × 2.95 and 1 × 3.30 at the captured prices. **Stock**: `SKU-000101` 9,891 → 9,889 and `SKU-000102` 9,906 → 9,905, exactly 2 and 1. **Lookup**: `COMPLETED`. **Repeat Complete**: 409 `{"error":"TRANSACTION_NOT_OPEN"}`. | PASS |
| B | Integration test `integration.CheckoutSagaTest#insufficientStockLeavesTheOpenBasketForScanningAndRetry`, in the T130 final gate | Passed: 409 `INSUFFICIENT_STOCK`, no stock change, the transaction stays `OPEN`, and scanning and retry work | PASS |
| C | Live. Stop Inventory; Start, Scan, and Lookup. Restart Transaction and scan again. Then Complete, start Inventory, and poll Lookup. | **While Inventory was down**: Start 201, Scan 200, and Lookup 200 `OPEN`, also after the Transaction restart (itemCount 2). **Complete**: 500 `{"error":"INTERNAL_ERROR"}` after 7,675 ms, within the 10 s limit. Lookup was still `OPEN`. **After Inventory started**: Lookup showed `COMPLETED` after 5,856 ms. Stock of `SKU-000201` fell 9,938 → 9,936, exactly the 2 units. A repeat Complete gave 409 `TRANSACTION_NOT_OPEN`. | PASS |
| D | **Live**: Transaction was started directly with `java -Dselfcheckout.faults=checkout.afterApplyReply:halt …`, as the reconciled quickstart now shows. Then Start, Scan `SKU-000301` twice, Complete, and a normal restart. **Matrix**: `integration.CheckoutRecoveryFaultTest#everyInjectedFaultSettlesExactlyOnceWithoutManualSteps` ran in the T130 gate. | **Complete**: the Transaction JVM halted after Inventory's reply, and the gateway returned 502. **While Transaction was down**: Inventory's operation was `1:APPLIED`, stock fell 9,964 → 9,962, and the attempt was `1:PENDING`. **After a normal restart**: Lookup showed `COMPLETED` within 12 ms, with attempt `1:COMPLETED`, `client_outcome=NONE` (no response was recorded). Stock stayed at 9,962, so it changed once. A repeat Complete gave 409. No restoration ran (`restored=0`). The fault-matrix test passed. | PASS |
| E | Integration tests in `integration.BoundedWaitTest`, in the T130 final gate: `stillPendingAtTheBoundGives500WithNoChangeAfterOneDatabaseCheck`, `aSettlementAsOpenAcceptsTheWaitingScanOrBeginsTheNextAttemptInSerialOrder`, `aSettlementAsCompletedAnswersTheWaitersWith409SoonAfterItsCommitWithNoDatabaseCheck`, and `aWaiterInAnotherProcessSeesTheSettlementThroughItsBackstopAndGivesTheSameSerialOutcome` | All passed: a waiting Scan returns 500 with no basket change, and the next request follows the settled state | PASS |
| F | Live Load Client run (`--stations=10 --duration=60`). Analytics was stopped 15 s in and restarted 25 s later. | **Client**: 4,435 transactions and 46,792 scans, with 0 errors on Start, Scan, and Complete. **Backlog**: 20,566 undelivered `accepted_scan` rows during the outage, drained to 0 when `db/validate-invariants.ps1` first ran 2 s after the client ended. **Analytics**: `last_position` 46,800 and the published window up to 46,500, the highest eligible boundary. **Validator**: PASS for every service and cross-service reconciliation, including window recomputation. | PASS |
| G | Live: each service user ran `SELECT COUNT(*)` on each peer's table: `cs6510_transaction.transaction`, `cs6510_inventory.inventory`, and `cs6510_analytics.accepted_scan` | All 6 ordered pairs failed with `ERROR 1142 (42000): SELECT command denied to user …` | PASS |
| H | Live Load Client run (`--stations=10 --duration=90`), continuing from F's state without a reset. Inventory, then Transaction, then Analytics was stopped for about 4–8 s each, 12 s apart. Each outage was probed through the gateway. | See *Scenario H* below | PASS |

### Scenario H

**Peer routes during each outage**:
- Inventory down: `/items` 502, `POST /transactions` 201, `/analytics/popular-items` 200.
- Transaction down: `/items` 200, `POST /transactions` 502, `/analytics/popular-items` 200.
- Analytics down: `/items` 200, `POST /transactions` 201, `/analytics/popular-items` 502.

**Client**: 4,154 completed transactions and 57,750 scans. The errors were:
- **Start**: 41 errors, all 502 while Transaction was down.
- **Scan**: 9 errors, all 502 while Transaction was down.
- **Complete**: 1,366 errors.
  - 1 was a 502 while Transaction was down.
  - The other 1,365 were the business rejection 409 `INSUFFICIENT_STOCK`. With no reset after F, the skewed Load Client sold out `SKU-000001`, whose stock reached 0, as in the stress runs. Inventory recorded `rejected` operations accordingly.

No Complete failed during the Inventory outage. Its only failures were the 409s and the one 502 above, and the longest Complete took 5,714 ms, within the 8 s budget.

**After settlement**: `db/validate-invariants.ps1` returned PASS on its first attempt:
- 0 undelivered scans;
- Analytics' event count equal to Transaction's delivered `accepted_scan` rows, so no acknowledged scan was lost;
- per-SKU stock reconciled with completed sales;
- every Inventory `APPLIED` key matched a `COMPLETED` attempt.

**Latest status lines**:
- Transaction: `pending_work=no pending_attempts=0 alarmed_attempts=0 undelivered_scans=0`.
- Analytics: `pending_work=no unpublished_windows=0`.
- Inventory: `restored=0 blocked=0`.

## FR-054 counts after all scenarios

The final validator run, after the rerun of A, C, and D, returned PASS. It reported 2 background completions (`client_outcome=PENDING_ERROR`), one from each run of C, whose Complete answered 500 and then settled. It also reported 2 completions without a recorded response (`NONE`), one from each run of D, whose Transaction halted before it could respond.

The load runs added none, and Inventory reported 0 restored and 0 blocked operations.

## Notes

- **Rerun of A, C, and D**: the first run's driver read stock as the character code of the first digit and dropped error bodies. The behavior was the same: the same statuses, `COMPLETED` after 5,854 ms in C, and `1:APPLIED` then `COMPLETED` in D. A, C, and D were rerun at 05:37–05:38 with the driver fixed, against the same deployment state, and the table shows the rerun. The sell-out of `SKU-000001` does not affect the SKUs those scenarios use.
- **Port conflict**: the first start of the driver failed because Inventory found port 8082 briefly held: "Address already in use". This machine's dynamic TCP port range is 1024–15000, so outbound connections can borrow service ports. The driver now restarts a service that exits during startup. No service code changed.
