# US1 scenario A: sale events without Notification Service (T034, SC-001)

Feature 007, quickstart scenario A. Run on 2026-11-12 at 16:32 local time (UTC-8) at commit `b2e48fc`, with the jars of CP1.
The evidence is in [us1-scenario-a/](us1-scenario-a/).

## Command

```powershell
load-client/reports/notification-service/us1-scenario-a/scenario-a.ps1 -NginxBin C:\Users\18422\cs6510-tools\nginx-1.31.6\nginx.exe
```

The four `*_DB_PASSWORD` variables and `KAFKA_HOME` must be in the shell. [`scenario-a.ps1`](us1-scenario-a/scenario-a.ps1) works
as follows:
1. Resets the baseline, then starts Inventory, Transaction, Analytics, and the gateway. It checks that nothing listens on
   Notification's port 8084.
2. Drives the baskets below through the gateway. A = `SKU-000001`, B = `SKU-000002`, and R = `SKU-000010`; R's stock is changed
   with Inventory's own credentials.
3. Exports the evidence with `db/export-notification-evidence.ps1`, and counts the records of
   `selfcheckout.sale-completed.v1`. The baseline reset leaves that topic empty, so the count is this run's records.
4. Runs `ReconcileMain --mode=fixed` with an empty snapshot (`subscribers/snapshot.json`, `ready.json` at version 0).

## Baskets and answers

| Basket | Steps | Client answers | Attempts (`checkout_attempt`) |
|---|---|---|---|
| `86d97b56…` | 3×A + 1×B, Complete | 200 | 1 COMPLETED |
| `f02f0088…` | 2×R with R's stock at 1, Complete; stock raised to 2, Complete | 409 `INSUFFICIENT_STOCK`, then 200 | 1 REJECTED (`INSUFFICIENT_STOCK`), 2 COMPLETED |
| `86d97b56…` again | a repeated Complete | 409 `TRANSACTION_NOT_OPEN` | no new attempt |
| `dcd7a581…` | A + B scanned, never completed | – | none (transaction stays OPEN) |
| `e6f72460…` | Transaction restarted with `-Dselfcheckout.faults=checkout.afterRecordCompletion:delay:12000` (via `JAVA_TOOL_OPTIONS`); 2×B + A, Complete | 500 `INTERNAL_ERROR` after the 8 s response budget; the transaction then reached COMPLETED in the background | 1 COMPLETED |

Transaction was then restarted once more without the fault, and the script waited 10 s before stopping.

## Result

**PASS for SC-001.** There is exactly one `SaleCompleted` per COMPLETED transaction, with matching lines, and none for anything
else:

| Check | Result |
|---|---|
| COMPLETED transactions | 3 |
| `SaleCompleted` outbox rows (`exports/sale-events.tsv`) | 3, one per completed sale, all decoded |
| Records on `selfcheckout.sale-completed.v1` | 3 |
| Lines of each event vs. the sale's `accepted_scan` counts | equal (3×A + 1×B; 2×R; 1×A + 2×B) |
| Each event's completion time vs. the sale's `completed_at` | equal to the millisecond |
| `aggregate_key` | `sale:<transaction id>` for all three |
| Event for the rejected attempt 1, the repeated Complete, the scans-only basket, or the two restarts | none |
| Reconciler `saleEvents` category | 0 |

The reconciler's verdict is **FAIL**, with `selectionMismatch` = 3 and every other category 0 (`reconciliation.json`). That is
expected: Notification never ran, so no sale has a `sale_selection` row, and a completed sale with no selection fails
`selectionMismatch` by definition. The quickstart's sentence "the check passes" does not hold literally for this scenario. Here the
check that applies is the `saleEvents` category, plus the topic count.

## Observations

- **The background completion is recorded as `client_outcome = SUCCESS`**, although the client received a 500. Transaction
  claims the response when T3 commits, and the delayed hook runs after that commit, so the claim is made before the 8 s budget
  expires and the waiter answers 500. This is feature 006's behavior for this fault point, not a 007 change: the sale event is
  correct either way. In the load tests the fault is never set.
- **The first attempt of the script stopped at the first 409.** Windows PowerShell 5.1 had already read the error body, so it
  parsed as null. The script now reads `ErrorDetails.Message`. That attempt's partial run was discarded; the baseline reset of
  the rerun replaced its data.
