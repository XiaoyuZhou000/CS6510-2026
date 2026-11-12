# US3 smoke: subscriber test application and reconciliation (T080)

Feature 007, phase 5 (User Story 3), recorded 2026-11-11 on branch `feature/notification-service`.

| Step | Outcome |
|---|---|
| `subscriber-client/run-tests.sh` | **PASS**: 48 tests, 0 failed (T064–T067, plus `SubscriberThreadTest` and `DecodeSaleEventsTest`) |
| Quickstart scenario C (SC-003) | **PASS**: every injected mismatch fails with exactly its category and key (below) |
| Quickstart scenario B | **Not run**: blocked, see below |
| Quickstart scenario D, plus the snapshot comparison (SC-002, SC-013) | **Not run**: blocked, see below |

## Scenario C: reconciliation detects every mismatch

`ReconcileFaultInjectionTest` builds a passing run directory (`SyntheticRun`: 3 subscribers, 3 sales, 4 owed
notifications, and 1 redelivery). It checks that the clean copy passes, then applies each injection to a fresh copy.
Each injection must make exactly one category non-zero, and that category's samples must name the affected key.

| Injection | Category | Key |
|---|---|---|
| removed `DELIVERED` row | `missingNotDelivered` | `txn-2/SKU-000003/sub-0003` |
| row left `PENDING` | `missingNotDelivered` | `txn-1/SKU-000002/sub-0002` |
| removed receipt | `missingDeliveredNotReceived` | `txn-1/SKU-000002/sub-0002` |
| extra receipt | `unexpected` | `txn-3/SKU-000004/sub-0002` |
| extra row | `unexpected` | `txn-3/SKU-000004/sub-0003` |
| undecodable receipt | `unexpected` | `receipt:sub-0003@1` |
| wrong row quantity | `quantityMismatch` | `txn-1/SKU-000001/sub-0001` |
| wrong received quantity | `quantityMismatch` | `txn-2/SKU-000003/sub-0003` |
| second row identity for one key | `duplicate` | `txn-1/SKU-000002/sub-0001` |
| second received identity for one key | `duplicate` | `txn-1/SKU-000002/sub-0002` |
| selection at another version | `selectionMismatch` | `txn-2` |
| missing selection of a sale with no recipient | `selectionMismatch` | `txn-3` |
| history row above the ready version | `fixedSubscriptions` | `version:4` |
| history disagreeing with the snapshot | `fixedSubscriptions` | `sub-0003` |
| `PARKED` row | `parked` | `txn-1/SKU-000002/sub-0001` |
| parked event | `parked` | `event:e-bad` |
| missing `SaleCompleted` row | `saleEvents` | `txn-2` |
| second `SaleCompleted` row | `saleEvents` | `txn-1` |
| `SaleCompleted` lines that differ from the sale | `saleEvents` | `txn-3` |

Redeliveries are counted (`totals.redeliveries`) but never fail a run (`ReconcilerTest`).

## Blocked: scenarios B and D

Both scenarios need the deployment to run. The user-level `NOTIFICATION_DB_PASSWORD` is still unset, and on
2026-11-11 neither the `cs6510_notification` schema nor the `cs6510_notification_svc` user existed.
`db/bootstrap.ps1` has therefore not been run for feature 007. Without it, `db/reset-baseline.ps1` (step 2 of the
harness) and Notification Service itself cannot start.

Checked offline instead:
- **Evidence export**: `db/export-notification-evidence.ps1` ran its Transaction half against the local database.
  It exported `sales.tsv` with 89,538 rows, and an empty `sale-events.tsv`, because that database predates 007's
  `SaleCompleted`. As expected, it stopped at the Notification credentials.
- **Scripts**: all four changed PowerShell scripts parse.

Once the password is set and bootstrap has run, the commands to run are:

```powershell
server/benchmark-notification.ps1 -Subscribers 100 -Seed 20261111 -Workload default -RunName us3-smoke/d-default-1 `
    -NginxBin C:\Users\18422\cs6510-tools\nginx-1.31.6\nginx.exe -IdleCpuCores 2.0 `
    -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights
server/benchmark-notification.ps1 -Subscribers 100 -Seed 20261111 -Workload default -RunName us3-smoke/d-default-2 ...
```

Then compare the two `subscribers/snapshot.json` files byte for byte (SC-013), and run scenario B with
`SubscriberMain --assignments=b.json`.
