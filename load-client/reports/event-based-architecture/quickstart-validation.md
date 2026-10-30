# Quickstart Validation: Event-Based Architecture

Outcomes of the scenarios in `specs/006-event-based-architecture/quickstart.md` §5, recorded as each user story's tasks run them. Scenarios not listed yet belong to later stories.

## Scenario A: Saga success and insufficient stock (T078, US1)

**Run**: 2026-10-29, on the benchmark machine. The broker ran natively (Kafka 4.1.0, flush at the broker default). `db/reset-baseline.ps1` reset the schemas and topics. Then Inventory, Transaction, and Analytics started from the built jars, and Nginx from `deploy/nginx.conf`. Every request below went through the gateway on port 8080.

| Step | Expected | Observed |
|---|---|---|
| Complete a basket of two `SKU-000001` and one `SKU-000002` | 200 with the receipt; stock reduced exactly | 200, `itemCount` 3, `totalAmount` 2.90; stock 10000 → 9998 and 10000 → 9999 |
| Complete two units of `SKU-000003` with one unit in stock | 409 `INSUFFICIENT_STOCK`, no stock change, basket OPEN | 409; stock stayed 1; lookup `OPEN` |
| Outbox rows afterwards | Every row has `published_at` | Transaction 2 rows, Inventory 2 rows, 0 unpublished in either; 0 parked |
| Transaction's log | No checkout RPC | No `applyCheckout` |

## Scenario F: fault hooks (T099, US3)

`integration/CheckoutEventFaultTest` and `messaging/ConsumerCrashAfterCommitTest` passed against the live broker with service processes (2026-10-29).

- **`CheckoutEventFaultTest`**: halted a service at ten Saga hooks and restarted it.
  - Transaction: `checkout.afterBegin`, `outbox.beforeSend`, `outbox.afterConfirm`, `consumer.afterHandlerCommit`, `checkout.outcome.beforeCommit`, `checkout.afterRecordCompletion`.
  - Inventory: `outbox.beforeSend`, `inventory.apply.afterCommit`, `outbox.afterConfirm`, `consumer.afterHandlerCommit`.

  Every attempt settled COMPLETED once, with one deduction and one APPLIED operation. No stock was restored or blocked, no attempt was left pending, and no event was parked.
- **`ConsumerCrashAfterCommitTest`**: Inventory's and then Transaction's consumer halted between the handler's commit and the offset commit. After the restart, the redelivered request deducted nothing, Inventory re-sent its recorded outcome under the same `event_id`, and the redelivered `StockDeducted` was a no-op.

## Scenario G: poison event (T099, US3)

**Run**: the deployment of scenario A. `support.PublishRawRecord selfcheckout.checkout.v1 poison-key not-an-event-envelope` was published directly to the broker.

| Expected | Observed |
|---|---|
| Parked in Inventory's `dead_letter_event` within about 1 s | Parked after 861 ms: reason `UNDECODABLE`, null `event_id`, topic `selfcheckout.checkout.v1`, `park_count` 1 |
| Other partitions keep flowing | A checkout made right afterwards completed with 200 |
| Visible in the status line | `status service=inventory applied=2 rejected=1 restored=0 blocked=0 unpublished_outbox=0 parked_events=1 checkout_consumer_lag=0` |

## Direct restoration (SC-016)

`messaging.RestorationEventDirectTest` runs alone against Inventory Service over the live broker. Its result is reported apart from the end-to-end Saga results, because no production path produces `StockRestorationRequested` (FR-039, D8).

- **Result**: 5 tests, 0 failed, 0 skipped. This was the same in the direct-restoration section of the full `run-tests.sh` runs on 2026-10-29.
- **What it covers**:
  - An applied attempt is restored once; a redelivered request re-sends its recorded `StockRestored`, and a later request is `ALREADY_RESTORED`.
  - Ten concurrent requests restore once.
  - A rejected attempt has nothing to restore.
  - An absent attempt is blocked, and its delayed `CheckoutRequested` gets `StockDeductionRefused` with no deduction.
  - An Inventory halt at `inventory.restore.afterCommit` is redelivered after the restart and restores nothing twice.
