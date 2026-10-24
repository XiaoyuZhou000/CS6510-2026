# Internal gRPC Contracts

**Sources**: [inventory_service.proto](../../../server/contracts/src/main/proto/selfcheckout/inventory/v1/inventory_service.proto), [analytics_ingest.proto](../../../server/contracts/src/main/proto/selfcheckout/analytics/v1/analytics_ingest.proto) (the files in `proto/` point there) | **Requirements**: FR-009–FR-013, FR-026–FR-032, FR-041–FR-043 | **Decisions**: R2, R5, R6, R8, R10, R11

These are the only synchronous service-to-service interfaces. Gateway traffic, client traffic, and the Scan path never use them (FR-012).

## Servers and clients

| Service (proto) | Server | Client | Port | Calls |
|---|---|---|---:|---|
| `selfcheckout.inventory.v1.InventoryService` | Inventory Service | Transaction Service | 9092 (`INVENTORY_GRPC_PORT`) | Catalog replication, checkout apply, outcome, reserved restore |
| `selfcheckout.analytics.v1.AnalyticsIngestService` | Analytics Service | Transaction Service (outbox relay) | 9093 (`ANALYTICS_GRPC_PORT`) | Scan event delivery |

Transaction Service runs no gRPC server. Gateway and public clients never reach gRPC ports.

## Conventions (R2)

- **Channels**: one `ManagedChannel` per target per process, created at startup, reused, and shut down by the service's shutdown hook. Channels connect lazily, so a peer that is down at startup does not prevent startup.
- **Transport**: plaintext on the local host or a private network. Transport security and authentication are out of scope (spec Assumptions).
- **Executors**: server handlers run on virtual threads.
- **Layering**: a handler only translates between protobuf messages and the owning service's business operations. Business rules live in the business package, and SQL lives in database adapters. gRPC and generated `contracts.*` types appear only in a service's `rpc` package and its composition root, which an architecture test enforces.
- **Retries**: done in application code, never through gRPC service-config retry. Every retried call reuses its idempotency identity.
- **Errors**: a gRPC error description never reaches a public HTTP client (FR-017).

## Status code usage

| Status | Meaning in these contracts | Caller action |
|---|---|---|
| `OK` | The request was processed, and the response carries the business outcome, including INSUFFICIENT_STOCK and INVALID_REQUEST | Act on the outcome |
| `INVALID_ARGUMENT` | Malformed request: missing key, empty or oversized lists, duplicate SKUs, non-positive quantity, field too long | Programming fault: never retry; log; the attempt or batch stays pending and is alarmed |
| `FAILED_PRECONDITION` | `ApplyCheckout` reused an attempt identifier with a different payload | Integrity fault: never treat as rejection; keep the attempt pending; raise an alarm (R5) |
| `UNAVAILABLE` | Server or its database is temporarily unavailable | Uncertain: retry with backoff under the same identity |
| `DEADLINE_EXCEEDED` | The caller's deadline expired; the server may have committed | Uncertain: reconcile by repeating under the same identity |
| `ABORTED` | Contention persisted after internal deadlock or lock-wait retries | Uncertain: retry |
| `RESOURCE_EXHAUSTED` | Overload | Uncertain: retry with backoff |
| `CANCELLED`, `UNKNOWN`, `INTERNAL` | Unexpected failure | Uncertain: retry with backoff; alarm if it persists |

An uncertain status never becomes a business rejection, never triggers stock restoration, and never yields a client success (FR-011, FR-030, FR-031).

## Deadlines and retry policy (R6)

| RPC | Per-call deadline | Retry policy | Identity |
|---|---|---|---|
| `GetCatalogVersion` | 2 s | Every 30 s (`CATALOG_REFRESH_SECONDS`); backoff from 1 s to 30 s on failure | Read-only |
| `GetCatalog` | 5 s | At startup and on a version change; backoff from 1 s to 30 s | Read-only |
| `ApplyCheckout` | 2 s (`INVENTORY_RPC_DEADLINE_MS`) | Inside the 8 s Complete budget: retry after 50, 100, 200, and 400 ms. Then the recovery worker uses backoff from 100 ms to 5 s, indefinitely | `(transaction_id, attempt_number)` |
| `GetCheckoutOutcome` | 2 s | Diagnostics and validation only | Read-only |
| `RestoreCheckout` | 2 s | When used: retried until confirmed | `(transaction_id, attempt_number)` |
| `DeliverScanEvents` | 5 s (`ANALYTICS_RPC_DEADLINE_MS`) | Backoff from 100 ms to 5 s; undelivered rows are reselected and re-sent | `(transaction_id, scan_sequence)` per event |

## InventoryService

### `GetCatalogVersion` and `GetCatalog`

- `GetCatalogVersion` reads the `catalog_meta` singleton, which is one primary-key read. If the version differs from Inventory's in-memory catalog cache, the cache reloads (R8).
- `GetCatalog` returns every product in ascending SKU order, with exact `price_cents`, from that cache. A snapshot and its version always come from the same load.
- Both are read-only and repeatable.
- Transaction Service replaces its replica only when the fetched version differs from the stored one (R4).

### `ApplyCheckout`

**Request validation**: these failures return `INVALID_ARGUMENT` and nothing is recorded.
- `attempt.transaction_id` is 1–40 characters, and `attempt_number` is at least 1.
- `lines` is non-empty and has at most 2,000 entries, with unique SKUs of 1–20 characters and quantities of at least 1.

**Processing**: one local transaction at READ COMMITTED (R8, R20). The design budget (R22) is 3 statements and 1 commit when applied, and 2 statements and 1 commit when rejected. The outcome row and the stock change always share the commit.

| Step | Statement | Effect |
|---|---|---|
| 1 | `SELECT sku, stock_quantity FROM inventory WHERE sku IN (…) ORDER BY sku FOR UPDATE` | Locks every referenced stock row in ascending SKU order. A concurrent duplicate of the same attempt waits here. |
| 2 | None (in memory) | Determine the outcome: a SKU missing from the result gives INVALID_REQUEST with the first unknown SKU; a line exceeding stock gives INSUFFICIENT_STOCK with the first such SKU in SKU order; otherwise APPLIED. |
| 3 | INSERT `checkout_operation` | Records the outcome with the canonical `lines` text. If the primary key already exists: roll back and read the recorded row. Different `lines` give `FAILED_PRECONDITION`. A RESTORED or BLOCKED record gives `STOCK_RESTORED`. Otherwise the recorded outcome is returned with `replayed = true`. |
| 4 | `UPDATE inventory SET stock_quantity = CASE sku WHEN … END WHERE sku IN (…)` (APPLIED only) | Deducts every line in one statement. The `CHECK ≥ 0` constraint guards against faults. |
| 5 | Commit | Everything above commits together. A crash before commit leaves no trace, so a later replay processes the attempt fresh. |
- **Contention**: deadlocks and lock-wait timeouts are retried inside the server. If retries are exhausted, the server returns `ABORTED`. Contention is never reported as INSUFFICIENT_STOCK (FR-026).
- **Defense in depth**: a second APPLIED operation for the same `transaction_id` violates the unique `applied_transaction_id` index. The server rejects it with `FAILED_PRECONDITION` and raises an integrity alarm (FR-029).

### `GetCheckoutOutcome`

Returns the recorded outcome, or `NOT_PROCESSED` when no record exists. It never changes state. Used by diagnostics, tests, and invariant validation.

### `RestoreCheckout` (reserved; FR-032)

| Record state | Effect | Result |
|---|---|---|
| APPLIED | Add the recorded lines back to stock, in ascending SKU order; mark RESTORED | `RESTORED`, with the lines |
| RESTORED | None | `ALREADY_RESTORED`, with the lines |
| REJECTED | None; the attempt already applied nothing | `NOTHING_TO_RESTORE` |
| BLOCKED | None | `NOTHING_TO_RESTORE` |
| Absent | Insert a BLOCKED tombstone, so any later apply is refused | `NOTHING_TO_RESTORE` |

- **Normal checkout**: never invokes this RPC.
- **Operational use**: no operational procedure uses it in this iteration. A future tool must first confirm with Transaction Service that the attempt's transaction is not COMPLETED. If the attempt is pending, Transaction then settles it as REJECTED with reason `STOCK_RESTORED` (FR-030).
- **Verification**: direct tests against Inventory cover duplicates, crashes, retries, and ordering. These results are reported separately from end-to-end Saga results (SC-012).

## AnalyticsIngestService

### `DeliverScanEvents`

**Request validation**: these failures return `INVALID_ARGUMENT`.
- The batch has 1–1,000 events. A repeated identity inside one batch is not an error: it is counted as a duplicate during processing.
- `transaction_id` is 1–40 characters, `scan_sequence` is at least 1, `sku` is 1–20 characters, and `product_name` is 1–100 characters.

**Processing**: one Analytics local transaction (R11, R12), with a design budget (R22) of at most 5 statements and 1 commit per batch:
1. Lock `scan_sequence`.
2. Select which identities already exist.
3. Drop duplicates, including repeats inside the batch. Assign consecutive positions in batch order and store the new events with one multi-row INSERT.
4. Upsert `product_name` only for names not already in the in-memory map.
5. Advance `scan_sequence`, commit, and return `stored` and `duplicates`, which sum to the batch size.

Positions are assigned in this transaction. There is no separate materializer step. See [scan-event-delivery.md](scan-event-delivery.md#3-ingestion-with-position-assignment-analytics-service-r11).

**Failure**: a database failure returns `UNAVAILABLE` after rollback, and the relay retries. A commit whose outcome is unknown also returns `UNAVAILABLE`, and the retry is deduplicated. `OK` is sent only after the ingestion commit returns, so it means every event in the batch is durable in Analytics, with its position and deduplication key.

## Compatibility and contract tests (FR-013, FR-056)

- **Additive evolution**: changes inside `v1` are additive only, and retired fields are `reserved`. A breaking change requires a new package version, such as `v2`, served alongside `v1` until all clients move.
- **Contract tests** (`server/tests/rpc`):
  - every outcome and status above;
  - idempotent replay and payload mismatch;
  - concurrent duplicate `ApplyCheckout` calls;
  - deadline expiry with the server committing after the client gave up;
  - `RestoreCheckout` in every state;
  - duplicate and oversized ingestion batches.
- **Test setup**: the tests run against the real adapters and a disposable schema, through both the in-process transport and a real port.
- **Interoperability check**: a client stub and a server built in separate compilation steps from the same `.proto` interoperate.
