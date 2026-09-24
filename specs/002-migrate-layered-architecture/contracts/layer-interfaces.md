# Layer Interface Contracts

These are behavioral contracts for implementation; signatures are illustrative Java shapes, not
complete implementation code. Immutable records live beside the interface that exposes them.

## Implemented collaborator map

| Caller | Interface | Implementation |
|---|---|---|
| `TransactionHttpHandler` | `TransactionOperations` | `TransactionService` |
| `CatalogHttpHandler` | `CatalogOperations` | `StoreQueryService` |
| `InventoryHttpHandler` | `InventoryOperations` | `StoreQueryService` |
| `AnalyticsHttpHandler` | `AnalyticsOperations` | `AnalyticsService` |
| `TransactionService` | `TransactionStore` | `JdbcTransactionStore` |
| `TransactionService` | `CheckoutCompletionStore` | `JdbcCheckoutCompletionStore` |
| `CatalogCache` | `CatalogStore` | `JdbcCatalogStore` |
| `StoreQueryService` | `InventoryStore` | `JdbcInventoryStore` |
| `AnalyticsService` | `PopularWindowStore` | `JdbcPopularWindowStore` |

`api.Main` creates these collaborators and adapts `AnalyticsOperations.recordAcceptedScan` to the
transaction layer's `AcceptedScanSink`. No request handler retains a store or JDBC adapter.

## API → transaction

### `TransactionOperations`

| Operation | Input | Success | Domain failures |
|---|---|---|---|
| `start` | non-blank station ID | new `OPEN` transaction view | invalid station ID |
| `scan` | transaction ID, SKU | one-unit scan view; accepted-scan sink called once | transaction missing/not open; SKU missing |
| `complete` | transaction ID | committed receipt | missing/not open; empty basket; insufficient stock |
| `get` | transaction ID | current live or durable transaction view | transaction missing |

The implementation owns basket lifecycle, price capture, totals, completion decisions, and the
accepted-scan callback. It exposes no HTTP status, JSON, JDBC, or DAO type.

### `CatalogOperations`

`listItems()` returns the complete immutable catalog in stable seed/load order. The API cannot see
the cache or store used to obtain it.

### `InventoryOperations`

`listLowStock(optionalThreshold)` resolves the threshold policy and returns current alerts. It
never exposes database rows. Negative/invalid public query syntax is rejected by API validation.

## API → analytics

### `AnalyticsOperations`

| Operation | Contract |
|---|---|
| `recordAcceptedScan(sku)` | Records one accepted physical-unit scan; never decrements stock; does not wait for checkpoint persistence |
| `latestPopularItems(limit)` | Returns the latest successfully persisted computed window, with zero/epoch values before the first checkpoint |
| `shutdown()` | Stops new checkpoint work and performs the implementation's orderly worker shutdown |

The API uses only `latestPopularItems`. `api.Main` wires `recordAcceptedScan` to the transaction
layer's `AcceptedScanSink` during construction.

## Transaction/analytics → database access

Database contracts convert infrastructure failures to a single transport-neutral store failure;
SQL exceptions and connections never cross the layer.

### `CatalogStore`

- `loadAll()` returns all catalog records in stable order.

### `TransactionStore`

- `insertOpen(transactionId, stationId)` durably creates an `OPEN` row.
- `findById(transactionId)` returns durable transaction metadata and completed aggregates or
  absence.

### `InventoryStore`

- `findLowStock(optionalThreshold)` returns current committed inventory/catalog projections.
- It provides no public decrement method; decrements are reachable only through atomic completion.

### `CheckoutCompletionStore`

`completeAtomically(command)` accepts transaction ID, total, and immutable lines sorted by SKU and
returns exactly one typed result:

- `COMPLETED(completedAt)`: status, lines, and all inventory decrements committed.
- `NOT_OPEN`: no write committed because the durable transaction was absent or not `OPEN`.
- `INSUFFICIENT_STOCK(sku)`: no write committed because at least one conditional decrement failed.
- Store failure: no success is reported; the adapter rolls back before propagating failure.

The JDBC adapter must use one connection/transaction and may not return `COMPLETED` before commit.

### `PopularWindowStore`

- `readMaxWindowEnd()` seeds restart recovery.
- `writeWindow(snapshot)` atomically writes one header and up to ten ranks.
- `readLatest(limit)` returns the latest committed header and ordered ranks, or absence.

The analytics layer, not the store, decides snapshot boundaries, counts, ranking, and retry order.

## Failure translation

Transaction and analytics contracts expose stable domain/store failures without status numbers.
`ApiErrorMapper` is the only component that maps them to HTTP status and `{error,message}`. Unknown
infrastructure failures map to `500 INTERNAL_ERROR` without leaking SQL or credentials.
