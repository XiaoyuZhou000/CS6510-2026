# Data Model: Durable Checkout and Recoverable Popularity

All tables live in the single `cs6510_selfcheckout` MySQL database. Existing catalog, inventory, transaction, line, and popularity tables remain; `db/init.sql` is the schema/reset source. The model below uses the SQL Materializer redesign. Physical database commit order is not a required analytics order.

## SQL Materializer schema update (2026-10-13)

[sql-materializer-design.md](sql-materializer-design.md) defines the ordering and recovery rules. `accepted_scan.event_id` is now an AUTO_INCREMENT primary key used only as unique identity. `position` is nullable and unique. Transaction Service inserts a NULL-position event with the basket update and does not write `scan_sequence`. Analytics Materializer assigns positions in batches, updating `scan_sequence.last_position` in the same commit. Pending rows are discovered through `position IS NULL`, so a late lower-ID commit cannot be missed. Analytics reads only the committed positioned prefix.

## Catalog item and inventory

| Entity | Fields and keys | Rules |
|---|---|---|
| `catalog_item` | `sku` PK, `name`, `price` DECIMAL(10,2) | 2,000 unique seeded SKUs; name and price stable for one run. Transaction reads identity/price; Inventory owns catalog responses; Analytics reads display names. |
| `inventory` | `sku` PK/FK to catalog, `stock_quantity`, `low_stock_threshold` | `stock_quantity >= 0`; stock changes only during committed completion. Inventory reads current committed quantities. |

## Transaction and accepted scan

| Entity | Fields and keys | Rules |
|---|---|---|
| `transaction` | `transaction_id` PK, `station_id`, `status`, `total_amount`, `started_at`, `completed_at` | Create as `OPEN` and commit before 201. `total_amount` reflects committed OPEN lines, then remains the receipt total. `started_at` comes from MySQL. `COMPLETED` requires nonempty lines and all stock deductions in one commit. Existing `CANCELLED` enum value remains for compatibility; no cancellation operation is added. |
| `transaction_line` | `(transaction_id, sku)` PK, `quantity`, `unit_price` | Upsert on every successful one-unit scan; quantity positive; price captured from catalog. Rows exist while OPEN and remain after completion. Sum of line quantities and `quantity × unit_price` equals transaction item count and total. These same rows form the receipt and stock decrement quantities. |
| `scan_sequence` | singleton row `sequence_key=1` PK, `last_position` BIGINT | Initialized to zero. Only Analytics Materializer locks and updates it, atomically with a bounded position-assignment batch. Scan acceptance never locks it. |
| `accepted_scan` | `event_id` BIGINT AUTO_INCREMENT PK, nullable UNIQUE `position`, `transaction_id` FK, `sku` FK, `unit_price`, `accepted_at` | One raw event per accepted Scan. Event ID is identity only and may have gaps or commit out of order. Position is assigned once by Materializer. Pending and position indexes support discovery and ordered polling. |

**Scan state transition:** Under the transaction-row lock, a valid request updates one line and total, inserts one unpositioned raw event, and commits. Rejection commits none. Repeated HTTP Scan means another unit; replay of an assigned position means the same event.

**Completion state transition:** Lock the transaction row, then use its committed lines to conditionally decrement inventory rows in sorted SKU order. Commit the status change and all decrements together. If any stock check fails, rollback all changes and leave the transaction `OPEN`. Repeated completion of `COMPLETED` returns `TRANSACTION_NOT_OPEN` without stock changes. A racing scan and completion serialize on the same transaction lock, so a scan either commits before completion and is included, or sees non-OPEN state and is rejected.

## Popularity and recovery

| Entity | Fields and keys | Rules |
|---|---|---|
| `popular_window` | `window_id` PK, `window_start`, `window_end` UNIQUE, `computed_at` | Complete intervals have exactly 1,000 positions. Ends are 1,000, 1,500, 2,000, ... and start is `end-999`. Header and ranks are committed atomically with checkpoint advancement. `window_id` is a storage key, never the ordering rule. |
| `popular_item` | `(window_id, rank_pos)` PK, `sku` FK, `scan_count` | Zero to ten rows per window; ranks contiguous from 1, ordered count descending and SKU ascending on ties. Each count is from accepted scans in that window, regardless of completion. |
| `analytics_checkpoint` | singleton `checkpoint_key=1` PK, `last_window_end` BIGINT | Initialized to zero; references the latest completely committed boundary. Persisting a window inserts header/ranks and advances this value in one InnoDB transaction. It may move only from 0→1,000 or `W→W+500`. No per-event durable cursor advances independently of this checkpoint. |

**Recovery state transition:** Materializer rediscovers all committed NULL-position events. Analytics starts from window checkpoint `W`, prewarms `W-999..W` when nonzero, and processes `W+1` onward. The poll cursor is disposable. A crash before batch or window commit rolls it back; a crash after commit finds durable progress and does not duplicate results.

## Database access by service

| Service | Reads | Writes |
|---|---|---|
| Transaction | `transaction`, `transaction_line`, `catalog_item`, `inventory` for completion | `transaction`, `transaction_line`, `accepted_scan`, `inventory` |
| Inventory | `catalog_item`, `inventory` | None in required HTTP workflows |
| Analytics | `accepted_scan`, `scan_sequence`, `catalog_item`, `analytics_checkpoint`, `popular_window`, `popular_item` | `accepted_scan.position`, `scan_sequence`, `popular_window`, `popular_item`, `analytics_checkpoint` |

The sequence and checkpoint singleton rows are separate in this design. Scan acceptance never waits for Analytics' queue, Materializer, or result persistence. A committed Scan may await position assignment; recovery finds it through the NULL-position predicate without interpreting temporary absence as a permanent gap.
