# Data Model: Durable Checkout and Recoverable Popularity

All tables live in the single `cs6510_selfcheckout` MySQL database. Existing catalog, inventory, transaction, line, and popularity tables remain; `db/init.sql` is the schema/reset source. Names below describe the selected commit-time position design and its constraints. The feature requirement also permits a database-backed materializer to assign immutable, unique, gapless analytics positions after concurrent scan commits; physical database commit order is not a required analytics order.

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
| `scan_sequence` | singleton row `sequence_key=1` PK, `last_position` BIGINT | Initialized to zero. The row is locked and incremented within the same transaction that commits one accepted scan. In this design, positions are positive, contiguous, and commit-visible in order; rollback restores the previous value. |
| `accepted_scan` | `position` BIGINT PK, `transaction_id` FK, `sku` FK, `unit_price`, `accepted_at` | Exactly one immutable event per successful scan, including scans on transactions that never complete. Position is the global order, never `AUTO_INCREMENT`; indexes on `transaction_id` and primary-key position support reconciliation and ordered polling. Its insert and associated line/total update commit or roll back together. Rows are retained for recovery and audit during this iteration. |

**Scan state transition:** Under a lock on the transaction row, `OPEN + valid SKU + one request` updates one line and total, reserves the next sequence position, inserts one event, and commits; the transaction stays `OPEN`. Unknown, invalid, or non-OPEN requests commit no line, total, sequence, or event change. A repeated valid HTTP request is another scan. An internal replay of one `position` is the same event.

**Completion state transition:** Lock the transaction row, then use its committed lines to conditionally decrement inventory rows in sorted SKU order. Commit the status change and all decrements together. If any stock check fails, rollback all changes and leave the transaction `OPEN`. Repeated completion of `COMPLETED` returns `TRANSACTION_NOT_OPEN` without stock changes. A racing scan and completion serialize on the same transaction lock, so a scan either commits before completion and is included, or sees non-OPEN state and is rejected.

## Popularity and recovery

| Entity | Fields and keys | Rules |
|---|---|---|
| `popular_window` | `window_id` PK, `window_start`, `window_end` UNIQUE, `computed_at` | Complete intervals have exactly 1,000 positions. Ends are 1,000, 1,500, 2,000, ... and start is `end-999`. Header and ranks are committed atomically with checkpoint advancement. `window_id` is a storage key, never the ordering rule. |
| `popular_item` | `(window_id, rank_pos)` PK, `sku` FK, `scan_count` | Zero to ten rows per window; ranks contiguous from 1, ordered count descending and SKU ascending on ties. Each count is from accepted scans in that window, regardless of completion. |
| `analytics_checkpoint` | singleton `checkpoint_key=1` PK, `last_window_end` BIGINT | Initialized to zero; references the latest completely committed boundary. Persisting a window inserts header/ranks and advances this value in one InnoDB transaction. It may move only from 0→1,000 or `W→W+500`. No per-event durable cursor advances independently of this checkpoint. |

**Recovery state transition:** At startup, read checkpoint `W`; if nonzero, load accepted positions `W-999..W` into the ring without publishing. Consume `W+1` onward in strict position order. The in-memory cursor and partial next window are disposable. A crash before window commit leaves `W` unchanged, so the candidate is recomputed; a crash after commit finds the new `W` and resumes from it. A duplicate persistence attempt must match the already committed bounds and ranks or fail visibly. The latest popular-items query reads only committed windows and may temporarily show the prior complete boundary while Analytics catches up.

## Database access by service

| Service | Reads | Writes |
|---|---|---|
| Transaction | `transaction`, `transaction_line`, `catalog_item`, `inventory` for completion | `transaction`, `transaction_line`, `scan_sequence`, `accepted_scan`, `inventory` |
| Inventory | `catalog_item`, `inventory` | None in required HTTP workflows |
| Analytics | `accepted_scan`, `catalog_item`, `analytics_checkpoint`, `popular_window`, `popular_item` | `popular_window`, `popular_item`, `analytics_checkpoint` |

The sequence and checkpoint singleton rows are separate in this design. Scan acceptance never waits for Analytics' queue or result persistence; Analytics failure cannot roll back a committed scan. With a materializer design, a committed scan may await position assignment, and recovery must find it without interpreting temporary absence as a permanent gap.
