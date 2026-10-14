# Durable Scan and Analytics Checkpoint Contract

This contract records the selected commit-time position design. The feature specification also permits database-backed assignment after concurrent scan commits, provided the assigned order is deterministic, durable, recoverable, immutable, unique, and gapless for committed materialized events. In that design, consumers read only the committed contiguous materialized prefix and recovery includes committed scans still awaiting assignment. Neither design requires analytics order to equal physical database commit order.

## Successful scan commit

1. Transaction Service starts one MySQL transaction and locks the target `transaction` row by primary key. Missing/non-OPEN transactions and invalid SKUs return the existing errors, with no durable side effects.
2. It updates the one-unit basket line and durable running total, then locks the singleton `scan_sequence` row, allocates `last_position + 1`, inserts exactly one `accepted_scan(position, transaction_id, sku, unit_price, accepted_at)`, and updates the sequence row.
3. It commits before HTTP 200. SQL failure rolls back all changes. A failed or uncertain commit is never reported as success unless the code can prove the commit happened; a lost response after a successful commit follows the current ambiguous-retry semantics.

`accepted_scan.position` is the analytics order. All writers use the same lock order; the sequence lock is held through commit. Readers of the selected design can rely on positions 1..N being committed and contiguous. Neither an auto-increment key nor `accepted_at` is a consumer cursor. A transaction completion never adds a scan event.

## Ordered consumption and pipeline

The Analytics poller reads `WHERE position > :cursor ORDER BY position LIMIT :batch` on fresh committed reads. It sends immutable `(position, sku)` messages through a bounded FIFO ingress queue to the Window Filter. The Window Filter checks `position = lastApplied + 1`; it ignores an exact repeated position/payload already applied in the current process, rejects a conflicting duplicate, and pauses/fails visibly on a missing next position instead of advancing past it. It maintains the most recent 1,000 SKUs and emits an immutable snapshot only for absolute position ends 1,000 and every 500 thereafter. The separate Ranking Filter orders count descending then SKU ascending and emits up to ten immutable ranks. The separate Persistence Filter consumes those ranked windows in FIFO order and retries the oldest before later ones.

Database polling and queue backpressure affect Analytics only. Transaction Service does not enqueue into Analytics or wait for its availability. The database keeps all accepted scans, including those committed while Analytics is down.

## Atomic publication

For a candidate window ending at `E`, Persistence opens one MySQL transaction and locks `analytics_checkpoint`. It requires `E=1000` when checkpoint is zero, otherwise `E=checkpoint+500`; it requires start `E-999`. It inserts one `popular_window` header and all ranked `popular_item` rows, advances checkpoint to `E`, and commits. A unique constraint on `popular_window.window_end` prevents duplicate boundaries. If a retry finds `E` already committed, it verifies identical bounds and ranks and treats it as already done; a difference is an integrity failure. No newer boundary is published before its predecessor.

The public endpoint selects the latest committed window by `window_end`, with its ranks. Before the first 1,000 scans it returns the existing empty shape. While a new window is incomplete, queued, retrying, or uncommitted, it returns the previous committed boundary. The existing `limit` validation and at-most-ten behavior remain.

## Crash table

| Crash point | Durable state | Recovery action |
|---|---|---|
| Before scan commit | No line/total/event/sequence change | Caller gets no success; retry is a new attempt. |
| After scan commit, before response | Line, total, event, and position exist | Lookup reflects the scan; a retry means another unit if transaction is still OPEN. |
| Before or during event consumption | Last window checkpoint unchanged | Replay accepted scans from the checkpoint and resume. |
| After partial in-memory window processing | Checkpoint unchanged | Discard in-memory state and replay the uncheckpointed tail. |
| Before window transaction commit | No header, ranks, or checkpoint change | Recompute and retry the same boundary. |
| After window commit, before worker records success | Header, ranks, and checkpoint all present | Bootstrap from new checkpoint; duplicate delivery verifies equivalence. |
| During database outage | No uncommitted write becomes public | Retry from committed state when MySQL returns. |

At startup, checkpoint `W=0` means replay from position 1. For `W>0`, prewarm the ring with `W-999..W` without emitting, then process `W+1` onward. The in-memory poll cursor is not a durable checkpoint. Every complete eligible boundary eventually reaches the checkpoint once Analytics and MySQL are available.
