# Durable Scan and Analytics Checkpoint Contract

This contract uses the SQL Materializer design in [sql-materializer-design.md](../sql-materializer-design.md).

## Successful Scan commit

Transaction Service locks the checkout row, validates OPEN status and SKU, updates one basket unit and running total, and inserts exactly one `accepted_scan` row with a unique immutable `event_id` and NULL `position`. These changes commit together before HTTP 200. Rejection or SQL failure leaves no line, total, or event change. The Scan does not lock `scan_sequence`; completion serializes with it only on the checkout row. Event IDs are identities and are never commit-order positions or consumer cursors.

## Position assignment

Analytics Materializer locks `scan_sequence`, selects at most 256 committed pending rows using `WHERE position IS NULL ORDER BY event_id ... FOR UPDATE SKIP LOCKED` under READ COMMITTED isolation, updates their positions consecutively, advances `last_position`, and commits once. Batches are serialized by the checkpoint row. A lower event ID that commits after a higher one remains eligible because discovery never uses an event-ID high-water mark. The deterministic order is event ID among committed pending rows visible to each batch. A unique nullable position constraint, event primary key, and one Materializer-owned update path preserve distinct identities and immutable assigned positions.

A crash before commit rolls back the whole batch; a crash after commit leaves every position and the checkpoint durable. Retrying or running two Materializers only considers still-NULL events. An auto-increment ID gap after rollback has no effect on gapless analytics positions. Scan acceptance remains available if Analytics is stopped.

## Ordered consumption and publication

Analytics polls committed `position > cursor ORDER BY position LIMIT batch` rows and requires adjacency. A completed Materializer batch is visible atomically, so the assigned prefix is gapless. The bounded FIFO Window, Ranking, and Persistence stages preserve 1,000-scan windows at ends 1,000, 1,500, 2,000, and so on, top ten by count descending and SKU ascending, and immutable messages.

For each complete window, Persistence locks `analytics_checkpoint`, inserts its header and ranks, advances the window checkpoint, and commits once. A duplicate verifies existing bounds and ranks; a conflict fails visibly. Only committed complete windows are public. At startup, checkpoint `W=0` replays from position 1; for `W>0`, the ring prewarms from `W-999..W` and consumes `W+1` onward. Materializer independently resumes from NULL positions. The in-memory poll cursor is disposable.

| Crash point | Durable state and recovery |
|---|---|
| Before Scan commit | No basket change or raw event; no success acknowledgement. |
| After Scan commit, before response | Basket and raw event persist; a client retry is another physical-unit Scan. |
| Before Materializer commit | Assignment and checkpoint roll back; pending rows are found again. |
| After Materializer commit | Complete positioned batch and checkpoint persist; retry ignores assigned rows. |
| During event processing | Replay from committed window checkpoint, including overlap. |
| Before window commit | No partial window is visible; recompute it. |
| After window commit | Header, ranks, and checkpoint persist; retry verifies equivalence. |
