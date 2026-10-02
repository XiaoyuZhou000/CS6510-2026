# Internal Contract: Analytics Pipeline Messages

This is an internal analytics-layer contract. It does not replace or extend the public OpenAPI.

## Topology

```text
AcceptedScanSink
      |
      v
AnalyticsService facade
      |
      v  unbounded FIFO BlockingQueue
Window Filter (one worker)
      |
      v  unbounded FIFO BlockingQueue
Ranking Filter (one worker)
      |
      v  unbounded FIFO BlockingQueue
Persistence Filter (one worker) -> PopularWindowStore
```

No processing filter invokes a downstream filter. Only immutable data or end-of-stream messages
cross a pipe. The facade owns queues, workers, lifecycle state, and failure coordination.

## Ingress message

`AcceptedScan(sku)` contains one required non-blank SKU of at most 20 characters. It does not carry
a producer-assigned position. A successful facade call means the message was placed before any
shutdown marker while the pipeline was healthy; a rejected call emits a structured record and
throws. The transaction layer may isolate that failure, but analytics must not report acceptance.

## Window-to-Ranking message

`WindowSnapshot(windowStart, windowEnd, skus)` contains inclusive coherent bounds and exactly
1,000 SKUs in analytics order. The list is defensively copied. The first fresh snapshot is
`[1,1000]`; later uninterrupted ends advance by 500. A restarted pipeline without ring recovery
collects 1,000 new events before its first new snapshot.

## Ranking-to-Persistence message

`RankedWindow(windowStart, windowEnd, ranks)` preserves snapshot bounds and contains at most ten
`RankedItem(rank, sku, scanCount)` values. Ranks are contiguous from one, SKU values are unique,
counts are positive, and order is count descending then SKU ascending.

## End-of-stream messages

Each typed pipe has an immutable end marker distinct from data. The facade places only the ingress
marker, under the same lock used for admission. Window forwards completion only after consuming all
earlier scans and publishing all complete snapshots; Ranking forwards only after all earlier
snapshots; Persistence terminates only after all earlier ranked windows commit. No partial window
is emitted at drain.

## Failure and retry

- A filter may communicate data only by its output queue; a terminal-failure callback to the facade
  is control-plane coordination, not data processing.
- `StoreFailure` is retryable. Persistence retains the current oldest message and never dequeues a
  newer one until success.
- Unexpected unchecked failures mark the pipeline failed, close ingestion, identify the stage in a
  structured record, and terminate or interrupt peers.
- `latestPopularItems` continues to read only the latest committed `PopularWindowStore` result and
  remains usable after a worker failure.

## Package boundaries

- All filters, pipe messages, and operational logging belong to `analytics`.
- Ranking has no database collaborator.
- Persistence alone writes through `database.PopularWindowStore`.
- No pipeline type depends on HTTP, `api`, JDBC, SQL, transaction implementation types, or a fifth
  shared/domain package.
