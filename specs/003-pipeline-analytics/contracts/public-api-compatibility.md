# External Contract: Public API Compatibility

`spec/self-checkout-openapi.yaml` remains the sole authoritative public contract and is not modified
by this feature. The load client is also unchanged.

## Popular-items endpoint

`GET /analytics/popular-items` retains the optional integer `limit` query parameter (default 10)
and returns HTTP 200 with exactly these response fields:

- `windowSize`, always 1,000
- `slideInterval`, always 500
- `windowStart` and `windowEnd`, describing the latest committed complete window
- `computedAt`, an ISO date-time from durable persistence
- `items`, each containing exactly `sku`, `name`, `scanCount`, and `rank`

The endpoint reads only `PopularWindowStore.readLatest`; it does not inspect a queue, ring buffer,
in-flight snapshot, health flag, or worker. Before the first committed result it preserves the
existing empty response. A non-integer or negative limit remains a 400 API error, an unsupported
method remains 405, and an unmatched path remains 404 according to existing handler behavior.

## Other compatibility requirements

- Transaction, catalog, inventory, and all remaining routes retain their request/response shapes,
  statuses, and behavior.
- A pipeline delay or failure cannot roll back an admitted basket scan, decrement stock early, or
  weaken atomic/idempotent completion.
- No public health route, log endpoint, queue-depth field, retry field, or pipeline state is added.
- `db/init.sql` and the canonical reset baseline remain unchanged.
- `load-client/` runs without source or argument changes for default and stress validation.
