# Internal Contract: Pipeline Lifecycle and Observability

## Lifecycle

The facade starts three named workers (`analytics-window`, `analytics-ranking`, and
`analytics-persistence`) and enters `RUNNING` only when they are available. Shutdown atomically
rejects further ingestion, changes to `DRAINING`, and inserts the ingress end marker.

All workers share one absolute shutdown deadline configured by `ANALYTICS_SHUTDOWN_TIMEOUT_MS`.
The default is 5,000 ms. A clean drain ends in `TERMINATED`; deadline expiry or interruption forces
worker interruption and ends in `FORCED` or preserves an existing `FAILED` state. A forced outcome
must never be logged as clean. Repeated shutdown calls are safe and do not insert another marker.

## JSON-lines record shape

Each operational event is one JSON object on one stderr line. Values containing user or exception
text are JSON-escaped; logs omit database credentials, SQL text, stack traces, and basket data.

Required common fields:

| Field | Type | Meaning |
|---|---|---|
| `timestamp` | string | UTC ISO-8601 instant |
| `eventType` | string | Stable uppercase event identifier |
| `stage` | string | `INGRESS`, `WINDOW`, `RANKING`, `PERSISTENCE`, or `PIPELINE` |

Event requirements:

| Event | Additional required fields |
|---|---|
| `BACKLOG` | `queue`, `queueDepth` |
| `PERSISTENCE_RETRY` | `retryCount`, `windowStart`, `windowEnd`, `errorType` |
| `INGESTION_REJECTED` | `state`, `reason` |
| `DRAIN_STARTED` | `state`, queue depths for the start of drain |
| `SHUTDOWN_COMPLETED` | `shutdownOutcome` = `DRAINED` |
| `SHUTDOWN_FORCED` | `shutdownOutcome` = `TIMEOUT` or `INTERRUPTED` |
| `WORKER_FAILED` | `errorType`, sanitized `reason` |

Backlog records are emitted at increasing high-water thresholds rather than once per event. The
threshold is injectable for deterministic tests; production starts at a documented value and
doubles after each record. Queue depth is diagnostic and not used as an accounting invariant.

Example shape (illustrative values only):

```json
{"timestamp":"2026-09-30T20:00:00Z","eventType":"PERSISTENCE_RETRY","stage":"PERSISTENCE","retryCount":3,"windowStart":501,"windowEnd":1500,"errorType":"StoreFailure"}
```

## Observable guarantees

- Every injected persistence failure produces a retry record with the affected window and attempt.
- Every post-close or unhealthy ingestion attempt produces an ingestion-rejected record before the
  facade throws.
- The first unexpected filter failure produces one terminal worker-failed record naming its stage;
  simultaneous follow-on failures do not overwrite the first cause.
- Every shutdown produces a drain-started record and exactly one terminal shutdown outcome.
- No operational state is exposed through HTTP or persisted in MySQL.
