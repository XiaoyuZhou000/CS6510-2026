# Public API Compatibility Contract

`spec/self-checkout-openapi.yaml` remains authoritative and is not modified by this migration.
The unchanged load client requires exact success statuses, synchronous completion, JSON object
responses, and tolerance of `{}` as the completion request body.

| Operation | Exact success | Required compatibility |
|---|---:|---|
| `GET /items` | 200 | `{items:[{sku,name,price}]}`; non-empty baseline catalog |
| `POST /transactions` | 201 | body `{stationId}`; returns transaction ID, station, `OPEN`, zero count/total, start time |
| `POST /transactions/{id}/items` | 200 | body `{sku}`; accepts exactly one unit; returns item and updated basket totals |
| `POST /transactions/{id}/complete` | 200 | tolerates empty object body; returns committed receipt and lines |
| `GET /transactions/{id}` | 200 | returns live or durable transaction view |
| `GET /inventory/low-stock` | 200 | optional integer `threshold`; returns generated time and alerts |
| `GET /analytics/popular-items` | 200 | optional non-negative integer `limit`; returns 1000/500 settings, bounds, time, ranks |

All contract errors retain `{error:string,message:string}`. Existing endpoint-specific 400, 404,
and 409 outcomes remain; unexpected infrastructure failures return 500 without internal details.

Compatibility tests must cover every route, method mismatch, required input, response field and
type, configured/default query behavior, missing resource, duplicate completion, and empty basket.
The load client is an additional end-to-end validator, not a replacement for those checks.
