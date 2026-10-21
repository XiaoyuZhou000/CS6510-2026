# Public HTTP and Routing Contract

**Requirements**: FR-003, FR-014–FR-017, FR-020, FR-025, FR-027, FR-034–FR-036, FR-053 | **Decision**: R14

`spec/self-checkout-openapi.yaml` stays authoritative and byte-identical. The unchanged Load Client and the existing contract tests are the oracle. This document records only what the microservice deployment must keep, and how internal conditions surface.

## Base URL and routing

The gateway keeps `deploy/nginx.conf` as it is today: listen on 8080, with exact routes and upstream connection reuse.

| Public route | Owner | Upstream (HTTP) |
|---|---|---|
| `GET /items` | Inventory | `127.0.0.1:8082` |
| `GET /inventory/low-stock` | Inventory | `127.0.0.1:8082` |
| `POST /transactions` | Transaction | `127.0.0.1:8081` |
| `POST /transactions/{id}/items` | Transaction | `127.0.0.1:8081` |
| `POST /transactions/{id}/complete` | Transaction | `127.0.0.1:8081` |
| `GET /transactions/{id}` | Transaction | `127.0.0.1:8081` |
| `GET /analytics/popular-items` | Analytics | `127.0.0.1:8083` |

- **Timeouts**: connect 1 s, read 30 s. The read timeout exceeds the 8 s checkout budget.
- **Unreachable upstream**: an upstream connection failure, or a gateway timeout (`error_page 504 =502`), yields the gateway's existing 502.
- **Internal ports**: gRPC ports 9092 and 9093 are never routed.
- **Port changes**: changing a private HTTP port requires the matching edit to `deploy/nginx.conf`, as today.

## Preserved responses

Success and error responses are exactly as summarized in the spec's *Established External Behavior* table, with the same validation precedence:
- **Scan**: request validation (400), then `TRANSACTION_NOT_FOUND`, then `TRANSACTION_NOT_OPEN`, then `SKU_NOT_FOUND`.
- **Complete**: `TRANSACTION_NOT_FOUND`, then `TRANSACTION_NOT_OPEN`, then `EMPTY_BASKET`, then `INSUFFICIENT_STOCK`.

The existing handlers (`TransactionHttpHandler`, `CatalogHttpHandler`, `InventoryHttpHandler`, `AnalyticsHttpHandler`) and their serialization are reused unchanged.

## Internal conditions and their public responses (FR-017)

| Internal condition | Public response |
|---|---|
| Checkout attempt still pending when the 8 s budget ends | 500 `INTERNAL_ERROR` ("Internal server error"). Not a cancellation; lookup shows the settled state later. |
| Scan or Complete waited the full bound on a pending attempt | 500 `INTERNAL_ERROR`; nothing changed |
| Inventory definitively rejected an attempt as INVALID_REQUEST or STOCK_RESTORED | 500 `INTERNAL_ERROR`; transaction OPEN with its basket |
| Transaction has never obtained product information | Scan: 500 `INTERNAL_ERROR`, never 404 `SKU_NOT_FOUND` |
| Own database failure in a reachable service | 500 `INTERNAL_ERROR`, as today; uncommitted writes are never acknowledged |
| Owning service process unreachable | Gateway 502 |
| Inventory definitively reports insufficient stock within the budget | 409 `INSUFFICIENT_STOCK`, as today |

No condition produces a success or a 4xx business error, and no response contains gRPC or SQL detail. A mapping test enumerates every internal failure type and asserts that it maps to 500.

## Lookup semantics

- `GET /transactions/{id}` never waits and never calls another service. It reports `OPEN` while a checkout attempt is pending, even if Inventory has already applied the stock effect, and `COMPLETED` once completion is durable.
- Receipt lines and `completedAt` are returned only by the Complete call that completes the transaction. A repeated Complete returns 409 `TRANSACTION_NOT_OPEN` (FR-036).

## Availability by route

See the spec's *Availability and Failure Behavior* table. Summary:

| Stopped component | Routes affected | Routes unaffected |
|---|---|---|
| Inventory | `/items` and `/inventory/low-stock` (502). Complete returns 500 after the budget, and the attempt stays pending. | Start, Scan, and Lookup for transactions without a pending checkout; popular items |
| Analytics | `/analytics/popular-items` (502) | All others; events wait in the outbox |
| Transaction | `/transactions/**` (502) | `/items`, `/inventory/low-stock`, `/analytics/popular-items` |
| Gateway | All public routes | None; internal state is unaffected |
