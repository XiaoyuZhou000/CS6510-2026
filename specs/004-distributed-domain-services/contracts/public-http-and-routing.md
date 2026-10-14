# Public HTTP and Routing Contract

`spec/self-checkout-openapi.yaml` and existing handler/contract tests define request bodies, success/error bodies, HTTP statuses, validation, and synchronous behavior. This document assigns ownership without changing that contract. The one public base URL remains `http://localhost:8080` by default.

| Method and path | Owner | Default private target |
|---|---|---|
| `GET /items` | Inventory | `127.0.0.1:8082` |
| `POST /transactions` | Transaction | `127.0.0.1:8081` |
| `POST /transactions/{transactionId}/items` | Transaction | `127.0.0.1:8081` |
| `POST /transactions/{transactionId}/complete` | Transaction | `127.0.0.1:8081` |
| `GET /transactions/{transactionId}` | Transaction | `127.0.0.1:8081` |
| `GET /inventory/low-stock` | Inventory | `127.0.0.1:8082` |
| `GET /analytics/popular-items` | Analytics | `127.0.0.1:8083` |

The static reverse proxy uses exact matches for `/items`, `/inventory/low-stock`, and `/analytics/popular-items`; `/transactions` and its descendants route to Transaction. It forwards the method, raw path/query, body, and relevant headers without changing successful or application-error responses. Existing handlers remain responsible for validation and domain error codes. Unexpected database failures in a reachable service retain HTTP 500 `INTERNAL_ERROR` JSON. A stopped owner produces a proxy 502/503; the proxy does not invent a 200, 400, or business error. Other owners' routes remain reachable while one service restarts, provided the proxy and database are available.

`POST /transactions/{id}/items` remains non-idempotent at the HTTP level: each valid call represents one physical unit. A lost response after a committed scan can be ambiguous to the caller; aggregate lookup shows persisted state, and the API gains no request-id header. Repeated completion still yields HTTP 409 `TRANSACTION_NOT_OPEN` without a second deduction.

The default load client uses only the single public base URL. No domain service calls another over HTTP or gRPC.
