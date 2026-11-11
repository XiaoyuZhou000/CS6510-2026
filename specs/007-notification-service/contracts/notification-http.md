# Contract: Notification Service HTTP Interface

These endpoints are additive (FR-032). They are served by Notification Service on port 8084 and reached through the gateway at `http://localhost:8080` via `location ^~ /notifications/`. None of the seven existing routes, and none of `spec/self-checkout-openapi.yaml`, changes. Bodies are JSON in UTF-8. Errors use the established `{"error": "<CODE>", "message": "<text>"}` shape, and no body names Kafka, a topic offset, or SQL.

`spec/notification-openapi.yaml` (version 1.0.0) is the versioned machine-readable form of this contract, written during implementation and checked by contract tests.

## Subscription endpoints (public)

### `POST /notifications/subscribers` — register

Request: `{"subscriberId": "sub-0001", "skus": ["SKU-000017", "SKU-001234"]}`

| Case | Status | Body |
|---|---|---|
| New subscriber, or a cancelled one registering again | 201 | Subscriber view |
| Active subscriber with the same SKU set (an idempotent repeat) | 200 | Subscriber view, with the version of the change that produced it |
| Active subscriber with a different SKU set | 409 `SUBSCRIBER_EXISTS` | — |
| Missing or invalid `subscriberId` | 400 `INVALID_SUBSCRIBER_ID` | — |
| Missing or empty `skus` | 400 `MISSING_SKUS` | — |
| An SKU not in the catalog | 400 `UNKNOWN_SKU` (message names it) | — |
| Malformed JSON | 400 `INVALID_REQUEST` | — |
| Catalog not yet loaded, database unavailable, or commit failed or unknown | 500 `INTERNAL_ERROR` | — (the change is not applied, or its outcome is unknown; repeating it is safe) |

**Subscriber view**: `{"subscriberId", "status": "ACTIVE"|"CANCELLED", "skus": [sorted], "channel": "<topic name>", "channelReady": bool, "subscriptionVersion": n}`

`subscriptionVersion` is the history version at which this state took effect (FR-007). A response with status 200 or 201 is the commit confirmation.

### `PATCH /notifications/subscribers/{subscriberId}` — add and remove SKUs

Request: `{"add": ["SKU-000500"], "remove": ["SKU-000017"]}`. At least one of the two lists must be non-empty, and an SKU may not appear in both.

| Case | Status |
|---|---|
| Effective change | 200, subscriber view at the new version |
| No-op (adding held SKUs, removing absent ones) | 200, current view, with no new version |
| Result would be empty | 409 `LAST_SKU` |
| Subscriber cancelled | 409 `SUBSCRIBER_CANCELLED` |
| Unknown subscriber | 404 `SUBSCRIBER_NOT_FOUND` |
| Invalid body or an unknown SKU | 400 `INVALID_REQUEST`, `UNKNOWN_SKU` |
| Failure | 500 `INTERNAL_ERROR` (not applied, or outcome unknown) |

### `DELETE /notifications/subscribers/{subscriberId}` — cancel

The response is 200 with the view (`status: "CANCELLED"`, `skus: []`), at a new version the first time and at the same version on a repeat. An unknown subscriber gives 404 `SUBSCRIBER_NOT_FOUND`. Notifications already selected for this subscriber are still delivered (rule 5).

### `GET /notifications/subscribers/{subscriberId}`

The response is 200 with the view, or 404 `SUBSCRIBER_NOT_FOUND`. It reads committed state and never waits.

**Validation precedence**: request shape (400), then subscriber existence (404), then state conflicts (409), then SKU validity (400 `UNKNOWN_SKU`).

## Operational endpoints (not part of the public contract)

These are reachable on 8084 and through the gateway, and are documented for operators and the harness (FR-030).

| Endpoint | Purpose |
|---|---|
| `GET /notifications/ops/status` | `{subscriptionVersion, indexVersion, activeSubscribers, catalogLoaded, salesSelected, notifications: {pending, delivered, parked}, failures: {charged, uncharged}, parkedEvents, alarms: [...], saleConsumerLag, lastConfirmedSendAt}` |
| `GET /notifications/ops/parked?subscriberId=&limit=` | Parked notifications with `notificationId`, `saleId`, `sku`, `subscriberId`, `chargedFailures`, `lastError`, and `parkedAt`, plus parked events |
| `POST /notifications/ops/parked/retry` | Body `{"subscriberId": optional}`. Moves matching parked rows back to `PENDING` with their identity kept; answers `{"retried": n}` |
| `POST /notifications/ops/faults` | Test-only, enabled when `NOTIFICATION_TEST_FAULTS=1` (otherwise 404). Body `{"faults": "<selfcheckout.faults value>"}` sets the property, and `{"faults": ""}` clears it (N13) |

## Gateway

`deploy/nginx.conf` adds:

```text
upstream notification_service { server 127.0.0.1:8084; }
location ^~ /notifications/ { proxy_pass http://notification_service; }
```

An unreachable Notification Service yields the gateway's existing 502 body for `/notifications/**` only. Every other location is byte-identical to feature 006.
