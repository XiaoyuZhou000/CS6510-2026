# Error Codes (`ApiError.error` values)

The OpenAPI contract's `ApiError` schema types `error` as a plain string (illustrative example:
`"TRANSACTION_NOT_OPEN"`), not an enum, so implementations are free to choose specific values as
long as the HTTP status code and response shape match the spec. This table fixes the values this
implementation uses, so tests and any future debugging have a stable reference.

| HTTP status | `error` value | Endpoint(s) | Meaning |
|---|---|---|---|
| 400 | `MISSING_STATION_ID` | `POST /transactions` | `stationId` absent or blank in the request body |
| 404 | `TRANSACTION_NOT_FOUND` | `.../items`, `.../complete`, `GET /transactions/{id}` | no transaction exists for the given ID |
| 404 | `SKU_NOT_FOUND` | `POST /transactions/{id}/items` | scanned SKU is not in the catalog |
| 409 | `TRANSACTION_NOT_OPEN` | `.../items`, `.../complete` | transaction exists but is COMPLETED or CANCELLED |
| 409 | `EMPTY_BASKET` | `POST /transactions/{id}/complete` | transaction is OPEN but has zero scanned units |
| 409 | `INSUFFICIENT_STOCK` | `POST /transactions/{id}/complete` | discovered during the completion transaction (research.md §2); a resolved ambiguity — see that section for rationale |

All values are returned exactly as written above (uppercase snake case), matching the style of the
OpenAPI spec's own example (`TRANSACTION_NOT_OPEN`).
